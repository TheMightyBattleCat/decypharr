// The cold sweep repair path is the opt-in (config.Repair.Par2RepairOnSweep)
// counterpart to the warm one in par2_warm_sweep.go: when a sweep finds a
// file with dead segments and the cache-only warm pass could not fix it, try
// a normal PAR2 pass - fetching intact data from Usenet - before the
// re-grab. A pass that does not complete falls through to the re-grab in the
// same healBrokenEntry call, so an unrepairable file is still re-grabbed in
// this sweep rather than the next one.
package manager

import (
	"context"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// par2SweepStartGrace is how long runNowAndWait keeps polling after a pass
// stops showing as running or queued, for its terminal progress phase to
// land. RunNow marks the pass queued before returning, so this is not a
// start-up allowance.
const par2SweepStartGrace = 30 * time.Second

// coldSweepWanted reports whether the sweep may try a PAR2 pass on a file it
// finds with dead segments: the part of coldSweepEligible known before the
// file is probed. The probe uses it to decide whether counting every dead
// article is worth the time (see Repair.recordDeadSegments).
func coldSweepWanted(cfg config.RepairConfig) bool {
	return cfg.Par2RepairOnSweep && cfg.Par2RepairEnabled() && cfg.Par2RepairMode != config.Par2RepairModeManual
}

// beyondPar2Recovery reports whether damage of deadBytes is more than a
// release's PAR2 data could ever rebuild. Rebuilding a slice takes one
// recovery slice of the same size, so the recovery data needed is at least
// as large as the dead articles, and the recovery slices are held inside the
// PAR2 files: dead articles bigger than every retained PAR2 file put
// together cannot be repaired, whatever the slice size. Both sizes come from
// the NZB, so they carry the same encoding overhead. False when either is
// unknown (0).
func beyondPar2Recovery(deadBytes, par2Bytes int64) bool {
	return deadBytes > 0 && par2Bytes > 0 && deadBytes > par2Bytes
}

// retainedPar2Bytes is the total size of the PAR2 files kept for nzbID, or 0
// when it is not known: nothing retained (the files may still be found from
// the source NZB), or a file with no recorded size.
func (p *Par2Repair) retainedPar2Bytes(nzbID string) int64 {
	nzb, err := p.manager.usenet.GetNZB(nzbID)
	if err != nil || nzb == nil {
		return 0
	}
	var total int64
	for _, f := range nzb.Par2Files {
		if f.Size <= 0 {
			return 0
		}
		total += f.Size
	}
	return total
}

// coldSweepEligible is the pure gate for one broken file: the setting and
// PAR2 are on, the trigger mode is automatic, the probe found dead segments
// (not a decode or assembly failure - PAR2 cannot fix those, and a file with
// nothing pending could "complete" a pass that changed nothing), this file
// has damage pending in the overlay, and in threshold mode the release's
// pending dead segments reach the threshold.
func coldSweepEligible(cfg config.RepairConfig, reason string, filePending, releasePending int) bool {
	if !coldSweepWanted(cfg) {
		return false
	}
	if reason != "usenet_segment_missing" || filePending <= 0 {
		return false
	}
	if cfg.Par2RepairMode == config.Par2RepairModeAutoThreshold {
		return releasePending >= cfg.Par2RepairMinSegments
	}
	return true
}

// coldSweepRepair runs one PAR2 pass per release among broken whose files
// pass coldSweepEligible, returning the InfoHash values whose pass completed.
// Passes run one at a time across the whole sweep (coldPar2Slot): each reads
// a full release from Usenet, and the sweep's workers would otherwise start
// several at once. This runs inside a probe worker, so when several damaged
// releases qualify, workers queue for the slot and the sweep slows to one
// repair at a time. A release whose pass did not complete has its handler
// claim put back to handlerRegrab, so the re-grab that follows, and
// releaseRegrabClaims after it, see the claim routeAutoRepair made.
func (r *Repair) coldSweepRepair(ctx context.Context, broken []storage.BrokenFile) map[string]struct{} {
	fixed := make(map[string]struct{})
	p := r.manager.par2Repair
	if p == nil || r.manager.usenet == nil {
		return fixed
	}
	cfg := config.Get().Repair
	if !cfg.Par2RepairOnSweep {
		return fixed
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// What the probe confirmed missing, per release: a release whose dead
	// articles outweigh its PAR2 data is not worth a pass.
	confirmedDead := make(map[string]int64)
	for _, bf := range broken {
		confirmedDead[bf.InfoHash] += bf.ConfirmedDeadBytes
	}

	tried := make(map[string]struct{})
	for _, bf := range broken {
		if bf.InfoHash == "" {
			continue
		}
		if _, done := tried[bf.InfoHash]; done {
			continue
		}
		pending, err := r.manager.usenet.OverlayPendingRepair(bf.InfoHash)
		if err != nil {
			continue
		}
		releasePending := 0
		for _, segs := range pending {
			releasePending += len(segs)
		}
		if !coldSweepEligible(cfg, bf.Reason, len(pending[bf.FileName]), releasePending) {
			continue
		}
		tried[bf.InfoHash] = struct{}{}
		if dead, par2Bytes := confirmedDead[bf.InfoHash], p.retainedPar2Bytes(bf.InfoHash); beyondPar2Recovery(dead, par2Bytes) {
			r.logger.Info().Str("entry", bf.EntryName).Int64("dead_bytes", dead).Int64("par2_bytes", par2Bytes).
				Msg("Repair: sweep PAR2 skipped, the dead articles are larger than all the release's PAR2 data; re-grabbing")
			continue
		}
		if usable, why := p.par2Usable(bf.InfoHash); !usable {
			r.logger.Debug().Str("entry", bf.EntryName).Str("reason", why).
				Msg("Repair: sweep PAR2 skipped, not usable for this release")
			continue
		}
		if !r.acquireColdPar2Slot(ctx) {
			return fixed
		}
		r.logger.Info().Str("entry", bf.EntryName).Int("pending_segments", releasePending).
			Msg("Repair: trying PAR2 before re-grabbing (sweep repair enabled)")
		completed := p.runNowAndWait(ctx, bf.InfoHash)
		r.coldPar2Slot.Unlock()
		if completed {
			fixed[bf.InfoHash] = struct{}{}
			continue
		}
		if ctx.Err() != nil {
			return fixed
		}
		r.handlers.Set(bf.InfoHash, handlerRegrab)
		r.logger.Info().Str("entry", bf.EntryName).
			Msg("Repair: sweep PAR2 did not complete; re-grabbing in this sweep")
	}
	return fixed
}

// waitForInFlightPass waits until no pass for nzbID is running or queued,
// then reports whether it left the release repaired: nothing pending in the
// overlay. Judged by outcome rather than progress record, because a pass
// still queued when the wait began has no record yet and the latest one
// belongs to an older pass. False if ctx ends first.
func (p *Par2Repair) waitForInFlightPass(ctx context.Context, nzbID string) bool {
	for p.IsRunning(nzbID) || p.IsQueued(nzbID) {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(par2WarmSweepPollInterval):
		}
	}
	pending, err := p.manager.usenet.OverlayPendingRepair(nzbID)
	if err != nil {
		return false
	}
	for _, segs := range pending {
		if len(segs) > 0 {
			return false
		}
	}
	return true
}

// acquireColdPar2Slot takes coldPar2Slot, giving up if ctx ends first.
func (r *Repair) acquireColdPar2Slot(ctx context.Context) bool {
	for !r.coldPar2Slot.TryLock() {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(time.Second):
		}
	}
	return true
}

// runNowAndWait starts a pass for nzbID with RunNow and waits until that
// pass has ended, returning true only if it reached Par2PhaseCompleted. It
// never returns false while the pass is still running or queued - a caller
// that re-grabs on false would otherwise delete the entry under a pass still
// writing its patches - except when ctx ends, whose callers stop instead of
// re-grabbing. A pass that ends without a terminal progress phase (an early
// return) counts as not completed. There is no overall timeout: the pass
// ends itself at par2JobTimeout.
func (p *Par2Repair) runNowAndWait(ctx context.Context, nzbID string) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	prior, _ := p.Progress(nzbID)
	priorStart := prior.StartedAt
	if err := p.RunNow(nzbID); err != nil {
		if !p.IsRunning(nzbID) && !p.IsQueued(nzbID) {
			return false
		}
		// A pass is already running or queued. Returning false would let
		// the caller re-grab under it, so wait for it to end.
		return p.waitForInFlightPass(ctx, nzbID)
	}

	var idleSince time.Time
	for {
		if snap, ok := p.Progress(nzbID); ok && snap.StartedAt.After(priorStart) {
			switch snap.Phase {
			case Par2PhaseCompleted:
				return true
			case Par2PhaseFailed:
				return false
			}
		}
		if p.IsRunning(nzbID) || p.IsQueued(nzbID) {
			idleSince = time.Time{}
		} else if idleSince.IsZero() {
			idleSince = time.Now()
		} else if time.Since(idleSince) > par2SweepStartGrace {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(par2WarmSweepPollInterval):
		}
	}
}
