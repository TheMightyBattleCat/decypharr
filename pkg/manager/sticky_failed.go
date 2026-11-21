package manager

import (
	"context"
	"slices"
	"sync/atomic"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

type sampleBudgetKey struct{}

// stickyRegrabAutoRepairKey carries the probing run's auto-repair value (a
// sweep's autoRepair, a Recheck's fix) to regrabStickyFailed, which re-grabs
// from inside the probe and so must pass the same gate as the run's own heals.
// A context without it never re-grabs.
type stickyRegrabAutoRepairKey struct{}

func contextWithStickyRegrabAutoRepair(ctx context.Context, autoRepair bool) context.Context {
	return context.WithValue(ctx, stickyRegrabAutoRepairKey{}, autoRepair)
}

func contextWithSampleBudget(ctx context.Context, b *sweepSampleBudget) context.Context {
	return context.WithValue(ctx, sampleBudgetKey{}, b)
}

func sampleBudgetFromContext(ctx context.Context) *sweepSampleBudget {
	v, _ := ctx.Value(sampleBudgetKey{}).(*sweepSampleBudget)
	return v
}

// stickyFailedSampleBudget is the per-sweep byte budget for damage-sampling
// across all sticky-failed files. When exhausted, remaining files get
// INCONCLUSIVE. Hardcoded for now — config exposure is a later block.
const stickyFailedSampleBudget int64 = 2 << 30 // 2 GiB

// stickyFailedAction is reconcileStickyFailed's verdict on what to do about
// one sticky-Failed overlay record once the damage sampler has run.
type stickyFailedAction int

const (
	stickyFailedNone   stickyFailedAction = iota // PAR2 usable — leave to existing path
	stickyFailedClear                            // sampler says CLEAN — clear verdict
	stickyFailedRegrab                           // sampler says BROKEN — regrab
)

// sweepSampleBudget tracks the remaining byte budget across one sweep run.
// Shared by every reconcileStickyFailed call within a single executeSweep.
type sweepSampleBudget struct {
	remaining atomic.Int64
	exhausted atomic.Bool
}

func newSweepSampleBudget(bytes int64) *sweepSampleBudget {
	b := &sweepSampleBudget{}
	b.remaining.Store(bytes)
	return b
}

func (b *sweepSampleBudget) isExhausted() bool {
	if b == nil {
		return false
	}
	return b.exhausted.Load()
}

// decideStickyFailedAction is reconcileStickyFailed's pure policy core,
// factored out for table testing without a real usenet client or PAR2 worker.
// Only consulted when CheckFile reported healthy AND the overlay verdict is
// VerdictFailed.
//
// par2Usable=true always returns stickyFailedNone: the existing
// playback-triggered path handles this.
//
// When par2 is not usable, the sampler result maps directly:
//   - CLEAN (all dead recovered + spread clean) -> stickyFailedClear
//   - BROKEN -> stickyFailedRegrab
//   - INCONCLUSIVE -> stickyFailedNone (no action on absent evidence)
func decideStickyFailedAction(par2Usable bool, sampleResult usenet.SampleVerdict) stickyFailedAction {
	if par2Usable {
		return stickyFailedNone
	}
	switch sampleResult {
	case usenet.VerdictClean:
		return stickyFailedClear
	case usenet.VerdictBroken:
		return stickyFailedRegrab
	default:
		return stickyFailedNone
	}
}

// reconcileStickyFailed resolves the sticky-verdict deadlock: Store.Decide
// sets VerdictFailed when damage tips past the padding caps, and
// recomputeVerdictLocked explicitly refuses to un-fail it. Meanwhile
// probeNZBFile's CheckFile (a lenient STAT probe) reports healthy if ANY
// configured provider holds the article. So a file can sit VerdictFailed
// forever, reported healthy every sweep, with no automatic remediation.
//
// When PAR2 is not usable, this runs the stratified damage sampler: a real
// BODY fetch of a spread sample of segments (not just STAT) through the
// NNTP client. The sampler's verdict replaces the lenient CheckFile result.
//
// When PAR2 is usable, this returns (res, false) unchanged — the existing
// playback-triggered PAR2 path handles it.
//
// Returns (res, true) when this call fully decided the file's outcome.
func (r *Repair) reconcileStickyFailed(ctx context.Context, c *candidate, entry *storage.Entry, name string, res fileResult) (fileResult, bool) {
	if !res.healthy || r.manager.usenet == nil {
		return res, false
	}
	nzbID := entry.InfoHash
	if r.manager.usenet.OverlayVerdict(nzbID, name) != overlay.VerdictFailed {
		return res, false
	}
	// Failed with every dead segment patched: PAR2 already repaired it, so
	// there is nothing to sample and nothing to re-grab.
	if cleared, err := r.manager.usenet.OverlayUnfailRepaired(nzbID); err == nil && slices.Contains(cleared, name) {
		r.logger.Info().Str("entry", entry.Name).Str("file", name).
			Msg("Repair: sticky-failed file has every dead segment patched; cleared the failed verdict")
		return res, false
	}

	var par2Usable bool
	if r.manager.par2Repair != nil {
		par2Usable, _ = r.manager.par2Repair.par2Usable(nzbID)
	}

	// When PAR2 is usable and no sampler data is available, leave to the
	// existing playback-triggered PAR2 path. But we still run the sampler
	// below so that if extrapolated damage exceeds PAR2 recovery capacity,
	// we can skip straight to regrab instead of queueing a doomed repair.
	budget := sampleBudgetFromContext(ctx)
	if par2Usable && (budget == nil || budget.isExhausted()) {
		return res, false
	}
	if !par2Usable && budget != nil && budget.isExhausted() {
		r.logger.Info().Str("entry", entry.Name).Str("file", name).
			Msg("Repair: sticky-failed file skipped, per-sweep sample budget exhausted")
		return res, true
	}

	sampleCtx := usenet.ContextForVerificationRead(ctx)
	sampleResult, err := r.manager.usenet.RunDamageSample(sampleCtx, nzbID, name, usenet.SampleOpts{})
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		if par2Usable {
			return res, false
		}
		r.logger.Info().Err(err).Str("entry", entry.Name).Str("file", name).
			Msg("Repair: sticky-failed damage sample failed, inconclusive")
		return res, true
	}

	// When PAR2 is usable, check whether extrapolated damage exceeds the
	// available recovery capacity. If so, queueing a PAR2 job is pointless
	// — skip straight to regrab.
	if par2Usable {
		capacity := r.manager.par2Repair.par2RecoveryCapacity(nzbID)
		if capacity >= 0 && sampleResult.ExtrapolatedDeadCount > capacity {
			r.logger.Info().Str("entry", entry.Name).Str("file", name).
				Int("extrapolated_dead", sampleResult.ExtrapolatedDeadCount).
				Int("recovery_capacity", capacity).
				Msg("Repair: extrapolated damage exceeds PAR2 recovery capacity, skipping to regrab")
			return r.regrabStickyFailed(ctx, c, entry, name, res), true
		}
		return res, false
	}

	switch decideStickyFailedAction(false, sampleResult.Verdict) {
	case stickyFailedClear:
		if sampleResult.RecordedDeadRecovered == 0 && len(r.recordedDeadIndices(nzbID, name)) > 0 {
			r.logger.Info().Str("entry", entry.Name).Str("file", name).
				Msg("Repair: sticky-failed sample clean but recorded dead not all recovered, inconclusive")
			return res, true
		}
		r.clearStickyFailed(entry, name, nzbID)
		return res, true
	case stickyFailedRegrab:
		return r.regrabStickyFailed(ctx, c, entry, name, res), true
	default:
		r.logger.Info().Str("entry", entry.Name).Str("file", name).
			Str("verdict", string(sampleResult.Verdict)).
			Msg("Repair: sticky-failed sample inconclusive, no action")
		return res, true
	}
}

// recordedDeadIndices returns the overlay's recorded-dead (non-patched)
// segment indices for nzbID/name.
func (r *Repair) recordedDeadIndices(nzbID, name string) []int {
	var indices []int
	for _, d := range r.unpatchedDeadSegments(nzbID, name) {
		indices = append(indices, d.Index)
	}
	return indices
}

// unpatchedDeadSegments returns the overlay's dead and padded (non-patched)
// segment records for nzbID/name: the ones OverlayClearFileDamage deletes.
func (r *Repair) unpatchedDeadSegments(nzbID, name string) []overlay.DeadSegment {
	if r.manager.usenet == nil {
		return nil
	}
	m, err := r.manager.usenet.OverlayManifest(nzbID)
	if err != nil {
		return nil
	}
	fe := m.Files[name]
	if fe == nil {
		return nil
	}
	var segs []overlay.DeadSegment
	for _, d := range fe.DeadSegments {
		if d.Status != overlay.StatusPatched {
			segs = append(segs, d)
		}
	}
	return segs
}

// clearStickyFailed un-fails a file the damage sampler proved genuinely
// recovered. It uses OverlayClearFileDamage (not OverlayDeleteFile) so any
// PAR2-patched segments — real recovered bytes — are preserved while the
// verdict and dead/padded records are removed. It also un-poisons the
// failedFiles cache so the next read re-verifies from scratch.
//
// A padded segment's zeros are in the DFS cache with no marker, and the
// clear deletes the only record of where they are. So the records are read
// first, and once the file is un-failed their cached ranges are forgotten
// and a live reader is told to fetch them again (forgetCachedPads). That
// runs after the clear, so a read in between re-fetches the articles that
// came back rather than padding again from the old records.
func (r *Repair) clearStickyFailed(entry *storage.Entry, name, nzbID string) {
	pads := r.unpatchedDeadSegments(nzbID, name)
	patchesPreserved, err := r.manager.usenet.OverlayClearFileDamage(nzbID, name)
	if err != nil {
		r.logger.Warn().Err(err).Str("entry", entry.Name).Str("file", name).
			Msg("Repair: failed to clear resolved sticky-failed overlay record")
		return
	}
	r.manager.usenet.ClearFailedFile(nzbID, name)
	r.forgetCachedPads(entry, nzbID, name, pads)
	evt := r.logger.Info().Str("entry", entry.Name).Str("file", name)
	if patchesPreserved {
		evt.Bool("patches_preserved", true)
	}
	evt.Msg("Repair: sticky-failed verdict cleared by damage sample; file genuinely recovered")
}

// forgetCachedPads drops the cached copies of a cleared file's padded
// segments, as a PAR2 pass does for the segments it patches (encrypted
// files get the same CBC widening, via buildDeadOutputRanges).
func (r *Repair) forgetCachedPads(entry *storage.Entry, nzbID, name string, pads []overlay.DeadSegment) {
	if len(pads) == 0 || r.manager.par2Repair == nil {
		return
	}
	nzb, err := r.manager.usenet.GetNZB(nzbID)
	if err != nil || nzb == nil {
		r.logger.Warn().Err(err).Str("entry", entry.Name).Str("file", name).Int("padded", len(pads)).
			Msg("Repair: cannot read the NZB record, cached pads of the cleared file not forgotten")
		return
	}
	r.manager.par2Repair.invalidateCachedRanges(nzb, nzbID, entry.Name, map[string][]overlay.DeadSegment{name: pads})
}

// regrabStickyFailed routes a sticky-Failed file the sampler confirmed still
// broken to the same guarded blocklist+re-search path a live playback failure
// uses. auto=true so regrabGuard applies. It re-grabs only when the probing
// run may heal now (autoRepairStillOn: auto-repair or Recheck's fix on, no
// post-restore hold, brake not tripped); otherwise the file is listed
// broken and left to the run's own broken-entry handling.
func (r *Repair) regrabStickyFailed(ctx context.Context, c *candidate, entry *storage.Entry, name string, res fileResult) fileResult {
	res.healthy = false
	res.reason = "sticky_failed_sample_broken"
	autoRepair, _ := ctx.Value(stickyRegrabAutoRepairKey{}).(bool)
	if !r.autoRepairStillOn(ctx, autoRepair) {
		// Listed broken for the run's own handling, which heals a broken
		// entry only under the same gate.
		res.broken = true
		r.logger.Debug().Str("entry", entry.Name).Str("file", name).
			Msg("Repair: sticky-failed regrab skipped; healing is off for this run")
		return res
	}
	// The heal's PAR2 pass takes the release's claim over, so hold it first,
	// as handleAutoDamage does. Refused means another re-grab or pass owns
	// the release: leave it to that one, and don't list the file broken, or
	// the run's own heal would start a second one unclaimed.
	release, ok := r.handlers.TryAcquireRegrab(entryNZBID(entry))
	if !ok {
		r.logger.Debug().Str("entry", entry.Name).Str("file", name).
			Msg("Repair: sticky-failed regrab skipped; entry already being handled")
		return res
	}
	defer release()
	acted, guardReason, err := r.repairPlaybackFileNow(ctx, c.name, name, true, false)
	switch {
	case err != nil:
		r.logger.Warn().Err(err).Str("entry", entry.Name).Str("file", name).
			Msg("Repair: sticky-failed regrab attempt errored")
	case !acted:
		r.logger.Debug().Str("entry", entry.Name).Str("file", name).Str("reason", guardReason).
			Msg("Repair: sticky-failed regrab did not act")
	}
	return res
}
