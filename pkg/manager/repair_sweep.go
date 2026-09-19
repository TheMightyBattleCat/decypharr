package manager

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/puzpuzpuz/xsync/v4"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// candidate is the unit of work for a sweep. One per entry-folder.
//
// item is loaded lazily: enumeration records only the entry name (cheap,
// index-only) so a sweep doesn't decode and hold every entry body at once.
// probeEntry populates item just before probing and the worker releases it
// straight after, bounding resident entry bodies to the in-flight worker count
// rather than the whole store.
type candidate struct {
	name       string
	item       *storage.EntryItem
	arrName    string
	arrKind    storage.ArrKind
	contentMap map[string]arr.ContentFile // file_name -> Arr metadata when source=arr
}

// healCache memoizes per-infohash auto-heal results within one sweep so
// duplicate torrent sightings don't trigger repeated re-inserts. A stored
// nil means "healed"; a non-nil error means "previously failed".
type healCache struct {
	sf      singleflight.Group
	results *xsync.Map[string, error] // infohash -> heal error (nil if healed)
}

func newHealCache() *healCache {
	return &healCache{results: xsync.NewMap[string, error]()}
}

// do runs fix at most once per infohash, deduplicating concurrent callers via
// singleflight and memoizing the result for subsequent calls.
func (c *healCache) do(infoHash string, fix func() error) error {
	if c == nil || infoHash == "" {
		return fix()
	}
	if v, ok := c.results.Load(infoHash); ok {
		return v
	}
	_, err, _ := c.sf.Do(infoHash, func() (any, error) {
		if v, ok := c.results.Load(infoHash); ok {
			return nil, v
		}
		err := fix()
		c.results.Store(infoHash, err)
		return nil, err
	})
	return err
}

// fileResult is the outcome of probing one file in an entry.
type fileResult struct {
	name     string
	infoHash string
	protocol config.Protocol
	healthy  bool
	broken   bool
	reason   string // populated only when broken or unknown

	// decodeConclusive is true when this file's ffprobe frame-decode pass
	// actually ran to a verdict this probe (clean, or a concrete decode
	// error) rather than being cut short by a timeout or context
	// cancellation. probeFiles rolls this up into decodeRan, which gates
	// probeEntry's DecodeVerifiedAt stamp. Zero-value false when no decode
	// pass was attempted (skipDecode, or no ffprobe checker attached).
	decodeConclusive bool

	// decodeCoverage records how much of the file that decode pass covered:
	// decodeCoverageFull for the normal spread across the whole duration,
	// decodeCoveragePartial when the file has no usable container index and
	// only a bounded head was scanned. Empty when no decode ran. rollupDecode
	// reduces it to the weakest value across the entry's healthy files, which
	// probeEntry persists alongside DecodeVerifiedAt.
	decodeCoverage string

	// unverifiedReason says why a healthy file is left unverified
	// (storage.UnverifiedFile.Reason): its decode check reached no verdict, or
	// it ends before its Matroska index. Empty when the file was verified or
	// no decode check was due. shortBytes is the tail-truncation shortfall.
	unverifiedReason string
	shortBytes       int64
	// unverifiedCause and unverifiedDetail say what ffprobe printed, for a
	// decoded_with_errors reason.
	unverifiedCause  string
	unverifiedDetail string
}

// executeSweep is the body of a sweep: enumerate, filter due, probe, repair.
func (r *Repair) executeSweep(ctx context.Context, run *storage.RepairRun, opts RepairRunOptions, stopState *repairStopState) {
	cfg := r.cfg()
	log := r.logger.With().Str("run_id", run.ID).Logger()

	// Padding suppression and PAR2 auto-enqueue deferral are now scoped
	// per-entry: probeEntry marks each entry in the overlay's sweep set around
	// its own ffprobe probe and clears it after, so playback of every entry
	// not currently being probed keeps full padding + PAR2 protection while
	// the sweep runs. The sweep's own escalation still queues PAR2 passes
	// explicitly (EnqueueUrgent), which bypasses the per-entry gate.

	ctx = r.attachFFProbeChecker(ctx, log)
	ctx = contextWithSampleBudget(ctx, newSweepSampleBudget(stickyFailedSampleBudget))

	// Resolve auto-repair once: when off, the repair sweep is a pure health check —
	// it probes and records broken state but attempts no debrid re-insert and
	// no Arr delete/re-search. This also decides what happens to whatever was
	// found broken so far if a StopSchedule cuts the repair sweep short.
	autoRepair := cfg.AutoRepair
	if opts.AutoRepair != nil {
		autoRepair = *opts.AutoRepair
	}

	log.Info().Str("source", string(cfg.Source)).Msg("Sweep: selecting candidates")
	candidates, err := r.enumerateCandidates(ctx, cfg)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			r.finishCancelledRepairSweep(ctx, run, stopState, autoRepair, "context cancelled during selection", nil)
			return
		}
		log.Error().Err(err).Msg("Sweep: enumeration failed")
		r.finalizeRun(run, storage.RepairRunFailed, err.Error(), "")
		return
	}
	if ctx.Err() != nil {
		r.finishCancelledRepairSweep(ctx, run, stopState, autoRepair, "context cancelled after selection", nil)
		return
	}

	due, skipped := r.filterDueCandidates(candidates, opts.IgnoreLastChecked)
	// The full candidate set is only needed to compute `due`; drop it now so the
	// EntryItems we filtered out don't pin memory for the whole probe pass.
	candidates = nil
	protocolScope := r.effectiveProtocolScope(opts)
	due = r.filterCandidatesByProtocol(due, protocolScope)

	// Build the Arr reference set once per run and reuse it for both:
	// (1) dropping fully-superseded managed-source candidates before probing,
	// and (2) skipping individually-superseded files within any candidate's
	// probe pass (the season-pack case: some files superseded, entry still a
	// valid candidate because its other files are still referenced). A nil
	// refs (build failed, or zero eligible Arrs) means "couldn't determine
	// anything" everywhere it's used below - never treated as "nothing is
	// referenced".
	var refs map[string]map[string]string
	if len(due) > 0 {
		var refsErr error
		refs, refsErr = r.buildArrReferencedSet(ctx)
		if refsErr != nil {
			log.Debug().Err(refsErr).Msg("Repair sweep: failed to build Arr reference set; probing without supersession filtering")
			refs = nil
		}
	}

	// Arr-source repair sweeps only ever enumerate entries an Arr currently
	// references, so an already-superseded entry never reaches this point.
	// Managed-source repair sweeps enumerate every entry in storage regardless, so a
	// broken entry the Arrs have since replaced can otherwise sit here being
	// re-probed (and re-confirmed broken) forever; drop those before probing.
	if cfg.Source == config.RepairSourceManaged {
		due = r.dropSupersededCandidates(due, refs, log)
	}

	run.Stats.Candidates = len(due)
	run.Stats.SkippedFresh = skipped
	sc := &supersessionContext{refs: refs}

	// Order candidates oldest-checked-first (never-checked entries sort
	// first, since their LastCheckedAt is the zero time). This is what makes
	// a StopSchedule-truncated repair sweep make guaranteed forward progress: any
	// entry probed today moves to the back of the queue (its LastCheckedAt
	// becomes "now"), so tomorrow's truncated repair sweep naturally picks up where
	// today's left off instead of re-rolling a random subset of `due`.
	//
	// This slice also doubles as the candidate list considered by this run,
	// used to scope a stop-schedule repair pass.
	names := r.orderCandidatesByLastChecked(due)

	run.Stage = storage.RepairStageProbing
	r.saveRun(run)
	log.Info().Int("due", len(due)).Int("skipped_fresh", skipped).Str("protocol", protocolScope).Bool("auto_repair", autoRepair).Msg("Sweep: probing")

	heal := newHealCache()
	err = r.probeAndHealCandidates(ctx, run, due, names, heal, opts, autoRepair, sc)
	due = nil
	if err != nil {
		if errors.Is(err, context.Canceled) {
			r.finishCancelledRepairSweep(ctx, run, stopState, autoRepair, "context cancelled during probing", names)
			return
		}
		log.Error().Err(err).Msg("Sweep: probing failed")
		r.finalizeRun(run, storage.RepairRunFailed, err.Error(), "")
		return
	}
	if ctx.Err() != nil {
		r.finishCancelledRepairSweep(ctx, run, stopState, autoRepair, "context cancelled after probing", names)
		return
	}

	r.finalizeRun(run, storage.RepairRunCompleted, "", "")
	log.Info().
		Int("probed", run.Stats.Probed).
		Int("broken", run.Stats.Broken).
		Int("healthy", run.Stats.Healthy).
		Int("repaired", run.Stats.Repaired).
		Int("repair_failed", run.Stats.RepairFailed).
		Int64("skipped_superseded_files", sc.skipped.Load()).
		Msg("Sweep: completed")
}

// finishCancelledRepairSweep is reached whenever the repair sweep's context is cancelled
// (StopRun, StopSchedule, or process shutdown). When the cancellation came
// from a StopSchedule firing, the run is finalized as completed (not
// cancelled) and, when autoRepair is on, a final repair pass runs over
// whatever this repair sweep found broken among the candidates it considered
// (names). When autoRepair is off, nothing further happens to those entries.
//
// A user-initiated StopRun already wrote RepairRunCancelled to storage before
// calling cancel; finalizeRun preserves that status regardless of what's
// passed here, so the StopRun path is unaffected.
func (r *Repair) finishCancelledRepairSweep(ctx context.Context, run *storage.RepairRun, stopState *repairStopState, autoRepair bool, reason string, names []string) {
	stopped := stopState != nil && stopState.get()
	if !stopped {
		r.finalizeRun(run, storage.RepairRunCancelled, "", reason)
		return
	}

	log := r.logger.With().Str("run_id", run.ID).Logger()
	log.Info().Bool("auto_repair", autoRepair).Msg("Repair sweep: stop schedule fired; finishing run")

	if autoRepair && len(names) > 0 {
		// Use a fresh, un-cancelled context for the final repair pass: the
		// probe pass was cut short, but the repair pass over what's already
		// known-broken is a short, bounded set of Arr calls and should be
		// allowed to complete. Bound it so a misbehaving Arr can't hang.
		repairCtx, cancel := context.WithTimeout(detachedRepairContext(ctx, r.parentCtx), repairStopFinalRepairTimeout)
		defer cancel()

		healths, _ := r.collectBrokenHealths(names, true)
		if healths.Size() > 0 {
			run.Stage = storage.RepairStageRepairing
			r.saveRun(run)
			r.repairBroken(repairCtx, run, healths, true)
		}
	}

	run.CancelReason = ""
	r.finalizeRun(run, storage.RepairRunCompleted, "", "stopped by schedule: "+reason)
}

// detachedRepairContext returns a context that is not already cancelled, for
// use by the post-stop repair pass. Falls back to the repair service's parent
// context (or background) when the run's own context has already been
// cancelled.
func detachedRepairContext(runCtx, parentCtx context.Context) context.Context {
	if runCtx.Err() == nil {
		return runCtx
	}
	if parentCtx != nil {
		return parentCtx
	}
	return context.Background()
}

// probeAndHealCandidates fans out across candidates with cfg.Repair.Workers
// concurrency. Each entry then probes its own files internally with at most
// repairFilesPerEntry concurrency, so total file probes in flight = workers × 2.
//
// names gives the iteration order (see orderCandidatesByLastChecked):
// g.Go is called in this order, so with N workers the oldest-checked N
// candidates start first. If the run is cut short by a StopSchedule, the
// candidates that didn't get a chance to start remain oldest-first for the
// next repair sweep.
//
// Healing is folded into the per-entry pass: probeEntry runs auto-heal (debrid
// re-insert) inline, and when an entry is still broken afterwards this kicks
// off the Arr delete/blocklist/re-search for that one entry — so there's no
// separate end-of-run repair pass holding every health in memory. All healing
// is gated on autoRepair.
func (r *Repair) probeAndHealCandidates(ctx context.Context, run *storage.RepairRun, candidates map[string]*candidate, names []string, heal *healCache, opts RepairRunOptions, autoRepair bool, sc *supersessionContext) error {
	// run.Stats has plain int fields, so a single mutex guards every mutation
	// and the saveRun that follows it.
	var runMu sync.Mutex

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(1, r.workers()))

	for _, name := range names {
		c := candidates[name]
		if c == nil {
			continue
		}
		g.Go(func() error {
			if gctx.Err() != nil {
				return gctx.Err()
			}

			h, decode := r.probeEntry(gctx, run.ID, c, heal, opts, autoRepair, sc)
			if h == nil {
				// Entry vanished or had no files between enumeration and probe;
				// skip without counting. Release any loaded body.
				c.item = nil
				c.contentMap = nil
				return nil
			}

			// Still broken after the inline debrid re-insert: escalate to the
			// Arr delete + re-search for just this entry, and release any
			// regrab claim routeAutoRepair took while probing.
			if h.Status == storage.HealthBroken {
				r.finalizeBrokenEntry(gctx, run, &runMu, name, h, autoRepair)
				if autoRepair {
					run.MarkHealed(name)
				}
			}

			runMu.Lock()
			run.Stats.Probed++
			if decode.skipped {
				run.Stats.DecodeSkipped++
			}
			if decode.unverified {
				run.Stats.Unverified++
			}
			switch h.Status {
			case storage.HealthHealthy:
				run.Stats.Healthy++
			case storage.HealthBroken:
				run.Stats.Broken++
			case storage.HealthUnknown, storage.HealthUnsupported:
				run.Stats.Unknown++
			}
			r.saveRun(run)
			runMu.Unlock()

			// Release this entry's body so it can be collected immediately
			// rather than lingering until the run ends.
			c.item = nil
			c.contentMap = nil
			return nil
		})
	}
	return g.Wait()
}

// probeEntry probes one entry: marks it repairing, probes its files (≤2 in
// parallel), runs auto-heal on broken torrents (only when autoRepair is set),
// then persists final health.
//
// The second return value reports what happened to the entry's decode
// verification; the caller folds it into run.Stats.DecodeSkipped and
// run.Stats.Unverified.
func (r *Repair) probeEntry(ctx context.Context, runID string, c *candidate, heal *healCache, opts RepairRunOptions, autoRepair bool, sc *supersessionContext) (*storage.EntryHealth, entryDecode) {
	s := r.manager.storage
	// Lazily load the entry body. Enumeration only recorded the name, so the
	// store isn't fully decoded up front. A vanished or empty entry is a skip
	// (nil tells the worker not to count it).
	if c.item == nil {
		item, err := s.GetEntryItem(c.name)
		if err != nil || item == nil || len(item.Files) == 0 {
			return nil, entryDecode{}
		}
		c.item = item
	}

	// Correct file sizes that disagree with the stored meta before anything
	// reads them: a stale size cuts the served file short and would be probed
	// (and fingerprinted) as such. The corrected item changes the fingerprint,
	// so a decode verification stamped on the short file does not carry over.
	if u := r.manager.usenet; u != nil && syncEntrySizesFromMeta(s, u, c.item, r.logger) {
		if item, err := s.GetEntryItem(c.name); err == nil && item != nil && len(item.Files) > 0 {
			c.item = item
		}
	}

	// Mark every nzbID backing this entry as under a sweep probe for the
	// duration of probeEntry. While marked, the fetcher refuses to pad the
	// entry's dead segments (the real 430 propagates so this run's ffprobe
	// sees the corruption and the sweep re-grabs it) and PAR2 auto-enqueue is
	// deferred for it. Scoped to just this entry: playback of every other
	// entry keeps full padding + PAR2 protection while the sweep runs. A
	// season pack whose files were individually re-grabbed can span more than
	// one InfoHash, so mark each distinct one.
	if u := r.manager.usenet; u != nil {
		seen := make(map[string]struct{}, len(c.item.Files))
		for _, f := range c.item.Files {
			if f == nil || f.InfoHash == "" {
				continue
			}
			if _, ok := seen[f.InfoHash]; ok {
				continue
			}
			seen[f.InfoHash] = struct{}{}
			u.OverlayMarkSweepEntry(f.InfoHash)
		}
		defer func() {
			for h := range seen {
				u.OverlayClearSweepEntry(h)
			}
		}()
	}

	h, _ := s.GetEntryHealth(c.name)
	if h == nil {
		h = &storage.EntryHealth{EntryName: c.name}
	}
	previous := h.Status

	// Live update: surface 'repairing' before we start the probes.
	h.PreviousStatus = previous
	h.Status = storage.HealthRepairing
	h.ActiveRunID = runID
	h.Protocol = ""
	r.saveHealth(h)

	// Drop any file this run's Arr reference set shows as already superseded
	// (unreferenced, or referenced but backed by a different InfoHash - a
	// healthy duplicate serves it) before probing anything: no STAT/provider
	// check, no ffprobe check when that's wired in, and no BrokenFiles entry
	// for it. Without this, a season pack whose broken episodes were
	// individually re-grabbed stays a valid probe candidate (its other files
	// are still referenced) but the dead episodes - still physically present
	// - would otherwise get re-probed and re-marked broken every repair sweep,
	// undoing what the "Clear replaced" pass or a prior partial-supersession
	// trim already cleared.
	names := orderedFilenames(c.item)
	names = r.filterSupersededFiles(c.item, names, sc)

	currentFP := storage.EntryItemRepairFingerprint(c.item)
	decodeVerified := !h.DecodeVerifiedAt.IsZero() && h.DecodeVerifiedFingerprint == currentFP
	if decodeVerified && time.Since(h.DecodeVerifiedAt) >= r.decodeVerifyTTL() {
		// The fingerprint still matches (file set unchanged) but it's been
		// long enough since the last deep decode pass that mid-file rot -
		// expired articles, dropped retention - could have gone undetected:
		// the container header and STAT sample stay intact even when the
		// compressed payload has rotted. Re-verify instead of trusting a
		// stale pass indefinitely. A pass below refreshes DecodeVerifiedAt to
		// now, re-arming the TTL.
		decodeVerified = false
		r.logger.Debug().Str("entry", c.item.Name).Dur("since_verified", time.Since(h.DecodeVerifiedAt)).Msg("Repair: decode verification stale (TTL expired), re-running decode windows")
	}
	if opts.ForceDecodeVerification && decodeVerified {
		// Operator asked for a full re-verify of this entry despite the
		// fingerprint match. Re-run the decode windows; a pass below refreshes
		// DecodeVerifiedAt to now (the !decodeVerified branch), a fail clears
		// the fingerprint so later sweeps re-check it too.
		decodeVerified = false
		r.logger.Info().Str("entry", c.item.Name).Msg("Repair: force-decode recheck - re-running decode verification despite fingerprint match")
	}
	if decodeVerified {
		// Debug on a sweep (thousands of entries, this fires constantly);
		// Info on a targeted recheck so the operator sees it against the few
		// entries they asked about and doesn't read the run as "skipped".
		evt := r.logger.Debug()
		if opts.Recheck {
			evt = r.logger.Info()
		}
		evt.Str("entry", c.item.Name).Msg("Repair: decode already verified for this fingerprint, skipping decode windows")
	}

	results, decodeRan, decodeCoverage, decodeAttempted := r.probeFiles(ctx, c, names, opts, decodeVerified)
	if autoRepair {
		r.autoHealResults(ctx, results, heal)
	}

	broken := r.brokenFiles(c, results)
	final := rollupStatus(results)

	h.Status = final
	h.FileCount = len(names)
	h.BrokenFiles = broken
	h.BrokenCount = len(broken)
	h.UnverifiedFiles = unverifiedFiles(c, results)
	h.UnverifiedRunID = runID
	h.Fingerprint = currentFP
	h.LastCheckedAt = time.Now()
	h.NextCheckDueAt = h.LastCheckedAt.Add(r.recheckInterval())
	h.Dirty = false
	h.DirtyReason = ""
	h.ActiveRunID = ""
	h.PreviousStatus = ""
	if proto := firstProtocol(results); proto != "" {
		h.Protocol = proto
	}
	switch final {
	case storage.HealthHealthy:
		h.LastOKAt = h.LastCheckedAt
		h.FailureReason = ""
		if decodeRan {
			h.DecodeVerifiedAt = h.LastCheckedAt
			h.DecodeVerifiedFingerprint = currentFP
			h.DecodeVerifiedCoverage = decodeCoverage
		}
	case storage.HealthBroken:
		h.LastFailedAt = h.LastCheckedAt
		h.FailureReason = topReason(broken)
	}
	if final != storage.HealthHealthy {
		h.DecodeVerifiedAt = time.Time{}
		h.DecodeVerifiedFingerprint = ""
		h.DecodeVerifiedCoverage = ""
	}

	r.saveHealth(h)
	return h, entryDecode{
		skipped:    !decodeAttempted,
		unverified: h.IsUnverified(),
	}
}

// entryDecode is what became of one probed entry's decode verification.
type entryDecode struct {
	// skipped: no decode windows ran - verified earlier for this fingerprint,
	// or decode checks are off.
	skipped bool
	// unverified: the entry is healthy, but the probe left at least one of its
	// files unverified (storage.EntryHealth.IsUnverified).
	unverified bool
}

// probeFiles fans per-file probes inside a single entry, capped at
// repairFilesPerEntry concurrent workers. Alongside the results it returns
// what probeEntry may stamp for the entry's decode verification - see
// rollupDecode - and whether decode windows were run at all.
func (r *Repair) probeFiles(ctx context.Context, c *candidate, names []string, opts RepairRunOptions, skipDecode bool) ([]fileResult, bool, string, bool) {
	// If decode verification is disabled in config, force skip for sweeps -
	// unless this is an explicit operator force-decode recheck, which is a
	// deliberate "deep-check this one thing right now" and overrides the
	// standing config toggle. Import gate is unaffected - it calls
	// checkConfirmed directly with skipDecode=false.
	if !skipDecode && !opts.ForceDecodeVerification && !config.Get().Repair.FFProbeDecodeCheckEnabled() {
		skipDecode = true
	}
	results := make([]fileResult, len(names))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(repairFilesPerEntry)
	for i, name := range names {
		g.Go(func() error {
			if gctx.Err() != nil {
				results[i] = fileResult{name: name, reason: "context_cancelled"}
				return nil
			}
			results[i] = r.probeFile(gctx, c, name, opts, skipDecode)
			return nil
		})
	}
	_ = g.Wait()

	decodeRan, coverage := rollupDecode(results, skipDecode)
	return results, decodeRan, coverage, !skipDecode
}

// rollupDecode reduces an entry's per-file decode outcomes to what probeEntry
// may stamp: whether a decode verification ran for the entry, and how much of
// the entry it covered.
//
// decodeRan gates probeEntry's DecodeVerifiedAt stamp. It's true only
// when a frame-decode pass was attempted for this entry (skipDecode
// false, after probeFiles' config override) AND every file that
// contributes to a healthy rollup completed its decode windows
// conclusively - i.e. not cut short by an ffprobe timeout or a context
// cancellation, which return inconclusively (see ffprobeChecker.check).
// One healthy file with an inconclusive decode blocks the stamp for the
// whole entry, so a slow cold REMUX read that times out mid-sweep no
// longer arms the 30-day skip as though it had verified clean. This also
// covers the case where no ffprobe checker was attached (FFProbeCheck
// off): the STAT-healthy files carry decodeConclusive=false and block
// the stamp.
//
// Non-healthy files don't gate this: a broken rollup clears the stamp
// anyway, and a file that errored out before reaching ffprobe
// (usenet_probe_error, protocol_skipped, ...) is already outside the
// healthy verdict rollupStatus computes. A transiently-errored file in
// an otherwise-healthy entry can therefore still leave the entry stamped
// without that file's decode having run; it self-heals at the TTL and
// never suppresses that file's own STAT probe, so the residual window is
// narrow and bounded.
//
// coverage is the WEAKEST value across the healthy files: one file scanned as
// a bounded head makes the entry partial, since the stamp is per entry and
// must not claim more than was actually read. Empty whenever decodeRan is
// false.
func rollupDecode(results []fileResult, skipDecode bool) (decodeRan bool, coverage string) {
	if skipDecode {
		return false, ""
	}
	coverage = decodeCoverageFull
	for _, fr := range results {
		if !fr.healthy {
			continue
		}
		if !fr.decodeConclusive {
			return false, ""
		}
		if fr.decodeCoverage == decodeCoveragePartial {
			coverage = decodeCoveragePartial
		}
	}
	return true, coverage
}

// probeFile checks one file. NZB probes use usenet.CheckFile. Torrent probes
// use the provider CheckFile endpoint unless this run requests unrestrict-link
// probing. When the protocol probe passes and an ffprobe checker is attached
// to ctx (Repair.FFProbeCheck), the file is additionally validated by reading
// it back over WebDAV - this is the only check that can catch a STAT-alive,
// BODY-dead file or a mis-assembled container, since NNTP/provider CheckFile
// never look past the article/link's existence.
//
// For an NZB file CheckFile reports healthy on, reconcileStickyFailed gets a
// chance to override that: CheckFile tolerates ANY configured provider
// (including backups) serving the article, which can paper over a sticky
// VerdictFailed overlay record forever with no automatic remediation (see
// reconcileStickyFailed's doc comment for the full deadlock). When it
// reports handled=true it has already fully decided this file's outcome
// (regrabbed it, or cleared the verdict on strict evidence) - the generic
// ffprobe re-check below is skipped for it, not run a second time.
func (r *Repair) probeFile(ctx context.Context, c *candidate, name string, opts RepairRunOptions, skipDecode bool) fileResult {
	file := c.item.Files[name]
	res := fileResult{name: name}

	if file == nil || file.InfoHash == "" {
		res.reason = "missing_infohash"
		return res
	}
	res.infoHash = file.InfoHash

	entry, err := r.manager.GetEntry(file.InfoHash)
	if err != nil || entry == nil {
		res.reason = "entry_not_found"
		return res
	}
	res.protocol = entry.Protocol
	if !repairProtocolMatches(r.effectiveProtocolScope(opts), entry.Protocol) {
		res.reason = "protocol_skipped"
		return res
	}

	if entry.IsNZB() {
		res = r.probeNZBFile(ctx, entry, name, res)
		if reconciled, handled := r.reconcileStickyFailed(ctx, c, entry, name, res); handled {
			return reconciled
		}
	} else {
		res = r.probeTorrentFile(ctx, entry, file, name, res, opts)
	}

	if res.healthy {
		if checker := ffprobeCheckerFromContext(ctx); checker != nil {
			u := r.manager.usenet
			// Mechanism A (pre-ffprobe): a 430 already surfaced for this entry
			// during the sweep window - a concurrent playback read hit a dead
			// article. The file is broken; don't spend the sweep's NNTP
			// bandwidth pulling known-dead articles through ffprobe just to
			// reach the same verdict.
			if u != nil && u.OverlayIsSweepDead(res.infoHash) {
				r.logger.Warn().Str("entry", c.name).Str("file", name).
					Msg("Repair: dead segments detected during sweep probe; skipping ffprobe")
				res.healthy = false
				res.broken = true
				res.reason = "sweep_dead_segment"
				return res
			}

			// Import geometry: slices spliced from another volume (stored meta,
			// free) and a served length short of the Matroska Segment (one
			// article's prefix, in memory). The length check runs even when
			// decoding is skipped: a decode stamp does not prove the file is
			// whole - on a production install 26 of 29 files missing a volume or more
			// carried one - and ffprobe reads the same article right after.
			if entry.IsNZB() {
				geo := r.checkImportGeometry(ctx, res.infoHash, name, true)
				switch {
				case geo.reason != "":
					r.logger.Warn().Str("entry", c.name).Str("file", name).Str("reason", geo.reason).
						Int("spliced_boundaries", geo.splices).Int64("short_bytes", geo.shortBytes).Bool("missing_start", geo.missingStart).
						Msg("Repair: file was assembled wrong at import; the posting is fine, so a re-grab keeps the release")
					res.healthy = false
					res.broken = true
					res.reason = geo.reason
					return res
				case geo.tailTruncated:
					// Plays, but its index is cut, so every decode check forward-
					// scans and ends inconclusive. Not stamped: listed Unverified,
					// even on an entry whose decode was verified earlier.
					r.logger.Warn().Str("entry", c.name).Str("file", name).Int64("short_bytes", geo.shortBytes).
						Msg("Repair: file ends before its Matroska index (tail truncated at import); skipping decode checks, replace it from Unverified if seeking matters")
					res.decodeConclusive = false
					res.unverifiedReason = reasonTailTruncated
					res.shortBytes = geo.shortBytes
					return res
				case geo.singleSplice:
					r.logger.Warn().Str("entry", c.name).Str("file", name).
						Msg("Repair: one volume boundary serves another volume's article (assembled wrong at import); not re-grabbed automatically, re-grab by hand")
				}
				// A short volume before a full one, or a splice, is how a file
				// stored out of volume order shows in its meta. Reading the
				// volumes' headers settles it before a decode check spends its
				// budget on the jumps.
				if geo.singleSplice || geo.shortVolume {
					if vo, checked := r.checkVolumeOrder(ctx, res.infoHash, name); checked && vo.misordered {
						return r.markVolumeOrder(c.name, name, vo, res)
					}
				}
			}

			exp := expectedRuntimeFor(c, name)
			// The sweep runs the same decode check as the import gate, so it
			// gets the same byte cap - otherwise a file the gate admitted on
			// a spent budget would simply re-spend it here (moving the cost
			// to the nightly sweep rather than removing it).
			budget := verifyBudgetFor(exp.Bytes)
			registerVerifyBudget(res.infoHash, name, budget)
			sig := NewDeadSegmentSignal()
			registerDeadSignal(res.infoHash, name, sig)
			unmarkSweep := registerSweepVerification(res.infoHash, name)
			started := time.Now()
			cause := &unverifiedCause{}
			ok, reason, conclusive, coverage := checker.checkConfirmed(contextWithUnverifiedCause(ctx, cause), c.name, name, exp, skipDecode, sig, budget)
			unmarkSweep()
			unregisterDeadSignal(res.infoHash, name, sig)
			unregisterVerifyBudget(res.infoHash, name, budget)
			res.decodeConclusive = conclusive
			res.decodeCoverage = coverage
			if ok && !conclusive && !skipDecode {
				res.unverifiedReason = cause.get()
				res.unverifiedCause, res.unverifiedDetail = cause.decodeCause, cause.detail
				// Misordered volumes decode through as valid Matroska from
				// elsewhere in the file, which lands here rather than broken.
				if entry.IsNZB() && (res.unverifiedReason == unverifiedDecodeErrors || res.unverifiedReason == unverifiedReadBudget) {
					if vo, checked := r.checkVolumeOrder(ctx, res.infoHash, name); checked && vo.misordered {
						res = r.markVolumeOrder(c.name, name, vo, res)
					}
				}
			}

			if !ok {
				res.healthy = false
				res.broken = true
				res.reason = reason
			} else if u != nil && u.OverlayIsSweepDead(res.infoHash) {
				// Mechanism A (post-ffprobe): ffprobe's sampling windows
				// missed the damage, but a 430 was confirmed for this entry
				// while the probe ran. Override the healthy verdict.
				r.logger.Warn().Str("entry", c.name).Str("file", name).
					Msg("Repair: ffprobe passed but dead segments confirmed during probe; overriding to broken")
				res.healthy = false
				res.broken = true
				res.reason = "sweep_dead_segment_ffprobe_override"
			}
			r.logFileVerdict(c.name, name, exp.Bytes, budget, time.Since(started), res, skipDecode)
		}
	} else if res.broken {
		if checker := ffprobeCheckerFromContext(ctx); checker != nil {
			// The STAT/provider probe already classified this file broken
			// (dead segments recorded, PAR2 terminal, or routed to re-grab).
			// An ffprobe container read would only spend the sweep's time
			// pulling known-dead articles to reach the same verdict - skip it
			// and let the existing broken classification stand.
			r.logger.Debug().Str("entry", c.name).Str("file", name).Msg("Repair: skipping ffprobe — STAT confirmed dead segments")
			r.logFileVerdict(c.name, name, expectedRuntimeFor(c, name).Bytes, nil, 0, res, false)
		}
	}
	return res
}

// markVolumeOrder leaves a file whose volumes are stored out of order healthy
// but unverified with reasonVolumeOrder: it plays, with jumps, and Replace
// re-grabs it keeping the release.
func (r *Repair) markVolumeOrder(entryName, name string, vo volumeOrderVerdict, res fileResult) fileResult {
	r.logger.Warn().Str("entry", entryName).Str("file", name).Int("volumes", vo.volumes).Int("out_of_place", vo.outOfPlace).
		Int("position", vo.position).Int("volume_number", vo.number).Str("source", vo.source).
		Msg("Repair: archive volumes are stored out of order (assembled wrong at import); replace it from Unverified")
	res.decodeConclusive = false
	res.unverifiedReason = reasonVolumeOrder
	res.unverifiedCause = ""
	res.unverifiedDetail = vo.detail()
	return res
}

// logFileVerdict writes the one INFO line a sweep gives each file it checks,
// for someone following the journal: the verdict, the file, and what the check
// cost. The checker's DEBUG lines keep the per-probe detail in the log file.
func (r *Repair) logFileVerdict(entryName, file string, size int64, budget *VerifyBudget, took time.Duration, res fileResult, skipDecode bool) {
	ev := r.logger.Info().Str("entry", entryName).Str(logger.FieldSubject, file)
	if size > 0 {
		ev = ev.Int64(logger.FieldSize, size)
	}
	if rate := budget.MiBPerSec(); rate > 0 {
		ev = ev.Float64(logger.FieldRate, rate)
	}
	if took > 0 {
		ev = ev.Dur(logger.FieldTook, took)
	}
	switch {
	case res.broken:
		ev.Str(logger.FieldStatus, logger.StatusFail).Str("reason", res.reason).Msg("broken")
	case skipDecode:
		ev.Str(logger.FieldStatus, logger.StatusOK).Str(logger.FieldNote, "decode verified earlier").Msg("healthy")
	case !res.decodeConclusive:
		ev.Str(logger.FieldStatus, logger.StatusWarn).Str(logger.FieldNote, "decode not verified").Str("reason", res.unverifiedReason).Msg("inconclusive")
	case res.decodeCoverage == decodeCoveragePartial:
		ev.Str(logger.FieldStatus, logger.StatusOK).Str(logger.FieldNote, "start of file only").Msg("verified")
	default:
		ev.Str(logger.FieldStatus, logger.StatusOK).Msg("verified")
	}
}

func (r *Repair) probeNZBFile(ctx context.Context, entry *storage.Entry, name string, res fileResult) fileResult {
	if r.manager.usenet == nil {
		res.reason = "usenet_client_not_configured"
		return res
	}
	err := r.manager.usenet.CheckFile(ctx, entry.InfoHash, name)
	if err == nil {
		if r.par2TerminalWithDamage(entry.InfoHash, name) {
			// CheckFile is a lenient STAT probe - it passes if ANY configured
			// provider (backups included) still holds a sampled article. That
			// papers over a release the overlay has recorded real dead/padded
			// segments for once PAR2 has already given up on it: the entry
			// then sits "healthy" every sweep with no automatic remediation
			// (the playback PAR2 path won't re-try a terminal verdict, and
			// nothing else re-grabs without a fresh probe failure). Escalate
			// it to the same guarded re-grab a segment-missing probe gets.
			r.logger.Info().Str("entry", entry.Name).Str("nzb_id", entry.InfoHash).Str("file", name).
				Msg("Repair: sweep escalating PAR2-terminal entry to regrab (overlay damage recorded, STAT probe lenient-healthy)")
			return r.routeAutoRepair(entry, res)
		}
		res.healthy = true
		return res
	}
	if errors.Is(err, customerror.UsenetSegmentMissingError) {
		r.recordDeadSegments(ctx, entry, name)
		return r.routeAutoRepair(entry, res)
	}
	res.reason = "usenet_probe_error"
	return res
}

// par2TerminalWithDamage reports whether nzbID's PAR2 repair state is terminal
// (see Par2Repair.par2Usable / par2_repair.go's GetPar2RepairState().Terminal
// read) AND the overlay store still has at least one non-patched dead segment
// recorded for this specific file - checked via the same OverlayPendingRepair
// accessor the padding import gate uses (see paddingImportGate). Both must
// hold: a terminal state alone could be stale after a manual clear, and
// overlay damage alone is still the PAR2 path's job until PAR2 gives up.
func (r *Repair) par2TerminalWithDamage(nzbID, fileName string) bool {
	if r.manager.usenet == nil || r.manager.storage == nil {
		return false
	}
	state, err := r.manager.storage.GetPar2RepairState(nzbID)
	if err != nil || state == nil || !state.Terminal {
		return false
	}
	pending, err := r.manager.usenet.OverlayPendingRepair(nzbID)
	if err != nil {
		return false
	}
	return len(pending[fileName]) > 0
}

// routeAutoRepair applies the coordinated auto-repair policy
// (decideAutoRepairAction, source=sweep) to a file the sweep just confirmed
// has a segment-missing failure, claiming the handler registry before acting
// so a concurrent playback-failure escalation never double-re-grabs the same
// entry (nzbID = entry.InfoHash - see repair_handler_registry.go).
//
// PAR2 is a playback-only mechanism (see decideAutoRepairAction's doc
// comment): with source=sweep, decideAutoRepairAction always resolves a real
// verdict to autoActionRegrab, regardless of the PAR2 toggle - the sweep runs
// with a cold DFS cache (nothing is playing), so PAR2 there would fetch the
// entire release from Usenet instead of the cache-warm slices it relies on to
// be cheap. Since the outcome for source=sweep never varies, this routes
// straight to the re-grab path rather than consulting the policy for an
// already-known answer.
func (r *Repair) routeAutoRepair(entry *storage.Entry, res fileResult) fileResult {
	nzbID := entry.InfoHash

	if !r.handlers.TryAcquire(nzbID, handlerRegrab) {
		if state, err := r.manager.storage.GetPar2RepairState(nzbID); err == nil && state != nil && state.Terminal {
			// PAR2 already proved this release unrepairable - reclaim the
			// stuck terminal claim for the sweep's own regrab path instead of
			// deferring to it forever (Set clears terminal atomically).
			r.handlers.Set(nzbID, handlerRegrab)
		} else {
			// Already being handled (an in-flight re-grab from another
			// candidate, or a rare race with a manual action) - defer to it.
			res.reason = "usenet_segment_missing_deferred"
			return res
		}
	}
	// Released once healBrokenEntry has processed this candidate's broken
	// files - see probeAndHealCandidates.
	res.broken = true
	res.reason = "usenet_segment_missing"
	return res
}

// recordDeadSegments re-probes name for the specific segments confirmed
// missing (CheckFile above only reports pass/fail) and persists them to the
// overlay store, so the reader's padding policy and the PAR2 repair worker
// see this damage without needing to rediscover it themselves. Returns the
// resulting verdict (VerdictClean if the detail probe itself failed or found
// nothing - callers must not queue PAR2 repair on that).
func (r *Repair) recordDeadSegments(ctx context.Context, entry *storage.Entry, name string) overlay.Verdict {
	if r.manager.usenet == nil {
		return overlay.VerdictClean
	}
	missing, err := r.manager.usenet.CheckFileDetailed(ctx, entry.InfoHash, name)
	if err != nil || len(missing) == 0 {
		return overlay.VerdictClean
	}
	for _, seg := range missing {
		if err := r.manager.usenet.RecordOverlayDead(entry.InfoHash, name, seg.Index, seg.MessageID, seg.Bytes); err != nil {
			r.logger.Debug().Err(err).Str("entry", entry.Name).Str("file", name).Int("segment", seg.Index).Msg("Repair: failed to record dead segment in overlay")
		}
	}
	verdict := r.manager.usenet.OverlayVerdict(entry.InfoHash, name)
	r.logger.Debug().Str("entry", entry.Name).Str("file", name).Int("missing_segments", len(missing)).Str("verdict", string(verdict)).Msg("Repair: recorded dead segments from sweep probe")
	return verdict
}

func (r *Repair) probeTorrentFile(ctx context.Context, entry *storage.Entry, file *storage.File, name string, res fileResult, opts RepairRunOptions) fileResult {
	// No configured debrid holds it, or re-insertion already gave up: broken
	// for a blocklisting re-grab, without asking any debrid.
	if reason := debridGoneReason(entry, r.debridConfigured); reason != "" {
		res.broken = true
		res.reason = reason
		return res
	}
	client := r.manager.ProviderClient(entry.ActiveProvider)
	if client == nil {
		res.reason = "provider_client_not_found"
		return res
	}
	if opts.UnrestrictLink {
		return r.probeTorrentFileByUnrestrict(entry, file, name, res, client)
	}
	if !client.SupportsCheck() {
		res.reason = "provider_check_unsupported"
		return res
	}
	link := linkOf(entry, name)
	if link == "" {
		res.broken = true
		res.reason = "missing_provider_link"
		return res
	}
	err := client.CheckFile(ctx, file.InfoHash, link)
	if err == nil {
		res.healthy = true
		return res
	}
	if errors.Is(err, customerror.HosterUnavailableError) {
		res.broken = true
		res.reason = "hoster_unavailable"
	} else {
		res.reason = "provider_probe_error"
	}
	return res
}

func (r *Repair) probeTorrentFileByUnrestrict(entry *storage.Entry, file *storage.File, name string, res fileResult, client debrid.Client) fileResult {
	placement := entry.GetActiveProvider()
	if placement == nil {
		res.reason = "placement_not_found"
		return res
	}
	placementFile := placement.Files[name]
	if placementFile == nil {
		res.reason = "placement_file_not_found"
		return res
	}
	if placementFile.Link == "" && placementFile.Id == "" {
		res.broken = true
		res.reason = "missing_provider_link"
		return res
	}

	debridFile := &debridTypes.File{
		Id:        placementFile.Id,
		Link:      placementFile.Link,
		Path:      placementFile.Path,
		Name:      file.Name,
		Size:      file.Size,
		ByteRange: file.ByteRange,
		Deleted:   file.Deleted,
	}
	downloadLink, err := client.GetDownloadLink(placement.ID, debridFile)
	if err == nil && !downloadLink.Empty() {
		res.healthy = true
		return res
	}
	if err == nil || errors.Is(err, debridTypes.EmptyDownloadLinkError) || errors.Is(err, customerror.HosterUnavailableError) {
		res.broken = true
		if errors.Is(err, customerror.HosterUnavailableError) {
			res.reason = "hoster_unavailable"
		} else {
			res.reason = "empty_download_link"
		}
		return res
	}
	res.reason = "unrestrict_link_error"
	return res
}

// autoHealResults walks broken torrent infohashes and tries one re-insert per
// infohash (singleflighted). On success, every file in that infohash group is
// marked healthy.
func (r *Repair) autoHealResults(ctx context.Context, results []fileResult, heal *healCache) {
	byHash := make(map[string][]int)
	for i, res := range results {
		if !res.broken || res.protocol != config.ProtocolTorrent || res.infoHash == "" || noReinsertReason(res.reason) {
			continue
		}
		byHash[res.infoHash] = append(byHash[res.infoHash], i)
	}
	if len(byHash) == 0 {
		return
	}
	for infoHash, indices := range byHash {
		entry, err := r.manager.GetEntry(infoHash)
		if err != nil || entry == nil {
			continue
		}
		err = heal.do(infoHash, func() error {
			return r.manager.ReinsertEntry(ctx, entry)
		})
		if err != nil {
			continue
		}
		for _, i := range indices {
			results[i].broken = false
			results[i].healthy = true
			results[i].reason = "repaired"
		}
	}
}

// brokenFiles flattens broken results into BrokenFile records, attaching Arr
// identifiers so the repair pass can delete + re-search.
func (r *Repair) brokenFiles(c *candidate, results []fileResult) []storage.BrokenFile {
	out := make([]storage.BrokenFile, 0)
	for _, res := range results {
		if !res.broken {
			continue
		}
		bf := storage.BrokenFile{
			EntryName: c.name,
			FileName:  res.name,
			InfoHash:  res.infoHash,
			Protocol:  res.protocol,
			Reason:    res.reason,
		}
		if file, ok := c.item.Files[res.name]; ok && file != nil {
			bf.Size = file.Size
			if bf.InfoHash == "" {
				bf.InfoHash = file.InfoHash
			}
		}
		if cf, ok := c.contentMap[res.name]; ok {
			bf.ArrName = c.arrName
			bf.ArrKind = c.arrKind
			bf.MediaID = cf.Id
			bf.EpisodeID = cf.EpisodeId
			bf.ArrFileID = cf.FileId
			bf.TargetPath = cf.TargetPath
			bf.SourcePath = cf.Path
			if bf.Size == 0 {
				bf.Size = cf.Size
			}
		}
		out = append(out, bf)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FileName < out[j].FileName })
	return out
}

// rollupStatus collapses per-file results into a single EntryHealth status.
// Any broken file fails the entry; otherwise healthy wins over unknown.
func rollupStatus(results []fileResult) storage.HealthStatus {
	if len(results) == 0 {
		return storage.HealthUnknown
	}
	hasBroken, hasHealthy := false, false
	for _, res := range results {
		if res.broken {
			hasBroken = true
		}
		if res.healthy {
			hasHealthy = true
		}
	}
	switch {
	case hasBroken:
		return storage.HealthBroken
	case hasHealthy:
		return storage.HealthHealthy
	default:
		return storage.HealthUnknown
	}
}

func firstProtocol(results []fileResult) config.Protocol {
	for _, res := range results {
		if res.protocol != "" {
			return res.protocol
		}
	}
	return ""
}

// repairBroken runs the Arr delete + re-search heal over a set of already-known
// broken entries without reprobing. Two callers: FixBroken (manual, a batch
// entry-point) and finishCancelledRepairSweep's stop-schedule final pass
// (automatic, part of the sweep itself). automatic distinguishes the two -
// only the automatic caller is subject to r.regrabGuard, via
// healBrokenEntryGuarded, matching repairPlaybackFileNow's own auto/manual
// split. A manual Fix always proceeds and is never blocked by a guard trip.
func (r *Repair) repairBroken(ctx context.Context, run *storage.RepairRun, healths *xsync.Map[string, *storage.EntryHealth], automatic bool) {
	var statsMu sync.Mutex
	healths.Range(func(name string, h *storage.EntryHealth) bool {
		if ctx != nil && ctx.Err() != nil {
			return false
		}
		if run.WasHealed(name) {
			return true
		}
		if automatic {
			r.healBrokenEntryGuarded(ctx, run, &statsMu, name, h, false)
		} else {
			r.healBrokenEntry(ctx, run, &statsMu, name, h, false)
		}
		return true
	})
}

// healBrokenEntryGuarded runs healBrokenEntry for one entry the SCHEDULED
// sweep found broken and decided to heal automatically - unlike
// finalizeBrokenEntry's other automatic route (repairPlaybackFileNow, for
// playback failures and the sticky-failed sampler), this path went straight
// to healBrokenEntry with no r.regrabGuard consultation at all, so a
// genuinely dead release blocklisted-and-re-searched by the sweep, then
// re-blocklisted again on the next sweep once the Arr's replacement also
// turns out dead, could repeat indefinitely. Consults the guard once per
// distinct Arr media identity among h's broken files, mirroring
// repairPlaybackFileNow's check-before-act shape: if ANY identity is
// currently terminal or trips on this call, the whole entry is left
// unhealed this pass (matching markRegrabGuardTripped's own entry-level
// granularity - EntryHealth has no finer-grained per-file terminal surface)
// rather than issuing a blocklist the guard has already decided against.
// Manual callers (FixBroken, RecheckEntry fix=true) must keep calling
// healBrokenEntry directly - never this.
func (r *Repair) healBrokenEntryGuarded(ctx context.Context, run *storage.RepairRun, statsMu *sync.Mutex, name string, h *storage.EntryHealth, bulkOverride bool) {
	if h == nil {
		return
	}
	for _, bf := range h.BrokenFiles {
		identity := regrabIdentityKey(bf.ArrName, bf.MediaID, bf.EpisodeID, name)
		if allowed, guardReason, firstTrip := r.regrabGuard.checkAndRecord(identity, name); !allowed {
			logEvt := r.logger.Debug()
			if firstTrip {
				logEvt = r.logger.Warn()
			}
			logEvt.Str("entry", name).Str("file", bf.FileName).Str("reason", guardReason).
				Msg("Repair: stopping automatic re-grab, every candidate so far shares the same missing articles")
			if firstTrip {
				r.markRegrabGuardTripped(name, bf.FileName, h, guardReason)
			}
			return
		}
	}
	r.healBrokenEntry(ctx, run, statsMu, name, h, bulkOverride)
}

// healBrokenEntry runs the Arr delete + blocklist + re-search for one broken
// entry, then deletes the entry when it's fully broken and every file was
// handled. It does not verify the outcome: SearchMissing/MarkHistoryFailed only
// queue a download in the Arr — the replacement lands minutes-to-hours later,
// so the next scheduled sweep is where verification happens. statsMu guards
// run.Stats across concurrent entries.
func (r *Repair) healBrokenEntry(ctx context.Context, run *storage.RepairRun, statsMu *sync.Mutex, name string, h *storage.EntryHealth, bulkOverride bool) {
	if h == nil || h.Status != storage.HealthBroken {
		return
	}

	// Try a cache-only PAR2 pass before ever touching the Arr: it's free (no
	// provider bandwidth for intact data - see warmSweepRepair) whenever it
	// applies, so it's always worth trying first. Files it actually fixes
	// are dropped from h right here, exactly as if the probe had never
	// flagged them broken - the remainder falls through to the unchanged
	// delete + blocklist + re-search heal below.
	//
	// bulkOverride skips it: it flags a deliberate user "Delete & re-search
	// selected" bulk action (overlay GUI) where the user has already chosen
	// to discard this copy, so a multi-minute warm PAR2 solve per file is
	// just a stall for a repair they don't want.
	parRepaired := false
	var fixed map[string]struct{}
	if !bulkOverride {
		// PAR2 cannot fix a file assembled wrong at import; don't spend a
		// pass on one.
		damaged := make([]storage.BrokenFile, 0, len(h.BrokenFiles))
		for _, bf := range h.BrokenFiles {
			if !keepReleaseReason(bf.Reason) {
				damaged = append(damaged, bf)
			}
		}
		fixed = r.warmSweepRepair(ctx, damaged)
	}
	if len(fixed) > 0 {
		remaining := make([]storage.BrokenFile, 0, len(h.BrokenFiles))
		for _, bf := range h.BrokenFiles {
			if _, ok := fixed[bf.InfoHash]; !ok {
				remaining = append(remaining, bf)
			}
		}
		filesFixed := len(h.BrokenFiles) - len(remaining)
		h.BrokenFiles = remaining
		h.BrokenCount = len(remaining)

		statsMu.Lock()
		run.Stats.Repaired++
		parRepaired = true
		r.saveRun(run)
		statsMu.Unlock()
		r.logger.Info().Str("entry", name).Int("files_fixed", filesFixed).
			Msg("Repair: resolved via warm PAR2 sweep (cache-only, no re-grab needed)")

		if len(remaining) == 0 {
			h.Status = storage.HealthHealthy
			h.FailureReason = ""
			h.LastRepairAt = time.Now()
			h.LastOKAt = time.Now()
			r.saveHealth(h)
			return
		}
	}

	// An entry's broken files normally all belong to one Arr, but a merged
	// candidate can span more — group defensively.
	byArr := make(map[string][]arr.ContentFile)
	keepRelease := make(map[string]map[int]bool) // Arr -> FileIds re-grabbed without blocklisting
	for _, bf := range h.BrokenFiles {
		if bf.ArrName == "" || bf.ArrFileID == 0 {
			continue
		}
		if keepReleaseReason(bf.Reason) {
			if keepRelease[bf.ArrName] == nil {
				keepRelease[bf.ArrName] = make(map[int]bool)
			}
			keepRelease[bf.ArrName][bf.ArrFileID] = true
		}
		byArr[bf.ArrName] = append(byArr[bf.ArrName], arr.ContentFile{
			Id:        bf.MediaID,
			EpisodeId: bf.EpisodeID,
			FileId:    bf.ArrFileID,
			Name:      bf.FileName,
			Path:      bf.SourcePath,
			Size:      bf.Size,
			IsBroken:  true,
		})
	}
	if len(byArr) == 0 {
		return
	}

	succeeded := make(map[string]struct{}, len(byArr))
	anyActioned := false
	attempted := false
	for arrName, files := range byArr {
		if ctx != nil && ctx.Err() != nil {
			return
		}
		a := r.manager.arr.Get(arrName)
		if a == nil {
			continue
		}
		actioned, cancelled := r.repairArrFiles(ctx, run, statsMu, a, files, keepRelease[arrName])
		if cancelled {
			return
		}
		succeeded[arrName] = struct{}{}
		attempted = true
		if actioned {
			anyActioned = true
		}
	}

	// Repaired counts entries whose repair genuinely did something - at least
	// one Arr delete/blocklist/re-search call actually succeeded - not merely
	// attempted, so a heal where every download-client call errored lands in
	// RepairFailed instead. An entry already credited by the warm PAR2 pass
	// above is excluded from RepairFailed here even if its Arr-side remainder
	// comes back empty-handed - it was still repaired this run, just not by
	// this path. Granularity stays per-entry to match Broken/Probed/Healthy:
	// a season pack with three broken episodes that all get blocklisted +
	// re-searched is one repaired entry, not three.
	if anyActioned {
		statsMu.Lock()
		run.Stats.Repaired++
		r.saveRun(run)
		statsMu.Unlock()
	} else if attempted && !parRepaired {
		statsMu.Lock()
		run.Stats.RepairFailed++
		r.saveRun(run)
		statsMu.Unlock()
	}

	r.finalizeEntryRepair(name, h, succeeded)
}

// finalizeBrokenEntry is probeAndHealCandidates' per-entry tail for a still-
// broken result: heal it (only when autoRepair is on - a pure health-check
// sweep records broken state without acting on it), then always release any
// regrab claim routeAutoRepair took while probing.
//
// The release must NOT be gated on autoRepair: routeAutoRepair claims the
// handlerRegrab slot the moment it confirms a segment-missing failure,
// regardless of autoRepair. Gating the release on autoRepair too (as this
// function replaces) left a health-check-only sweep (autoRepair=false)
// stranding that claim for the registry's full TTL, silently blocking
// HandlePlaybackFailure's re-grab and PAR2's enqueue for the same file - the
// same stranded-claim shape AutoEnqueue's Par2Repair-toggle fix addressed,
// just reached via the sweep instead of the padding path.
// releaseRegrabClaims itself only releases handlerRegrab claims, so this is
// always a safe no-op for torrent-protocol brokens that never went through
// routeAutoRepair.
func (r *Repair) finalizeBrokenEntry(ctx context.Context, run *storage.RepairRun, statsMu *sync.Mutex, name string, h *storage.EntryHealth, autoRepair bool) {
	if autoRepair {
		r.healBrokenEntryGuarded(ctx, run, statsMu, name, h, false)
	}
	r.releaseRegrabClaims(h)
}

// releaseRegrabClaims releases the handler-registry slot routeAutoRepair
// claimed (autoActionRegrab -> handlers.TryAcquire(nzbID, handlerRegrab))
// for every distinct nzbID among h's broken files, now that healBrokenEntry
// has acted on them - so a later decision for the same entry isn't blocked
// by a stale claim. Only releases entries this sweep's own auto-regrab path
// actually claimed (kind == handlerRegrab): a broken file whose InfoHash
// happens to also be mid-PAR2-pass for a DIFFERENT file in the same
// candidate must keep that claim untouched, and a manual action that raced
// in and overwrote the claim (Set) always wins over this cleanup.
func (r *Repair) releaseRegrabClaims(h *storage.EntryHealth) {
	if h == nil {
		return
	}
	seen := make(map[string]struct{}, len(h.BrokenFiles))
	for _, bf := range h.BrokenFiles {
		if bf.InfoHash == "" {
			continue
		}
		if _, ok := seen[bf.InfoHash]; ok {
			continue
		}
		seen[bf.InfoHash] = struct{}{}
		if kind, terminal, exists := r.handlers.State(bf.InfoHash); exists && !terminal && kind == handlerRegrab {
			r.handlers.Release(bf.InfoHash)
		}
	}
}

// repairArrFiles deletes the broken files in one Arr, blocklists their grabs,
// and re-searches anything without a grab record. Returns true when the delete
// succeeded (so the caller may consider the files handled). Concurrency is
// bounded by the sweep's worker count; Sonarr/Radarr handle that many in-flight
// API calls fine, and the actual search/grab work is paced by the Arr's own
// command queue regardless of how the calls arrive.
//
// Files in keepRelease (by FileId) were assembled wrong at import from a
// posting that is fine: they are deleted and re-searched, never blocklisted,
// so the Arr can grab the same release and the fixed parser imports it right.
// A grab they share with a blocklisted file is still blocklisted - the Arr
// has one history record for the whole grab - and that is logged.
func (r *Repair) repairArrFiles(ctx context.Context, run *storage.RepairRun, statsMu *sync.Mutex, a *arr.Arr, files []arr.ContentFile, keepRelease map[int]bool) (actioned bool, cancelled bool) {
	// Look up the grab history per broken file. Files whose grab record exists
	// get blocklisted via MarkHistoryFailed (which Sonarr/Radarr auto-re-searches
	// when "Redownload Failed" is on — the default). Files with no grab record
	// (history trimmed, manual import) fall back to an explicit SearchMissing.
	//
	// HistoryIDs are deduped per arr — a season-pack grab covers multiple broken
	// files but only needs one history/failed POST.
	historyIDs := make(map[int]struct{})
	keptGrabs := make(map[int]string) // grab history ID -> a keep-release file in it
	needSearch := make([]arr.ContentFile, 0)
	for _, f := range files {
		if ctx != nil && ctx.Err() != nil {
			return false, true
		}
		var mediaID int
		switch a.Type {
		case arr.Sonarr:
			mediaID = f.EpisodeId
		case arr.Radarr:
			mediaID = f.Id
		}
		if mediaID == 0 {
			needSearch = append(needSearch, f)
			continue
		}
		id, _, herr := a.FindGrabHistoryID(mediaID)
		if herr != nil || id == 0 {
			needSearch = append(needSearch, f)
			continue
		}
		if keepRelease[f.FileId] {
			keptGrabs[id] = f.Name
			needSearch = append(needSearch, f)
			continue
		}
		historyIDs[id] = struct{}{}
	}
	for id, name := range keptGrabs {
		if _, blocklisted := historyIDs[id]; blocklisted {
			r.logger.Warn().Str("arr", a.Name).Int("history_id", id).Str("file", name).
				Msg("Repair: blocklisting a grab that also holds a file only assembled wrong at import - another file in it is damaged")
		}
	}

	// Clear the EpisodeFile/MovieFile rows first so the upcoming re-search isn't
	// rejected by upgrade-only quality logic. A delete failure is NOT fatal to
	// the repair: the captured FileId can be stale (a prior repair cycle for
	// this same entry already replaced the file, so this ID no longer exists
	// and Sonarr 500s on the bulk delete). When that happens the row we wanted
	// gone is effectively gone anyway, and — more importantly — we must still
	// blocklist + re-search so the Arr fetches a fresh copy. Aborting here was
	// the cause of the playback-repair churn loop: delete fails → return →
	// nothing re-searched → file stays broken → next playback 430 repeats.
	// The re-grab's import is not an Arr "upgrade" (the file row is gone by
	// then), so no webhook will name these old files. Remember them for the
	// Plex reaper, which waits for the replacement before removing the
	// "Unavailable" version they leave behind.
	for _, f := range files {
		if f.Path != "" {
			r.manager.PlexReaper().Enqueue(PlexReapNotice{
				Source: ReapSourceRepair, StalePaths: []string{f.Path}, ArrName: a.Name, MediaID: f.Id,
			})
		}
	}

	if err := a.DeleteFiles(ctx, files); err != nil {
		r.logger.Warn().Err(err).Str("arr", a.Name).
			Msg("Repair: DeleteFiles failed (continuing to blocklist + re-search anyway)")
	} else {
		actioned = true
	}

	// Blocklist each unique grab. Errors here are non-fatal: a missing blocklist
	// is bad but DeleteFiles already cleared the rows, so the fallback
	// SearchMissing below still has a chance to recover.
	for id := range historyIDs {
		if ctx != nil && ctx.Err() != nil {
			break
		}
		if err := a.MarkHistoryFailed(id); err != nil {
			r.logger.Warn().Err(err).Str("arr", a.Name).Int("history_id", id).Msg("Repair: MarkHistoryFailed failed")
		} else {
			actioned = true
		}
	}

	// SearchMissing only for files without a grab record. With one,
	// MarkHistoryFailed's auto-re-search covers the same ground without creating
	// an extra command row.
	if len(needSearch) > 0 {
		if err := a.SearchMissing(ctx, needSearch); err != nil {
			r.logger.Warn().Err(err).Str("arr", a.Name).Msg("Repair: SearchMissing fallback failed")
		} else {
			actioned = true
		}
	}

	// Repaired itself is incremented by the caller (healBrokenEntry), once per
	// entry rather than once per file here - this save just keeps live
	// progress (Probed/Broken/etc., already mutated elsewhere under statsMu)
	// visible to a concurrent poller mid-run.
	statsMu.Lock()
	r.saveRun(run)
	statsMu.Unlock()
	return actioned, false
}

// keepEntryForReGrab reports whether a fully broken entry stays after its
// files are re-searched: every broken file has an import-fault reason (tail
// truncated, volumes out of order, a volume missing or spliced at import), so
// its re-grab keeps the release. A re-grab that never lands then leaves the
// entry to fix or import by hand; the replacement supersedes it when it lands.
// A file broken in the posting itself lets the entry go.
func keepEntryForReGrab(files []storage.BrokenFile) bool {
	return len(files) > 0 && !slices.ContainsFunc(files, func(bf storage.BrokenFile) bool { return !keepReleaseReason(bf.Reason) })
}

// finalizeEntryRepair stamps LastRepairAt and, when the entry is fully broken
// and every broken file was handled (Arr-deleted + re-searched), deletes it.
// Partial-broken entries are left in place so their healthy files survive.
func (r *Repair) finalizeEntryRepair(name string, h *storage.EntryHealth, succeeded map[string]struct{}) {
	now := time.Now()

	shouldDelete := h.BrokenCount > 0 && h.BrokenCount == h.FileCount
	hashes := make(map[string]struct{})
	if shouldDelete {
		for _, bf := range h.BrokenFiles {
			if bf.ArrName == "" || bf.ArrFileID == 0 {
				shouldDelete = false
				break
			}
			if _, ok := succeeded[bf.ArrName]; !ok {
				shouldDelete = false
				break
			}
			if bf.InfoHash != "" {
				hashes[bf.InfoHash] = struct{}{}
			}
		}
		if len(hashes) == 0 {
			shouldDelete = false
		}
		if keepEntryForReGrab(h.BrokenFiles) {
			shouldDelete = false
		}
	}

	if !shouldDelete {
		h.LastRepairAt = now
		r.saveHealth(h)
		// A partial repair (some but not all of the entry's files were broken,
		// or a full delete wasn't safe - e.g. a missing Arr file ID) still
		// blocklisted + re-searched whatever succeeded above, and that heal
		// action deserves a log line just as much as a full deletion does -
		// otherwise it's a Repaired count with no corresponding evidence of
		// what actually happened.
		if len(succeeded) > 0 {
			r.logger.Info().
				Str("entry", name).
				Int("broken_files", h.BrokenCount).
				Int("total_files", h.FileCount).
				Str(logger.FieldStatus, logger.StatusWarn).
				Str(logger.FieldNote, fmt.Sprintf("%d of %d files re-searched", h.BrokenCount, h.FileCount)).
				Msg("Repair: partially repaired entry - blocklisted + re-searched broken files, entry kept")
		}
		return
	}

	for hash := range hashes {
		if err := r.manager.DeleteEntry(hash, true); err != nil {
			r.logger.Warn().Err(err).Str("entry", name).Str("infohash", hash).Msg("Repair: failed to delete fully-broken entry after re-search")
			continue
		}
		r.logger.Info().Str("entry", name).Str("infohash", hash).Str(logger.FieldStatus, logger.StatusWarn).
			Msg("Repair: deleted fully-broken entry after re-search")
	}
	// Entry fully removed: drop any lingering health record so a
	// cut-short sweep can't try to re-heal a torrent that's gone.
	r.markBrokenHealthCleared(h, now)
}

// === Candidate enumeration ===

func (r *Repair) enumerateCandidates(ctx context.Context, cfg config.RepairConfig) (map[string]*candidate, error) {
	if cfg.Source == config.RepairSourceManaged {
		return r.enumerateManagedCandidates(ctx)
	}
	return r.enumerateArrCandidates(ctx, cfg)
}

func (r *Repair) filterCandidatesByProtocol(in map[string]*candidate, scope string) map[string]*candidate {
	if repairProtocolMatches(scope, config.ProtocolAll) {
		return in
	}
	out := make(map[string]*candidate, len(in))
	for name, c := range in {
		filtered := r.filterCandidateByProtocol(c, scope)
		if filtered != nil {
			out[name] = filtered
		}
	}
	return out
}

func (r *Repair) filterCandidateByProtocol(c *candidate, scope string) *candidate {
	if c == nil {
		return nil
	}
	// Restricted scope needs per-file protocols, so the body must be present.
	// For lazily-enumerated candidates load it here (only the due subset
	// reaches this point, so it doesn't reintroduce a whole-store decode).
	if c.item == nil {
		item, err := r.manager.GetEntryItem(c.name)
		if err != nil || item == nil {
			return nil
		}
		c.item = item
	}
	files := make(map[string]*storage.File, len(c.item.Files))
	for name, file := range c.item.Files {
		if file == nil || file.Deleted || file.InfoHash == "" {
			continue
		}
		entry, err := r.manager.GetEntry(file.InfoHash)
		if err != nil || entry == nil {
			continue
		}
		if repairProtocolMatches(scope, entry.Protocol) {
			files[name] = file
		}
	}
	if len(files) == 0 {
		return nil
	}

	item := *c.item
	item.Files = files
	filtered := *c
	filtered.item = &item
	if c.contentMap != nil {
		filtered.contentMap = make(map[string]arr.ContentFile, len(c.contentMap))
		for name, content := range c.contentMap {
			if _, ok := files[name]; ok {
				filtered.contentMap[name] = content
			}
		}
	}
	return &filtered
}

func (r *Repair) enumerateManagedCandidates(ctx context.Context) (map[string]*candidate, error) {
	// Names only: GetEntryItems walks the in-memory index without reading or
	// decoding any entry body. Bodies are loaded per-entry in probeEntry and
	// released by the worker, so the sweep never holds the whole store's worth
	// of decoded EntryItems in memory at once. Entries that turn out to be
	// empty are skipped when their body is loaded.
	out := make(map[string]*candidate)
	for name := range r.manager.storage.GetEntryItems() {
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		default:
		}
		out[name] = &candidate{name: name}
	}
	return out, nil
}

func (r *Repair) enumerateArrCandidates(ctx context.Context, cfg config.RepairConfig) (map[string]*candidate, error) {
	out := make(map[string]*candidate)
	var mu sync.Mutex

	arrs := r.eligibleArrs(cfg.Arrs)
	if len(arrs) == 0 {
		return out, nil
	}

	g, gctx := errgroup.WithContext(ctx)
	for _, a := range arrs {
		g.Go(func() error {
			sub, err := r.collectArrMediaCandidates(gctx, a, "")
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
				r.logger.Warn().Err(err).Str("arr", a.Name).Msg("Sweep: GetMedia failed; skipping arr")
				return nil
			}
			mu.Lock()
			mergeCandidates(out, sub)
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}

// collectArrMediaCandidates resolves an Arr's media (or a specific media-id
// within that Arr) to entry-keyed candidates.
func (r *Repair) collectArrMediaCandidates(ctx context.Context, a *arr.Arr, mediaID string) (map[string]*candidate, error) {
	out := make(map[string]*candidate)
	media, err := a.GetMedia(ctx, mediaID)
	if err != nil {
		return nil, err
	}
	kind := arrKindFromType(a.Type)
	for _, content := range media {
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		default:
		}
		for entryPath, files := range collectArrFiles(content) {
			name := filepath.Clean(filepath.Base(entryPath))
			item, err := r.manager.GetEntryItem(name)
			if err != nil || item == nil {
				continue
			}
			c, ok := out[name]
			if !ok {
				c = &candidate{
					name:       name,
					item:       item,
					arrName:    a.Name,
					arrKind:    kind,
					contentMap: make(map[string]arr.ContentFile),
				}
				out[name] = c
			}
			if c.contentMap == nil {
				c.contentMap = make(map[string]arr.ContentFile)
			}
			for _, f := range files {
				f.EntryName = name
				f.IsSymlink = true
				c.contentMap[f.TargetPath] = f
			}
		}
	}
	return out, nil
}

func mergeCandidates(dst, src map[string]*candidate) {
	for name, c := range src {
		existing, ok := dst[name]
		if !ok {
			dst[name] = c
			continue
		}
		if existing.arrName == "" {
			existing.arrName = c.arrName
			existing.arrKind = c.arrKind
		}
		if existing.contentMap == nil {
			existing.contentMap = make(map[string]arr.ContentFile)
		}
		maps.Copy(existing.contentMap, c.contentMap)
	}
}

func (r *Repair) eligibleArrs(filter []string) []*arr.Arr {
	all := r.manager.arr.GetAll()
	wanted := make(map[string]struct{}, len(filter))
	for _, name := range filter {
		if name = strings.TrimSpace(name); name != "" {
			wanted[name] = struct{}{}
		}
	}
	out := make([]*arr.Arr, 0, len(all))
	for _, a := range all {
		if a == nil || a.Host == "" || a.Token == "" || a.SkipRepair {
			continue
		}
		if len(wanted) > 0 {
			if _, ok := wanted[a.Name]; !ok {
				continue
			}
		}
		out = append(out, a)
	}
	return out
}

func (r *Repair) filterDueCandidates(in map[string]*candidate, ignoreLastChecked bool) (map[string]*candidate, int) {
	if ignoreLastChecked {
		return in, 0
	}
	recheck := r.recheckInterval()
	now := time.Now()
	out := make(map[string]*candidate, len(in))
	skipped := 0
	for name, c := range in {
		h, _ := r.manager.storage.GetEntryHealth(name)
		if h != nil && !h.IsDue(now, recheck) {
			skipped++
			continue
		}
		out[name] = c
	}
	return out, skipped
}

// orderCandidatesByLastChecked returns the names of `due` sorted by
// EntryHealth.LastCheckedAt ascending - entries never checked (zero time)
// sort first, then least-recently-checked, etc. Ties (e.g. multiple
// never-checked entries) break on name for a stable, deterministic order
// across runs.
//
// This ordering is what lets a StopSchedule-truncated repair sweep make guaranteed
// forward progress across days: probing an entry updates its LastCheckedAt
// immediately, so it sorts to the back of tomorrow's queue.
func (r *Repair) orderCandidatesByLastChecked(due map[string]*candidate) []string {
	type ordered struct {
		name          string
		lastCheckedAt time.Time
	}
	items := make([]ordered, 0, len(due))
	for name := range due {
		var lastCheckedAt time.Time
		if h, _ := r.manager.storage.GetEntryHealth(name); h != nil {
			lastCheckedAt = h.LastCheckedAt
		}
		items = append(items, ordered{name: name, lastCheckedAt: lastCheckedAt})
	}
	sort.Slice(items, func(i, j int) bool {
		if !items[i].lastCheckedAt.Equal(items[j].lastCheckedAt) {
			return items[i].lastCheckedAt.Before(items[j].lastCheckedAt)
		}
		return items[i].name < items[j].name
	})
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.name
	}
	return out
}

// === Manual rechecks (webhooks + API) ===

func (r *Repair) collectBrokenHealths(names []string, requireArrFile bool) (*xsync.Map[string, *storage.EntryHealth], int) {
	wanted := make(map[string]struct{}, len(names))
	for _, n := range names {
		if n = strings.TrimSpace(n); n != "" {
			wanted[n] = struct{}{}
		}
	}

	healths := xsync.NewMap[string, *storage.EntryHealth]()
	_ = r.manager.storage.ForEachEntryHealth(func(h *storage.EntryHealth) error {
		if h == nil || h.Status != storage.HealthBroken {
			return nil
		}
		if len(wanted) > 0 {
			if _, ok := wanted[h.EntryName]; !ok {
				return nil
			}
		}
		if requireArrFile {
			if len(h.BrokenFiles) == 0 {
				return nil
			}
			if !slices.ContainsFunc(h.BrokenFiles, hasArrFile) {
				return nil
			}
		}
		healths.Store(h.EntryName, h)
		return nil
	})
	return healths, len(wanted)
}

func (r *Repair) markBrokenHealthCleared(h *storage.EntryHealth, at time.Time) {
	if h == nil {
		return
	}
	if _, err := r.manager.storage.GetEntryItem(h.EntryName); err != nil {
		_ = r.manager.storage.DeleteEntryHealth(h.EntryName)
		return
	}
	h.Status = storage.HealthUnknown
	h.BrokenFiles = nil
	h.FailureReason = ""
	h.LastRepairAt = at
	h.Dirty = false
	h.DirtyReason = ""
	h.NextCheckDueAt = time.Time{}
	r.saveHealth(h)
}

func isAlreadyClearedFileError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not found") ||
		strings.Contains(msg, "file does not exist") ||
		strings.Contains(msg, "file is deleted")
}

// FixBroken triggers the Arr delete + re-search pass on currently-broken
// entries without reprobing. When names is empty, every entry with
// Status=broken in storage is fixed. Returns the new RepairRun record
// immediately; the actual fix runs in the background.
//
// Use this from the UI when a previous sweep already identified broken
// entries and the user wants to act on them without paying for another
// probe pass.
func (r *Repair) FixBroken(ctx context.Context, names []string) (*storage.RepairRun, error) {
	if ctx == nil {
		ctx = r.parentCtx
	}

	// Every broken entry is a candidate, including ones recorded without Arr
	// identifiers: the background pass looks those up before acting.
	healths, wantedCount := r.collectBrokenHealths(names, false)
	if healths.Size() == 0 {
		return nil, errors.New("no broken entries to fix")
	}

	r.mu.Lock()
	if r.activeRunID != "" {
		id := r.activeRunID
		r.mu.Unlock()
		return nil, fmt.Errorf("repair already running (run %s)", id)
	}
	runCtx, cancel := context.WithCancel(ctx)
	source := "fix-broken:all"
	if wantedCount > 0 {
		source = fmt.Sprintf("fix-broken:%d", wantedCount)
	}
	run := &storage.RepairRun{
		ID:        uuid.NewString(),
		Trigger:   storage.RepairTriggerManual,
		Status:    storage.RepairRunRunning,
		Stage:     storage.RepairStageRepairing,
		StartedAt: time.Now(),
		Source:    source,
	}
	run.Stats.Candidates = healths.Size()
	r.activeRunID = run.ID
	r.cancelRun = cancel
	r.mu.Unlock()

	if err := r.manager.storage.SaveRepairRun(run); err != nil {
		r.mu.Lock()
		r.activeRunID = ""
		r.cancelRun = nil
		r.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("failed to persist repair run: %w", err)
	}

	r.runWG.Go(func() {
		defer func() {
			r.mu.Lock()
			if r.activeRunID == run.ID {
				r.activeRunID = ""
				r.cancelRun = nil
			}
			r.mu.Unlock()
			cancel()
		}()
		r.resolveBrokenArrContext(runCtx, healths)
		// Drop or trim any candidate the Arrs no longer reference before
		// acting on it - "Fix" must never blocklist or re-search on behalf
		// of a file the Arr already replaced with a working copy.
		before := healths.Size()
		r.filterSupersededHealths(runCtx, healths)
		run.Stats.Cleared += before - healths.Size()
		unowned := r.unownedBrokenHealths(healths)
		r.repairBroken(runCtx, run, healths, false)
		if runCtx.Err() != nil {
			r.finalizeRun(run, storage.RepairRunCancelled, "", "context cancelled during repair")
			return
		}
		r.finalizeRun(run, storage.RepairRunCompleted, unownedSummary(unowned), "")
		r.logger.Info().
			Str("run_id", run.ID).
			Int("candidates", run.Stats.Candidates).
			Int("repaired", run.Stats.Repaired).
			Int("repair_failed", run.Stats.RepairFailed).
			Msg("FixBroken: completed")
	})
	return run, nil
}

// hasArrFile reports whether bf carries what healBrokenEntry needs to delete
// and re-search it through its Arr.
func hasArrFile(bf storage.BrokenFile) bool {
	return bf.ArrName != "" && bf.ArrFileID != 0
}

// resolveBrokenArrContext looks up the Arr identifiers a manual Fix needs for
// broken files recorded without them. Several paths mark a file broken
// without resolving its Arr (a 430 found by a verification read, a
// managed-source sweep, a tripped regrab guard), and healBrokenEntry skips a
// file with no Arr file id, so Fix used to do nothing for such an entry.
// Each eligible Arr's library is listed once, and only when a candidate needs
// it.
func (r *Repair) resolveBrokenArrContext(ctx context.Context, healths *xsync.Map[string, *storage.EntryHealth]) {
	missingArrFile := func(bf storage.BrokenFile) bool { return !hasArrFile(bf) }
	pending := make(map[string]*storage.EntryHealth)
	healths.Range(func(name string, h *storage.EntryHealth) bool {
		if slices.ContainsFunc(h.BrokenFiles, missingArrFile) {
			pending[name] = h
		}
		return true
	})
	for _, a := range r.eligibleArrs(nil) {
		if len(pending) == 0 || ctx.Err() != nil {
			return
		}
		cands, err := r.collectArrMediaCandidates(ctx, a, "")
		if err != nil {
			r.logger.Warn().Err(err).Str("arr", a.Name).Msg("Fix: could not list the Arr's media to resolve broken files")
			continue
		}
		for name, h := range pending {
			if n := fillBrokenArrContext(h, cands[name]); n > 0 {
				r.logger.Info().Str("entry", name).Str("arr", a.Name).Int("files", n).
					Msg("Fix: found the Arr for broken files recorded without one")
			}
			if !slices.ContainsFunc(h.BrokenFiles, missingArrFile) {
				delete(pending, name)
			}
		}
	}
}

// fillBrokenArrContext copies c's Arr identifiers onto each broken file in h
// that has none and returns how many it filled. A file is filled only when
// the entry still serves the same upload the broken verdict was made on (equal
// InfoHash): a same-named replacement must not be deleted on the old copy's
// verdict, and filterSupersededHealths clears that case instead.
func fillBrokenArrContext(h *storage.EntryHealth, c *candidate) int {
	if h == nil || c == nil || c.arrName == "" || c.item == nil {
		return 0
	}
	filled := 0
	for i := range h.BrokenFiles {
		bf := &h.BrokenFiles[i]
		if hasArrFile(*bf) {
			continue
		}
		cf, ok := c.contentMap[bf.FileName]
		if !ok || cf.FileId == 0 {
			continue
		}
		current, ok := c.item.Files[bf.FileName]
		if !ok || current == nil || bf.InfoHash == "" || current.InfoHash != bf.InfoHash {
			continue
		}
		bf.ArrName = c.arrName
		bf.ArrKind = c.arrKind
		bf.MediaID = cf.Id
		bf.EpisodeID = cf.EpisodeId
		bf.ArrFileID = cf.FileId
		bf.TargetPath = cf.TargetPath
		bf.SourcePath = cf.Path
		if bf.Size == 0 {
			bf.Size = cf.Size
		}
		filled++
	}
	return filled
}

// unownedBrokenHealths returns, sorted, the entries in healths that Fix cannot
// act on because none of their broken files has an Arr file, logging each.
func (r *Repair) unownedBrokenHealths(healths *xsync.Map[string, *storage.EntryHealth]) []string {
	var out []string
	healths.Range(func(name string, h *storage.EntryHealth) bool {
		if !slices.ContainsFunc(h.BrokenFiles, hasArrFile) {
			r.logger.Warn().Str("entry", name).Str("reason", h.FailureReason).
				Msg("Fix: no Arr owns this entry's broken files; nothing to delete or re-search")
			out = append(out, name)
		}
		return true
	})
	sort.Strings(out)
	return out
}

// unownedSummary is the run error recorded for entries Fix could not act on.
func unownedSummary(names []string) string {
	switch len(names) {
	case 0:
		return ""
	case 1:
		return names[0] + ": no Arr owns its broken files, nothing to re-search"
	default:
		return fmt.Sprintf("%d entries have no Arr owning their broken files, nothing to re-search (first: %s)", len(names), names[0])
	}
}

// ClearBroken removes currently-broken files from the local mount state. It
// deliberately does not call Arrs, mark history failed, or trigger re-search.
func (r *Repair) ClearBroken(ctx context.Context, names []string) (*storage.RepairRun, error) {
	if ctx == nil {
		ctx = r.parentCtx
	}

	healths, wantedCount := r.collectBrokenHealths(names, false)
	if healths.Size() == 0 {
		return nil, errors.New("no broken files to clear")
	}

	r.mu.Lock()
	if r.activeRunID != "" {
		id := r.activeRunID
		r.mu.Unlock()
		return nil, fmt.Errorf("repair already running (run %s)", id)
	}
	runCtx, cancel := context.WithCancel(ctx)
	source := "clear-broken:all"
	if wantedCount > 0 {
		source = fmt.Sprintf("clear-broken:%d", wantedCount)
	}
	run := &storage.RepairRun{
		ID:        uuid.NewString(),
		Trigger:   storage.RepairTriggerManual,
		Status:    storage.RepairRunRunning,
		Stage:     storage.RepairStageRepairing,
		StartedAt: time.Now(),
		Source:    source,
	}
	run.Stats.Candidates = healths.Size()
	r.activeRunID = run.ID
	r.cancelRun = cancel
	r.mu.Unlock()

	if err := r.manager.storage.SaveRepairRun(run); err != nil {
		r.mu.Lock()
		r.activeRunID = ""
		r.cancelRun = nil
		r.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("failed to persist repair run: %w", err)
	}

	r.runWG.Go(func() {
		defer func() {
			r.mu.Lock()
			if r.activeRunID == run.ID {
				r.activeRunID = ""
				r.cancelRun = nil
			}
			r.mu.Unlock()
			cancel()
		}()
		r.clearBroken(runCtx, run, healths)
		if runCtx.Err() != nil {
			r.finalizeRun(run, storage.RepairRunCancelled, "", "context cancelled during clear")
			return
		}
		r.finalizeRun(run, storage.RepairRunCompleted, "", "")
		r.logger.Info().
			Str("run_id", run.ID).
			Int("candidates", run.Stats.Candidates).
			Int("cleared", run.Stats.Cleared).
			Int("clear_failed", run.Stats.RepairFailed).
			Msg("ClearBroken: completed")
	})
	return run, nil
}

func (r *Repair) clearBroken(ctx context.Context, run *storage.RepairRun, healths *xsync.Map[string, *storage.EntryHealth]) {
	now := time.Now()
	healths.Range(func(name string, h *storage.EntryHealth) bool {
		if ctx != nil && ctx.Err() != nil {
			return false
		}
		if h == nil {
			return true
		}
		if len(h.BrokenFiles) == 0 {
			r.markBrokenHealthCleared(h, now)
			run.Stats.Cleared++
			r.saveRun(run)
			return true
		}

		remaining := make([]storage.BrokenFile, 0, len(h.BrokenFiles))
		for _, bf := range h.BrokenFiles {
			if err := r.manager.RemoveTorrentFile(bf.EntryName, bf.FileName); err != nil {
				if isAlreadyClearedFileError(err) {
					run.Stats.Cleared++
					r.saveRun(run)
					continue
				}
				r.logger.Warn().Err(err).Str("entry", bf.EntryName).Str("file", bf.FileName).Msg("ClearBroken: failed to remove broken file from mount")
				run.Stats.RepairFailed++
				remaining = append(remaining, bf)
				continue
			}
			run.Stats.Cleared++
			r.saveRun(run)
		}

		h.LastRepairAt = now
		h.BrokenFiles = remaining
		if len(remaining) == 0 {
			r.markBrokenHealthCleared(h, now)
			return true
		}

		h.Status = storage.HealthBroken
		h.FailureReason = topReason(remaining)
		r.saveHealth(h)
		return true
	})
}

// HandlePlaybackFailure is the coordination entry point for a live playback
// read that just hit a hard, permanent article-not-found (BODY 430) -
// definitive proof the segment's body is truly gone, not merely slow. It
// replaces calling RepairPlaybackFileNow directly: rather than always
// re-grabbing (this feature's original, pre-coordination behavior), it
// consults the auto-repair policy (decideAutoRepairAction) over this file's
// current overlay verdict and whether PAR2 repair is enabled, then the
// handler registry, so a live playback failure never races a concurrent
// sweep pass or the PAR2 worker into double-handling the same entry:
//
//   - PAR2 disabled AND verdict failed (the only quadrant that re-grabs):
//     claim the entry's handler slot and call RepairPlaybackFileNow exactly
//     as before this policy existed. If something else already claimed the
//     slot, defer to it - do nothing.
//   - PAR2 enabled (verdict degraded or failed): queue a PAR2 pass on the
//     urgent lane (EnqueueUrgent) instead of re-grabbing - the user is
//     watching right now, so this jumps ahead of anything the background
//     sweep already queued. A PAR2-terminal outcome for this entry marks it
//     unrepairable for the overlay GUI's manual "Delete & re-search"; it
//     never falls back to an automatic re-grab (see Par2Repair.runJob).
//   - Anything else (clean verdict, or the entry/verdict can't be resolved -
//     e.g. no overlay tracking for this release) falls back to
//     RepairPlaybackFileNow directly, exactly as before this policy existed.
//     Padding itself already suppresses within-cap 430s from ever reaching
//     countErrors/escalatePlaybackFailure in the first place (see
//     pkg/usenet/fs/reader's handleConfirmedMissing) - this function only
//     ever sees a failure once padding has already declined to cover it.
//
// Returns acted=true only when this call actually did something (a
// delete+blocklist+re-search ran, or a PAR2 pass was queued) - false for
// every no-op branch (registry busy, cooldown active, regrab guard tripped,
// or nothing to do because padding already covers the damage), with reason
// explaining which. The caller (escalatePlaybackFailure) uses this to log
// "done" only when real work happened, instead of on every nil-error return
// regardless of whether anything ran.
func (r *Repair) HandlePlaybackFailure(ctx context.Context, entryName, fileName string) (acted bool, reason string, err error) {
	entry, err := r.manager.GetEntryByName(entryName, fileName)
	if err != nil || entry == nil || entry.InfoHash == "" || r.manager.usenet == nil {
		// Can't resolve enough to consult the policy (non-NZB entry, no
		// overlay/usenet tracking, or the file isn't where we expect it) -
		// fall back to the pre-coordination default. Still the automatic
		// path (see repairPlaybackFileNow's auto flag) - the regrab guard
		// still applies if Arr context can be resolved further in.
		return r.repairPlaybackFileNow(ctx, entryName, fileName, true, false)
	}
	nzbID := entry.InfoHash

	var par2Usable bool
	if r.manager.par2Repair != nil {
		par2Usable, _ = r.manager.par2Repair.par2Usable(nzbID)
	}
	verdict := r.manager.usenet.OverlayVerdict(nzbID, fileName)

	switch decideAutoRepairAction(RepairSourcePlayback, par2Usable, verdict) {
	case autoActionRegrab:
		if !r.handlers.TryAcquire(nzbID, handlerRegrab) {
			r.logger.Debug().Str("entry", entryName).Str("file", fileName).
				Msg("playback repair: entry already being handled; skipping re-grab")
			return false, "entry already being handled by another repair mechanism", nil
		}
		defer r.handlers.Release(nzbID)
		return r.repairPlaybackFileNow(ctx, entryName, fileName, true, false)
	case autoActionQueuePar2:
		if r.manager.par2Repair != nil {
			// proximity=0: a live playback read just hit this damage right
			// now, the same "playhead is already here" urgency as a
			// precache-triggered job's closest possible proximity (see
			// Precache.recordReadiness).
			r.manager.par2Repair.EnqueueUrgent(nzbID, 0)
		}
		return true, "", nil
	default:
		return false, fmt.Sprintf("no action needed (verdict=%s)", verdict), nil
	}
}

// ClaimManualAutoRepairOverride force-claims nzbID's auto-repair
// handler-registry slot for a manual, user-initiated re-grab, clearing any
// terminal mark left by an automatic PAR2 failure in the process. Call this
// before a manual "Delete & re-search" style action (see the overlay GUI's
// handleOverlayResearch) so it always proceeds and re-evaluates the entry
// from scratch, exactly like Par2Repair.RunNow's own manual override, rather
// than being silently blocked by whatever the automatic path last claimed.
func (r *Repair) ClaimManualAutoRepairOverride(nzbID string) {
	if r == nil || r.handlers == nil || nzbID == "" {
		return
	}
	r.handlers.Set(nzbID, handlerRegrab)
}

// ReleaseManualAutoRepairOverride releases the claim
// ClaimManualAutoRepairOverride took, once the manual action it guarded has
// finished (success or failure) - so it doesn't sit blocking a later
// automatic decision for the registry's full stale-claim TTL.
func (r *Repair) ReleaseManualAutoRepairOverride(nzbID string) {
	if r == nil || r.handlers == nil || nzbID == "" {
		return
	}
	r.handlers.Release(nzbID)
}

// RepairPlaybackFileNow repairs a file that just failed playback WITHOUT
// re-probing it, as a manual, user-initiated action (the overlay GUI's
// "Delete & re-search" - see handleOverlayResearch) - always proceeds and
// clears any regrab-guard terminal mark for this file, exactly like
// ClaimManualAutoRepairOverride bypasses the handler registry for the same
// kind of manual override. Automatic callers (playback-failure escalation)
// go through HandlePlaybackFailure, which calls the gated, auto=true form
// of repairPlaybackFileNow instead.
func (r *Repair) RepairPlaybackFileNow(ctx context.Context, entryName, fileName string) error {
	_, _, err := r.repairPlaybackFileNow(ctx, entryName, fileName, false, false)
	return err
}

// RepairPlaybackFileNowForBulkResearch is RepairPlaybackFileNow for the
// overlay GUI's "Delete & re-search selected" bulk action. The user has
// explicitly chosen to discard this copy across a whole selection, so it
// skips the cache-only warm PAR2 pass (a multi-minute per-file stall for a
// repair they don't want) and bypasses the per-entry playbackRepairCooldown
// (a selection spanning several files of one season-pack entry must action
// every one, not collapse to the first under the 2-minute anti-churn timer).
func (r *Repair) RepairPlaybackFileNowForBulkResearch(ctx context.Context, entryName, fileName string) error {
	_, _, err := r.repairPlaybackFileNow(ctx, entryName, fileName, false, true)
	return err
}

// repairPlaybackFileNow repairs a file that just failed playback WITHOUT
// re-probing it. The triggering read already hit a hard article-not-found
// (BODY 430) — that is definitive proof the body is missing, so a confirming
// BODY re-probe is redundant and, worse, unreliable: a re-probe sample may not
// hit the exact dead segments the sequential read did, rolling the file up as
// "healthy" and suppressing the repair. We trust the read: this resolves the
// file's Arr mapping (no probe) and goes straight to delete + blocklist +
// re-search for the single played entry.
//
// auto distinguishes an automatic caller (HandlePlaybackFailure) from a
// manual one (RepairPlaybackFileNow): only an automatic caller is subject to
// r.regrabGuard, which stops the delete+blocklist+re-search loop once every
// candidate release tried so far shares the same missing articles (see
// regrab_guard.go) - a manual retry always overrides and resets the guard's
// count for this file instead of being blocked by it.
//
// bulkOverride flags the overlay GUI's "Delete & re-search selected" bulk
// action, where the user has already chosen to discard this copy. It skips
// the cache-only warm PAR2 pass (healBrokenEntry) and bypasses the per-entry
// playbackRepairCooldown - a bulk selection spanning several files of one
// season-pack entry must action every one, not collapse to the first under
// the anti-churn timer. Only ever set via RepairPlaybackFileNowForBulkResearch.
//
// acted/reason mirror HandlePlaybackFailure's return values - see its doc
// comment.
func (r *Repair) repairPlaybackFileNow(ctx context.Context, entryName, fileName string, auto, bulkOverride bool) (acted bool, reason string, err error) {
	if entryName == "" {
		return false, "", errors.New("entry name is empty")
	}

	// Manager-level per-entry cooldown. This must be checked here (not only in
	// the per-file Downloaders) because a repair recreates the CacheItem and
	// its Downloaders, resetting that object's own cooldown — so a still-dead
	// re-grab would otherwise re-escalate immediately. Claim the slot before
	// doing any work so concurrent callers for the same entry collapse to one.
	//
	// The key is normalized (see normalizeCooldownKey) so it matches across the
	// casing/punctuation variants the same episode's releases use, which lets
	// the import-failure path (ClearPlaybackRepairCooldown) release it the
	// instant a re-grabbed replacement is rejected as body-dead — so the next
	// playback can immediately try the next candidate instead of waiting out
	// the timer.
	cooldownKey := normalizeCooldownKey(entryName)
	r.playbackRepairMu.Lock()
	if r.lastPlaybackRepair == nil {
		r.lastPlaybackRepair = make(map[string]time.Time)
	}
	if last, ok := r.lastPlaybackRepair[cooldownKey]; ok && !bulkOverride && time.Since(last) < playbackRepairCooldown {
		r.playbackRepairMu.Unlock()
		remaining := (playbackRepairCooldown - time.Since(last)).Round(time.Second)
		r.logger.Debug().
			Str("entry", entryName).
			Dur("since_last", time.Since(last)).
			Msg("playback repair: skipped, entry within cooldown")
		return false, fmt.Sprintf("within cooldown, %s remaining", remaining), nil
	}
	r.lastPlaybackRepair[cooldownKey] = time.Now()
	r.playbackRepairMu.Unlock()

	item, err := r.manager.GetEntryItem(entryName)
	if err != nil || item == nil {
		return false, "", fmt.Errorf("entry %q not found", entryName)
	}

	// Detach from the caller's (short-lived) context: the escalation cancels
	// its context as soon as this returns, and the Arr delete/search calls must
	// outlive that.
	runCtx := r.parentCtx
	if runCtx == nil {
		runCtx = context.Background()
	}

	// Resolve which Arr owns this entry and the per-file Arr identifiers needed
	// to delete + re-search. This is the same lookup the probe uses, minus any
	// verification.
	c := &candidate{name: entryName, item: item}
	r.attachArrContext(runCtx, c)
	if len(c.contentMap) == 0 || c.arrName == "" {
		return false, "", fmt.Errorf("no Arr owns entry %q; cannot re-acquire", entryName)
	}

	// Build the broken-file set. Scope to the single file that failed when we
	// can match it; otherwise fall back to every Arr-known file in the entry
	// (a single-file movie entry, or a filename we couldn't line up).
	h := &storage.EntryHealth{EntryName: entryName, Status: storage.HealthBroken}
	matched := false
	var firstMediaID, firstEpisodeID int
	for name, cf := range c.contentMap {
		if fileName != "" && name != fileName && filepath.Base(name) != filepath.Base(fileName) {
			continue
		}
		if !matched {
			firstMediaID, firstEpisodeID = cf.Id, cf.EpisodeId
		}
		matched = true
		bf := storage.BrokenFile{
			EntryName:  entryName,
			FileName:   name,
			Protocol:   config.ProtocolNZB,
			Reason:     "playback body missing (430)",
			ArrName:    c.arrName,
			ArrKind:    c.arrKind,
			MediaID:    cf.Id,
			EpisodeID:  cf.EpisodeId,
			ArrFileID:  cf.FileId,
			TargetPath: cf.TargetPath,
			SourcePath: cf.Path,
			Size:       cf.Size,
		}
		// InfoHash pins this BrokenFile to the entry that was actually
		// broken - without it, fileSuperseded (supersession.go) can
		// never tell a healthy replacement apart from the same
		// still-dead entry (an empty InfoHash only ever proves
		// "unreferenced", never "referenced but different"), so an
		// entry superseded via this path sits in the broken list
		// forever even after a working re-grab replaced it. Same field,
		// same source (item.Files[name].InfoHash), as the sweep-probe
		// path's brokenFiles().
		if file, ok := item.Files[name]; ok && file != nil {
			bf.InfoHash = file.InfoHash
		}
		h.BrokenFiles = append(h.BrokenFiles, bf)
	}
	if !matched {
		return false, "", fmt.Errorf("file %q not found among Arr-known files for entry %q", fileName, entryName)
	}
	h.BrokenCount = len(h.BrokenFiles)

	// Stable identity (independent of nzbID, which changes every re-grab
	// cycle - see regrab_guard.go) for a broken file's underlying Arr media
	// record.
	identity := regrabIdentityKey(c.arrName, firstMediaID, firstEpisodeID, entryName)
	if !auto {
		// Manual override: always proceed, and reset the guard so a fresh
		// automatic streak starts counting from zero after this.
		r.regrabGuard.clear(identity)
	} else if allowed, guardReason, firstTrip := r.regrabGuard.checkAndRecord(identity, entryName); !allowed {
		logEvt := r.logger.Debug()
		if firstTrip {
			logEvt = r.logger.Warn()
		}
		logEvt.Str("entry", entryName).Str("file", fileName).Str("reason", guardReason).
			Msg("playback repair: stopping automatic re-grab, every candidate so far shares the same missing articles")
		if firstTrip {
			r.markRegrabGuardTripped(entryName, fileName, h, guardReason)
		}
		return false, guardReason, nil
	}
	// FileCount is the entry's total tracked file count (matching probeEntry's
	// h.FileCount = len(names)) - finalizeEntryRepair's shouldDelete check
	// (BrokenCount == FileCount) is how it tells "this repair covered the
	// entry's ENTIRE content, safe to delete the old entry now" apart from
	// "only one episode of a season pack failed, the rest are untouched and
	// must survive". Leaving this unset (zero) made shouldDelete permanently
	// false here, so a 430 playback repair's blocklist+re-search never
	// deleted the superseded entry - and, by extension via DeleteEntry, never
	// reaped its overlay record either - even for the common single-file
	// case where deletion is exactly correct.
	h.FileCount = len(c.contentMap)

	r.logger.Info().
		Str("entry", entryName).
		Str("file", fileName).
		Int("files_to_repair", h.BrokenCount).
		Msg("Repair: playback failure — deleting + re-searching without re-probe")

	pseudo := &storage.RepairRun{ID: "playback-" + entryName, Stats: storage.RepairRunStats{}}
	var statsMu sync.Mutex
	r.healBrokenEntry(runCtx, pseudo, &statsMu, entryName, h, bulkOverride)
	return true, "", nil
}

// markRegrabGuardTripped surfaces a regrab-guard trip for manual action,
// reusing the existing broken-file health surface (EntryHealth.
// FailureReason, already rendered by the repair health list GUI) rather
// than the delete+blocklist+re-search action the guard just refused to
// take. h is the EntryHealth repairPlaybackFileNow already built for this
// failure (broken-file set intact) - only its status/reason are repointed
// at the guard's terminal verdict before persisting.
func (r *Repair) markRegrabGuardTripped(entryName, fileName string, h *storage.EntryHealth, reason string) {
	h.Status = storage.HealthBroken
	h.FailureReason = reason
	h.LastFailedAt = time.Now()
	h.LastCheckedAt = time.Now()
	r.saveHealth(h)
	r.logger.Warn().
		Str("entry", entryName).
		Str("file", fileName).
		Str("reason", reason).
		Msg("playback repair: marked terminal-unrepairable, manual action required")
}

// RegrabImportGrab blocklists a grab that failed the import-time
// availability gate (see Downloader.importAvailabilityGate) so the Arr's own
// "redownload failed" handling (on by default) picks a fresh candidate. The
// entry has deliberately NOT been added to storage yet at this point - the
// import is being rejected, not repaired - so this can't resolve the grab
// via manager.GetEntryItem/GetMedia the way RepairPlaybackFileNow and the
// sweep do for already-imported files. Instead it looks up the Arr's own
// grab history directly by download ID (entry.InfoHash - the same ID the Arr
// received back when it originally sent this release to decypharr), which
// exists from the moment of the grab regardless of import status.
//
// Claims the handler registry around the call so a concurrent sweep/PAR2
// pass discovering the same nzbID (unlikely pre-import, but the registry
// dedup is cheap insurance) can't double-handle it.
//
// Subject to r.regrabGuard, same as repairPlaybackFileNow's automatic path:
// this is the loop an Arr can drive entirely on its own (grab a replacement,
// have it rejected here, auto-re-search, repeat), so it needs the same
// two-strikes-and-stop protection or a genuinely dead release just cycles
// forever, one blocklisted candidate at a time. entry.Category is set to the
// owning Arr's own Name at grab time (see manager/usenet.go), so a.Name here
// resolves to the identical string repairPlaybackFileNow's content-matched
// arrName does for the same entry - strikes from both paths land on the same
// identity.
func (r *Repair) RegrabImportGrab(ctx context.Context, entry *storage.Entry, fileName, reason string) error {
	if entry == nil || entry.InfoHash == "" {
		return errors.New("entry is required")
	}
	nzbID := entry.InfoHash
	if r.handlers != nil {
		if !r.handlers.TryAcquire(nzbID, handlerRegrab) {
			r.logger.Debug().Str("entry", entry.Name).Str("file", fileName).
				Msg("Import: entry already being handled; skipping re-grab")
			return nil
		}
		defer r.handlers.Release(nzbID)
	}

	a := r.manager.arr.GetOrCreate(entry.Category)
	if a == nil || a.Host == "" || a.Token == "" {
		return fmt.Errorf("no arr configured for category %q", entry.Category)
	}

	history := a.GetHistory(entry.InfoHash, "1") // eventType 1 = grabbed
	if history == nil || len(history.Records) == 0 {
		return fmt.Errorf("no grab history found for %q in arr %q", entry.Name, a.Name)
	}
	record := history.Records[0]

	var mediaID, episodeID int
	switch a.Type {
	case arr.Sonarr:
		mediaID, episodeID = record.SeriesID, record.EpisodeID
	case arr.Radarr:
		mediaID = record.MovieID
	}
	identity := regrabIdentityKey(a.Name, mediaID, episodeID, entry.Name)
	if allowed, guardReason, firstTrip := r.regrabGuard.checkAndRecord(identity, entry.Name); !allowed {
		logEvt := r.logger.Debug()
		if firstTrip {
			logEvt = r.logger.Warn()
		}
		logEvt.Str("entry", entry.Name).Str("file", fileName).Str("reason", guardReason).
			Msg("Import: stopping automatic re-grab, every candidate so far shares the same missing articles")
		if firstTrip {
			h := &storage.EntryHealth{
				EntryName: entry.Name,
				Status:    storage.HealthBroken,
				BrokenFiles: []storage.BrokenFile{{
					EntryName: entry.Name,
					FileName:  fileName,
					Protocol:  config.ProtocolNZB,
					Reason:    reason,
					ArrName:   a.Name,
					ArrKind:   arrKindFromType(a.Type),
					MediaID:   mediaID,
					EpisodeID: episodeID,
					InfoHash:  entry.InfoHash,
				}},
			}
			r.markRegrabGuardTripped(entry.Name, fileName, h, guardReason)
		}
		// Not an error: the import-availability gate rejects a
		// confirmed-dead file unconditionally regardless of this return
		// value (see importAvailabilityGate) - refusing to blocklist here
		// just stops decypharr from actively feeding the Arr another hunt
		// for this identity, it doesn't change whether this import is
		// accepted.
		return nil
	}

	if err := a.MarkHistoryFailed(record.ID); err != nil {
		return fmt.Errorf("failed to blocklist grab: %w", err)
	}
	r.logger.Info().
		Str("entry", entry.Name).
		Str("file", fileName).
		Str("arr", a.Name).
		Str("reason", reason).
		Msg("Import: blocklisted broken grab; arr will re-search")
	return nil
}

// normalizeCooldownKey reduces an entry/release name to lowercase alphanumerics
// so the same episode's differently-formatted releases collapse to one key
// (e.g. "Grant.S05E07.The.Wisdom...REAL.REPACK...1-NTb" and
// "grant.s05e07.the.wisdom...real.repack...ntb" map identically). This lets the
// playback-repair cooldown set in RepairPlaybackFileNow be matched and released
// by the import-failure path even though the re-grabbed release name differs in
// casing/punctuation from the originally-broken entry name.
func normalizeCooldownKey(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for _, r := range strings.ToLower(name) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// ClearPlaybackRepairCooldown releases the playback-repair cooldown for an entry
// so the next playback failure can immediately trigger the next repair attempt.
// It is called when a re-grabbed replacement is rejected (e.g. body-dead at
// import): that release is already blocklisted, so there is no reason to make
// the user wait out the cooldown before the next candidate is tried. Matching is
// by normalized key, so it works despite the re-grab's release name differing
// from the original broken entry's name. Safe to call with names that were never
// in cooldown (no-op).
func (r *Repair) ClearPlaybackRepairCooldown(name string) {
	if name == "" {
		return
	}
	key := normalizeCooldownKey(name)
	r.playbackRepairMu.Lock()
	defer r.playbackRepairMu.Unlock()
	if _, ok := r.lastPlaybackRepair[key]; ok {
		delete(r.lastPlaybackRepair, key)
		r.logger.Debug().Str("entry", name).Msg("playback repair: cooldown cleared after failed re-grab")
	}
}

// RecheckEntry kicks off a recheck for a single entry and returns
// immediately with an in-progress EntryHealth ack. The actual probe and
// optional fix run in the background. With fix=true, broken Arr-known files
// trigger delete + re-search after probing.
func (r *Repair) RecheckEntry(ctx context.Context, entryName string, fix bool) (*storage.EntryHealth, error) {
	if entryName == "" {
		return nil, errors.New("entry name is empty")
	}
	h, _ := r.manager.storage.GetEntryHealth(entryName)
	if h != nil && h.ActiveRunID != "" {
		return nil, fmt.Errorf("entry is being probed by run %s", h.ActiveRunID)
	}

	item, err := r.manager.GetEntryItem(entryName)
	if err != nil || item == nil {
		return nil, fmt.Errorf("entry %q not found", entryName)
	}

	// One ID per recheck: with the entry name alone, two rechecks of the same
	// entry could not be told apart by a caller polling for the result.
	runID := fmt.Sprintf("recheck-%s-%d", entryName, time.Now().UnixNano())
	c := &candidate{name: entryName, item: item}
	done := make(chan struct{})
	r.rechecks.Store(runID, done)

	if ctx == nil {
		ctx = r.parentCtx
	}
	r.runWG.Go(func() {
		defer r.finishRecheck(entryName, runID, done)
		runCtx := r.attachFFProbeChecker(ctx, r.logger)

		// Build the Arr reference set once for this recheck and reuse it both
		// for the whole-entry supersession check below and for probeEntry's
		// own per-file filter (the partial case: some files superseded, the
		// entry still worth probing for what's left). A nil refs (build
		// failed, or zero eligible Arrs) means "couldn't determine anything" -
		// probeEntry then probes every file exactly as before this existed.
		refs, refsErr := r.buildArrReferencedSet(ctx)
		if refsErr != nil {
			r.logger.Debug().Err(refsErr).Str("entry", entryName).Msg("Recheck: failed to build Arr reference set; probing without supersession filtering")
			refs = nil
		}

		// Whole-entry supersession check: a broken entry whose files no Arr
		// references anymore was already replaced elsewhere - probing it just
		// re-confirms "broken" for a release the library stopped using weeks
		// ago. A file an Arr individually re-grabbed out of an otherwise
		// still-active entry (season-pack case) is dropped from the broken
		// list and excluded from this probe so the probe pass doesn't
		// immediately re-add it.
		if existing, _ := r.manager.storage.GetEntryHealth(entryName); refs != nil && existing != nil &&
			existing.Status == storage.HealthBroken && len(existing.BrokenFiles) > 0 {
			if res := classifySupersession(existing, refs); len(res.superseded) > 0 {
				exclude := make(map[string]struct{}, len(res.superseded))
				for _, bf := range res.superseded {
					exclude[bf.FileName] = struct{}{}
				}
				cleared, aerr := r.applySupersession(existing, res, r.cfg().CleanupSuperseded)
				if aerr != nil {
					r.logger.Warn().Err(aerr).Str("entry", entryName).Msg("Recheck: failed to apply supersession")
				} else if cleared {
					// Every broken file (or the whole entry) was superseded -
					// don't probe the dead release's own articles at all.
					return
				} else {
					c.item = excludeFilesFromItem(c.item, exclude)
				}
			}
		}

		sc := &supersessionContext{refs: refs}
		if fix {
			r.attachArrContext(runCtx, c)
		}
		heal := newHealCache()
		final, _ := r.probeEntry(runCtx, runID, c, heal, RepairRunOptions{Recheck: true}, fix, sc)
		// probeEntry's routeAutoRepair may have claimed the handler-registry
		// regrab slot for a segment-missing file regardless of fix - release
		// it on every exit from here on (including the !fix early return just
		// below) so a read-only recheck doesn't strand the claim for the rest
		// of the registry's TTL, blocking a later re-grab or PAR2 enqueue for
		// the same file. releaseRegrabClaims is a no-op when nothing was
		// claimed. Mirrors probeAndHealCandidates/executeRecheckMedia's use of
		// finalizeBrokenEntry, but calls the release primitive directly since
		// finalizeBrokenEntry's heal branch (healBrokenEntryGuarded) is not
		// what RecheckEntry's fix=true path uses.
		defer r.releaseRegrabClaims(final)
		if !fix || final.Status != storage.HealthBroken {
			return
		}
		pseudo := &storage.RepairRun{ID: runID, Stats: storage.RepairRunStats{}}
		var statsMu sync.Mutex
		r.healBrokenEntry(ctx, pseudo, &statsMu, entryName, final, false)
	})

	// Return an in-memory ack reflecting the freshly-started recheck. The
	// real EntryHealth in storage is updated by probeEntry shortly after -
	// or, if the entry turns out to be fully superseded, cleared instead.
	if h == nil {
		h = &storage.EntryHealth{EntryName: entryName}
	}
	h.Status = storage.HealthRepairing
	h.ActiveRunID = runID
	return h, nil
}

// recheckDoneRetention is how long WaitRecheck can still see a recheck that
// has ended.
const recheckDoneRetention = 10 * time.Minute

// finishRecheck runs when a RecheckEntry run ends, however it ends (probed,
// fixed, or cleared as superseded without a probe): it stamps the entry's
// health record with the run, when one still exists, and wakes WaitRecheck.
// Before this a recheck that found the entry superseded returned without
// touching last_checked_at, so a caller polling for it never saw an end.
func (r *Repair) finishRecheck(entryName, runID string, done chan struct{}) {
	if h, err := r.manager.storage.GetEntryHealth(entryName); err == nil && h != nil {
		h.LastRecheckRunID = runID
		h.LastRecheckFinishedAt = time.Now()
		if h.ActiveRunID == runID {
			h.ActiveRunID = ""
		}
		r.saveHealth(h)
	}
	close(done)
	time.AfterFunc(recheckDoneRetention, func() { r.rechecks.Delete(runID) })
}

// WaitRecheck blocks until the recheck runID ends, timeout passes or ctx is
// done, and reports whether the recheck ended. An unknown run ID (never
// started here, or ended more than recheckDoneRetention ago) reports false.
func (r *Repair) WaitRecheck(ctx context.Context, runID string, timeout time.Duration) bool {
	v, ok := r.rechecks.Load(runID)
	if !ok {
		return false
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-v.(chan struct{}):
		return true
	case <-t.C:
	case <-ctx.Done():
	}
	return false
}

// RecheckMedia kicks off a recheck for every entry that an Arr's media-id
// resolves to and returns immediately with the in-progress RepairRun. The
// actual probing + repair runs in the background so HTTP callers don't have
// to block. With arrName="" the first eligible Arr that resolves entries
// wins. fix runs the same delete + re-search pass a sweep would. forceDecode
// re-runs the expensive frame-decode verification even on entries whose decode
// fingerprint still matches (see RepairRunOptions.ForceDecodeVerification).
// Honors the singleton run lock.
func (r *Repair) RecheckMedia(ctx context.Context, arrName, mediaID string, fix, forceDecode bool) (*storage.RepairRun, error) {
	mediaID = strings.TrimSpace(mediaID)
	if mediaID == "" {
		return nil, errors.New("media_id is required")
	}
	if ctx == nil {
		ctx = r.parentCtx
	}

	// Validate arr selection synchronously so callers fail-fast on bad input.
	arrs, err := r.resolveArrsForMedia(arrName)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	if r.activeRunID != "" {
		id := r.activeRunID
		r.mu.Unlock()
		return nil, fmt.Errorf("repair already running (run %s)", id)
	}
	runCtx, cancel := context.WithCancel(ctx)
	run := &storage.RepairRun{
		ID:        uuid.NewString(),
		Trigger:   storage.RepairTriggerManual,
		Status:    storage.RepairRunRunning,
		Stage:     storage.RepairStageSelecting,
		StartedAt: time.Now(),
		Source:    fmt.Sprintf("media:%s/%s", arrName, mediaID),
	}
	r.activeRunID = run.ID
	r.cancelRun = cancel
	r.mu.Unlock()

	if err := r.manager.storage.SaveRepairRun(run); err != nil {
		r.mu.Lock()
		r.activeRunID = ""
		r.cancelRun = nil
		r.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("failed to persist repair run: %w", err)
	}

	r.runWG.Go(func() {
		defer func() {
			r.mu.Lock()
			if r.activeRunID == run.ID {
				r.activeRunID = ""
				r.cancelRun = nil
			}
			r.mu.Unlock()
			cancel()
		}()
		r.executeRecheckMedia(runCtx, run, arrs, arrName, mediaID, fix, forceDecode)
	})
	return run, nil
}

// executeRecheckMedia is the body of a media recheck. Mirrors executeSweep
// but scoped to a specific media-id resolved through one or more Arrs.
func (r *Repair) executeRecheckMedia(ctx context.Context, run *storage.RepairRun, arrs []*arr.Arr, arrName, mediaID string, fix, forceDecode bool) {
	ctx = r.attachFFProbeChecker(ctx, r.logger)
	candidates := make(map[string]*candidate)
	var lastErr error
	for _, a := range arrs {
		if ctx.Err() != nil {
			break
		}
		sub, err := r.collectArrMediaCandidates(ctx, a, mediaID)
		if err != nil {
			lastErr = err
			r.logger.Trace().Err(err).Str("arr", a.Name).Str("media_id", mediaID).Msg("RecheckMedia: GetMedia failed")
			continue
		}
		mergeCandidates(candidates, sub)
		// When the caller didn't pin a specific Arr, the first Arr to resolve
		// non-empty entries wins. Avoids double-probing when sonarr+radarr
		// share a folder root.
		if arrName == "" && len(sub) > 0 {
			break
		}
	}

	if len(candidates) == 0 {
		msg := fmt.Sprintf("media id %q resolved no entries", mediaID)
		if lastErr != nil {
			msg += " (last error: " + lastErr.Error() + ")"
		}
		r.finalizeRun(run, storage.RepairRunCompleted, msg, "")
		return
	}

	run.Stats.Candidates = len(candidates)
	run.Stage = storage.RepairStageProbing
	r.saveRun(run)

	// Same reference-set build/reuse pattern as executeSweep: built once,
	// shared by every candidate's per-file supersession filter in
	// probeEntry. A nil refs means "couldn't determine anything" - probing
	// proceeds unfiltered.
	refs, refsErr := r.buildArrReferencedSet(ctx)
	if refsErr != nil {
		r.logger.Debug().Err(refsErr).Str("media_id", mediaID).Msg("RecheckMedia: failed to build Arr reference set; probing without supersession filtering")
		refs = nil
	}
	sc := &supersessionContext{refs: refs}

	heal := newHealCache()
	mediaNames := make([]string, 0, len(candidates))
	for name := range candidates {
		mediaNames = append(mediaNames, name)
	}
	err := r.probeAndHealCandidates(ctx, run, candidates, mediaNames, heal, RepairRunOptions{Recheck: true, ForceDecodeVerification: forceDecode}, fix, sc)
	candidates = nil
	if err != nil {
		if errors.Is(err, context.Canceled) {
			r.finalizeRun(run, storage.RepairRunCancelled, "", "context cancelled during probing")
			return
		}
		r.finalizeRun(run, storage.RepairRunFailed, err.Error(), "")
		return
	}
	if ctx.Err() != nil {
		r.finalizeRun(run, storage.RepairRunCancelled, "", "context cancelled during repair")
		return
	}

	r.finalizeRun(run, storage.RepairRunCompleted, "", "")
	r.logger.Info().
		Str("run_id", run.ID).
		Str("arr", arrName).
		Str("media_id", mediaID).
		Int("candidates", run.Stats.Candidates).
		Int("broken", run.Stats.Broken).
		Int("repaired", run.Stats.Repaired).
		Int("decode_skipped", run.Stats.DecodeSkipped).
		Int("unverified", run.Stats.Unverified).
		Bool("fix", fix).
		Bool("force_decode", forceDecode).
		Int64("skipped_superseded_files", sc.skipped.Load()).
		Msg("RecheckMedia: completed")
}

func (r *Repair) resolveArrsForMedia(arrName string) ([]*arr.Arr, error) {
	if arrName != "" {
		a := r.manager.arr.Get(arrName)
		if a == nil {
			return nil, fmt.Errorf("arr %q not found", arrName)
		}
		if a.Host == "" || a.Token == "" {
			return nil, fmt.Errorf("arr %q is not configured", arrName)
		}
		if a.SkipRepair {
			return nil, fmt.Errorf("arr %q has skip_repair set", arrName)
		}
		return []*arr.Arr{a}, nil
	}
	all := r.eligibleArrs(nil)
	if len(all) == 0 {
		return nil, errors.New("no eligible arrs configured")
	}
	return all, nil
}

// attachArrContext walks Arrs looking for the entry's symlink targets so a
// single-entry fix can reach back into the Arr that owns the file.
func (r *Repair) attachArrContext(ctx context.Context, c *candidate) {
	for _, a := range r.eligibleArrs(nil) {
		if ctx.Err() != nil {
			return
		}
		media, err := a.GetMedia(ctx, "")
		if err != nil {
			continue
		}
		kind := arrKindFromType(a.Type)
		for _, content := range media {
			for entryPath, files := range collectArrFiles(content) {
				if filepath.Clean(filepath.Base(entryPath)) != c.name {
					continue
				}
				if c.contentMap == nil {
					c.contentMap = make(map[string]arr.ContentFile)
				}
				c.arrName = a.Name
				c.arrKind = kind
				for _, f := range files {
					f.EntryName = c.name
					f.IsSymlink = true
					c.contentMap[f.TargetPath] = f
				}
			}
		}
	}
}

// === helpers ===

func orderedFilenames(item *storage.EntryItem) []string {
	if item == nil {
		return nil
	}
	out := make([]string, 0, len(item.Files))
	for name, f := range item.Files {
		if f == nil || f.Deleted {
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func topReason(files []storage.BrokenFile) string {
	if len(files) == 0 {
		return ""
	}
	counts := make(map[string]int)
	for _, f := range files {
		if f.Reason != "" {
			counts[f.Reason]++
		}
	}
	best, bestN := "", 0
	for reason, n := range counts {
		if n > bestN {
			best = reason
			bestN = n
		}
	}
	if best != "" {
		return best
	}
	return files[0].Reason
}

// collectArrFiles groups Arr content files by their resolved symlink-target
// parent directory. The parent is the on-disk entry-folder name.
func collectArrFiles(media arr.Content) map[string][]arr.ContentFile {
	out := make(map[string][]arr.ContentFile)
	for _, f := range media.Files {
		target := readSymlinkTarget(f.Path)
		if target == "" {
			continue
		}
		f.IsSymlink = true
		dir, name := filepath.Split(target)
		f.TargetPath = name
		entryPath := filepath.Clean(dir)
		out[entryPath] = append(out[entryPath], f)
	}
	return out
}

func readSymlinkTarget(path string) string {
	path = filepath.Clean(path)
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		return ""
	}
	target, err := os.Readlink(path)
	if err != nil {
		return ""
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(filepath.Dir(path), target)
	}
	return target
}

func arrKindFromType(t arr.Type) storage.ArrKind {
	switch t {
	case arr.Sonarr:
		return storage.ArrKindSonarr
	case arr.Radarr:
		return storage.ArrKindRadarr
	case arr.Lidarr:
		return storage.ArrKindLidarr
	case arr.Readarr:
		return storage.ArrKindReadarr
	default:
		return storage.ArrKindOther
	}
}
