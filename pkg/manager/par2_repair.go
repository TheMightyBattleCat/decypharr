// The PAR2 repair worker reconstructs confirmed-dead Usenet articles (see
// pkg/usenet/overlay) using PAR2 recovery data instead of falling straight to
// a delete + re-search. It is a manager-level background service: one job at
// a time (the pass reads a meaningful fraction of a release over NNTP, so
// running several concurrently would just contend with itself), deduped by
// nzbID, and gated by the repair sweep's Schedule/StopSchedule window (see
// Repair.repairWindowOpen) - the same bandwidth-heavy-work-hours reasoning
// StopSchedule already applies to sweeps.
//
// Escalation ordering end to end: a padded segment enqueues here; on success
// the segment is patched and padding for it stops on the next read. On any
// failure - no PAR2 data, too much damage, a fetch or verification failure -
// the job hands the entry to the existing playback-repair path
// (Repair.RepairPlaybackFileNow), exactly the delete + re-search a live 430
// would have triggered if padding didn't exist. With config.Repair.Par2Repair
// disabled this worker never does anything but fall through to that same
// legacy path; with PlaybackPadding also disabled, EnqueueRepair is never
// even called (see pkg/usenet/fs/reader), so the whole feature is inert.
package manager

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/notifications"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

const (
	// par2QueueDepth bounds how many distinct NZBs can be waiting for a PAR2
	// pass at once. Generous: entries are deduped, so this is a ceiling on
	// distinct broken entries queued at the same moment, not on throughput.
	par2QueueDepth = 256

	// par2RetryInterval is how often a job deferred by a closed repair
	// window (see Repair.repairWindowOpen) is reconsidered.
	par2RetryInterval = time.Minute

	// par2JobTimeout bounds one NZB's whole PAR2 pass (index fetch through
	// verification), so a stalled provider can't wedge the worker forever.
	par2JobTimeout = 20 * time.Minute

	// par2ArticleFetchTimeout bounds a single article fetch within a job.
	par2ArticleFetchTimeout = 60 * time.Second

	// md5_16kSize is the PAR2-defined sample size for tie-breaking a
	// posted-file/FileDesc length match.
	md5_16kSize = 16384
)

// articleFetchFunc downloads and yEnc-decodes a single NNTP article. Narrowed
// from *usenet.Usenet to just this one method so the byte-range-fetching
// helpers below (postedFileFetcher, fetchWholePar2File, computeMD5_16k) can
// be exercised with a fake in tests, without needing a real, provider-backed
// Usenet client.
type articleFetchFunc func(ctx context.Context, messageID string) ([]byte, error)

// par2VolPattern matches both real-world PAR2 recovery-volume naming
// conventions (case-insensitive): par2cmdline's "<base>.volSTART+COUNT.par2"
// (e.g. "release.vol3+2.par2" holds 2 recovery slices starting at exponent
// 3) and MultiPar/par2j's "<base>.volSTART-END.par2" using a hyphen instead
// of a plus, with an inclusive end index rather than a count (e.g.
// "release.vol3-4.par2" holds 2 slices, exponents 3 and 4) - found live
// against a real MultiPar-created Usenet release during validation, where
// treating "-" as a plus-equivalent undercounted by one per file and, worse,
// a naive "+"-only regex didn't match these filenames at all, computing zero
// available recovery volumes for a release that was fully PAR2-protected.
// This lets the job compute how many recovery slices are available, and
// which files are smallest, from filenames alone, with no need to fetch
// anything first; see censusPar2Volumes for the count math per separator.
var par2VolPattern = regexp.MustCompile(`(?i)\.vol(\d+)([+-])(\d+)\.par2$`)

// Par2Repair is the manager-level PAR2 repair worker.
type Par2Repair struct {
	manager *Manager
	repair  *Repair
	logger  zerolog.Logger

	mu     sync.Mutex
	queued map[string]struct{}
	queue  chan string
	// deferred holds nzbIDs dequeued from queue but not yet run because the
	// repair window was closed (see readyToRun) - loop's local pending slice
	// mirrors this set so IsQueued can see them too.
	deferred map[string]struct{}

	// active holds the nzbID of the in-flight job, nil when idle. Read by
	// IsRunning for the overlay management API's repair-status introspection.
	active atomic.Pointer[string]

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewPar2Repair builds the worker. Call Start to begin processing.
func NewPar2Repair(m *Manager, repair *Repair) *Par2Repair {
	return &Par2Repair{
		manager:  m,
		repair:   repair,
		logger:   logger.New("par2-repair"),
		queued:   make(map[string]struct{}),
		deferred: make(map[string]struct{}),
		queue:    make(chan string, par2QueueDepth),
	}
}

// Start begins the worker's single processing goroutine.
func (p *Par2Repair) Start(ctx context.Context) {
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.wg.Add(1)
	go p.loop()
}

// Stop cancels any in-flight job and waits for the worker goroutine to exit.
func (p *Par2Repair) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
}

// IsQueued reports whether nzbID currently has a PAR2 pass waiting to run
// (queued or deferred by a closed repair window).
func (p *Par2Repair) IsQueued(nzbID string) bool {
	if p == nil || nzbID == "" {
		return false
	}
	p.mu.Lock()
	_, q := p.queued[nzbID]
	_, d := p.deferred[nzbID]
	p.mu.Unlock()
	return q || d
}

// IsRunning reports whether nzbID's PAR2 pass is the one currently executing.
func (p *Par2Repair) IsRunning(nzbID string) bool {
	if p == nil || nzbID == "" {
		return false
	}
	if id := p.active.Load(); id != nil {
		return *id == nzbID
	}
	return false
}

// AutoEnqueue is the automatic-trigger entry point - called from the reader's
// padding path (via overlay.Store's repair-enqueue callback) and the repair
// sweep, never from an explicit GUI action (see Enqueue for that). Gated by
// config.Repair.Par2RepairMode:
//   - Par2RepairModeManual: never queues, so the caller's damage stays
//     exactly as PAR2-repair-disabled behavior left it (padded, or handed to
//     the legacy re-grab path by the caller itself).
//   - Par2RepairModeAutoThreshold: only queues once deadSegments reaches
//     Par2RepairMinSegments; smaller damage is left padded rather than
//     spending provider bandwidth on a PAR2 pass.
//   - Par2RepairModeAutoAll (default): always queues, same as this feature's
//     original (non-configurable) behavior.
func (p *Par2Repair) AutoEnqueue(nzbID string, deadSegments int) {
	if p == nil || nzbID == "" {
		return
	}
	cfg := config.Get().Repair
	switch cfg.Par2RepairMode {
	case config.Par2RepairModeManual:
		return
	case config.Par2RepairModeAutoThreshold:
		if deadSegments < cfg.Par2RepairMinSegments {
			return
		}
	}
	p.Enqueue(nzbID)
}

// Enqueue schedules nzbID for a PAR2 repair pass. Deduped: a burst of padded
// segments across one playback session collapses to a single pass. Safe to
// call from any goroutine. Unlike AutoEnqueue, this is never gated by
// Par2RepairMode - it is the explicit-trigger primitive used by both
// AutoEnqueue and the GUI's manual "repair now" action.
func (p *Par2Repair) Enqueue(nzbID string) {
	if p == nil || nzbID == "" {
		return
	}
	if !p.par2ShouldAutoEnqueue(nzbID) {
		return
	}
	p.mu.Lock()
	if _, dup := p.queued[nzbID]; dup {
		p.mu.Unlock()
		return
	}
	p.queued[nzbID] = struct{}{}
	p.mu.Unlock()

	select {
	case p.queue <- nzbID:
	default:
		p.logger.Warn().Str("entry", nzbID).Msg("par2 repair queue full; dropping request")
		p.mu.Lock()
		delete(p.queued, nzbID)
		p.mu.Unlock()
	}
}

// RunNow immediately starts a PAR2 repair pass for nzbID from an explicit,
// user-initiated GUI action. Unlike Enqueue (which lands in loop's normal
// queue), it does NOT wait for the repair sweep's StopSchedule window - a
// schedule that limits automatic, bandwidth-heavy background work has no
// bearing on something the user explicitly asked to run right now. It still
// goes through the same NNTP connection pool and bandwidth accounting as
// every other fetch - there is no unthrottled fast path. Returns an error if
// PAR2 repair is disabled outright, or if a pass for this nzbID is already
// queued/deferred/running.
func (p *Par2Repair) RunNow(nzbID string) error {
	if p == nil || nzbID == "" {
		return fmt.Errorf("nzbID is required")
	}
	if !config.Get().Repair.Par2RepairEnabled() {
		return fmt.Errorf("par2 repair is disabled")
	}
	if p.IsRunning(nzbID) {
		return fmt.Errorf("par2 repair already running for this entry")
	}

	p.mu.Lock()
	_, dupQueued := p.queued[nzbID]
	_, dupDeferred := p.deferred[nzbID]
	if dupQueued || dupDeferred {
		p.mu.Unlock()
		return fmt.Errorf("par2 repair already queued for this entry")
	}
	p.queued[nzbID] = struct{}{}
	p.mu.Unlock()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() {
			p.mu.Lock()
			delete(p.queued, nzbID)
			p.mu.Unlock()
		}()
		p.runJob(nzbID)
	}()
	return nil
}

// Availability reports whether nzbID's pending overlay damage looks
// repairable via PAR2 WITHOUT fetching any article data: PAR2 metadata
// (posted-file layout and retained recovery/index files) is present, and the
// filename-derived recovery-slice count covers a conservative upper bound on
// the damaged slices (one per pending dead segment - see
// estimateNeededSlices). A true result is not a guarantee - the exact
// damaged-slice count is only known once the index is parsed - but a false
// result means a repair pass would certainly fail, so it's safe to use for a
// GUI's "repairable" hint without running one.
func (p *Par2Repair) Availability(nzbID string) (repairable bool, reason string) {
	if p == nil || p.manager.usenet == nil {
		return false, "usenet client not configured"
	}
	if !config.Get().Repair.Par2RepairEnabled() {
		return false, "par2 repair disabled"
	}
	nzb, err := p.manager.usenet.GetNZB(nzbID)
	if err != nil {
		return false, "nzb record not found"
	}
	if len(nzb.Par2Source) == 0 {
		return false, "no posted-file layout retained for this release"
	}
	if len(nzb.Par2Files) == 0 {
		return false, "no par2 metadata retained for this release"
	}

	pending, err := p.manager.usenet.OverlayPendingRepair(nzbID)
	if err != nil {
		return false, "failed to read overlay state"
	}
	needed := estimateNeededSlices(pending)
	if needed == 0 {
		return false, "no damage pending repair"
	}
	if needed > par2.MaxRepairSlices {
		return false, fmt.Sprintf("%d damaged segments exceeds the %d-slice repair cap", needed, par2.MaxRepairSlices)
	}

	vols, _ := censusPar2Volumes(nzb.Par2Files)
	var available uint32
	for _, v := range vols {
		available += v.count
	}
	if available < needed {
		return false, fmt.Sprintf("only %d recovery slices retained, need at least %d", available, needed)
	}
	return true, ""
}

// postedLayoutMatches reports whether posted and nzbFile are the same
// physical posting: identical segment count, in order, with identical
// message IDs. True only for a file posted directly (not extracted from an
// archive) - see Verify.
func postedLayoutMatches(posted storage.PostedFileRef, nzbFile *storage.NZBFile) bool {
	if nzbFile == nil || len(posted.Segments) != len(nzbFile.Segments) {
		return false
	}
	for i := range posted.Segments {
		if posted.Segments[i].MessageID != nzbFile.Segments[i].MessageID {
			return false
		}
	}
	return true
}

// Verify re-checks file's bytes against the PAR2 FileDesc's whole-file MD5
// for its matched posted file, proving a past repair was byte-exact. It
// reassembles the posted file from overlay patches (for segments PAR2
// already repaired) and fresh NNTP fetches (for everything else), then
// hashes the result - a real, end-to-end check, not a re-read of what
// par2.Repair already verified at write time.
//
// Only supported for a file posted directly (not extracted from an
// archive): PAR2 protects the POSTED file, and overlay patches are stored
// as final, POST-extraction bytes (see overlay.Store.WritePatch) - for an
// extracted archive member those are not the same bytes as the posted
// article, so there is no way to reassemble a verifiable posted-file byte
// stream from them. ok=false with a non-empty reason (nil error) covers
// that and every other "can't verify" case that isn't itself a fetch/parse
// failure.
func (p *Par2Repair) Verify(ctx context.Context, nzbID, file string) (pass bool, reason string, err error) {
	if p == nil || p.manager.usenet == nil {
		return false, "", fmt.Errorf("usenet client not configured")
	}
	u := p.manager.usenet

	nzb, err := u.GetNZB(nzbID)
	if err != nil {
		return false, "", fmt.Errorf("load NZB record: %w", err)
	}
	nzbFile := nzb.GetFileByName(file)
	if nzbFile == nil {
		return false, "", fmt.Errorf("file %q not found in entry", file)
	}
	if len(nzb.Par2Files) == 0 {
		return false, "no par2 metadata retained for this release", nil
	}

	var posted *storage.PostedFileRef
	for i := range nzb.Par2Source {
		if postedLayoutMatches(nzb.Par2Source[i], nzbFile) {
			posted = &nzb.Par2Source[i]
			break
		}
	}
	if posted == nil {
		return false, "file is extracted from an archive - par2 protects the posted archive volume, not this member, so its bytes can't be verified from patches", nil
	}

	_, indexFiles := censusPar2Volumes(nzb.Par2Files)
	if len(indexFiles) == 0 {
		return false, "no par2 index file retained for this release", nil
	}
	var sources []par2.Source
	for _, f := range indexFiles {
		data, ferr := fetchWholePar2File(ctx, u.FetchArticle, f)
		if ferr != nil {
			p.logger.Debug().Err(ferr).Str("entry", nzbID).Str("file", f.Name).Msg("par2 verify: index file fetch failed")
			continue
		}
		sources = append(sources, par2.Source{Name: f.Name, Data: data})
	}
	if len(sources) == 0 {
		return false, "", fmt.Errorf("failed to fetch any par2 index file")
	}
	idx, err := par2.ParseIndex(sources)
	if err != nil {
		return false, "", fmt.Errorf("parse par2 index: %w", err)
	}

	postedRef := *posted
	postedFiles := []par2.PostedFile{{
		Name:   postedRef.Name,
		Length: postedRef.Size,
		MD5_16k: func() ([16]byte, error) {
			return computeMD5_16k(ctx, u.FetchArticle, postedRef)
		},
	}}
	matches, err := par2.MatchFiles(idx, postedFiles)
	if err != nil {
		return false, "", fmt.Errorf("match posted file against par2 index: %w", err)
	}
	if len(matches) == 0 {
		return false, "posted file did not match any par2 FileDesc", nil
	}
	fd, ok := idx.Files[matches[0].FileID]
	if !ok {
		return false, "", fmt.Errorf("no FileDesc for matched posted file")
	}

	h := md5.New()
	for i, seg := range nzbFile.Segments {
		if data, ok := u.OverlayPatchBytes(nzbID, file, i); ok {
			h.Write(data)
			continue
		}
		fetchCtx, cancel := context.WithTimeout(ctx, par2ArticleFetchTimeout)
		data, ferr := u.FetchArticle(fetchCtx, seg.MessageID)
		cancel()
		if ferr != nil {
			return false, "", fmt.Errorf("fetch segment %d: %w", i, ferr)
		}
		h.Write(data)
	}

	var sum [16]byte
	copy(sum[:], h.Sum(nil))
	if sum != fd.FileMD5 {
		return false, "whole-file MD5 mismatch", nil
	}
	return true, "", nil
}

func (p *Par2Repair) loop() {
	defer p.wg.Done()

	var pending []string
	ticker := time.NewTicker(par2RetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case nzbID := <-p.queue:
			p.mu.Lock()
			delete(p.queued, nzbID)
			p.mu.Unlock()
			if p.readyToRun() {
				p.runJob(nzbID)
			} else {
				p.mu.Lock()
				p.deferred[nzbID] = struct{}{}
				p.mu.Unlock()
				pending = append(pending, nzbID)
			}
		case <-ticker.C:
			if len(pending) == 0 || !p.readyToRun() {
				continue
			}
			todo := pending
			pending = nil
			p.mu.Lock()
			for _, id := range todo {
				delete(p.deferred, id)
			}
			p.mu.Unlock()
			for _, id := range todo {
				if p.ctx.Err() != nil {
					return
				}
				p.runJob(id)
			}
		}
	}
}

func (p *Par2Repair) readyToRun() bool {
	return config.Get().Repair.Par2RepairEnabled() && p.repair.repairWindowOpen()
}

// runJob runs one NZB's PAR2 repair pass end to end. Every exit path either
// leaves the overlay state as-is (nothing to do, or a genuine "can't tell
// yet" error worth retrying later) or falls through to the legacy repair
// path - it never partially patches and calls it done.
func (p *Par2Repair) runJob(nzbID string) {
	start := time.Now()
	if p.manager.usenet == nil {
		return
	}

	id := nzbID
	p.active.Store(&id)
	defer p.active.Store(nil)

	entry, entryErr := p.manager.GetEntry(nzbID)
	if entryErr != nil || entry == nil {
		// A ghost overlay record - the backing entry is already gone
		// (deleted, superseded, re-grabbed under a different nzbID). There
		// is nothing to repair on behalf of and nothing to fall back to
		// legacy repair for either; mark it terminal so the automatic path
		// stops re-enqueuing it (Commit A/B's cleanup hooks should reap the
		// overlay record itself shortly, but a race is not a reason to keep
		// hammering it in the meantime).
		terminalErr := fmt.Errorf("entry no longer exists")
		p.logger.Info().Str("entry", nzbID).Msg("par2 repair: entry no longer exists; marking unrepairable")
		p.recordAttempt(&storage.Par2RepairAttempt{
			ID:         uuid.NewString(),
			EntryName:  nzbID,
			NzbID:      nzbID,
			StartedAt:  start,
			Duration:   time.Since(start),
			Outcome:    storage.Par2RepairOutcomeUnavailable,
			FailReason: terminalErr.Error(),
		})
		p.recordPar2Outcome(nzbID, terminalErr)
		return
	}
	entryName := entry.Name

	ctx, cancel := context.WithTimeout(p.ctx, par2JobTimeout)
	defer cancel()

	pending, err := p.manager.usenet.OverlayPendingRepair(nzbID)
	if err != nil || len(pending) == 0 {
		return
	}
	deadSegments := 0
	for _, segs := range pending {
		deadSegments += len(segs)
	}
	p.logger.Info().Str("entry", entryName).Int("dead_segments", deadSegments).Msg("par2 repair queued")

	var readBytes int64
	var slicesRepaired int
	if err := p.runRepair(ctx, nzbID, entryName, pending, &readBytes, &slicesRepaired); err != nil {
		canary := errors.Is(err, par2.ErrChecksumMismatch)
		class := classifyPar2Failure(err)
		p.logger.Info().Err(err).Str("entry", entryName).Bool("crc_canary", canary).Bool("terminal", class.terminal).Msg("par2 repair unavailable")
		p.recordAttempt(&storage.Par2RepairAttempt{
			ID:         uuid.NewString(),
			EntryName:  entryName,
			NzbID:      nzbID,
			StartedAt:  start,
			Duration:   time.Since(start),
			Outcome:    storage.Par2RepairOutcomeFailed,
			ReadBytes:  readBytes,
			FailReason: err.Error(),
			CRCCanary:  canary,
		})
		p.recordPar2Outcome(nzbID, err)
		p.notifyFailed(entryName, err, canary)
		// Only a terminal failure - one backoff can never fix - falls
		// through to the legacy delete+blocklist+re-search path. A
		// transient failure (a slow provider, a context deadline) instead
		// waits out its backoff and tries PAR2 again; the file is still
		// playable (padded) in the meantime, so there is no urgency to
		// escalate to a full re-grab over what may just be a blip.
		if class.terminal {
			p.fallbackToLegacy(entryName, pending)
		}
		return
	}

	p.logger.Info().
		Str("entry", entryName).
		Int("segments_patched", deadSegments).
		Dur("duration", time.Since(start)).
		Msg("par2 repair completed")
	p.recordAttempt(&storage.Par2RepairAttempt{
		ID:              uuid.NewString(),
		EntryName:       entryName,
		NzbID:           nzbID,
		StartedAt:       start,
		Duration:        time.Since(start),
		Outcome:         storage.Par2RepairOutcomeCompleted,
		ReadBytes:       readBytes,
		SlicesRepaired:  slicesRepaired,
		SegmentsPatched: deadSegments,
	})
	p.recordPar2Outcome(nzbID, nil)
	p.notifyCompleted(entryName, deadSegments, time.Since(start))
}

// recordAttempt persists a to the compact PAR2 repair-attempt history the
// overlay management API exposes (see storage.Par2RepairAttempt). Best
// effort: a storage failure here must never affect the repair pass itself,
// so it's only logged.
func (p *Par2Repair) recordAttempt(a *storage.Par2RepairAttempt) {
	if err := p.manager.storage.SavePar2RepairAttempt(a); err != nil {
		p.logger.Warn().Err(err).Str("entry", a.EntryName).Msg("failed to persist par2 repair attempt")
	}
}

// notifyCompleted and notifyFailed fire the PAR2-specific notification
// events (see config.EventPar2RepairComplete/EventPar2RepairFailed) - a
// no-op if notifications aren't configured/enabled for these events.
func (p *Par2Repair) notifyCompleted(entryName string, segmentsPatched int, dur time.Duration) {
	if p.manager.Notifications == nil {
		return
	}
	p.manager.Notifications.Notify(notifications.Event{
		Type:    config.EventPar2RepairComplete,
		Status:  "success",
		Message: fmt.Sprintf("PAR2 repair completed for %q: %d segment(s) patched in %s", entryName, segmentsPatched, dur.Round(time.Second)),
	})
}

// notifyFailed reports a failed PAR2 pass, flagging whether it was the CRC
// canary (par2.ErrChecksumMismatch) - the important failure mode, since it
// means reconstructed or trusted-intact data provably didn't match its
// recorded checksum, not merely that recovery data was unavailable.
func (p *Par2Repair) notifyFailed(entryName string, err error, crcCanary bool) {
	if p.manager.Notifications == nil {
		return
	}
	msg := fmt.Sprintf("PAR2 repair failed for %q: %v", entryName, err)
	if crcCanary {
		msg += " (CRC canary: reconstructed or trusted-intact data failed checksum verification)"
	}
	p.manager.Notifications.Notify(notifications.Event{
		Type:    config.EventPar2RepairFailed,
		Status:  "error",
		Message: msg,
		Error:   err,
	})
}

// runRepair does the actual work; every error return means "PAR2 couldn't
// handle this," triggering the legacy fallback in the caller. It never
// returns a nil error after only partially patching pending's segments.
func (p *Par2Repair) runRepair(ctx context.Context, nzbID, entryName string, pending map[string][]overlay.DeadSegment, readBytes *int64, slicesRepaired *int) error {
	u := p.manager.usenet

	// fetch wraps every article fetch this pass makes so runJob can report a
	// real (not estimated) read_bytes total in the persisted attempt log -
	// used in place of u.FetchArticle everywhere below.
	fetch := func(fctx context.Context, messageID string) ([]byte, error) {
		data, err := u.FetchArticle(fctx, messageID)
		if err == nil && readBytes != nil {
			atomic.AddInt64(readBytes, int64(len(data)))
		}
		return data, err
	}

	nzb, err := u.GetNZB(nzbID)
	if err != nil {
		return fmt.Errorf("load NZB record: %w", err)
	}
	if len(nzb.Par2Files) == 0 || len(nzb.Par2Source) == 0 {
		if err := u.BackfillPar2Refs(ctx, nzbID); err != nil {
			return fmt.Errorf("no PAR2 data and backfill failed: %w", err)
		}
		nzb, err = u.GetNZB(nzbID)
		if err != nil {
			return fmt.Errorf("reload NZB record after backfill: %w", err)
		}
		if len(nzb.Par2Files) == 0 || len(nzb.Par2Source) == 0 {
			return fmt.Errorf("no PAR2 data available")
		}
	}

	vols, indexFiles := censusPar2Volumes(nzb.Par2Files)
	var available uint32
	for _, v := range vols {
		available += v.count
	}

	// A rough, name-only upper bound on how many damaged slices we could
	// possibly need (actual count depends on slice size, known only once the
	// index is parsed) - catch the hopeless case before fetching anything.
	if available == 0 {
		return fmt.Errorf("no PAR2 recovery volumes retained")
	}

	// Fetch every index (non-volume) file, plus enough of the smallest
	// recovery volumes to plausibly cover every dead segment - "fully
	// reading the small vols is acceptable v1" per the design; we don't
	// attempt to skip individual articles within a volume file.
	var sources []par2.Source
	for _, f := range indexFiles {
		data, err := fetchWholePar2File(ctx, fetch, f)
		if err != nil {
			p.logger.Debug().Err(err).Str("entry", entryName).Str("file", f.Name).Msg("par2: index file fetch failed; relying on volume-embedded metadata")
			continue
		}
		sources = append(sources, par2.Source{Name: f.Name, Data: data})
	}

	needed := estimateNeededSlices(pending)
	var fetchedSlices uint32
	for _, v := range vols {
		if fetchedSlices >= needed {
			break
		}
		data, err := fetchWholePar2File(ctx, fetch, v.ref)
		if err != nil {
			p.logger.Debug().Err(err).Str("entry", entryName).Str("file", v.ref.Name).Msg("par2: recovery volume fetch failed")
			continue
		}
		sources = append(sources, par2.Source{Name: v.ref.Name, Data: data})
		fetchedSlices += v.count
	}
	if len(sources) == 0 {
		return fmt.Errorf("failed to fetch any PAR2 file")
	}

	idx, err := par2.ParseIndex(sources)
	if err != nil {
		return fmt.Errorf("parse PAR2 index: %w", err)
	}

	// Match every posted file in the release (not just the ones with dead
	// segments - intact slices needed for the streaming pass can belong to
	// ANY file in the recovery set) to its PAR2 FileID.
	posted := make([]par2.PostedFile, len(nzb.Par2Source))
	for i, f := range nzb.Par2Source {
		f := f
		posted[i] = par2.PostedFile{
			Name:   f.Name,
			Length: f.Size,
			MD5_16k: func() ([16]byte, error) {
				return computeMD5_16k(ctx, fetch, f)
			},
		}
	}
	matches, err := par2.MatchFiles(idx, posted)
	if err != nil {
		return fmt.Errorf("match posted files: %w", err)
	}
	if len(matches) == 0 {
		return fmt.Errorf("no posted file matched the PAR2 recovery set")
	}

	fetchers := make(map[[16]byte]*postedFileFetcher, len(matches))
	msgIDRange := make(map[string]postedRange)
	for _, m := range matches {
		file := nzb.Par2Source[m.PostedIndex]
		f := newPostedFileFetcher(ctx, fetch, file)
		fetchers[m.FileID] = f
		var off int64
		for _, seg := range file.Segments {
			msgIDRange[seg.MessageID] = postedRange{fileID: m.FileID, start: off, end: off + seg.Bytes}
			off += seg.Bytes
		}
	}

	// Map every dead segment (by message ID - NOT by the logical/extracted
	// filename padding recorded it under, which may be an extracted-archive
	// member with no posted-file identity of its own) to its damaged slice
	// set within the posted file PAR2 actually protects.
	damagedSet := make(map[int64]struct{})
	type deadRef struct {
		file string
		seg  overlay.DeadSegment
		rng  postedRange
	}
	var deadRefs []deadRef
	for file, segs := range pending {
		for _, seg := range segs {
			rng, ok := msgIDRange[seg.MessageID]
			if !ok {
				return fmt.Errorf("dead segment %s (file %q) is not part of any matched posted file", seg.MessageID, file)
			}
			slices, err := idx.DamagedSlices(rng.fileID, rng.start, rng.end)
			if err != nil {
				return fmt.Errorf("map dead segment %s to slices: %w", seg.MessageID, err)
			}
			for _, s := range slices {
				damagedSet[s] = struct{}{}
			}
			deadRefs = append(deadRefs, deadRef{file: file, seg: seg, rng: rng})
		}
	}

	damaged := make([]int64, 0, len(damagedSet))
	for s := range damagedSet {
		damaged = append(damaged, s)
	}
	sort.Slice(damaged, func(i, j int) bool { return damaged[i] < damaged[j] })

	k := len(damaged)
	if k == 0 {
		return fmt.Errorf("no damaged slices resolved (nothing to repair)")
	}
	if k > par2.MaxRepairSlices {
		return fmt.Errorf("%d damaged slices exceeds the %d repair cap", k, par2.MaxRepairSlices)
	}
	if k > len(idx.Recovery) {
		return fmt.Errorf("%d damaged slices but only %d recovery slices fetched/available", k, len(idx.Recovery))
	}

	recovery := make([]par2.RecoverySlice, k)
	for i, ref := range idx.Recovery[:k] {
		src := sources[ref.Source].Data
		if ref.Offset+ref.Length > int64(len(src)) {
			return fmt.Errorf("recovery slice %d out of range in %s", i, sources[ref.Source].Name)
		}
		recovery[i] = par2.RecoverySlice{
			Exponent: ref.Exponent,
			Data:     src[ref.Offset : ref.Offset+ref.Length],
		}
	}

	sliceSource := &jobSliceSource{idx: idx, fetchers: fetchers}
	repaired, err := par2.Repair(idx, damaged, recovery, sliceSource)
	if err != nil {
		return fmt.Errorf("repair: %w", err)
	}
	if slicesRepaired != nil {
		*slicesRepaired = len(repaired)
	}

	repairedByIndex := make(map[int64][]byte, len(repaired))
	for _, rs := range repaired {
		repairedByIndex[rs.Index] = rs.Data
	}

	for _, dr := range deadRefs {
		data, err := extractPostedRange(idx, repairedByIndex, dr.rng.fileID, dr.rng.start, dr.rng.end)
		if err != nil {
			return fmt.Errorf("extract repaired bytes for %s: %w", dr.seg.MessageID, err)
		}
		if err := u.OverlayWritePatch(nzbID, dr.file, dr.seg.Index, data); err != nil {
			return fmt.Errorf("write patch for %s segment %d: %w", dr.file, dr.seg.Index, err)
		}
	}
	return nil
}

// fallbackToLegacy hands every file with pending dead segments to the
// existing playback-repair path (delete + re-search), exactly what a live
// 430 would have triggered without padding - gated by the same config flags
// escalatePlaybackFailure checks, since this is standing in for that exact
// escalation having fired.
func (p *Par2Repair) fallbackToLegacy(entryName string, pending map[string][]overlay.DeadSegment) {
	cfg := config.Get().Repair
	if !cfg.Enabled || !cfg.AutoRepair || !cfg.RepairOnPlaybackFailure {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	for file := range pending {
		if err := p.repair.RepairPlaybackFileNow(ctx, entryName, file); err != nil {
			p.logger.Debug().Err(err).Str("entry", entryName).Str("file", file).Msg("par2 repair failed; legacy fallback also failed")
		}
	}
}

// estimateNeededSlices is a cheap upper bound (one slice per dead segment)
// used only to decide when we've fetched "probably enough" recovery volumes
// before parsing the index - the real, exact k is computed from the parsed
// index afterward.
func estimateNeededSlices(pending map[string][]overlay.DeadSegment) uint32 {
	var n uint32
	for _, segs := range pending {
		n += uint32(len(segs))
	}
	return n
}

type par2Volume struct {
	ref   storage.Par2FileRef
	start uint32
	count uint32
}

// censusPar2Volumes splits files into recovery volumes (name-parsed for
// their exponent range, per par2VolPattern - both the par2cmdline "+count"
// and MultiPar/par2j "-end" naming conventions) and everything else (the
// index file(s)), with volumes sorted smallest-file-first - a recovery
// census from filenames alone, without fetching anything.
func censusPar2Volumes(files []storage.Par2FileRef) (vols []par2Volume, indexFiles []storage.Par2FileRef) {
	for _, f := range files {
		m := par2VolPattern.FindStringSubmatch(f.Name)
		if m == nil {
			indexFiles = append(indexFiles, f)
			continue
		}
		start, errS := strconv.ParseUint(m[1], 10, 32)
		second, errN := strconv.ParseUint(m[3], 10, 32)
		if errS != nil || errN != nil {
			indexFiles = append(indexFiles, f)
			continue
		}
		// "+": second is a slice COUNT (par2cmdline). "-": second is an
		// INCLUSIVE END index (MultiPar/par2j) - count = end - start + 1.
		var count uint64
		switch m[2] {
		case "+":
			count = second
		default: // "-"
			if second < start {
				indexFiles = append(indexFiles, f)
				continue
			}
			count = second - start + 1
		}
		if count == 0 {
			indexFiles = append(indexFiles, f)
			continue
		}
		vols = append(vols, par2Volume{ref: f, start: uint32(start), count: uint32(count)})
	}
	sort.Slice(vols, func(i, j int) bool { return vols[i].ref.Size < vols[j].ref.Size })
	return vols, indexFiles
}

// fetchWholePar2File downloads and concatenates every segment of a retained
// PAR2 file (index or recovery volume). PAR2 files are posted directly (not
// extracted from an archive), so their segments concatenate straight into
// the file's real bytes with no trimming.
func fetchWholePar2File(ctx context.Context, fetch articleFetchFunc, f storage.Par2FileRef) ([]byte, error) {
	out := make([]byte, 0, f.Size)
	for _, seg := range f.Segments {
		fetchCtx, cancel := context.WithTimeout(ctx, par2ArticleFetchTimeout)
		data, err := fetch(fetchCtx, seg.MessageID)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("fetch %s: %w", seg.MessageID, err)
		}
		out = append(out, data...)
	}
	return out, nil
}

// computeMD5_16k fetches just enough leading segments of a posted file to
// hash its first 16KB (or the whole file, if shorter) - MatchFiles only
// calls this for a file whose length ties with another candidate.
func computeMD5_16k(ctx context.Context, fetch articleFetchFunc, f storage.PostedFileRef) ([16]byte, error) {
	fetcher := newPostedFileFetcher(ctx, fetch, f)
	n := int64(md5_16kSize)
	if f.Size < n {
		n = f.Size
	}
	data, err := fetcher.ReadRange(0, n)
	if err != nil {
		return [16]byte{}, err
	}
	return md5.Sum(data), nil
}

type postedRange struct {
	fileID     [16]byte
	start, end int64
}

// postedFileFetcher serves arbitrary byte ranges of one posted file over
// NNTP, fetching only the segments actually overlapped and caching the most
// recently fetched one - sequential slice reads during the streaming repair
// pass repeatedly hit the same or the next segment.
type postedFileFetcher struct {
	ctx    context.Context
	fetch  articleFetchFunc
	length int64
	segs   []storage.Par2SegmentRef
	base   []int64 // base[i] = starting byte offset of segs[i] within the file

	cacheIdx  int
	cacheData []byte
}

func newPostedFileFetcher(ctx context.Context, fetch articleFetchFunc, f storage.PostedFileRef) *postedFileFetcher {
	base := make([]int64, len(f.Segments))
	var off int64
	for i, s := range f.Segments {
		base[i] = off
		off += s.Bytes
	}
	return &postedFileFetcher{ctx: ctx, fetch: fetch, length: f.Size, segs: f.Segments, base: base, cacheIdx: -1}
}

func (f *postedFileFetcher) segmentFor(offset int64) (int, error) {
	lo, hi := 0, len(f.base)
	for lo < hi {
		mid := (lo + hi) / 2
		if f.base[mid]+f.segs[mid].Bytes <= offset {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo >= len(f.segs) {
		return 0, fmt.Errorf("offset %d beyond the file's %d segments", offset, len(f.segs))
	}
	return lo, nil
}

func (f *postedFileFetcher) segmentData(idx int) ([]byte, error) {
	if f.cacheIdx == idx {
		return f.cacheData, nil
	}
	fetchCtx, cancel := context.WithTimeout(f.ctx, par2ArticleFetchTimeout)
	data, err := f.fetch(fetchCtx, f.segs[idx].MessageID)
	cancel()
	if err != nil {
		return nil, err
	}
	f.cacheIdx = idx
	f.cacheData = data
	return data, nil
}

// ReadRange returns exactly length bytes starting at start, zero-padded past
// the file's real length (the PAR2 final-slice padding convention).
func (f *postedFileFetcher) ReadRange(start, length int64) ([]byte, error) {
	out := make([]byte, length)
	pos := start
	written := int64(0)
	for written < length {
		if pos >= f.length {
			break
		}
		segIdx, err := f.segmentFor(pos)
		if err != nil {
			return nil, err
		}
		data, err := f.segmentData(segIdx)
		if err != nil {
			return nil, fmt.Errorf("fetch segment %d: %w", segIdx, err)
		}
		withinSeg := pos - f.base[segIdx]
		avail := int64(len(data)) - withinSeg
		if avail <= 0 {
			return nil, fmt.Errorf("segment %d shorter than its recorded size", segIdx)
		}
		// Never copy past the file's real length, even mid-segment: a
		// segment's decoded size can legitimately exceed what's left of the
		// file (the final segment of a file whose length isn't a multiple of
		// its segment size) - anything beyond f.length must stay the
		// zero-padding out sets it to, not real bytes from past EOF.
		n := min(avail, length-written, f.length-pos)
		copy(out[written:written+n], data[withinSeg:withinSeg+n])
		written += n
		pos += n
	}
	return out, nil
}

// jobSliceSource adapts per-posted-file fetchers into the single
// par2.SliceSource the streaming repair pass reads intact slices from.
type jobSliceSource struct {
	idx      *par2.Index
	fetchers map[[16]byte]*postedFileFetcher
}

func (s *jobSliceSource) ReadSlice(globalIdx int64) ([]byte, error) {
	fileID, local, err := s.idx.SliceLocation(globalIdx)
	if err != nil {
		return nil, err
	}
	f, ok := s.fetchers[fileID]
	if !ok {
		return nil, fmt.Errorf("no posted-file fetcher for file %x", fileID)
	}
	return f.ReadRange(local*s.idx.SliceSize, s.idx.SliceSize)
}

// extractPostedRange cuts [start, end) of posted file fileID out of the
// repaired slices map (global slice index -> exactly SliceSize bytes),
// concatenating across a multi-slice range. A dead article's posted-file
// range is always fully covered by the damaged slice set computed for it, so
// every slice this touches is guaranteed present in repairedByIndex.
func extractPostedRange(idx *par2.Index, repairedByIndex map[int64][]byte, fileID [16]byte, start, end int64) ([]byte, error) {
	base, err := idx.SliceBase(fileID)
	if err != nil {
		return nil, err
	}
	out := make([]byte, end-start)
	pos := start
	for pos < end {
		local := pos / idx.SliceSize
		global := base + local
		sliceData, ok := repairedByIndex[global]
		if !ok {
			return nil, fmt.Errorf("slice %d was not reconstructed", global)
		}
		withinSlice := pos - local*idx.SliceSize
		avail := idx.SliceSize - withinSlice
		need := end - pos
		n := min(avail, need)
		copy(out[pos-start:pos-start+n], sliceData[withinSlice:withinSlice+n])
		pos += n
	}
	return out, nil
}
