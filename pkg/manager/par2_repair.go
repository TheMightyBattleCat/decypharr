// The PAR2 repair worker reconstructs confirmed-dead Usenet articles (see
// pkg/usenet/overlay) using PAR2 recovery data instead of falling straight to
// a delete + re-search. It is a manager-level background service split into
// two lanes:
//
//   - BATCH: today's original behaviour. One job at a time (the pass reads a
//     meaningful fraction of a release over NNTP, so running several
//     concurrently would just contend with itself), deduped by nzbID, and
//     gated by the repair sweep's Schedule/StopSchedule window (see
//     Repair.repairWindowOpen) - the same bandwidth-heavy-work-hours
//     reasoning StopSchedule already applies to sweeps.
//   - URGENT: fed by playback-proximity-aware callers (see EnqueueUrgent,
//     wired up from the read-ahead/precache feature) that need a damaged
//     region fixed before the playhead reaches it. Runs immediately -
//     ignores the batch lane's off-peak window - at bounded concurrency, may
//     preempt an in-flight BATCH pass for the same nzbID, and its NNTP
//     fetches carry nntp.PriorityUrgent so they may draw a capped provider's
//     reserve band instead of being demoted to fills-only. Both lanes remain
//     gated by the bandwidth monitor's hard quota (QuotaBlocked) and by
//     config.Repair.Par2RepairEnabled.
//
// Escalation ordering end to end: a padded segment enqueues here; on success
// the segment is patched and padding for it stops on the next read. On any
// failure - no PAR2 data, too much damage, a fetch or verification failure -
// classifyPar2Failure decides whether it's worth retrying (backed off) or
// terminal. A terminal outcome marks the entry unrepairable in the handler
// registry (see repair_handler_registry.go) - surfaced in the overlay GUI
// for a MANUAL "Delete & re-search" - and never falls back to an automatic
// re-grab: this worker only ever runs with PAR2 repair enabled (see
// readyToRun/RunNow), and decideAutoRepairAction (repair_policy.go) never
// selects an automatic re-grab in that case. With config.Repair.Par2Repair
// disabled this worker is never queued at all - the caller's own
// decideAutoRepairAction call routes straight to the legacy re-grab path
// instead; with PlaybackPadding also disabled, EnqueueRepair is never even
// called (see pkg/usenet/fs/reader) for the padding-triggered path, though
// the sweep and playback-failure paths still queue this worker directly.
package manager

import (
	"container/heap"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"os"
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
	"github.com/sirrobot01/decypharr/internal/nntp"
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

	// maxIntactRepairRounds bounds how many times runRepair will expand the
	// damaged set and retry the solve after discovering an intact slice is
	// actually confirmed-missing (a hard 430 across every provider) rather
	// than just slow/unlucky. Each round is cheap relative to a fresh job
	// (no re-fetch of anything already in sources, no re-matching files) but
	// still bounded: a release with many genuinely-missing articles should
	// hit the recovery-coverage ceiling and abort with a clear terminal
	// reason well before this, not spin.
	maxIntactRepairRounds = 3

	// par2DefaultUrgentConcurrency is used when
	// config.Repair.Par2UrgentConcurrency is unset/non-positive.
	par2DefaultUrgentConcurrency = 2
)

// repairLane distinguishes the two Par2Repair worker lanes - see the package
// doc comment above.
type repairLane int

const (
	laneBatch repairLane = iota
	laneUrgent
)

func (l repairLane) String() string {
	if l == laneUrgent {
		return "urgent"
	}
	return "batch"
}

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

// urgentJob is one pending URGENT-lane request: an nzbID and how far
// playback currently is from the damaged region it's protecting, in
// wall-clock playback time - smaller proximity outranks larger. index is
// maintained by container/heap; seq breaks ties in submission order.
type urgentJob struct {
	nzbID     string
	proximity time.Duration
	seq       int64
	index     int
}

// urgentHeap is a min-heap of *urgentJob ordered by proximity (then seq).
type urgentHeap []*urgentJob

func (h urgentHeap) Len() int { return len(h) }
func (h urgentHeap) Less(i, j int) bool {
	if h[i].proximity != h[j].proximity {
		return h[i].proximity < h[j].proximity
	}
	return h[i].seq < h[j].seq
}
func (h urgentHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *urgentHeap) Push(x any) {
	item := x.(*urgentJob)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *urgentHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[:n-1]
	return item
}

// runningJob tracks one nzbID's in-flight repair pass, in whichever lane is
// running it, so EnqueueUrgent can preempt an in-flight BATCH pass for the
// same nzbID and so lane state can be surfaced for status/UI purposes.
type runningJob struct {
	lane      repairLane
	cancel    context.CancelFunc
	preempted atomic.Bool
}

// Par2Repair is the manager-level PAR2 repair worker. See the package doc
// comment for the BATCH/URGENT lane split.
type Par2Repair struct {
	manager *Manager
	repair  *Repair
	logger  zerolog.Logger

	// BATCH lane: unchanged from the original single-lane worker.
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

	// progress holds live, per-nzbID job progress (phase, slice/byte
	// counts) - see par2_progress.go. Updated frequently throughout
	// runJob/runRepair so a stall is visible within seconds via Progress,
	// not just at completion.
	progress par2ProgressTracker

	// URGENT lane: a proximity-ordered priority queue served by a bounded
	// pool of worker goroutines (see Start/urgentWorker).
	urgentMu   sync.Mutex
	urgentHeap urgentHeap
	urgentSet  map[string]*urgentJob
	urgentSeq  int64
	urgentWake chan struct{}

	// running tracks every nzbID currently executing in either lane, so
	// EnqueueUrgent can preempt a BATCH pass and so at most one pass per
	// nzbID is ever active regardless of lane.
	runningMu sync.Mutex
	running   map[string]*runningJob

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Progress returns nzbID's live (or most recently finished) PAR2 repair job
// progress, if any job has run for it this process.
func (p *Par2Repair) Progress(nzbID string) (Par2JobProgress, bool) {
	if p == nil {
		return Par2JobProgress{}, false
	}
	s, ok := p.progress.Get(nzbID)
	if !ok {
		return Par2JobProgress{}, false
	}
	return s.Snapshot(), true
}

// NewPar2Repair builds the worker. Call Start to begin processing.
func NewPar2Repair(m *Manager, repair *Repair) *Par2Repair {
	return &Par2Repair{
		manager:    m,
		repair:     repair,
		logger:     logger.New("par2-repair"),
		queued:     make(map[string]struct{}),
		deferred:   make(map[string]struct{}),
		queue:      make(chan string, par2QueueDepth),
		progress:   newPar2ProgressTracker(),
		urgentSet:  make(map[string]*urgentJob),
		urgentWake: make(chan struct{}, 1),
		running:    make(map[string]*runningJob),
	}
}

// Start begins the worker's BATCH loop plus a bounded pool of URGENT lane
// workers.
func (p *Par2Repair) Start(ctx context.Context) {
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.wg.Add(1)
	go p.loop()

	for range p.urgentConcurrency() {
		p.wg.Add(1)
		go p.urgentWorker()
	}
}

func (p *Par2Repair) urgentConcurrency() int {
	if n := config.Get().Repair.Par2UrgentConcurrency; n > 0 {
		return n
	}
	return par2DefaultUrgentConcurrency
}

// Stop cancels any in-flight job and waits for every worker goroutine
// (BATCH loop and URGENT pool) to exit.
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
// sweep, never from an explicit GUI action (see Enqueue for that). A no-op
// entirely when config.Repair.Par2Repair is disabled, regardless of
// Par2RepairMode: claiming the handler registry (via Enqueue) for a pass
// that readyToRun/RunNow will then never actually execute leaves a stale
// par2_queued claim sitting forever - which permanently blocks
// HandlePlaybackFailure's TryAcquire(handlerRegrab) for that file, even
// though decideAutoRepairAction correctly wants to re-grab it once PAR2 is
// off (see par2Usable). Confirmed live: a padded segment claims the slot,
// the worker's readyToRun() correctly refuses to run it, and every later
// playback failure on that same file silently no-ops forever with "entry
// already being handled by another repair mechanism" instead of ever
// re-grabbing - the regrab path is unreachable until the stale claim is
// manually cleared (GUI "Delete & re-search") or the process restarts.
// Otherwise gated by config.Repair.Par2RepairMode:
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
	if !cfg.Par2RepairEnabled() {
		return
	}
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

// Enqueue schedules nzbID for a BATCH-lane PAR2 repair pass. Deduped: a burst
// of padded segments across one playback session collapses to a single pass.
// Safe to call from any goroutine (this is exactly what
// overlay.Handle.EnqueueRepair does, from inside the reader's fetch path).
// Unlike AutoEnqueue, this is never gated by Par2RepairMode - it is the
// explicit-trigger primitive used by both AutoEnqueue and the GUI's manual
// "repair now" action. A no-op when nzbID is already being handled by the
// URGENT lane, which supersedes it.
func (p *Par2Repair) Enqueue(nzbID string) {
	p.enqueue(nzbID)
}

func (p *Par2Repair) enqueue(nzbID string) {
	if p == nil || nzbID == "" {
		return
	}
	if !p.par2ShouldAutoEnqueue(nzbID) || p.handledByUrgent(nzbID) {
		return
	}
	if p.repair != nil && p.repair.handlers != nil {
		if !p.repair.handlers.TryAcquire(nzbID, handlerPar2Queued) {
			// Already par2_queued/par2_running (the common case - repeated
			// calls for the same entry collapse to the first one, same as
			// the p.queued dedup below) or claimed by an in-flight regrab.
			// Either way, nothing new to do here.
			return
		}
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
		if p.repair != nil && p.repair.handlers != nil {
			p.repair.handlers.Release(nzbID)
		}
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

	// Manual override: force-claim the handler registry regardless of
	// whatever it currently holds (including a terminal mark from a prior
	// automatic PAR2 failure) - a user-initiated "repair now" always
	// proceeds and always re-evaluates from scratch, same as
	// par2ShouldAutoEnqueue's terminal gate not applying here either.
	if p.repair != nil && p.repair.handlers != nil {
		p.repair.handlers.Set(nzbID, handlerPar2Running)
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() {
			p.mu.Lock()
			delete(p.queued, nzbID)
			p.mu.Unlock()
		}()
		p.runJob(nzbID, laneBatch)
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
	return p.coverageSufficient(nzbID, nzb)
}

// coverageSufficient is Availability's damage-vs-recovery-coverage check,
// factored out so par2Usable can share it once it has separately confirmed
// nzb.Par2Source/Par2Files are present (or established they're
// backfillable, which this function does not attempt itself - see
// par2Usable). WITHOUT fetching any article data, same as Availability.
func (p *Par2Repair) coverageSufficient(nzbID string, nzb *storage.NZB) (sufficient bool, reason string) {
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

// par2Usable reports whether PAR2 is genuinely usable for nzbID right now -
// the input decideAutoRepairAction's policy consults for source=playback
// (see repair_policy.go). Unlike the config toggle alone, this also accounts
// for whether PAR2 metadata actually exists (or can be cheaply expected to
// exist) for THIS release: a FAILED file whose record predates PAR2
// retention (see commit 31b3594) has no Par2Files/Par2Source and, if its
// source .nzb is also gone from disk, can never be repaired via PAR2 no
// matter how long it waits - decideAutoRepairAction should route it to an
// immediate re-grab instead of leaving it terminal pending the sweep or
// manual action.
//
// "Backfillable" is checked as a cheap, network-free stat of the source .nzb
// file (BackfillPar2Refs, which this function never calls itself, does the
// actual - expensive, network-fetching - re-parse, lazily, only when a real
// PAR2 pass runs - see Par2Repair.runRepair). A backfillable release is
// optimistically usable=true here: if the backfill then fails when the PAR2
// worker actually runs, that pass fails and falls back to
// classifyPar2Failure exactly as it would for any other PAR2 failure - never
// a silent re-grab bypass.
func (p *Par2Repair) par2Usable(nzbID string) (usable bool, reason string) {
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
	if len(nzb.Par2Source) == 0 || len(nzb.Par2Files) == 0 {
		if nzb.Path == "" {
			return false, "no par2 metadata retained and source nzb no longer on disk"
		}
		if _, statErr := os.Stat(nzb.Path); statErr != nil {
			return false, "no par2 metadata retained and source nzb no longer on disk"
		}
		return true, ""
	}
	return p.coverageSufficient(nzbID, nzb)
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

// EnqueueUrgent schedules nzbID for an immediate URGENT-lane PAR2 repair
// pass, prioritized ahead of any URGENT job with a larger proximity.
// proximity is the playback-time gap between the current playhead and the
// damaged region nzbID protects - callers should re-call as that gap
// narrows (or a closer-in damaged region is discovered) so priority stays
// accurate; the smallest proximity ever reported for a still-pending nzbID
// wins. If a BATCH pass for the same nzbID is currently running, it is
// preempted (cancelled without falling back to legacy repair) so the URGENT
// lane can take over immediately. Safe to call from any goroutine; a no-op
// if p is nil or nzbID is empty.
func (p *Par2Repair) EnqueueUrgent(nzbID string, proximity time.Duration) {
	if p == nil || nzbID == "" {
		return
	}
	if proximity < 0 {
		proximity = 0
	}

	p.runningMu.Lock()
	if job, ok := p.running[nzbID]; ok && job.lane == laneBatch {
		job.preempted.Store(true)
		job.cancel()
	}
	p.runningMu.Unlock()

	p.urgentMu.Lock()
	if item, ok := p.urgentSet[nzbID]; ok {
		if proximity < item.proximity {
			item.proximity = proximity
			heap.Fix(&p.urgentHeap, item.index)
		}
		p.urgentMu.Unlock()
		return
	}
	p.urgentSeq++
	item := &urgentJob{nzbID: nzbID, proximity: proximity, seq: p.urgentSeq}
	p.urgentSet[nzbID] = item
	heap.Push(&p.urgentHeap, item)
	p.urgentMu.Unlock()

	select {
	case p.urgentWake <- struct{}{}:
	default:
	}
}

// handledByUrgent reports whether nzbID is already queued or actively
// running in the URGENT lane, in which case a BATCH enqueue for it is
// redundant.
func (p *Par2Repair) handledByUrgent(nzbID string) bool {
	p.urgentMu.Lock()
	_, queued := p.urgentSet[nzbID]
	p.urgentMu.Unlock()
	if queued {
		return true
	}
	p.runningMu.Lock()
	job, running := p.running[nzbID]
	p.runningMu.Unlock()
	return running && job.lane == laneUrgent
}

// popUrgent blocks until an URGENT job is available or ctx is done,
// returning the highest-priority (smallest-proximity) nzbID.
func (p *Par2Repair) popUrgent(ctx context.Context) (string, bool) {
	for {
		p.urgentMu.Lock()
		if len(p.urgentHeap) > 0 {
			item := heap.Pop(&p.urgentHeap).(*urgentJob)
			delete(p.urgentSet, item.nzbID)
			p.urgentMu.Unlock()
			return item.nzbID, true
		}
		p.urgentMu.Unlock()

		select {
		case <-ctx.Done():
			return "", false
		case <-p.urgentWake:
		}
	}
}

// urgentWorker is one member of the bounded URGENT-lane worker pool.
func (p *Par2Repair) urgentWorker() {
	defer p.wg.Done()
	for {
		nzbID, ok := p.popUrgent(p.ctx)
		if !ok {
			return
		}
		if !config.Get().Repair.Par2RepairEnabled() {
			continue
		}
		p.runJob(nzbID, laneUrgent)
	}
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
			if p.handledByUrgent(nzbID) {
				continue
			}
			if p.readyToRun() {
				p.runJob(nzbID, laneBatch)
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
				if p.handledByUrgent(id) {
					continue
				}
				p.runJob(id, laneBatch)
			}
		}
	}
}

// readyToRun gates the BATCH lane only: enabled, and within the repair
// sweep's off-peak window. The URGENT lane ignores the window entirely (see
// urgentWorker) - it exists specifically to act immediately.
func (p *Par2Repair) readyToRun() bool {
	return config.Get().Repair.Par2RepairEnabled() && p.repair.repairWindowOpen()
}

// runJob runs one NZB's PAR2 repair pass end to end in the given lane. Every
// exit path either leaves the overlay state as-is (nothing to do, a genuine
// "can't tell yet" error worth retrying later, or a BATCH pass preempted by
// the URGENT lane) or marks the entry terminal - it never partially patches
// and calls it done, and it never falls back to an automatic re-grab: with
// PAR2 repair enabled (the only way this worker ever runs - see
// readyToRun/RunNow/urgentWorker), decideAutoRepairAction never selects
// autoActionRegrab, so a terminal PAR2 failure surfaces in the overlay GUI
// for a MANUAL "Delete & re-search" instead (see handleOverlayResearch).
func (p *Par2Repair) runJob(nzbID string, lane repairLane) {
	start := time.Now()
	progress := p.progress.Start(nzbID, nzbID) // entry name backfilled below once resolved

	if p.repair != nil && p.repair.handlers != nil {
		// Normally already par2_queued (from Enqueue/EnqueueUrgent) or
		// par2_running (RunNow's manual override already Set it) - Transition
		// is a no-op if neither claim exists, which should not happen on the
		// normal enqueue path but is harmless if it ever does.
		p.repair.handlers.Transition(nzbID, handlerPar2Running)
	}
	// terminal decides, in the deferred release below, whether this job's
	// outcome sticks the handler registry's terminal mark (blocking further
	// automatic claims until a manual action clears it) or simply frees the
	// slot for whatever runs next.
	terminal := false
	defer func() {
		if p.repair == nil || p.repair.handlers == nil {
			return
		}
		if terminal {
			p.repair.handlers.MarkTerminal(nzbID)
		} else {
			p.repair.handlers.Release(nzbID)
		}
	}()

	if p.manager.usenet == nil {
		progress.SetPhase(Par2PhaseFailed)
		progress.SetLastError("usenet client not configured")
		return
	}

	id := nzbID
	p.active.Store(&id)
	defer p.active.Store(nil)

	entry, entryErr := p.manager.GetEntry(nzbID)
	if entryErr != nil || entry == nil {
		// A ghost overlay record - the backing entry is already gone
		// (deleted, superseded, re-grabbed under a different nzbID). There
		// is nothing to repair on behalf of; mark it terminal so the
		// automatic path stops re-enqueuing it (Commit A/B's cleanup hooks
		// should reap the overlay record itself shortly, but a race is not a
		// reason to keep hammering it in the meantime).
		terminalErr := fmt.Errorf("entry no longer exists")
		p.logger.Info().Str("entry", nzbID).Msg("par2 repair: entry no longer exists; marking unrepairable")
		progress.SetPhase(Par2PhaseFailed)
		progress.SetLastError(terminalErr.Error())
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
		terminal = true
		return
	}
	entryName := entry.Name
	progress.SetEntryName(entryName)

	jobCtx, jobCancel := context.WithCancel(p.ctx)
	timeoutCtx, timeoutCancel := context.WithTimeout(jobCtx, par2JobTimeout)
	defer timeoutCancel()
	if lane == laneUrgent {
		timeoutCtx = nntp.WithPriority(timeoutCtx, nntp.PriorityUrgent)
	}

	job := &runningJob{lane: lane, cancel: jobCancel}
	p.runningMu.Lock()
	p.running[nzbID] = job
	p.runningMu.Unlock()
	defer func() {
		p.runningMu.Lock()
		if p.running[nzbID] == job {
			delete(p.running, nzbID)
		}
		p.runningMu.Unlock()
		jobCancel()
	}()

	pending, err := p.manager.usenet.OverlayPendingRepair(nzbID)
	if err != nil || len(pending) == 0 {
		progress.SetPhase(Par2PhaseCompleted) // nothing pending - not a failure, just nothing to do
		return
	}
	deadSegments := 0
	for _, segs := range pending {
		deadSegments += len(segs)
	}
	p.logger.Info().Str("entry", entryName).Str("lane", lane.String()).Int("dead_segments", deadSegments).Msg("par2 repair queued")

	var readBytes int64  // Usenet bytes only - see runRepair's fetch wrapper
	var cacheBytes int64 // bytes sourced from the local DFS cache instead
	var slicesRepaired int
	if err := p.runRepair(timeoutCtx, nzbID, entryName, pending, &readBytes, &cacheBytes, &slicesRepaired, progress); err != nil {
		if job.preempted.Load() {
			p.logger.Debug().Str("entry", entryName).Msg("par2 repair preempted by urgent lane")
			return
		}
		canary := errors.Is(err, par2.ErrChecksumMismatch)
		class := classifyPar2Failure(err)
		p.logger.Info().Err(err).Str("entry", entryName).Str("lane", lane.String()).Bool("crc_canary", canary).Bool("terminal", class.terminal).
			Int64("cache_bytes", cacheBytes).Int64("usenet_bytes", readBytes).
			Msg("par2 repair unavailable")
		progress.SetPhase(Par2PhaseFailed)
		progress.SetLastError(err.Error())
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
		// A terminal failure - one backoff can never fix - marks the entry
		// unrepairable (see the deferred release above) instead of falling
		// back to a re-grab: PAR2 being enabled is exactly the quadrant
		// where decideAutoRepairAction never chooses autoActionRegrab, so
		// the file waits for a manual "Delete & re-search" rather than being
		// auto-re-grabbed out from under the user. A transient failure (a
		// slow provider, a context deadline) instead waits out its backoff
		// and tries PAR2 again; the file is still playable (padded) in the
		// meantime, so there is no urgency either way.
		if class.terminal {
			terminal = true
		}
		return
	}

	p.logger.Info().
		Str("entry", entryName).
		Str("lane", lane.String()).
		Int("segments_patched", deadSegments).
		Dur("duration", time.Since(start)).
		Int64("cache_bytes", cacheBytes).
		Int64("usenet_bytes", readBytes).
		Msg("par2 repair completed")
	progress.SetPhase(Par2PhaseCompleted)
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
func (p *Par2Repair) runRepair(ctx context.Context, nzbID, entryName string, pending map[string][]overlay.DeadSegment, readBytes, cacheBytes *int64, slicesRepaired *int, progress *par2JobProgressState) error {
	u := p.manager.usenet
	progress.SetPhase(Par2PhaseFetchingRecovery)

	// fetch wraps every article fetch this pass makes so runJob can report a
	// real (not estimated) read_bytes total in the persisted attempt log -
	// used in place of u.FetchArticle everywhere below.
	fetch := func(fctx context.Context, messageID string) ([]byte, error) {
		data, err := u.FetchArticle(fctx, messageID)
		if err == nil && readBytes != nil {
			n := int64(len(data))
			atomic.AddInt64(readBytes, n)
			progress.AddUsenetBytes(n)
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

	// Source intact slices from the local DFS cache where already present,
	// instead of always re-fetching them over NNTP: cacheSource is tried
	// first for every posted-file byte range the streaming pass needs (see
	// postedFileFetcher.ReadRange), falling straight through to fetch on any
	// miss (no mount, no mapping for this article, the range overlaps a
	// dead/padded segment, or it simply isn't cached right now). Nil-safe by
	// construction: a failed type assertion (rclone mode, no mount, mount
	// not ready) just means every ReadRange call behaves exactly as it did
	// before cache-sourcing existed.
	var cacheReader dfsCacheRangeReader
	if mgr := p.manager.MountManager(); mgr != nil {
		cacheReader, _ = mgr.(dfsCacheRangeReader)
	}
	cacheSource := &cacheSlicedSource{
		reader:      cacheReader,
		entryName:   entryName,
		byMessageID: buildCacheSegmentMap(nzb),
		deadRanges:  buildDeadOutputRanges(nzb, pending),
		cacheBytes:  cacheBytes,
		progress:    progress,
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
	var nextVolIdx int
	nextVolIdx, fetchedSlices, sources, _ = fetchMoreVolumes(ctx, fetch, vols, nextVolIdx, fetchedSlices, needed, sources, entryName, p.logger)
	progress.SetRecoveryVolsFetched(nextVolIdx)
	progress.SetRecoverySlices(int(fetchedSlices), int(needed))
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
		f := newPostedFileFetcher(ctx, fetch, file, cacheSource)
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

	// Each round attempts the solve with the current damaged set; a hard,
	// confirmed-across-every-provider 430 on what was assumed to be an
	// intact slice means that slice's data is genuinely gone too - not a
	// reason to keep retrying the SAME attempt (par2.Repair already gives up
	// on the first ReadSlice error it sees), but a reason to fold it into
	// the damaged set and retry the solve, if recovery coverage still
	// allows it. Bounded by maxIntactRepairRounds; a release with damage
	// beyond what the overlay had recorded aborts with a clear terminal
	// reason well before that, rather than spinning.
	var repaired []par2.RepairedSlice
	var damagedPos map[int64]struct{}
	for round := 0; ; round++ {
		k := len(damaged)
		if k == 0 {
			return fmt.Errorf("no damaged slices resolved (nothing to repair)")
		}
		if k > par2.MaxRepairSlices || uint32(k) > available {
			return fmt.Errorf("more damage than recorded; %d slices unrecoverable (recovery cap %d, %d slices retained)", k, par2.MaxRepairSlices, available)
		}

		// Top up recovery slice DATA for the current k if needed - discovering
		// more damage mid-pass can push k past the original name-only
		// estimate (needed) that sized the first fetch. Picks up exactly
		// where the last fetch left off; re-parses only when new sources
		// were actually added.
		if uint32(k) > fetchedSlices {
			var added int
			nextVolIdx, fetchedSlices, sources, added = fetchMoreVolumes(ctx, fetch, vols, nextVolIdx, fetchedSlices, uint32(k), sources, entryName, p.logger)
			progress.SetRecoveryVolsFetched(nextVolIdx)
			progress.SetRecoverySlices(int(fetchedSlices), k)
			if added > 0 {
				idx, err = par2.ParseIndex(sources)
				if err != nil {
					return fmt.Errorf("parse PAR2 index: %w", err)
				}
			}
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

		// intactOrder lists every slice par2.Repair's own streaming loop will
		// call ReadSlice for, in the exact ascending order it calls them
		// (idx.NumSlices() total, minus damaged) - the concurrent fetch pool
		// below processes this same set in parallel, so ReadSlice never
		// blocks on more than its own per-segment timeout once a result is
		// actually needed.
		damagedPos = make(map[int64]struct{}, len(damaged))
		for _, d := range damaged {
			damagedPos[d] = struct{}{}
		}
		intactOrder := make([]int64, 0, idx.NumSlices()-int64(len(damaged)))
		for s := int64(0); s < idx.NumSlices(); s++ {
			if _, isDamaged := damagedPos[s]; isDamaged {
				continue
			}
			intactOrder = append(intactOrder, s)
		}

		// Fetch intact slices through the same bounded, concurrent pool model
		// Download uses (pool.New().WithMaxGoroutines(ProcessingMaxConnections)),
		// instead of one strictly-sequential fetch at a time: a release with
		// thousands of segments previously meant thousands of sequential
		// up-to-60s fetch attempts, whose cumulative worst case could exceed
		// the whole job's deadline even with no single connection ever
		// "stuck" forever - concurrency divides that cumulative latency by
		// the connection limit instead.
		jobSource := &jobSliceSource{idx: idx, fetchers: fetchers}
		progress.SetIntactTotal(len(intactOrder))
		if len(intactOrder) == 0 {
			progress.SetPhase(Par2PhaseSolving)
		} else {
			progress.SetPhase(Par2PhaseStreamingIntact)
		}
		var intactRead int64
		intactTotal := int64(len(intactOrder))
		trackedFetch := func(i int64) ([]byte, error) {
			data, err := jobSource.ReadSlice(i)
			progress.AddIntactRead(1)
			if atomic.AddInt64(&intactRead, 1) == intactTotal {
				progress.SetPhase(Par2PhaseSolving)
			}
			return data, err
		}
		sliceSource := newConcurrentSliceSource(ctx, intactOrder, u.ProcessingMaxConnections(), trackedFetch)
		var repairErr error
		repaired, repairErr = par2.Repair(idx, damaged, recovery, sliceSource)
		if repairErr == nil {
			break
		}

		notFound := sliceSource.NotFoundIndices()
		newlyDamaged := make([]int64, 0, len(notFound))
		for _, ni := range notFound {
			if _, already := damagedPos[ni]; !already {
				newlyDamaged = append(newlyDamaged, ni)
			}
		}
		if len(newlyDamaged) == 0 || round >= maxIntactRepairRounds-1 {
			return fmt.Errorf("repair: %w", repairErr)
		}
		p.logger.Info().
			Str("entry", entryName).
			Int("newly_damaged", len(newlyDamaged)).
			Int("round", round+1).
			Msg("par2 repair: intact slice(s) confirmed missing across every provider; expanding damaged set and retrying")
		damaged = append(damaged, newlyDamaged...)
		sort.Slice(damaged, func(i, j int) bool { return damaged[i] < damaged[j] })
	}
	if slicesRepaired != nil {
		*slicesRepaired = len(repaired)
	}
	progress.SetPhase(Par2PhaseWriting)

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
		// A prior playback read may have permanently cached this file as
		// failed (see Usenet.shouldPoisonFailedFile) before this repair
		// patched its damage. Un-poison it now so the next read builds a
		// fresh reader against the now-repaired file instead of
		// short-circuiting on a stale cause. Covers both automatic repair
		// and a manual "repair now" (RunNow runs this exact same path).
		u.ClearFailedFile(nzbID, dr.file)
	}
	return nil
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

// fetchMoreVolumes fetches additional recovery volumes from vols (already
// sorted smallest-first), continuing from nextVolIdx, until fetchedSlices
// covers needed or vols is exhausted. Returns the updated position/count/
// sources so a later call - after discovering MORE damage than originally
// estimated (see runRepair's retry loop) - can pick up exactly where an
// earlier call left off, without re-fetching anything already in sources.
// The added return is how many new entries were appended to sources this
// call - callers only need to re-parse the PAR2 index when it's non-zero.
func fetchMoreVolumes(ctx context.Context, fetch articleFetchFunc, vols []par2Volume, nextVolIdx int, fetchedSlices, needed uint32, sources []par2.Source, entryName string, logger zerolog.Logger) (newNextVolIdx int, newFetchedSlices uint32, newSources []par2.Source, added int) {
	for nextVolIdx < len(vols) && fetchedSlices < needed {
		v := vols[nextVolIdx]
		nextVolIdx++
		data, err := fetchWholePar2File(ctx, fetch, v.ref)
		if err != nil {
			logger.Debug().Err(err).Str("entry", entryName).Str("file", v.ref.Name).Msg("par2: recovery volume fetch failed")
			continue
		}
		sources = append(sources, par2.Source{Name: v.ref.Name, Data: data})
		fetchedSlices += v.count
		added++
	}
	return nextVolIdx, fetchedSlices, sources, added
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
	fetcher := newPostedFileFetcher(ctx, fetch, f, nil)
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

	// cacheSource, when non-nil, is tried before every Usenet fetch below -
	// see cacheSlicedSource.readCached. Misses (no mapping, dead range, not
	// cached) fall straight through to the existing fetch path unchanged.
	cacheSource *cacheSlicedSource

	// cacheMu guards cacheIdx/cacheData - the concurrent intact-slice reader
	// (concurrentSliceSource) can call ReadRange for different slices of the
	// SAME posted file from multiple goroutines at once. The lock only ever
	// wraps the cheap check/store of this single-entry cache, never the
	// actual network fetch, so concurrent requests for DIFFERENT segments
	// still proceed in parallel - they just both (correctly) miss this
	// one-entry cache and fetch independently, exactly as if it weren't
	// there. Sequential callers (computeMD5_16k, and this fetcher before
	// concurrency existed) see identical behavior to before: an uncontended
	// mutex is effectively free.
	cacheMu   sync.Mutex
	cacheIdx  int
	cacheData []byte
}

func newPostedFileFetcher(ctx context.Context, fetch articleFetchFunc, f storage.PostedFileRef, cacheSource *cacheSlicedSource) *postedFileFetcher {
	base := make([]int64, len(f.Segments))
	var off int64
	for i, s := range f.Segments {
		base[i] = off
		off += s.Bytes
	}
	return &postedFileFetcher{ctx: ctx, fetch: fetch, length: f.Size, segs: f.Segments, base: base, cacheSource: cacheSource, cacheIdx: -1}
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
	f.cacheMu.Lock()
	if f.cacheIdx == idx {
		data := f.cacheData
		f.cacheMu.Unlock()
		return data, nil
	}
	f.cacheMu.Unlock()

	// Per-segment timeout, independent of how many OTHER segments are being
	// fetched concurrently right now: a single dead/stuck connection fails
	// this one fetch in par2ArticleFetchTimeout, not the whole job's
	// deadline. f.ctx (the job's own context, via par2JobTimeout) remains
	// the backstop if it's already closer than that.
	fetchCtx, cancel := context.WithTimeout(f.ctx, par2ArticleFetchTimeout)
	data, err := f.fetch(fetchCtx, f.segs[idx].MessageID)
	cancel()
	if err != nil {
		return nil, err
	}

	f.cacheMu.Lock()
	f.cacheIdx = idx
	f.cacheData = data
	f.cacheMu.Unlock()
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
		withinSeg := pos - f.base[segIdx]
		// Never copy past the file's real length, even mid-segment: a
		// segment's decoded size can legitimately exceed what's left of the
		// file (the final segment of a file whose length isn't a multiple of
		// its segment size) - anything beyond f.length must stay the
		// zero-padding out sets it to, not real bytes from past EOF. Sized
		// off the DECLARED segment length here (Par2SegmentRef.Bytes) so the
		// cache-vs-fetch decision below can be made before ever fetching
		// anything; the fetch path re-derives the same bound off the actual
		// fetched length, exactly as before this cache-sourcing existed.
		declaredAvail := f.segs[segIdx].Bytes - withinSeg
		if declaredAvail <= 0 {
			return nil, fmt.Errorf("segment %d shorter than its recorded size", segIdx)
		}
		n := min(declaredAvail, length-written, f.length-pos)

		if f.cacheSource != nil {
			if cached, ok := f.cacheSource.readCached(f.segs[segIdx].MessageID, withinSeg, n); ok {
				copy(out[written:written+n], cached)
				written += n
				pos += n
				continue
			}
		}

		data, err := f.segmentData(segIdx)
		if err != nil {
			return nil, fmt.Errorf("fetch segment %d: %w", segIdx, err)
		}
		avail := int64(len(data)) - withinSeg
		if avail <= 0 {
			return nil, fmt.Errorf("segment %d shorter than its recorded size", segIdx)
		}
		n = min(avail, length-written, f.length-pos)
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
