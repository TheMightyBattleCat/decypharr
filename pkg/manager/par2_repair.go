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
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/sourcegraph/conc/pool"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/notifications"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
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

	// par2JobTimeout is the base bound on one NZB's whole PAR2 pass (index
	// fetch through verification), so a stalled provider can't wedge the
	// worker forever. par2JobTimeoutFor extends it by release size.
	par2JobTimeout = 20 * time.Minute
	// par2JobTimeoutMax caps par2JobTimeoutFor.
	par2JobTimeoutMax = 4 * time.Hour
	// par2JobPassFloor is the throughput par2JobTimeoutFor allows each full
	// read of the release at, and par2JobPasses how many reads it allows
	// for: a discovery pass, the solve, and one more round.
	par2JobPassFloor = 4 << 20 // bytes per second
	par2JobPasses    = 3

	// par2ArticleFetchTimeout bounds a single article fetch within a job.
	par2ArticleFetchTimeout = 60 * time.Second

	// par2JobIdleTimeout cancels a repair that has made no forward progress
	// for this long WHILE in a network phase (fetching recovery or streaming
	// intact data). It intentionally does not apply during queued, solving
	// (pure CPU) or writing phases, where progress timestamps do not move for
	// legitimate reasons. Comfortably above par2ArticleFetchTimeout so a
	// single slow-but-valid article never trips it.
	par2JobIdleTimeout = 120 * time.Second

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

	// par2RecoveryMaxConsecutiveNotFound aborts the recovery-volume fetch loop
	// once this many volumes in a row come back article-not-found (430). When a
	// PAR2 posting has expired every volume 430s, and walking all ~20-100 of
	// them (provider rotation + timeout on each) wastes 10-20 minutes to reach a
	// verdict the first few misses already settled. Only genuine 430s count;
	// transient/transport errors reset the counter.
	par2RecoveryMaxConsecutiveNotFound = 3
	// par2FileFetchConcurrency is how many articles of one PAR2 file
	// fetchWholePar2File fetches at once. fetchMoreVolumes instead splits
	// the processing connection budget across the volumes in a wave.
	par2FileFetchConcurrency = 4

	// par2DefaultUrgentConcurrency is used when
	// config.Repair.Par2UrgentConcurrency is unset/non-positive.
	par2DefaultUrgentConcurrency = 2

	// par2RecoveryStatTimeout bounds the recovery-volume STAT pre-census (see
	// statRecoveryVolumes). Kept below par2JobIdleTimeout so a stalled STAT
	// batch can't itself trip the idle watchdog before its own deadline fires
	// - the pre-census is header-only and probes at most ~MaxRepairSlices
	// volumes, so a healthy run finishes in a few seconds.
	par2RecoveryStatTimeout = 90 * time.Second

	// par2PostedStatTimeout bounds the posted-file STAT damage sweep (see
	// statPostedFileDamage). Larger than par2RecoveryStatTimeout because it
	// probes every article of every matched posted file, not just a handful
	// of recovery volumes; the sweep runs in Par2PhaseProbing, which the idle
	// watchdog ignores, so this ceiling is the only bound that applies.
	par2PostedStatTimeout = 5 * time.Minute
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

// postedFetchFunc is articleFetchFunc for a posted file's articles, with an
// identity check the fetch applies to every provider's copy (see
// usenet.FetchArticleChecked and postedArticleMismatch). u.FetchArticleChecked
// satisfies it directly; tests wrap a plain fake with uncheckedPosted.
type postedFetchFunc func(ctx context.Context, messageID string, check usenet.ArticleCheck) ([]byte, error)

// uncheckedPosted adapts an articleFetchFunc that has no yEnc headers to hand
// a check (a test fake) into a postedFetchFunc that skips the check.
func uncheckedPosted(fetch articleFetchFunc) postedFetchFunc {
	return func(ctx context.Context, messageID string, _ usenet.ArticleCheck) ([]byte, error) {
		return fetch(ctx, messageID)
	}
}

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

// par2NumberedVolPattern matches recovery volumes named only by sequence
// number, with no slice range: "release.vol-03.par2" (seen on North Glen
// S20 NTb postings, vol-01..vol-08) or "release.vol03.par2".
var par2NumberedVolPattern = regexp.MustCompile(`(?i)\.vol-?\d+\.par2$`)

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
	// This entry is currently under an ffprobe sweep probe, which is already
	// pulling its articles over NNTP. An urgent PAR2 pass kicked off by the
	// padding path right now would just contend with that probe for provider
	// connections, so defer - the sweep's own escalation path (or a later
	// playback read) will re-trigger this once the probe clears the entry.
	// Scoped per-entry: an entry not under probe still auto-enqueues normally
	// even mid-sweep. Explicit user actions (RunNow, batch Enqueue) bypass
	// this: they don't route through AutoEnqueue.
	if p.manager.usenet != nil && p.manager.usenet.OverlayIsEntrySweepActive(nzbID) {
		p.logger.Debug().Str("entry", nzbID).Msg("par2 auto-enqueue deferred: sweep active")
		return
	}

	// proximity=0: damage was just found by warming/scanning right now, the
	// same "playhead is already here" urgency as repair_sweep.go's playback
	// caller passes.
	p.EnqueueUrgent(nzbID, 0)
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
	if state, err := p.manager.storage.GetPar2RepairState(nzbID); err == nil && state != nil && state.Terminal {
		return false, state.TerminalReason
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

// par2RecoveryCapacity returns the total number of recovery slices available
// for nzbID from the retained Par2Files metadata, or -1 if recovery capacity
// cannot be determined (no Par2Files, no par2 repair, etc.). Does NOT check
// whether PAR2 repair is enabled — callers already know par2Usable.
func (p *Par2Repair) par2RecoveryCapacity(nzbID string) int {
	if p == nil || p.manager.usenet == nil {
		return -1
	}
	nzb, err := p.manager.usenet.GetNZB(nzbID)
	if err != nil || len(nzb.Par2Files) == 0 {
		return -1
	}
	vols, _ := censusPar2Volumes(nzb.Par2Files)
	var available int
	for _, v := range vols {
		available += int(v.count)
	}
	return available
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

	// Resolve this posted file to its FileID from the persisted match cache
	// when it covers the release - saves the MD5-16k fetch MatchFiles would
	// do to break a length tie. Falls through to a real match otherwise.
	var matchedFileID [16]byte
	haveMatch := false
	if cached, ok := par2MatchFromCache(idx, nzb.Par2Source, nzb.Par2Match); ok {
		for _, m := range cached {
			if nzb.Par2Source[m.PostedIndex].Name == postedRef.Name {
				matchedFileID = m.FileID
				haveMatch = true
				break
			}
		}
	}
	if !haveMatch {
		postedFiles := []par2.PostedFile{{
			Name:   postedRef.Name,
			Length: postedRef.Size,
			MD5_16k: func() ([16]byte, error) {
				return computeMD5_16k(ctx, u.FetchArticleChecked, postedRef)
			},
		}}
		matches, skipped, err := par2.MatchFiles(idx, postedFiles)
		if err != nil {
			return false, "", fmt.Errorf("match posted file against par2 index: %w", err)
		}
		if len(matches) == 0 {
			if len(skipped) > 0 {
				return false, "", fmt.Errorf("match posted file against par2 index: %w", skipped[0].Err)
			}
			return false, "posted file did not match any par2 FileDesc", nil
		}
		matchedFileID = matches[0].FileID
	}
	fd, ok := idx.Files[matchedFileID]
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
		data, ferr := u.FetchArticleChecked(fetchCtx, seg.MessageID, func(meta *nntp.YencMetadata) string {
			return postedArticleMismatch(meta, i, 0, false, fd.Length)
		})
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

	// An urgent request must obey the same repairability gate as the batch
	// path: if the entry is marked unrepairable or is still inside its
	// backoff window, do not preempt a running job or queue a new one.
	// Playback padding already covers the viewing experience meanwhile.
	if !p.par2ShouldAutoEnqueue(nzbID) {
		return
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
			// The job was dequeued but PAR2 is now disabled, so runJob will not
			// run and would otherwise strand a handler claim inherited from a
			// preempted batch job. Free it so the entry is not blocked until TTL.
			if p.repair != nil && p.repair.handlers != nil {
				p.repair.handlers.Release(nzbID)
			}
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
	preempted := false
	defer func() {
		if p.repair == nil || p.repair.handlers == nil {
			return
		}
		switch {
		case terminal:
			p.repair.handlers.MarkTerminal(nzbID)
		case preempted:
			// Superseded by an urgent-lane job for the same entry, which will
			// claim and release the handler slot itself. Releasing here would
			// briefly hand the entry back to another caller mid-flight.
		default:
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
		p.recordPar2Outcome(nzbID, terminalErr, 0)
		terminal = true
		return
	}
	entryName := entry.Name
	progress.SetEntryName(entryName)

	jobCtx, jobCancel := context.WithCancel(p.ctx)
	var releaseBytes int64
	if hdr, herr := p.manager.usenet.GetNZBHeader(nzbID); herr == nil && hdr != nil {
		releaseBytes = hdr.TotalSize
	}
	timeoutCtx, timeoutCancel := context.WithTimeout(jobCtx, par2JobTimeoutFor(releaseBytes))
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

	// Idle watchdog: cancel a repair that stalls with no forward progress
	// during a network phase. Exits cleanly when the job ends (timeoutCtx is
	// cancelled by the defers above / the job timeout / preemption / shutdown).
	go p.watchIdle(timeoutCtx, jobCancel, progress)

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
	var deadSlicesDiscovered int // distinct dead slices the repair engine found across all rounds, beyond what the overlay already recorded
	if err := p.runRepair(timeoutCtx, nzbID, entryName, pending, &readBytes, &cacheBytes, &slicesRepaired, &deadSlicesDiscovered, progress); err != nil {
		if job.preempted.Load() {
			preempted = true
			p.logger.Debug().Str("entry", entryName).Msg("par2 repair preempted by urgent lane")
			return
		}
		canary := errors.Is(err, par2.ErrChecksumMismatch)
		class := p.recordPar2Outcome(nzbID, err, deadSlicesDiscovered)
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
		p.notifyFailed(entryName, err, canary)
		// A terminal failure - one backoff can never fix - marks the entry
		// unrepairable (see the deferred release above): PAR2 being enabled
		// is exactly the quadrant where decideAutoRepairAction never chooses
		// autoActionRegrab, so the sweep's own re-detection (source=sweep
		// always re-grabs a real verdict, see routeAutoRepair) remains the
		// backstop that eventually re-grabs it, and the overlay GUI still
		// offers a manual "Delete & re-search" in the meantime. A transient
		// failure (a slow provider, a context deadline) instead waits out its
		// backoff and tries PAR2 again; the file is still playable (padded)
		// in the meantime, so there is no urgency either way.
		//
		// On the URGENT lane specifically, a terminal verdict also gets an
		// immediate regrab attempt (see regrabOnTerminal) rather than waiting
		// for the sweep's cadence: the URGENT lane exists precisely because
		// something (a live viewer, or precache's playback-proximity
		// estimate) needs this file usable soon, so a release the sweep
		// won't reconsider until its next off-peak pass would otherwise sit
		// broken in the Arr for hours. This is purely additive - it never
		// replaces the sweep backstop above, only races ahead of it.
		if class.terminal {
			terminal = true
			if lane == laneUrgent {
				p.regrabOnTerminal(entry, entryName, pending)
			}
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
	p.recordPar2Outcome(nzbID, nil, 0)
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

// regrabOnTerminal is the URGENT lane's fast path for a PAR2 verdict that
// classifyPar2Failure just marked terminal: an immediate Arr file delete +
// blocklist + re-search, instead of leaving the file broken until the
// sweep's own cadence-driven routeAutoRepair gets to it (see runJob's
// terminal-handling comment above). Purely best-effort - any skip or
// failure here just means the sweep backstop still applies, so nothing is
// escalated above Debug.
//
// Goes through repairPlaybackFileNow, not RegrabImportGrab: on this path the
// file is already imported into the Arr, so a bare blocklist + re-search
// leaves the EpisodeFile/MovieFile row in place and the Arr treats the
// episode/movie as satisfied - it re-searches but grabs nothing.
// repairPlaybackFileNow self-resolves the ArrFileID (attachArrContext),
// deletes the file record so the media reverts to "wanted", then blocklists
// + re-searches. It runs its own single regrabGuard.checkAndRecord
// (auto=true), so this caller must never pre-check the guard itself, since
// checkAndRecord both checks AND records an attempt and double-calling it
// for one logical event would burn two of the guard's 2-per-24h strikes for
// what is really one.
func (p *Par2Repair) regrabOnTerminal(entry *storage.Entry, entryName string, pending map[string][]overlay.DeadSegment) {
	if p.repair == nil || entry == nil {
		return
	}

	a := p.manager.arr.GetOrCreate(entry.Category)
	if a == nil || a.Host == "" || a.Token == "" {
		p.logger.Debug().Str("entry", entryName).Msg("par2 repair: no arr associated with entry; skipping immediate regrab")
		return
	}

	// A PAR2 verdict is a whole-NZB outcome, not scoped to one file, so any
	// one of pending's still-damaged files is as representative as another
	// for the log fields and for the single-file scoping repairPlaybackFileNow
	// applies when the name lines up with an Arr-known file.
	fileName := entryName
	for name := range pending {
		fileName = name
		break
	}

	p.logger.Info().Str("entry", entryName).Str("file", fileName).
		Msg("par2 repair: terminal verdict on urgent lane; initiating immediate regrab")

	// No handlerRegrab claim is taken here: runJob already holds
	// par2_running for this nzbID until it returns, so we are the exclusive
	// handler. That is also why the old RegrabImportGrab call was inert on
	// this path - its own TryAcquire(handlerRegrab) could never win against
	// the running PAR2 job, so it logged "already being handled" and did
	// nothing.
	if _, reason, rerr := p.repair.repairPlaybackFileNow(p.ctx, entryName, fileName, true, false); rerr != nil {
		p.logger.Debug().Err(rerr).Str("entry", entryName).Msg("par2 repair: immediate regrab did not proceed")
	} else if reason != "" {
		p.logger.Debug().Str("entry", entryName).Str("reason", reason).Msg("par2 repair: immediate regrab skipped")
	}
}

// par2MatchFromCache reconstructs par2.MatchFiles' output from the persisted
// Par2Match cache (see storage.Par2MatchRef), with no network. It returns
// ok=false unless the cache pairs EVERY posted file in par2Source with a
// FileID that still exists in idx - a partial or drifted cache falls through
// to a real MatchFiles run rather than silently dropping a posted file from
// the repair (an unmatched file must stay visible so classifyMiss can reason
// about it).
func par2MatchFromCache(idx *par2.Index, par2Source []storage.PostedFileRef, cache []storage.Par2MatchRef) ([]par2.Match, bool) {
	if idx == nil || len(cache) == 0 || len(cache) != len(par2Source) {
		return nil, false
	}
	nameToIdx := make(map[string]int, len(par2Source))
	for i := range par2Source {
		nameToIdx[par2Source[i].Name] = i
	}
	seen := make([]bool, len(par2Source))
	out := make([]par2.Match, 0, len(cache))
	for _, m := range cache {
		pi, ok := nameToIdx[m.PostedName]
		if !ok || seen[pi] {
			return nil, false
		}
		if _, ok := idx.Files[m.FileID]; !ok {
			return nil, false
		}
		seen[pi] = true
		out = append(out, par2.Match{PostedIndex: pi, FileID: m.FileID, NameMismatch: m.NameMismatch})
	}
	for _, s := range seen {
		if !s {
			return nil, false
		}
	}
	return out, true
}

// par2MatchToCache builds the persistable cache from a MatchFiles result,
// but ONLY when that result fully resolved the release: no skips, and one
// distinct match per posted file. A partial result returns nil (not cached)
// so a later cache hit can never mask a file MatchFiles couldn't pair.
func par2MatchToCache(par2Source []storage.PostedFileRef, matches []par2.Match, skipped []par2.MatchSkip) []storage.Par2MatchRef {
	if len(skipped) != 0 || len(matches) != len(par2Source) {
		return nil
	}
	seen := make([]bool, len(par2Source))
	out := make([]storage.Par2MatchRef, 0, len(matches))
	for _, m := range matches {
		if m.PostedIndex < 0 || m.PostedIndex >= len(par2Source) || seen[m.PostedIndex] {
			return nil
		}
		seen[m.PostedIndex] = true
		out = append(out, storage.Par2MatchRef{
			PostedName:   par2Source[m.PostedIndex].Name,
			FileID:       m.FileID,
			NameMismatch: m.NameMismatch,
		})
	}
	return out
}

// earlyDamagedSliceCheck computes a lower bound on the number of damaged PAR2
// slices using only the PAR2 index and persisted segment geometry - no
// network. It matches pending (damaged) files against idx.Files by unique
// FileDesc.Length (exactly replicating MatchFiles' free-match path: one
// FileDesc AND one posted file of that length), OR, when a fully-resolved
// Par2Match cache is present, by that cache's exact name->FileID pairing -
// which also covers length-tied files the network-free heuristic must skip.
// The result is a lower bound: skipped files (ties with no cache, or names
// absent from par2Source) can only ADD more damaged slices, never remove
// them. Returns 0 if no pending file could be matched.
func earlyDamagedSliceCheck(
	idx *par2.Index,
	pending map[string][]overlay.DeadSegment, // filename -> dead segments
	par2Source []storage.PostedFileRef, // the NZB's Par2Source
	matchCache []storage.Par2MatchRef, // the NZB's Par2Match (may be empty)
	logger zerolog.Logger,
) int {
	if idx == nil || len(pending) == 0 {
		return 0
	}

	// A fully-resolved match cache gives an exact, network-free name->FileID
	// for every posted file, with none of the length-tie ambiguity the
	// heuristic below has to bail on. Use it in preference when present.
	var cachedFID map[string][16]byte
	if cached, ok := par2MatchFromCache(idx, par2Source, matchCache); ok {
		cachedFID = make(map[string][16]byte, len(cached))
		for _, m := range cached {
			cachedFID[par2Source[m.PostedIndex].Name] = m.FileID
		}
	}

	// length -> FileID, retaining only lengths carried by exactly one
	// FileDesc. MatchFiles buckets idx.Files by FileDesc.Length and takes the
	// free path only when a bucket holds a single FileID; a multi-FileDesc
	// bucket needs the MD5-16k tie-break, which this network-free check skips.
	lengthToFile := make(map[int64][16]byte)
	lengthTied := make(map[int64]bool)
	for id, fd := range idx.Files {
		if fd == nil {
			continue
		}
		if lengthTied[fd.Length] {
			continue
		}
		if _, seen := lengthToFile[fd.Length]; seen {
			delete(lengthToFile, fd.Length)
			lengthTied[fd.Length] = true
			continue
		}
		lengthToFile[fd.Length] = id
	}

	// Par2Source indexed by name (the pending map's keys are posted-file
	// names), plus a per-length posted-file count: MatchFiles' free path also
	// requires exactly one POSTED file of the length, comparing
	// PostedFileRef.Size directly against FileDesc.Length with no tolerance.
	srcByName := make(map[string]storage.PostedFileRef, len(par2Source))
	postedLenCount := make(map[int64]int, len(par2Source))
	for _, s := range par2Source {
		srcByName[s.Name] = s
		postedLenCount[s.Size]++
	}

	damagedSet := make(map[int64]struct{})
	for fname, deadSegs := range pending {
		src, ok := srcByName[fname]
		if !ok {
			continue // not in Par2Source -> can't cheaply match; lower bound stays safe
		}
		var fileID [16]byte
		if cachedFID != nil {
			fileID, ok = cachedFID[fname]
			if !ok {
				continue // pending file has no posted-file identity in the cache
			}
		} else {
			fileID, ok = lengthToFile[src.Size]
			if !ok {
				continue // no unique-length FileDesc for this size (tie or absent)
			}
			if postedLenCount[src.Size] != 1 {
				continue // >1 posted file of this length -> MatchFiles needs MD5-16k
			}
		}
		fd := idx.Files[fileID]
		if fd == nil {
			continue
		}

		// Replicate newPostedFileFetcher's network-free geometry: per-segment
		// byte bases/sizes from persisted Par2SegmentRef.Bytes anchored to
		// FileDesc.Length.
		base, segSizes := exactSegGeometry(src.Segments, fd.Length, logger)
		if len(base) != len(src.Segments) || len(segSizes) != len(src.Segments) {
			continue
		}
		pos := make(map[string]int, len(src.Segments))
		for i, sg := range src.Segments {
			pos[sg.MessageID] = i
		}

		for _, ds := range deadSegs {
			i, ok := pos[ds.MessageID]
			if !ok {
				continue
			}
			slices, err := idx.DamagedSlices(fileID, base[i], base[i]+segSizes[i])
			if err != nil {
				continue
			}
			for _, s := range slices {
				damagedSet[s] = struct{}{}
			}
		}
	}
	return len(damagedSet)
}

// runRepair does the actual work; every error return means "PAR2 couldn't
// handle this," triggering the legacy fallback in the caller. It never
// returns a nil error after only partially patching pending's segments.
func (p *Par2Repair) runRepair(ctx context.Context, nzbID, entryName string, pending map[string][]overlay.DeadSegment, readBytes, cacheBytes *int64, slicesRepaired *int, deadDiscovered *int, progress *par2JobProgressState) error {
	u := p.manager.usenet
	progress.SetPhase(Par2PhaseFetchingRecovery)

	// fetchPosted wraps every article fetch this pass makes so runJob can
	// report a real (not estimated) read_bytes total in the persisted attempt
	// log - used in place of u.FetchArticle everywhere below. A posted file's
	// articles pass an identity check (see postedArticleMismatch); PAR2 files
	// go through fetch, which has none.
	fetchPosted := func(fctx context.Context, messageID string, check usenet.ArticleCheck) ([]byte, error) {
		data, err := u.FetchArticleChecked(fctx, messageID, check)
		if err == nil && readBytes != nil {
			n := int64(len(data))
			atomic.AddInt64(readBytes, n)
			progress.AddUsenetBytes(n)
		}
		return data, err
	}
	fetch := func(fctx context.Context, messageID string) ([]byte, error) {
		return fetchPosted(fctx, messageID, nil)
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

	// Replace the filename-derived paper count with what the provider will
	// actually serve right now: STAT the smallest recovery volumes and drop
	// any that have themselves expired, so the capacity gates below (the
	// early arithmetic gate and the per-round k>available check) terminate a
	// borderline repair before spending minutes of 430s fetching volumes
	// that are gone. Best-effort - a STAT failure leaves vols untouched.
	statCtx, statCancel := context.WithTimeout(ctx, par2RecoveryStatTimeout)
	vols = statRecoveryVolumes(statCtx, p.logger, u.StatSegments, vols, entryName)
	statCancel()
	progress.Touch()

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

	// Early arithmetic-impossibility gate. The Main/FileDesc/IFSC packets are
	// duplicated into every PAR2 file, so the index (non-volume) files alone
	// yield SliceSize + every FileDesc.Length - enough to parse a usable
	// index before a single recovery volume is fetched. From that index plus
	// persisted segment geometry we compute a LOWER-BOUND damaged-slice count
	// (see earlyDamagedSliceCheck). If even the lower bound already exceeds
	// the repair cap or the name-only recovery census, the exact count can
	// only be worse: skip the entire recovery-volume fetch (many minutes of
	// 430s for a large release) and declare terminal now. The authoritative
	// exact gate in the solve loop below is unchanged.
	if earlyIdx, perr := par2.ParseIndex(sources); perr == nil {
		if earlyK := earlyDamagedSliceCheck(earlyIdx, pending, nzb.Par2Source, nzb.Par2Match, p.logger); earlyK > 0 {
			if earlyK > par2.MaxRepairSlices || uint32(earlyK) > available {
				p.logger.Info().
					Int("early_k", earlyK).
					Int("max_repair_slices", par2.MaxRepairSlices).
					Uint32("available", available).
					Str("entry", entryName).
					Msg("par2: repair provably unavailable before recovery fetch (early arithmetic check)")
				return fmt.Errorf("more damage than recorded; %d slices unrecoverable (recovery cap %d, %d slices retained)", earlyK, par2.MaxRepairSlices, available)
			}
		}
	}

	needed := estimateNeededSlices(pending)
	// fetchedSlices is the name-advertised slice count of the volumes fetched
	// so far. It's only good enough to size and report this first fetch; from
	// here on the code measures recovery coverage by len(idx.Recovery) (the
	// parsed reality), since a fetched-but-mis-served volume inflates this
	// count without contributing any usable slice - see topUpParsedRecovery.
	var fetchedSlices uint32
	var nextVolIdx int
	nextVolIdx, fetchedSlices, sources, _, err = fetchMoreVolumes(ctx, p.logger, fetch, vols, nextVolIdx, needed, fetchedSlices, sources, entryName, u.ProcessingMaxConnections())
	if err != nil {
		return fmt.Errorf("fetch recovery volumes: %w", err)
	}
	progress.SetRecoveryVolsFetched(nextVolIdx)
	progress.SetRecoverySlices(int(fetchedSlices), int(needed))
	if len(sources) == 0 {
		return fmt.Errorf("failed to fetch any PAR2 file")
	}

	idx, err := par2.ParseIndex(sources)
	if err != nil {
		return fmt.Errorf("parse PAR2 index: %w", err)
	}
	logSkippedPar2Packets(p.logger, entryName, idx)

	// No usable recovery slices - either every volume 430'd (fetchMoreVolumes'
	// consecutive-not-found abort) or every fetched volume was unparseable
	// (mis-served articles whose bad-MD5 packets ParseIndex now skips). Gate
	// on the PARSED count, not the name-advertised fetchedSlices accumulator:
	// a volume that fetched but yielded no recovery packets still bumps
	// fetchedSlices, so `fetchedSlices == 0` would miss that case and fall
	// through to a full MatchFiles pass before arriving at the same verdict.
	// The verdict is already predetermined: no recovery data means no repair.
	// Running MatchFiles + the dead-segment mapping first would MD5-16k every
	// posted file (~30s each while they're 430'ing too - ~9min for a
	// 269-volume release) only to arrive at this exact terminal error. Skip
	// straight to it. The message matches the par2TerminalSubstrings entry the
	// solve loop's own "N damaged but only 0 recovery" return uses, so
	// par2_backoff.go classifies it terminal.
	if len(idx.Recovery) == 0 {
		deadCount := 0
		for _, segs := range pending {
			deadCount += len(segs)
		}
		if deadCount == 0 {
			deadCount = 1
		}
		p.logger.Info().Str("entry", entryName).Int("damaged", deadCount).
			Msg("par2: no recovery slices fetched, skipping file matching")
		return fmt.Errorf("%d damaged slices but only 0 recovery slices fetched/available", deadCount)
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
				return computeMD5_16k(ctx, fetchPosted, f)
			},
		}
	}
	var matches []par2.Match
	var skipped []par2.MatchSkip
	if cached, ok := par2MatchFromCache(idx, nzb.Par2Source, nzb.Par2Match); ok {
		matches = cached
		p.logger.Debug().Str("entry", entryName).Int("files", len(cached)).
			Msg("par2 repair: reusing cached posted-file match set (skipping MD5-16k tie-break fetches)")
	} else {
		matches, skipped, err = par2.MatchFiles(idx, posted)
		if err != nil {
			return fmt.Errorf("match posted files: %w", err)
		}
		if ref := par2MatchToCache(nzb.Par2Source, matches, skipped); ref != nil {
			if serr := u.SaveNZBPar2Match(nzbID, ref); serr != nil {
				p.logger.Warn().Err(serr).Str("entry", entryName).
					Msg("par2 repair: failed to persist posted-file match cache")
			} else {
				p.logger.Debug().Str("entry", entryName).Int("files", len(ref)).
					Msg("par2 repair: cached posted-file match set for future attempts")
			}
		}
	}
	if len(matches) == 0 {
		return fmt.Errorf("no posted file matched the PAR2 recovery set")
	}

	// Posted files MatchFiles couldn't attempt this pass because breaking
	// their length tie (or the residual MD5-16k pass) needed real bytes it
	// failed to fetch/hash. Their absence from `matches` is usually
	// TRANSIENT, not structural - a "no PAR2 coverage for this file" failure
	// below traceable to one of these must not be classified terminal.
	// Keyed by posted-file name (what downstream errors can name); the
	// stored value is the fetch error, so classifyMiss can tell a genuinely
	// permanent miss (a hard 430 confirmed across every provider) from a
	// retryable one (a timeout or short read).
	transientUnmatch := make(map[string]error, len(skipped))
	for _, s := range skipped {
		name := nzb.Par2Source[s.PostedIndex].Name
		transientUnmatch[name] = s.Err
		p.logger.Warn().Err(s.Err).Str("entry", entryName).Str("file", name).
			Msg("par2 repair: posted-file match skipped this pass (transient fetch/hash failure) - file has no PAR2 coverage until retry")
	}

	// Every posted file's segments -> its own name, for ALL source files
	// (not just matched ones) so a dead segment inside a transiently-
	// unmatched file can be traced back to it below.
	msgIDToPosted := make(map[string]string)
	for i := range nzb.Par2Source {
		for _, seg := range nzb.Par2Source[i].Segments {
			msgIDToPosted[seg.MessageID] = nzb.Par2Source[i].Name
		}
	}

	fetchers := make(map[[16]byte]*postedFileFetcher, len(matches))
	for _, m := range matches {
		file := nzb.Par2Source[m.PostedIndex]
		fetchers[m.FileID] = newPostedFileFetcher(ctx, fetchPosted, file, cacheSource, idx.Files[m.FileID].Length, p.logger.With().Str("entry", entryName).Str("file", file.Name).Logger())
	}
	// Measure the real article boundaries of every file whose refs are only
	// estimates, before anything below maps a byte range to slices: dead
	// segment ranges, patches and the intact-slice stream all depend on them.
	// One article per file, fetched in parallel - it is the first article
	// the stream reads anyway, and stays cached for it.
	// Par2PhaseProbing: the idle watchdog ignores it, like the STAT sweep.
	progress.SetPhase(Par2PhaseProbing)
	gp := pool.New().WithMaxGoroutines(max(1, u.ProcessingMaxConnections()))
	for _, f := range fetchers {
		gp.Go(f.resolveGeometry)
	}
	gp.Wait()
	progress.SetPhase(Par2PhaseFetchingRecovery)
	progress.Touch()
	msgIDRange := make(map[string]postedRange)
	for _, m := range matches {
		f := fetchers[m.FileID]
		for i, seg := range nzb.Par2Source[m.PostedIndex].Segments {
			msgIDRange[seg.MessageID] = postedRange{fileID: m.FileID, start: f.base[i], end: f.base[i] + f.segSizes[i]}
		}
	}

	// classifyMiss explains why a FileID in the PAR2 index ended up with no
	// posted-file fetcher, and whether that miss can ever resolve on a bare
	// retry. A FileDesc that no retained posted file could possibly be
	// (nothing in Par2Source shares its length or name - e.g. Outpost 5's
	// .7z.010, described by the index but never retained) is STRUCTURAL and
	// terminal. A FileDesc whose posted file IS in Par2Source but was
	// skipped for a transient fetch/hash failure, or is present but
	// unmatched for a length/tie reason, is retryable.
	classifyMiss := func(fileID [16]byte) (reason string, terminal bool) {
		fd := idx.Files[fileID]
		if fd == nil {
			return "unknown FileDesc", true
		}
		for i := range nzb.Par2Source {
			ps := nzb.Par2Source[i]
			if ps.Size != fd.Length && ps.Name != fd.Name {
				continue
			}
			if skipErr, ok := transientUnmatch[ps.Name]; ok {
				if nntp.IsArticleNotFoundError(skipErr) {
					return fmt.Sprintf("posted file %q: backing article confirmed missing across all providers", ps.Name), true
				}
				return fmt.Sprintf("posted file %q failed to fetch/hash during matching", ps.Name), false
			}
			return fmt.Sprintf("posted file %q retained but unmatched (length/tie)", ps.Name), false
		}
		return fmt.Sprintf("no retained posted file for FileDesc %q (len %d)", fd.Name, fd.Length), true
	}

	// STAT every article of every matched posted file up front, so the full
	// damaged-slice set is known before the first solve. Without this the
	// round loop below discovers dead intact articles one at a time, and
	// each discovery re-streams every intact slice in the recovery set
	// (gigabytes) to find the next few. Runs in Par2PhaseProbing so the idle
	// watchdog ignores it; best-effort, a failed sweep just falls back to
	// per-round discovery.
	progress.SetPhase(Par2PhaseProbing)
	statCtx2, statCancel2 := context.WithTimeout(ctx, par2PostedStatTimeout)
	statControls := make([]string, 0, 8)
	for _, segs := range pending {
		for _, d := range segs {
			if len(statControls) == cap(statControls) {
				break
			}
			statControls = append(statControls, d.MessageID)
		}
	}
	statMissing, statSwept := statPostedFileDamage(statCtx2, p.logger, u.StatSegments, matches, nzb.Par2Source, statControls, entryName)
	statCancel2()
	progress.SetPhase(Par2PhaseFetchingRecovery)
	progress.Touch()

	// Map every dead segment (by message ID - NOT by the logical/extracted
	// filename padding recorded it under, which may be an extracted-archive
	// member with no posted-file identity of its own) to its damaged slice
	// set within the posted file PAR2 actually protects.
	damagedSet := make(map[int64]struct{})
	var deadRefs []par2DeadRef
	for file, segs := range pending {
		for _, seg := range segs {
			rng, ok := msgIDRange[seg.MessageID]
			if !ok {
				pn, isPosted := msgIDToPosted[seg.MessageID]
				if _, transient := transientUnmatch[pn]; isPosted && transient {
					return fmt.Errorf("dead segment %s (file %q) is not part of any matched posted file (transient: posted file %q failed to fetch/hash during matching)", seg.MessageID, file, pn)
				}
				return fmt.Errorf("dead segment %s (file %q) is not part of any matched posted file", seg.MessageID, file)
			}
			if _, err := idx.DamagedSlices(rng.fileID, rng.start, rng.end); err != nil {
				return fmt.Errorf("map dead segment %s to slices: %w", seg.MessageID, err)
			}
			deadRefs = append(deadRefs, par2DeadRef{file: file, seg: seg, rng: rng})
		}
	}

	// A segment recorded dead may not be: playback records one on a single
	// failover pass, and the article can be back (or was only slow) by the
	// time this runs. Fetch those the STAT sweep saw alive and prove them
	// against the PAR2 slice checksums; each one that passes is patched with
	// its real bytes and needs no parity. When that covers every dead
	// segment, the solve - a full read of the release - is skipped.
	statMissingSet := make(map[string]struct{}, len(statMissing))
	for _, mid := range statMissing {
		statMissingSet[mid] = struct{}{}
	}
	progress.SetPhase(Par2PhaseProbing)
	deadRefs, err = p.healFetchableDeadSegments(ctx, nzbID, entryName, nzb, idx, fetchers, deadRefs, statMissingSet)
	progress.SetPhase(Par2PhaseFetchingRecovery)
	progress.Touch()
	if err != nil {
		return err
	}
	if len(deadRefs) == 0 {
		p.invalidateRepairedRanges(nzb, nzbID, entryName, pending)
		return nil
	}
	for _, dr := range deadRefs {
		slices, err := idx.DamagedSlices(dr.rng.fileID, dr.rng.start, dr.rng.end)
		if err != nil {
			return fmt.Errorf("map dead segment %s to slices: %w", dr.seg.MessageID, err)
		}
		for _, s := range slices {
			damagedSet[s] = struct{}{}
		}
	}

	// Fold in the STAT sweep's confirmed-missing articles - intact slices
	// that are actually gone too. No deadRef for these: they are being
	// reconstructed only to run the solve, not patched for playback (same as
	// the round loop's own newlyDamaged handling).
	overlayDamaged := len(damagedSet)
	for _, mid := range statMissing {
		rng, ok := msgIDRange[mid]
		if !ok {
			continue
		}
		slices, derr := idx.DamagedSlices(rng.fileID, rng.start, rng.end)
		if derr != nil {
			continue
		}
		for _, s := range slices {
			damagedSet[s] = struct{}{}
		}
	}
	if extra := len(damagedSet) - overlayDamaged; extra > 0 {
		if deadDiscovered != nil {
			*deadDiscovered += extra
		}
		p.logger.Info().Str("entry", entryName).Int("slices", extra).
			Msg("par2 repair: STAT damage sweep found dead slices beyond the overlay's record")
	}

	damaged := make([]int64, 0, len(damagedSet))
	for s := range damagedSet {
		damaged = append(damaged, s)
	}
	sort.Slice(damaged, func(i, j int) bool { return damaged[i] < damaged[j] })

	// Pre-solve fetcher-coverage check. par2.Repair's streaming pass reads
	// every non-damaged slice through jobSliceSource; a slice whose FileID
	// has no fetcher only errors out when that pass reaches it - after
	// potentially gigabytes of intact-slice reads. Resolve coverage now, at
	// zero bytes. A file missing a fetcher for a STRUCTURAL reason (nothing
	// retained could be it) aborts here with the same classified error
	// jobSliceSource.ReadSlice would have produced. A file missing a fetcher
	// only TRANSIENTLY (its match needed bytes that failed to fetch/hash, or
	// an unresolved length tie) is folded into the damaged set instead: the
	// PAR2 recovery set fully describes it, so the solver rebuilds it from
	// parity like any other damage. Mirrors par2_warm_sweep.go's coverage
	// gate.
	uncov, cerr := uncoveredIntactFiles(idx, damagedSet, fetchers)
	if cerr != nil {
		return fmt.Errorf("pre-solve coverage check: %w", cerr)
	}
	var folded int
	for _, fid := range uncov {
		reason, terminal := classifyMiss(fid)
		if terminal {
			p.logger.Warn().Str("entry", entryName).Int("files", len(uncov)).
				Msg("par2 repair: intact slices have no posted-file fetcher - aborting before solve (0 bytes read)")
			return missingFetcherErr(fid, classifyMiss)
		}
		// Transient miss: fold this file's slices into the damaged set so
		// the solver reconstructs them from parity instead of aborting.
		// Cost: one recovery slice per file slice - almost always tiny
		// (.nfo, one dead .rar part). The round-loop recovery-cap check
		// escalates to a genuine terminal verdict if the enlarged damage
		// exceeds the recovery budget.
		fd := idx.Files[fid]
		if fd == nil {
			return missingFetcherErr(fid, classifyMiss)
		}
		fs, ferr := idx.DamagedSlices(fid, 0, fd.Length)
		if ferr != nil {
			return fmt.Errorf("fold uncovered file %x: %w", fid, ferr)
		}
		for _, s := range fs {
			if _, ok := damagedSet[s]; !ok {
				damagedSet[s] = struct{}{}
				folded++
			}
		}
		p.logger.Info().Str("entry", entryName).Str("file", fd.Name).
			Str("reason", reason).Int("slices", len(fs)).
			Msg("par2 repair: folding transiently-uncovered posted file into damaged set for reconstruction")
	}
	if folded > 0 {
		damaged = damaged[:0]
		for s := range damagedSet {
			damaged = append(damaged, s)
		}
		sort.Slice(damaged, func(i, j int) bool { return damaged[i] < damaged[j] })
		if deadDiscovered != nil {
			*deadDiscovered += folded
		}
	}

	// Each round attempts the solve with the current damaged set; a hard,
	// confirmed-across-every-provider 430 (or an every-provider corrupt
	// copy, or a short read) on what was assumed to be an intact slice
	// means that slice's data is gone too - not a reason to retry the SAME
	// attempt, but a reason to fold it into the damaged set and retry the
	// solve, if recovery coverage still allows it. par2.Repair streams past
	// such slices (par2.ErrSliceUnavailable) and reports every one found in
	// the pass, so a round adds all of them at once - the STAT sweep above
	// under-reports (a provider can answer STAT 223 for an article whose
	// BODY is gone everywhere), so this is where most of them surface. Bounded by maxIntactRepairRounds; a release with damage
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

		// Top up recovery slice DATA until we hold at least k PARSED recovery
		// slices - discovering more damage mid-pass can push k past the
		// original name-only estimate that sized the first fetch, and a volume
		// that fetched but was mis-served yields nothing once ParseIndex skips
		// its bad-MD5 packets. See topUpParsedRecovery.
		idx, nextVolIdx, sources, err = topUpParsedRecovery(
			ctx, p.logger, fetch, vols, nextVolIdx, k, idx, sources,
			entryName, u.ProcessingMaxConnections(), progress)
		if err != nil {
			return err
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
		jobSource := &jobSliceSource{idx: idx, fetchers: fetchers, classifyMiss: classifyMiss, logger: p.logger}
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
		// Stop the pass once the unreadable intact slices outnumber the
		// recovery slices left over: the next round would need more than
		// are retained, so the rest of the read only confirms the verdict
		// (Under Reef S11E06 read 2 GB to find 45 against 5 retained).
		spare := min(int(available), par2.MaxRepairSlices) - k
		repaired, repairErr = par2.RepairWith(idx, damaged, recovery, sliceSource, par2.RepairOptions{MaxUnavailable: max(0, spare)})
		if repairErr == nil {
			break
		}

		// A cancelled context (idle-timeout, job-deadline, preemption,
		// shutdown) surfaces here via concurrentSliceSource.ReadSlice, which
		// now wraps ctx.Err(). This is a transient interruption, not a
		// structural repair failure: the intact-slice fetch was cut off
		// mid-stream, so NotFoundIndices() is meaningless (the dropped
		// slices weren't 430s) and expanding the damaged set / retrying
		// would burn a round reconstructing slices that aren't actually
		// dead. Bail out with the raw error so the backoff classifier sees
		// context.Canceled/DeadlineExceeded (absent from par2TerminalSubstrings)
		// and reschedules instead of marking the entry unrepairable.
		if errors.Is(repairErr, context.Canceled) || errors.Is(repairErr, context.DeadlineExceeded) {
			p.logger.Warn().
				Str("entry", entryName).
				Int("round", round+1).
				Err(repairErr).
				Msg("par2 repair: intact-slice fetch interrupted by context cancellation; treating as transient")
			return repairErr
		}

		notFound := sliceSource.NotFoundIndices()
		newlyDamaged := make([]int64, 0, len(notFound))
		for _, ni := range notFound {
			if _, already := damagedPos[ni]; !already {
				newlyDamaged = append(newlyDamaged, ni)
			}
		}
		if len(newlyDamaged) > 0 && deadDiscovered != nil {
			// Rounds never overlap (each round's damagedPos guards against
			// re-adding an already-known slice), so a running sum across
			// rounds is exactly the distinct-slice count - no set needed.
			*deadDiscovered += len(newlyDamaged)
		}
		// Report WHY the slices were unreadable, not just how many. A hard 430
		// and a short decode are folded into the damaged set identically but
		// mean opposite things (see deadCause), and both used to be reported
		// as "confirmed missing across every provider" - a false statement for
		// the short-read case, and one that made a geometry bug on intact data
		// indistinguishable from genuine provider damage.
		confirmedMissing, shortRead, corrupt := sliceSource.DeadCauseCounts()
		if len(newlyDamaged) == 0 || round >= maxIntactRepairRounds-1 {
			type cause struct {
				n    int
				text string
			}
			var causes []cause
			for _, c := range []cause{
				{confirmedMissing, "confirmed missing across every provider"},
				{corrupt, "corrupt on every provider"},
				{shortRead, "decoded shorter than their recorded size"},
			} {
				if c.n > 0 {
					causes = append(causes, c)
				}
			}
			switch len(causes) {
			case 0:
			case 1:
				return fmt.Errorf("repair: %d intact slice(s) %s: %w", causes[0].n, causes[0].text, repairErr)
			default:
				parts := make([]string, len(causes))
				for i, c := range causes {
					parts[i] = fmt.Sprintf("%d %s", c.n, c.text)
				}
				return fmt.Errorf("repair: %d intact slice(s) unreadable - %s: %w", len(notFound), strings.Join(parts, ", "), repairErr)
			}
			return fmt.Errorf("repair: %w", repairErr)
		}
		p.logger.Info().
			Str("entry", entryName).
			Int("newly_damaged", len(newlyDamaged)).
			Int("confirmed_missing", confirmedMissing).
			Int("short_read", shortRead).
			Int("corrupt", corrupt).
			Str("providers", nntp.DescribeOutcomes(nntp.FailoverOutcomes(repairErr))).
			Int("round", round+1).
			Bool("stat_sweep_ran", statSwept).
			Msg("par2 repair: intact slice(s) unreadable; expanding damaged set and retrying")
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
		start, end, werr := readerPatchWindow(nzb, dr)
		if werr != nil {
			return fmt.Errorf("patch window for %s segment %d: %w", dr.file, dr.seg.Index, werr)
		}
		data, err := extractPostedRange(idx, repairedByIndex, dr.rng.fileID, start, end)
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

	p.invalidateRepairedRanges(nzb, nzbID, entryName, pending)
	return nil
}

// par2DeadRef is one recorded-dead segment and the posted-file byte range it
// covers.
type par2DeadRef struct {
	file string
	seg  overlay.DeadSegment
	rng  postedRange
}

// healFetchableDeadSegments patches every dead segment whose article can be
// fetched and proven intact, and returns the ones still needing parity.
//
// Proof is the PAR2 IFSC of every slice the segment's range touches, read
// fresh from Usenet (never the DFS cache, which may hold its zero-fill): a
// slice that passes carries exactly the posted bytes, so the patch cut from
// those slices is what a parity reconstruction would have produced. Only
// segments on exactly-measured geometry are tried - with estimated
// boundaries the slice reads are themselves suspect. Anything that fails, for
// any reason, stays for the solve exactly as before. Seen live 2026-09-22:
// all nine "dead" segments of Deep in Orbit S02E01 were intact on three
// providers, and the solve read 313 MB to fail on them.
func (p *Par2Repair) healFetchableDeadSegments(ctx context.Context, nzbID, entryName string, nzb *storage.NZB, idx *par2.Index, fetchers map[[16]byte]*postedFileFetcher, deadRefs []par2DeadRef, statMissing map[string]struct{}) ([]par2DeadRef, error) {
	u := p.manager.usenet
	remaining := deadRefs[:0:0]
	healed := 0
	for _, dr := range deadRefs {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		f := fetchers[dr.rng.fileID]
		if _, missing := statMissing[dr.seg.MessageID]; missing || f == nil || !f.exact {
			remaining = append(remaining, dr)
			continue
		}
		start, end, werr := readerPatchWindow(nzb, dr)
		if werr != nil {
			p.logger.Warn().Err(werr).Str("entry", entryName).Str("file", dr.file).Int("segment", dr.seg.Index).
				Msg("par2 repair: cannot place a patch for this dead segment; leaving it to the solve")
			remaining = append(remaining, dr)
			continue
		}
		data, ok := readVerifiedRange(idx, f, dr.rng, start, end)
		if !ok {
			remaining = append(remaining, dr)
			continue
		}
		if err := u.OverlayWritePatch(nzbID, dr.file, dr.seg.Index, data); err != nil {
			// e.g. a volume's first article, whose reader slot is shorter
			// than the posted article. Leave it to the solve, which meets
			// the same refusal at the end as it always has, rather than
			// abort a pass that can still repair the other segments.
			p.logger.Warn().Err(err).Str("entry", entryName).Str("file", dr.file).Int("segment", dr.seg.Index).
				Msg("par2 repair: verified bytes for a dead segment could not be patched; leaving it to the solve")
			remaining = append(remaining, dr)
			continue
		}
		u.ClearFailedFile(nzbID, dr.file)
		healed++
	}
	if healed > 0 {
		p.logger.Info().Str("entry", entryName).Int("healed", healed).Int("remaining", len(remaining)).
			Msg("par2 repair: dead segment(s) fetched intact and verified against PAR2 checksums; patched without parity")
	}
	return remaining, nil
}

// readVerifiedRange reads [start, end) of f - a window inside the dead
// article's posted range rng - fresh from Usenet by way of the whole PAR2
// slices covering it, and returns it only if every one of those slices
// passes its IFSC checksum.
func readVerifiedRange(idx *par2.Index, f *postedFileFetcher, rng postedRange, start, end int64) ([]byte, bool) {
	slices, err := idx.DamagedSlices(rng.fileID, rng.start, rng.end)
	if err != nil || len(slices) == 0 {
		return nil, false
	}
	verified := make(map[int64][]byte, len(slices))
	for _, s := range slices {
		fileID, local, err := idx.SliceLocation(s)
		if err != nil || fileID != rng.fileID {
			return nil, false
		}
		data, _, err := f.readRange(local*idx.SliceSize, idx.SliceSize, false)
		if err != nil {
			return nil, false
		}
		if ok, err := idx.VerifySliceChecksum(s, data); err != nil || !ok {
			return nil, false
		}
		verified[s] = data
	}
	out, err := extractPostedRange(idx, verified, rng.fileID, start, end)
	if err != nil {
		return nil, false
	}
	return out, true
}

// readerPatchWindow returns the byte range of dr's posted article that the
// overlay patch must hold: the reader's slot for that segment, which is
// [SegmentDataStart, +Bytes) inside the article.
//
// They differ whenever a posted article does not map one-to-one onto one
// extracted file's bytes - a RAR volume's first article carries the volume
// header ahead of the member's data, and its last can carry the next
// member's. Writing the whole article there was refused by
// overlay.Store.WritePatch ("repair and reader segment geometry disagree"),
// so a repaired first article of a volume could never be stored, and with
// the heal pass it would have failed the whole job.
func readerPatchWindow(nzb *storage.NZB, dr par2DeadRef) (start, end int64, err error) {
	file := nzb.GetFileByName(dr.file)
	if file == nil {
		return 0, 0, fmt.Errorf("file %q not in the NZB record", dr.file)
	}
	if dr.seg.Index < 0 || dr.seg.Index >= len(file.Segments) {
		return 0, 0, fmt.Errorf("segment %d beyond the file's %d segments", dr.seg.Index, len(file.Segments))
	}
	seg := file.Segments[dr.seg.Index]
	if seg.MessageID != dr.seg.MessageID {
		return 0, 0, fmt.Errorf("segment %d is %s in the NZB record, not %s", dr.seg.Index, seg.MessageID, dr.seg.MessageID)
	}
	want := seg.Bytes
	if want <= 0 {
		want = dr.rng.end - dr.rng.start - seg.SegmentDataStart
	}
	start = dr.rng.start + seg.SegmentDataStart
	end = start + want
	if seg.SegmentDataStart < 0 || start < dr.rng.start || end > dr.rng.end {
		return 0, 0, fmt.Errorf("reader slot [%d,+%d) does not fit the article's %d bytes", seg.SegmentDataStart, want, dr.rng.end-dr.rng.start)
	}
	return start, end, nil
}

// invalidateRepairedRanges drops stale copies of the ranges a repair just
// patched. The failedFiles un-poison the patch writers do only clears the
// in-memory permanent-failure record. Two caches can still hold a stale
// zero-fill copy of those ranges, from a prior playback that padded them
// before the repair produced real bytes - and neither consults the overlay
// patch on a plain cache hit (PatchBytes is only reached on a live
// article-fetch failure):
//  1. The persistent DFS mount cache - drop the patched output ranges so the
//     next read re-streams them (now served from the patch).
//  2. Any warm usenet streaming reader whose in-memory segment cache still
//     has the padded segment OnDisk - tear it down so the next Stream builds
//     a fresh reader. Idle-safe: a no-op while a viewer still holds the file.
func (p *Par2Repair) invalidateRepairedRanges(nzb *storage.NZB, nzbID, entryName string, pending map[string][]overlay.DeadSegment) {
	if fw := p.dfsCacheForgetter(); fw != nil {
		for file, rngs := range buildDeadOutputRanges(nzb, pending) {
			for _, r := range rngs {
				fw.ForgetCachedRange(entryName, file, r.start, r.end-r.start)
			}
		}
	}
	for file := range pending {
		p.manager.usenet.EvictCache(nzbID, file)
	}
}

// par2JobTimeoutFor is the deadline for one PAR2 job on a release of
// releaseBytes: par2JobTimeout plus par2JobPasses full reads at
// par2JobPassFloor, capped at par2JobTimeoutMax. A flat 20 minutes cancelled
// legitimate repairs of large releases mid-solve - Tale of Castles S08E05
// (6.3 GB, one discovery pass and one solve pass) needs 25 minutes - and a
// deadline counts as transient, so they retried and were cancelled forever.
// Stalls stay bounded by the idle watchdog (par2JobIdleTimeout), which is
// what the flat bound was really for.
func par2JobTimeoutFor(releaseBytes int64) time.Duration {
	d := par2JobTimeout
	if releaseBytes > 0 {
		d += time.Duration(par2JobPasses*releaseBytes/par2JobPassFloor) * time.Second
	}
	return min(d, par2JobTimeoutMax)
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
	vols, indexFiles = censusNumberedVolumes(vols, indexFiles)
	sort.Slice(vols, func(i, j int) bool { return vols[i].ref.Size < vols[j].ref.Size })
	return vols, indexFiles
}

// censusNumberedVolumes moves recovery volumes whose names carry no slice
// range (see par2NumberedVolPattern) out of indexFiles. Treated as index
// files, a release made only of those reported 0 recovery slices and was
// refused as unrepairable before anything was fetched.
//
// Their slice counts are estimated from sizes: every PAR2 file repeats the
// same metadata, so the smallest .par2 is that metadata alone, and the
// smallest step above it is one recovery slice plus its packet header. The
// estimate rounds up; it only sizes the fetch and the early capacity gate,
// and the solve gates on the slices actually parsed (topUpParsedRecovery). A
// numbered file no larger than the smallest stays an index file.
func censusNumberedVolumes(vols []par2Volume, indexFiles []storage.Par2FileRef) ([]par2Volume, []storage.Par2FileRef) {
	base := int64(-1)
	var numbered []storage.Par2FileRef
	for _, f := range indexFiles {
		if !strings.HasSuffix(strings.ToLower(f.Name), ".par2") {
			continue
		}
		if base < 0 || f.Size < base {
			base = f.Size
		}
		if par2NumberedVolPattern.MatchString(f.Name) {
			numbered = append(numbered, f)
		}
	}
	if len(numbered) == 0 {
		return vols, indexFiles
	}
	unit := int64(0)
	for _, f := range numbered {
		if d := f.Size - base; d > 0 && (unit == 0 || d < unit) {
			unit = d
		}
	}
	if unit == 0 {
		return vols, indexFiles
	}
	isVol := make(map[string]bool, len(numbered))
	for _, f := range numbered {
		n := (f.Size - base + unit - 1) / unit
		if n <= 0 {
			continue
		}
		isVol[f.Name] = true
		vols = append(vols, par2Volume{ref: f, count: uint32(n)})
	}
	kept := indexFiles[:0:0]
	for _, f := range indexFiles {
		if !isVol[f.Name] {
			kept = append(kept, f)
		}
	}
	return vols, kept
}

// statRecoveryVolumes replaces censusPar2Volumes' filename-derived paper
// count with what the provider will actually serve: it STATs the segments of
// the smallest recovery volumes - header only, no body download - and drops
// any volume with a confirmed-missing article from the returned list. A
// volume is unusable if ANY of its articles is gone (a partial RecvSlic
// packet can't be parsed), so one 430 condemns the whole volume.
//
// Only the volumes that could actually be reached before the MaxRepairSlices
// cap (smallest-first, plus one slack volume) are probed - fetchMoreVolumes
// walks the same order and stops at the cap, so STAT-ing the long tail would
// be wasted round trips. Volumes past that prefix keep their paper count, so
// the returned total stays a valid upper bound overall while being accurate
// for the part a repair would actually use.
//
// Best-effort: a whole-batch STAT error returns vols unchanged; an ambiguous
// per-article error (connection issue, never a definitive not-found) leaves
// its volume in place. Only a definitive article-not-found removes one.
func statRecoveryVolumes(
	ctx context.Context,
	logger zerolog.Logger,
	stat func(context.Context, []string) ([]nntp.StatResult, error),
	vols []par2Volume,
	entryName string,
) []par2Volume {
	if len(vols) == 0 {
		return vols
	}

	var cum uint32
	cut := 0
	for i, v := range vols {
		cum += v.count
		cut = i + 1
		if cum >= uint32(par2.MaxRepairSlices) {
			if i+1 < len(vols) {
				cut = i + 2 // one slack volume
			}
			break
		}
	}
	candidates := vols[:cut]

	var msgIDs []string
	for _, v := range candidates {
		for _, seg := range v.ref.Segments {
			msgIDs = append(msgIDs, seg.MessageID)
		}
	}
	if len(msgIDs) == 0 {
		return vols
	}

	start := time.Now()
	results, err := stat(ctx, msgIDs)
	if err != nil {
		logger.Warn().Err(err).Str("entry", entryName).
			Msg("par2: recovery-volume STAT pre-census failed; using name-only census")
		return vols
	}

	byID := make(map[string]nntp.StatResult, len(results))
	for _, r := range results {
		byID[r.MessageID] = r
	}
	dead := make(map[string]struct{})
	for _, v := range candidates {
		for _, seg := range v.ref.Segments {
			if r, ok := byID[seg.MessageID]; ok && !r.Available && nntp.IsArticleNotFoundError(r.Error) {
				dead[v.ref.Name] = struct{}{}
				break
			}
		}
	}

	var availBefore, availAfter uint32
	for _, v := range vols {
		availBefore += v.count
	}
	if len(dead) == 0 {
		logger.Debug().Str("entry", entryName).
			Int("volumes_probed", len(candidates)).
			Uint32("available", availBefore).
			Dur("duration", time.Since(start)).
			Msg("par2: recovery-volume STAT pre-census - every probed volume alive")
		return vols
	}

	alive := make([]par2Volume, 0, len(vols)-len(dead))
	for _, v := range vols {
		if _, gone := dead[v.ref.Name]; gone {
			continue
		}
		availAfter += v.count
		alive = append(alive, v)
	}
	logger.Info().
		Str("entry", entryName).
		Int("volumes_probed", len(candidates)).
		Int("volumes_dead", len(dead)).
		Int("volumes_alive", len(candidates)-len(dead)).
		Uint32("available_before", availBefore).
		Uint32("available_after", availAfter).
		Dur("duration", time.Since(start)).
		Msg("par2: recovery-volume STAT pre-census dropped expired volumes")
	return alive
}

// statPostedFileDamage STATs every article of every matched posted file and
// returns the message IDs confirmed missing (a definitive article-not-found
// across every provider). It lets runRepair seed the full damaged-slice set
// before the first solve, instead of discovering dead articles one round at
// a time - and every discovery round re-streams every intact slice in the
// recovery set (gigabytes) just to find the next handful. A dead article can
// belong to ANY posted file, not only one with overlay-recorded damage, so
// the sweep covers the whole matched set.
//
// The bool return is whether the sweep actually completed: false means a
// whole-batch STAT error (the caller falls back to per-round discovery,
// exactly as before this sweep existed). An individual ambiguous per-article
// error is simply not reported as missing - that segment is left for the
// round loop, same as any transient miss.
func statPostedFileDamage(
	ctx context.Context,
	logger zerolog.Logger,
	stat func(context.Context, []string) ([]nntp.StatResult, error),
	matches []par2.Match,
	par2Source []storage.PostedFileRef,
	controls []string,
	entryName string,
) (missing []string, completed bool) {
	// Calibrate first. Providers answer STAT 223 for articles whose body is
	// gone everywhere - eweka did so for all 15 dead articles of Game of
	// Castles S08E05, six others for Under Reef S11E06 - which makes a
	// sweep that reports nothing missing worthless. controls are this
	// release's own recorded-dead articles: playback already proved their
	// bodies unfetchable, so STAT must report them missing. When it does
	// not, the sweep is skipped rather than believed, and the first solve
	// pass finds the damage instead (it collects every unreadable slice).
	if len(controls) > 0 {
		results, err := stat(ctx, controls)
		if err != nil {
			logger.Warn().Err(err).Str("entry", entryName).
				Msg("par2: STAT control probe failed; damage will be found per-round instead")
			return nil, false
		}
		for _, r := range results {
			if r.Available || !nntp.IsArticleNotFoundError(r.Error) {
				logger.Warn().
					Str("entry", entryName).
					Str("message_id", r.MessageID).
					Msg("par2: a provider reports a known-dead article as present; skipping the STAT damage sweep for this release")
				return nil, false
			}
		}
	}

	var msgIDs []string
	for _, m := range matches {
		if m.PostedIndex < 0 || m.PostedIndex >= len(par2Source) {
			continue
		}
		for _, seg := range par2Source[m.PostedIndex].Segments {
			msgIDs = append(msgIDs, seg.MessageID)
		}
	}
	if len(msgIDs) == 0 {
		return nil, true
	}

	start := time.Now()
	results, err := stat(ctx, msgIDs)
	if err != nil {
		logger.Warn().Err(err).Str("entry", entryName).
			Msg("par2: posted-file STAT damage sweep failed; damage will be found per-round instead")
		return nil, false
	}
	for _, r := range results {
		if !r.Available && nntp.IsArticleNotFoundError(r.Error) {
			missing = append(missing, r.MessageID)
		}
	}
	logger.Info().
		Str("entry", entryName).
		Int("segments_probed", len(msgIDs)).
		Int("segments_missing", len(missing)).
		Int("controls", len(controls)).
		Dur("duration", time.Since(start)).
		Msg("par2: posted-file STAT damage sweep complete")
	return missing, true
}

// volumeFetchResult holds one parallel recovery-volume fetch outcome.
type volumeFetchResult struct {
	source   par2.Source
	count    uint32 // slice count from the par2Volume
	err      error
	notFound bool // true when err is article-not-found
}

// topUpParsedRecovery fetches more recovery volumes until at least `want`
// PARSED recovery slices (len(idx.Recovery)) are held, re-parsing the index
// after each batch that added a source.
//
// The measure is deliberately the parsed count, never the name-advertised
// slice count fetchMoreVolumes accumulates: a volume can fetch successfully
// yet contribute zero usable recovery slices once ParseIndex skips its
// bad-MD5 packets (a mis-served article - the divergence Fix D introduced by
// making the parser resilient instead of aborting). Gating on the advertised
// count lets that inflated number satisfy the gate while the real recovery
// set is still short, so the caller's "N damaged but only M recovery" wall
// fires with fetchable volumes still on the list.
//
// Each call to fetchMoreVolumes resumes at nextVolIdx, so no source is
// re-fetched. The loop stops when a pass adds no new source - the volume list
// is exhausted, or fetchMoreVolumes' own consecutive-not-found abort tripped.
// nextVolIdx advances by at least one whenever a source is added, so the loop
// is bounded by len(vols). A still-short recovery set on return is not an
// error here; the caller turns it into the terminal verdict.
func topUpParsedRecovery(
	ctx context.Context,
	logger zerolog.Logger,
	fetch articleFetchFunc,
	vols []par2Volume,
	nextVolIdx int,
	want int,
	idx *par2.Index,
	sources []par2.Source,
	entryName string,
	maxConc int,
	progress *par2JobProgressState,
) (*par2.Index, int, []par2.Source, error) {
	for want > len(idx.Recovery) {
		var added int
		var err error
		nextVolIdx, _, sources, added, err = fetchMoreVolumes(
			ctx, logger, fetch, vols, nextVolIdx,
			uint32(want), uint32(len(idx.Recovery)), sources, entryName, maxConc)
		if err != nil {
			return idx, nextVolIdx, sources, fmt.Errorf("fetch recovery volumes: %w", err)
		}
		if progress != nil {
			progress.SetRecoveryVolsFetched(nextVolIdx)
		}
		if added == 0 {
			break
		}
		idx, err = par2.ParseIndex(sources)
		if err != nil {
			return idx, nextVolIdx, sources, fmt.Errorf("parse PAR2 index: %w", err)
		}
		logSkippedPar2Packets(logger, entryName, idx)
		if progress != nil {
			progress.SetRecoverySlices(len(idx.Recovery), want)
		}
	}
	return idx, nextVolIdx, sources, nil
}

// computeRecoveryBatch selects the smallest prefix of vols whose cumulative
// slice count >= shortfall, plus one slack volume when available.
func computeRecoveryBatch(vols []par2Volume, shortfall uint32) []par2Volume {
	if len(vols) == 0 || shortfall == 0 {
		return nil
	}
	var cum uint32
	end := 0
	for i, v := range vols {
		cum += v.count
		end = i + 1
		if cum >= shortfall {
			if i+1 < len(vols) {
				end = i + 2
			}
			break
		}
	}
	return vols[:end]
}

// fetchMoreVolumes fetches recovery volumes in waves of up to maxConc
// parallel goroutines. Pre-computes the smallest target batch whose
// cumulative slice count satisfies the shortfall plus one slack volume,
// then processes it in maxConc-sized waves. Between waves, results are
// walked in volume order to maintain a consecutive-not-found streak:
// when the streak reaches par2RecoveryMaxConsecutiveNotFound the remaining
// waves are skipped. Transport/transient errors reset the streak,
// matching the prior sequential implementation's semantics.
//
// Returns the updated position/count/sources so a later call - after
// discovering MORE damage than originally estimated (see runRepair's retry
// loop) - can pick up exactly where an earlier call left off, without
// re-fetching anything already in sources. The added return is how many new
// entries were appended to sources this call - callers only need to
// re-parse the PAR2 index when it's non-zero.
func fetchMoreVolumes(
	ctx context.Context,
	logger zerolog.Logger,
	fetch articleFetchFunc,
	vols []par2Volume,
	nextVolIdx int,
	needed uint32,
	fetchedSlices uint32,
	sources []par2.Source,
	entryName string,
	maxConc int,
) (int, uint32, []par2.Source, int, error) {
	remaining := vols[nextVolIdx:]
	var shortfall uint32
	if needed > fetchedSlices {
		shortfall = needed - fetchedSlices
	}
	batch := computeRecoveryBatch(remaining, shortfall)
	if len(batch) == 0 {
		return nextVolIdx, fetchedSlices, sources, 0, nil
	}

	if maxConc < 1 {
		maxConc = 1
	}

	start := time.Now()
	consecutiveNotFound := 0
	added := 0
	var volsAttempted, volsNotFound, volsErrored int

	for waveStart := 0; waveStart < len(batch); {
		waveEnd := waveStart + maxConc
		if waveEnd > len(batch) {
			waveEnd = len(batch)
		}
		wave := batch[waveStart:waveEnd]
		volsAttempted += len(wave)

		results := make([]volumeFetchResult, len(wave))
		p := pool.New().WithMaxGoroutines(maxConc)

		for i, v := range wave {
			p.Go(func() {
				if ctx.Err() != nil {
					results[i] = volumeFetchResult{count: v.count, err: ctx.Err()}
					return
				}
				data, err := fetchPar2FileConcurrent(ctx, fetch, v.ref, maxConc/len(wave))
				if err != nil {
					if nntp.IsArticleNotFoundError(err) {
						results[i] = volumeFetchResult{count: v.count, err: err, notFound: true}
						return
					}
					results[i] = volumeFetchResult{count: v.count, err: err}
					return
				}
				results[i] = volumeFetchResult{
					source: par2.Source{Name: v.ref.Name, Data: data},
					count:  v.count,
				}
			})
		}

		p.Wait()

		// Walk results in volume order: maintain the consecutive-not-found
		// streak across wave boundaries, same semantics as the prior
		// sequential loop.
		abort := false
		for _, r := range results {
			if abort {
				break
			}
			switch {
			case r.notFound:
				consecutiveNotFound++
				volsNotFound++
				if consecutiveNotFound >= par2RecoveryMaxConsecutiveNotFound {
					abort = true
				}
			case r.err != nil:
				consecutiveNotFound = 0
				volsErrored++
			default:
				consecutiveNotFound = 0
				sources = append(sources, r.source)
				fetchedSlices += r.count
				added++
			}
		}

		waveStart = waveEnd
		nextVolIdx += len(wave)

		if abort || fetchedSlices >= needed {
			break
		}
	}

	logger.Info().
		Str("entry", entryName).
		Dur("duration", time.Since(start)).
		Int("volumes_attempted", volsAttempted).
		Int("volumes_fetched", added).
		Int("volumes_not_found", volsNotFound).
		Int("volumes_errored", volsErrored).
		Uint32("slices_fetched", fetchedSlices).
		Uint32("needed", needed).
		Msg("par2: recovery volume fetch complete")

	return nextVolIdx, fetchedSlices, sources, added, nil
}

// fetchWholePar2File downloads and concatenates every segment of a retained
// PAR2 file (index or recovery volume). PAR2 files are posted directly (not
// extracted from an archive), so their segments concatenate straight into
// the file's real bytes with no trimming.
//
// An article confirmed missing is left out rather than failing the file: a
// PAR2 file is a run of self-checking packets, so a dead article costs only
// the packets it overlaps, and par2.ParseIndex resumes at the next packet
// after the gap. Failing the whole file threw away every recovery slice in
// it - Under Reef S11E06's volumes each span 88-176 articles. The file is
// reported not-found only when every article is missing. Any other error
// (a timeout, a cancelled job) still fails it: that says nothing about the
// article.
func fetchWholePar2File(ctx context.Context, fetch articleFetchFunc, f storage.Par2FileRef) ([]byte, error) {
	return fetchPar2FileConcurrent(ctx, fetch, f, par2FileFetchConcurrency)
}

// fetchPar2FileConcurrent is fetchWholePar2File with its articles fetched
// workers at a time: a volume can run to 176 articles, and each dead one
// costs a full failover round (6-9 s seen on Under Reef S11E06), which one
// at a time made a two-volume fetch take nearly seven minutes.
func fetchPar2FileConcurrent(ctx context.Context, fetch articleFetchFunc, f storage.Par2FileRef, workers int) ([]byte, error) {
	type result struct {
		data []byte
		err  error
	}
	results := make([]result, len(f.Segments))
	fctx, cancel := context.WithCancel(ctx)
	defer cancel()
	p := pool.New().WithMaxGoroutines(max(1, workers))
	for i, seg := range f.Segments {
		p.Go(func() {
			if fctx.Err() != nil {
				results[i] = result{err: fctx.Err()}
				return
			}
			segCtx, segCancel := context.WithTimeout(fctx, par2ArticleFetchTimeout)
			data, err := fetch(segCtx, seg.MessageID)
			segCancel()
			if err != nil && !nntp.IsArticleNotFoundError(err) {
				cancel() // a real failure fails the file: stop the rest
			}
			results[i] = result{data: data, err: err}
		})
	}
	p.Wait()

	out := make([]byte, 0, f.Size)
	missing := 0
	var firstMissing error
	for i, r := range results {
		if r.err == nil {
			out = append(out, r.data...)
			continue
		}
		if nntp.IsArticleNotFoundError(r.err) && ctx.Err() == nil {
			if missing == 0 {
				firstMissing = fmt.Errorf("fetch %s: %w", f.Segments[i].MessageID, r.err)
			}
			missing++
			continue
		}
		// The first real failure, not a cancellation it caused.
		if errors.Is(r.err, context.Canceled) && ctx.Err() == nil {
			continue
		}
		return nil, fmt.Errorf("fetch %s: %w", f.Segments[i].MessageID, r.err)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if missing > 0 && missing == len(f.Segments) {
		return nil, firstMissing
	}
	return out, nil
}

// computeMD5_16k fetches just enough leading segments of a posted file to
// hash its first 16KB (or the whole file, if shorter) - MatchFiles only
// calls this for a file whose length ties with another candidate.
func computeMD5_16k(ctx context.Context, fetch postedFetchFunc, f storage.PostedFileRef) ([16]byte, error) {
	fetcher := newPostedFileFetcher(ctx, fetch, f, nil, 0, zerolog.Nop())
	// The persisted Size is an estimate unless the refs are real. The first
	// article's yEnc header carries the file's true size, and a file shorter
	// than 16 KB is hashed whole - hashing the estimate instead zero-pads a
	// small file (an .sfv, an .nfo) or reads past its only article.
	fetcher.resolveGeometry()
	n := int64(md5_16kSize)
	if fetcher.length < n {
		n = fetcher.length
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
	ctx      context.Context
	fetch    postedFetchFunc
	length   int64
	segs     []storage.Par2SegmentRef
	base     []int64 // base[i] = starting byte offset of segs[i] within the file
	segSizes []int64 // segSizes[i] = exact decoded byte count for segs[i]

	// trueLen is the file's real length when known (FileDesc.Length, or the
	// first article's =ybegin size once resolveGeometry has read it), else 0.
	trueLen int64
	// exact is true when base/segSizes are the real article boundaries -
	// real seed provenance (see segGeometryExact) or resolveGeometry measured
	// them - rather than scaled estimates.
	exact  bool
	logger zerolog.Logger

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

// trueLen, when greater than the posted file's own declared Size, overrides
// it as the fetcher's readable bound. The posted-file Size is a yEnc-decoded
// estimate that can under-count the final article by a few KB; when that
// happens ReadRange treats real trailing bytes as past-EOF and zero-pads
// them, corrupting whichever intact slice they fall in. Pass 0 to keep the
// declared estimate (e.g. computeMD5_16k, which only ever reads the leading
// 16KB and has no FileDesc to compare against).
// exactSegGeometry returns per-segment base offsets and decoded sizes.
// When trueLen (FileDesc.Length) is available, the seed segment's
// persisted Bytes is positive, and the seed segment's provenance is real
// (segs[0].Real - an actual yEnc probe, not an estimate), interior offsets
// use exact arithmetic (i * seedSize, with the final segment =
// trueLen - (n-1)*seedSize), eliminating the cumulative drift of the yEnc
// 0.97 overhead estimate. When trueLen is available but provenance is only
// estimated, sizes are accumulated from the persisted Par2SegmentRef.Bytes
// estimates but scaled so their sum still lands on trueLen, rather than
// trusting the raw per-segment estimates to add up correctly on their own.
// Otherwise (no trueLen) the persisted Par2SegmentRef.Bytes estimates are
// accumulated as-is - a graceful fallback for callers that lack FileDesc
// (computeMD5_16k passes trueLen=0).
// segGeometryExact reports whether exactSegGeometry uses the exact uniform
// branch for segs: real seed provenance, and a final article no larger than
// the seed.
func segGeometryExact(segs []storage.Par2SegmentRef, trueLen int64) bool {
	n := len(segs)
	if n == 0 {
		return false
	}
	seedSeg := segs[0].Bytes
	lastSeg := trueLen - int64(n-1)*seedSeg
	return trueLen > 0 && seedSeg > 0 && lastSeg > 0 && segs[0].Real && lastSeg <= seedSeg
}

func exactSegGeometry(segs []storage.Par2SegmentRef, trueLen int64, logger zerolog.Logger) (bases, sizes []int64) {
	n := len(segs)
	bases = make([]int64, n)
	sizes = make([]int64, n)
	if n == 0 {
		return
	}
	seedSeg := segs[0].Bytes
	lastSeg := trueLen - int64(n-1)*seedSeg
	exact := trueLen > 0 && seedSeg > 0 && lastSeg > 0 && segs[0].Real

	// A file's final article is never LARGER than a full one, so lastSeg >
	// seedSeg is proof that the seed size and FileDesc.Length disagree -
	// the uniform-interior assumption this branch rests on does not hold, and
	// committing to it would put every interior boundary in the wrong place.
	//
	// This was reachable, not theoretical: realPar2SegmentRefs used to accept
	// the yEnc header's declared total whenever it was within +/-1.5 x
	// segmentSize of n x segmentSize and then marked every ref Real, which
	// permitted a final segment up to 2.5x the seed. It now requires a final
	// article no larger than a full one, but records written before that keep
	// their refs, and trueLen arrives from a different authority
	// (FileDesc.Length) that nothing else cross-checks.
	//
	// The cost of getting this wrong is not a clean failure: an overstated
	// final segment makes readRange demand more bytes than the article holds,
	// which surfaces as ErrSegmentShort and gets folded into the damaged set
	// as if the posting were truncated - reconstructing intact data from
	// parity, inflating k toward the 64-slice cap, and potentially reaching a
	// terminal "unrepairable" verdict on a healthy release. Fall back to the
	// scaled accumulate, which anchors on trueLen without assuming uniformity.
	if exact && lastSeg > seedSeg {
		logger.Warn().
			Int64("seed_segment", seedSeg).
			Int64("implied_last_segment", lastSeg).
			Int64("trueLen", trueLen).
			Int("segments", n).
			Msg("exactSegGeometry: implied final segment exceeds the seed article size - persisted geometry and FileDesc.Length disagree; falling back to scaled accumulate")
		exact = false
	}

	if exact {
		for i := range segs {
			bases[i] = int64(i) * seedSeg
			sizes[i] = seedSeg
		}
		sizes[n-1] = lastSeg
		// Logged for the same reason the scaled branch below is: this is the
		// branch that hands the repair its byte offsets, and it was previously
		// the silent one - the safe path was visible and the load-bearing one
		// was not.
		logger.Debug().
			Int64("seed_segment", seedSeg).
			Int64("last_segment", lastSeg).
			Int64("trueLen", trueLen).
			Int("segments", n).
			Msg("exactSegGeometry: using exact uniform-interior geometry (real seed provenance)")
	} else if trueLen > 0 {
		// Scaled accumulate: anchor estimated sizes to trueLen (FileDesc.Length)
		// to prevent cumulative drift from the 0.97 yEnc overhead estimate.
		totalEstimated := int64(0)
		for _, s := range segs {
			totalEstimated += s.Bytes
		}
		if totalEstimated > 0 {
			scale := float64(trueLen) / float64(totalEstimated)
			logger.Debug().
				Float64("scale", scale).
				Int64("totalEstimated", totalEstimated).
				Int64("trueLen", trueLen).
				Msg("exactSegGeometry: using scaled accumulate (estimated provenance)")
			var off int64
			for i, s := range segs {
				bases[i] = off
				sizes[i] = int64(math.Round(float64(s.Bytes) * scale))
				off += sizes[i]
			}
			// Absorb rounding residual into the last segment
			sizes[len(segs)-1] = trueLen - bases[len(segs)-1]
		} else {
			var off int64
			for i, s := range segs {
				bases[i] = off
				sizes[i] = s.Bytes
				off += s.Bytes
			}
		}
	} else {
		// No trueLen anchor: plain accumulate
		var off int64
		for i, s := range segs {
			bases[i] = off
			sizes[i] = s.Bytes
			off += s.Bytes
		}
	}
	return
}

func newPostedFileFetcher(ctx context.Context, fetch postedFetchFunc, f storage.PostedFileRef, cacheSource *cacheSlicedSource, trueLen int64, logger zerolog.Logger) *postedFileFetcher {
	base, segSizes := exactSegGeometry(f.Segments, trueLen, logger)
	length := f.Size
	if trueLen > 0 {
		length = trueLen
	}
	return &postedFileFetcher{
		ctx: ctx, fetch: fetch, length: length, segs: f.Segments, base: base, segSizes: segSizes,
		trueLen: trueLen, exact: segGeometryExact(f.Segments, trueLen), logger: logger,
		cacheSource: cacheSource, cacheIdx: -1,
	}
}

// resolveGeometry replaces estimated article boundaries with measured ones.
//
// A posted file whose refs are estimates (Par2SegmentRef.Real false) gets its
// boundaries from exactSegGeometry's scaled accumulate, which only anchors the
// TOTAL: each estimate is off by its own tens to hundreds of bytes, so every
// boundary after the first is misplaced. Seen live 2026-09-22 on Deep in Orbit
// S02E01 FLAME: stored sizes of 717,0xx-717,5xx against real 716,800-byte
// articles. readRange then copied each intact slice from the wrong offset (an
// IFSC mismatch) or past the end of a real article (ErrSegmentShort), and the
// repair aborted as terminal on a release with nothing missing.
//
// Posters cut every article but the last to one size, and the first article's
// yEnc header states it (=ypart) along with the file's size (=ybegin). One
// fetch of article 0 therefore gives the exact uniform geometry, checked for
// consistency before it is used: the last article must come out no larger
// than the others. On any doubt the estimates stay, exactly as before.
//
// Not safe for concurrent use with readRange: call it once, before the
// fetcher is shared (runRepair and computeMD5_16k do).
func (f *postedFileFetcher) resolveGeometry() {
	if f.exact || len(f.segs) == 0 || f.fetch == nil {
		return
	}
	n := len(f.segs)
	// Article 0 is read first, as the stream would. If it is the dead one,
	// any other full-size article measures the same size from its own
	// =ypart offset, so try a couple more before giving up.
	candidates := min(n, geometryProbeArticles)
	if n > 1 {
		candidates = min(n-1, geometryProbeArticles) // never the last: it is short
	}
	var lastErr error
	for idx := 0; idx < candidates; idx++ {
		seed, size, err := f.measureArticle(idx)
		if err != nil {
			lastErr = err
			continue
		}
		f.applyUniformGeometry(seed, size)
		return
	}
	f.logger.Warn().Err(lastErr).Int("articles_tried", candidates).
		Msg("par2 geometry: could not measure the article size; keeping estimated segment sizes")
}

// geometryProbeArticles bounds how many leading articles resolveGeometry
// tries before keeping the estimates.
const geometryProbeArticles = 3

// measureArticle fetches article idx and returns the uniform article size
// and file size it implies. The single-entry segment cache keeps its bytes
// for the stream.
func (f *postedFileFetcher) measureArticle(idx int) (seed, size int64, err error) {
	var meta *nntp.YencMetadata
	check := f.identityCheck(idx)
	capture := func(m *nntp.YencMetadata) string {
		if reason := check(m); reason != "" {
			return reason
		}
		meta = m
		return ""
	}
	fetchCtx, cancel := context.WithTimeout(f.ctx, par2ArticleFetchTimeout)
	data, err := f.fetch(fetchCtx, f.segs[idx].MessageID, capture)
	cancel()
	if err != nil {
		return 0, 0, fmt.Errorf("article %d: %w", idx, err)
	}
	f.cacheMu.Lock()
	f.cacheIdx, f.cacheData = idx, data
	f.cacheMu.Unlock()

	seed = int64(len(data))
	size = f.trueLen
	switch {
	case meta != nil && meta.PartSize > 0:
		if meta.PartSize != seed || meta.Offset != int64(idx)*seed {
			return 0, 0, fmt.Errorf("article %d: yEnc part of %d bytes at offset %d (decoded %d) is not a whole uniform part", idx, meta.PartSize, meta.Offset, seed)
		}
		if meta.Size > 0 {
			if size == 0 {
				size = meta.Size
			} else if meta.Size != size {
				return 0, 0, fmt.Errorf("article %d: yEnc file size %d disagrees with the PAR2 file length %d", idx, meta.Size, size)
			}
		}
	case idx != 0:
		// Without a part header only the first article's offset is known.
		return 0, 0, fmt.Errorf("article %d: no yEnc part header to place it by", idx)
	}
	if size <= 0 || seed <= 0 {
		return 0, 0, fmt.Errorf("article %d: no file length to anchor the geometry", idx)
	}
	n := int64(len(f.segs))
	if last := size - (n-1)*seed; last <= 0 || last > seed {
		return 0, 0, fmt.Errorf("article %d: %d-byte articles leave a last article of %d bytes in a %d-byte file of %d articles", idx, seed, last, size, n)
	}
	return seed, size, nil
}

// applyUniformGeometry sets every article to seed bytes, the last to what
// remains of size.
func (f *postedFileFetcher) applyUniformGeometry(seed, size int64) {
	n := int64(len(f.segs))
	for i := range f.segs {
		f.base[i] = int64(i) * seed
		f.segSizes[i] = seed
	}
	f.segSizes[n-1] = size - (n-1)*seed
	f.length = size
	f.trueLen = size
	f.exact = true
	f.logger.Debug().Int64("seed_segment", seed).Int64("last_segment", f.segSizes[n-1]).Int64("file_length", size).
		Int64("segments", n).Msg("par2 geometry: measured uniform article size from a yEnc part header")
}

// identityCheck is the article check for segment idx of this file.
func (f *postedFileFetcher) identityCheck(idx int) usenet.ArticleCheck {
	return func(meta *nntp.YencMetadata) string {
		return postedArticleMismatch(meta, idx, f.base[idx], f.exact, f.trueLen)
	}
}

// postedArticleMismatch explains why meta is not article idx of a posted file,
// or returns "" when it could be. Providers can serve a different upload's
// article under a reused Message-ID (see reader.articleMismatch); that article
// is CRC-valid against its own header, so without this the repair either
// counts it short or reads its bytes as the file's.
//
// Three signals: the part number (idx+1), the file size from =ybegin, and -
// once the geometry is exact - the part's offset. Any one alone can disagree
// for a benign reason (an NZB that numbers parts its own way, an estimated
// size), so two must disagree before the copy is refused.
func postedArticleMismatch(meta *nntp.YencMetadata, idx int, wantOffset int64, exact bool, fileSize int64) string {
	if meta == nil || meta.PartSize <= 0 {
		return ""
	}
	bad := 0
	if meta.Part > 0 && meta.Part != int64(idx+1) {
		bad++
	}
	if fileSize > 0 && meta.Size > 0 && meta.Size != fileSize {
		bad++
	}
	if exact && meta.Offset != wantOffset {
		bad++
	}
	if bad < 2 {
		return ""
	}
	return fmt.Sprintf("yEnc part %d of %d at offset %d in a %d-byte file, want part %d at offset %d in a %d-byte file",
		meta.Part, meta.Total, meta.Offset, meta.Size, idx+1, wantOffset, fileSize)
}

func (f *postedFileFetcher) segmentFor(offset int64) (int, error) {
	lo, hi := 0, len(f.base)
	for lo < hi {
		mid := (lo + hi) / 2
		if f.base[mid]+f.segSizes[mid] <= offset {
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
	data, err := f.fetch(fetchCtx, f.segs[idx].MessageID, f.identityCheck(idx))
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

// ErrSegmentShort marks a posted-file segment whose fetched article decoded to
// fewer bytes than the offset the repair needs from it - a truncated backing
// post. The slice's data is effectively gone at this position, so
// concurrentSliceSource.run records it and runRepair's retry loop folds it into
// the damaged set for recovery-slice reconstruction, instead of aborting the
// pass with an unclassifiable string that retries forever.
var ErrSegmentShort = errors.New("segment data shorter than its recorded size")

// ReadRange returns exactly length bytes starting at start, zero-padded past
// the file's real length (the PAR2 final-slice padding convention). Bytes
// are served from the local DFS cache where possible (see readCached),
// otherwise fetched from Usenet.
func (f *postedFileFetcher) ReadRange(start, length int64) ([]byte, error) {
	out, _, err := f.readRange(start, length, true)
	return out, err
}

// readRange backs ReadRange, with two extra controls the repair job's
// IFSC-verified cache path needs (see jobSliceSource.ReadSlice):
// allowCache=false forces every byte to come from a fresh Usenet fetch, and
// usedCache reports whether any returned byte was served from the DFS cache
// (so the caller knows whether an IFSC re-check is worth doing).
func (f *postedFileFetcher) readRange(start, length int64, allowCache bool) (out []byte, usedCache bool, err error) {
	out = make([]byte, length)
	pos := start
	written := int64(0)
	for written < length {
		if pos >= f.length {
			break
		}
		segIdx, err := f.segmentFor(pos)
		if err != nil {
			return nil, usedCache, err
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
		declaredAvail := f.segSizes[segIdx] - withinSeg
		if declaredAvail <= 0 {
			return nil, usedCache, fmt.Errorf("segment %d shorter than its recorded size", segIdx)
		}
		n := min(declaredAvail, length-written, f.length-pos)

		if allowCache && f.cacheSource != nil {
			if cached, ok := f.cacheSource.readCached(f.segs[segIdx].MessageID, withinSeg, n); ok {
				copy(out[written:written+n], cached)
				written += n
				pos += n
				usedCache = true
				continue
			}
		}

		data, err := f.segmentData(segIdx)
		if err != nil {
			return nil, usedCache, fmt.Errorf("fetch segment %d: %w", segIdx, err)
		}
		avail := int64(len(data)) - withinSeg
		if avail <= 0 {
			// The fetched article decoded to fewer bytes than the offset this
			// slice needs from it - a truncated backing post. The slice's data
			// is unreadable here, so surface a typed error runRepair's retry
			// loop can fold into the damaged set for recovery-slice
			// reconstruction, instead of a plain string that aborts the whole
			// pass non-terminally forever (see concurrentSliceSource.run).
			return nil, usedCache, fmt.Errorf("segment %d: %w", segIdx, ErrSegmentShort)
		}
		n = min(avail, length-written, f.length-pos)
		copy(out[written:written+n], data[withinSeg:withinSeg+n])
		written += n
		pos += n
	}
	return out, usedCache, nil
}

// logSkippedPar2Packets warns when par2.ParseIndex had to skip packets whose
// packet MD5 did not verify (a mis-served / mis-decoded article in one of the
// sources - see par2.walkPackets). The index was still built from the
// remaining packets; this line is here to correlate a later "not enough
// recovery slices" verdict, or a re-parse that suddenly succeeded, with a
// transient bad article rather than genuine recovery-set corruption.
func logSkippedPar2Packets(logger zerolog.Logger, entryName string, idx *par2.Index) {
	if idx == nil || len(idx.SkippedPackets) == 0 {
		return
	}
	ev := logger.Warn().Str("entry", entryName).Int("count", len(idx.SkippedPackets))
	byType := make(map[string]int, 4)
	for _, s := range idx.SkippedPackets {
		byType[s.Type]++
	}
	for t, n := range byType {
		ev = ev.Int("skipped_"+t, n)
	}
	ev.Msg("par2 repair: skipped bad-checksum PAR2 packet(s) during index parse (mis-served article?) - index built from the rest")
}

// missingFetcherErr formats the error par2 repair returns when an intact
// slice maps to a FileID that has no posted-file fetcher. classifyMiss (see
// the closure of that name in runRepair) decides whether the miss is
// structural/terminal or transient/retryable; the "(transient: ...)" tag
// and the "no posted-file fetcher for file" prefix are both load-bearing
// for classifyPar2Failure downstream, so keep this the single place that
// builds the string.
func missingFetcherErr(fileID [16]byte, classifyMiss func(fileID [16]byte) (reason string, terminal bool)) error {
	reason, terminal := "", true
	if classifyMiss != nil {
		reason, terminal = classifyMiss(fileID)
	}
	if terminal {
		if reason != "" {
			return fmt.Errorf("no posted-file fetcher for file %x (%s)", fileID, reason)
		}
		return fmt.Errorf("no posted-file fetcher for file %x", fileID)
	}
	return fmt.Errorf("no posted-file fetcher for file %x (transient: %s)", fileID, reason)
}

// uncoveredIntactFiles scans every slice par2.Repair's streaming pass would
// read as intact - all of idx's slices except those in damagedSet - and
// returns the FileIDs, in first-seen order, whose posted file has no
// fetcher. An empty result means every intact slice is covered. Damaged
// slices are excluded on purpose: the solve reconstructs them, it never
// calls ReadSlice for them, so a file with slices only in the damaged set
// needs no fetcher.
func uncoveredIntactFiles(idx *par2.Index, damagedSet map[int64]struct{}, fetchers map[[16]byte]*postedFileFetcher) ([][16]byte, error) {
	seen := make(map[[16]byte]struct{})
	var out [][16]byte
	for s := int64(0); s < idx.NumSlices(); s++ {
		if _, isDamaged := damagedSet[s]; isDamaged {
			continue
		}
		fid, _, lerr := idx.SliceLocation(s)
		if lerr != nil {
			return nil, fmt.Errorf("slice %d: %w", s, lerr)
		}
		if _, has := fetchers[fid]; has {
			continue
		}
		if _, dup := seen[fid]; dup {
			continue
		}
		seen[fid] = struct{}{}
		out = append(out, fid)
	}
	return out, nil
}

// jobSliceSource adapts per-posted-file fetchers into the single
// par2.SliceSource the streaming repair pass reads intact slices from.
type jobSliceSource struct {
	idx      *par2.Index
	fetchers map[[16]byte]*postedFileFetcher
	// classifyMiss, when set, explains a fileID that has no fetcher and
	// reports whether the miss is structural (terminal) or transient
	// (retryable) - see the closure of the same name in runRepair. Nil is
	// treated as structural/terminal, preserving the prior behavior.
	classifyMiss func(fileID [16]byte) (reason string, terminal bool)
	// logger is used only for the cache-slice IFSC-mismatch warning; the
	// zero value (a disabled logger) is fine.
	logger zerolog.Logger
}

func (s *jobSliceSource) ReadSlice(globalIdx int64) ([]byte, error) {
	fileID, local, err := s.idx.SliceLocation(globalIdx)
	if err != nil {
		return nil, err
	}
	f, ok := s.fetchers[fileID]
	if !ok {
		return nil, missingFetcherErr(fileID, s.classifyMiss)
	}
	start := local * s.idx.SliceSize
	data, usedCache, err := f.readRange(start, s.idx.SliceSize, true)
	if err != nil {
		return nil, err
	}
	if !usedCache {
		return data, nil
	}

	// This slice was served (at least partly) from the local DFS cache. A
	// byte range that an earlier playback or read-ahead prefetch zero-filled
	// as padding, then persisted in the cache, reads back here as plausible
	// "intact" data: readCached's own guard (buildDeadOutputRanges) only
	// masks the CURRENT repair pass's pending segments, so a
	// historically-padded range is invisible to it. Verify the cached bytes
	// against the PAR2 IFSC before trusting them. On a clean mismatch, drop
	// them and take a fresh Usenet fetch, rather than let a stale slice burn
	// one of par2.Repair's two canary-abort slots (>=3 aborts the whole
	// repair) or silently corrupt the GF accumulators (1-2). A verify error
	// (no IFSC for this file) is left for Repair's own streaming pass to
	// surface exactly as before.
	verified, verr := s.idx.VerifySliceChecksum(globalIdx, data)
	if verr == nil && !verified {
		s.logger.Warn().Int64("slice", globalIdx).Str("file", fmt.Sprintf("%x", fileID)).
			Msg("par2 repair: cached intact slice failed IFSC verification (stale zero-fill) - refetching from NNTP")
		fresh, _, ferr := f.readRange(start, s.idx.SliceSize, false)
		if ferr != nil {
			return nil, ferr
		}
		// Verify the REFETCHED bytes too. A stale zero-fill in the cache is
		// only one of two explanations for the mismatch above; the other is
		// that our posted-file byte->slice offset mapping is drifted for this
		// file, in which case bytes straight off Usenet fail exactly the same
		// way. Without this check the two are indistinguishable: the warning
		// above positively asserts "stale zero-fill", a cause nothing
		// confirmed, and a fresh-but-wrong slice then silently burns one of
		// par2.Repair's two tolerated canary slots before the whole repair
		// aborts blaming "a drifted offset mapping". Saying which one it is
		// costs one hash of bytes we already hold.
		if freshOK, fverr := s.idx.VerifySliceChecksum(globalIdx, fresh); fverr == nil && !freshOK {
			s.logger.Warn().Int64("slice", globalIdx).Str("file", fmt.Sprintf("%x", fileID)).
				Msg("par2 repair: freshly-fetched intact slice ALSO failed IFSC - offset mapping is drifted for this file, not a stale cache")
		}
		return fresh, nil
	}
	return data, nil
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

// watchIdle cancels the repair job if it makes no progress for
// par2JobIdleTimeout while in a network-bound phase. It reads the progress
// timestamp (lock-free) and phase on a ticker and only accrues idle time in
// fetching_recovery / streaming_intact; any other phase resets the idle
// reference, so a long CPU solve or a queued wait can never trip it. Stops
// as soon as ctx is done (normal completion, timeout, preemption, shutdown).
func (p *Par2Repair) watchIdle(ctx context.Context, cancel context.CancelFunc, progress *par2JobProgressState) {
	if progress == nil {
		return
	}
	const pollInterval = 15 * time.Second
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			switch progress.Phase() {
			case Par2PhaseFetchingRecovery, Par2PhaseStreamingIntact:
				if time.Since(progress.LastUpdate()) >= par2JobIdleTimeout {
					p.logger.Warn().
						Str("phase", string(progress.Phase())).
						Dur("idle_for", time.Since(progress.LastUpdate())).
						Msg("par2 repair cancelled: no progress in a network phase")
					cancel()
					return
				}
			}
		}
	}
}
