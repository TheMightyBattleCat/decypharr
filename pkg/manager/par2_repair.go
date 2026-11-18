// The PAR2 repair worker reconstructs confirmed-dead Usenet articles (see
// pkg/usenet/overlay) using PAR2 recovery data instead of falling straight to
// a delete + re-search. It is a manager-level background service split into
// two lanes:
//
//   - BATCH: one job at a time, deduped by nzbID, gated by the repair
//     sweep's Schedule/StopSchedule window (see Repair.repairWindowOpen).
//     Nothing in production feeds it any more: Enqueue has only test
//     callers, so loop/readyToRun/repairWindowOpen never gate a real pass.
//   - URGENT: AutoEnqueue (padding) and playback and pre-cache damage all go
//     through EnqueueUrgent and run here, at any hour, at bounded
//     concurrency. Its
//     NNTP fetches carry nntp.PriorityUrgent so they may draw a capped
//     provider's reserve band instead of being demoted to fills-only. It is
//     gated by the bandwidth monitor's hard quota (QuotaBlocked) and by
//     config.Repair.Par2RepairEnabled.
//   - RunNow (manual "Repair now", warm and cold sweeps) runs runJob
//     directly in its own goroutine.
//
// Escalation ordering end to end: a padded segment enqueues here; on success
// the segment is patched, padding for it stops on the next read, and a file
// left with every dead segment patched is no longer failed. On any failure -
// no PAR2 data, too much damage, a fetch or verification failure -
// classifyPar2Failure decides whether it's worth retrying (backed off) or
// terminal. A terminal outcome marks the entry unrepairable in the handler
// registry (see repair_handler_registry.go), surfaced in the overlay GUI for
// a manual "Delete & re-search"; on the URGENT lane it also re-grabs straight
// away (regrabOnTerminal), and the sweep re-grabs a PAR2-terminal file with
// recorded damage. With config.Repair.Par2Repair disabled this worker is
// never queued - decideAutoRepairAction routes straight to the re-grab path.
package manager

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"regexp"
	"runtime/debug"
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
	// par2RefusalLogInterval bounds how often AutoEnqueue logs a threshold
	// refusal for one entry.
	par2RefusalLogInterval = 30 * time.Minute
	// par2JobPassFloor is the throughput par2JobTimeoutFor allows each full
	// read of the release at, and par2JobPasses how many reads it allows
	// for: a discovery pass, the solve, and one more round.
	par2JobPassFloor = 4 << 20 // bytes per second
	par2JobPasses    = 3

	// defaultLargeRepairBytes is the accumulator size (damaged slices x slice size)
	// above which a repair counts as large. Large repairs run one at a time
	// across every lane (see waitForLargeRepair); smaller ones run side by
	// side as before. It is the old per-repair memory limit, so every repair
	// that could run alongside another before still can.
	defaultLargeRepairBytes = 512 << 20

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

	// par2ManualSlots is how many passes started by RunNow (the GUI's
	// "repair now" and the repair sweeps) run at once. Further requests wait
	// for a slot, still reported as queued.
	par2ManualSlots = 2

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

// largeRepairBytes is defaultLargeRepairBytes; a var so tests can reach the
// gate with a small fixture.
var largeRepairBytes int64 = defaultLargeRepairBytes

// par2ArticleFetchTimeout bounds a single article fetch within a job. A
// variable only so tests can shorten it.
var par2ArticleFetchTimeout = 60 * time.Second

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
	// deleted is set by CancelEntry: the entry was deleted under the pass,
	// which then records nothing and notifies nobody.
	deleted atomic.Bool
	// done is closed once the job has deregistered, so a job for the same
	// nzbID can wait out a preempted one instead of running alongside it.
	done chan struct{}
	// merged counts URGENT requests that arrived while this (URGENT) job
	// ran, and mergedProximity is the smallest proximity among them. Both
	// are guarded by Par2Repair.runningMu and read by releaseRun. See
	// EnqueueUrgent and followUpMerged.
	merged          int
	mergedProximity time.Duration
	// entryName is the release name once the job has resolved it (guarded
	// by runningMu), so lines about the job can name it instead of its ID.
	entryName string
}

// Par2Repair is the manager-level PAR2 repair worker. See the package doc
// comment for the BATCH/URGENT lane split.
type Par2Repair struct {
	manager *Manager
	repair  *Repair
	logger  zerolog.Logger

	// refusalLogged rate-limits AutoEnqueue's threshold-refusal line: nzbID
	// -> when it was last logged.
	refusalLogged sync.Map

	// BATCH lane: unchanged from the original single-lane worker.
	mu     sync.Mutex
	queued map[string]struct{}
	queue  chan string
	// deferred holds nzbIDs dequeued from queue but not yet run because the
	// repair window was closed (see readyToRun) - loop's local pending slice
	// mirrors this set so IsQueued can see them too.
	deferred map[string]struct{}
	// dropped holds nzbIDs CancelEntry took out of the batch queue while
	// their request still sat in the channel: loop skips the next one it
	// dequeues for each (see takeQueued).
	dropped map[string]struct{}

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

	// pendingDamage overrides pendingDeadSegments' overlay lookup. Nil in
	// production; tests that build a Par2Repair without a Usenet client set
	// it.
	pendingDamage func(nzbID string) (int, error)

	// largeRepair is a one-slot gate held by the one large repair running
	// (see waitForLargeRepair). Nil means no gating (tests that build a
	// Par2Repair directly).
	largeRepair chan struct{}

	// manualSlots bounds the passes RunNow runs at once (see
	// par2ManualSlots); runNow allocates it on first use. manualWaiting
	// holds, per nzbID, the channel that cancels a RunNow request still
	// waiting for a slot. It stays until the request's goroutine ends, so
	// CancelEntry can tell a RunNow entry in queued from a batch request.
	// Both guarded by mu.
	manualSlots   chan struct{}
	manualWaiting map[string]chan struct{}

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
		dropped:    make(map[string]struct{}),
		queue:      make(chan string, par2QueueDepth),
		progress:   newPar2ProgressTracker(),
		urgentSet:  make(map[string]*urgentJob),
		urgentWake: make(chan struct{}, 1),
		running:    make(map[string]*runningJob),

		largeRepair: make(chan struct{}, 1),
	}
}

// waitForLargeRepair makes a large repair (k x sliceSize over
// largeRepairBytes) wait until no other large repair is running, so two
// multi-GB repairs never hold memory at once; the next one starts as soon as
// the running one returns, with no manual re-run. It is a no-op for a small
// repair, for a job that already holds the gate, and when p has no gate.
// While waiting the job shows as queued, a phase the idle watchdog ignores,
// and SetPhase refreshes the progress timestamp when it resumes. The wait
// still counts toward the job's own timeout: a wait cut short by it, or by
// preemption or shutdown, returns the context error, which
// classifyPar2Failure treats as transient (backed off and retried).
func (p *Par2Repair) waitForLargeRepair(ctx context.Context, progress *par2JobProgressState, entryName string, k int, sliceSize int64, held *bool) error {
	if *held || p.largeRepair == nil || int64(k)*sliceSize <= largeRepairBytes {
		return nil
	}
	select {
	case p.largeRepair <- struct{}{}:
		*held = true
		return nil
	default:
	}
	prev := progress.Phase()
	progress.SetPhase(Par2PhaseQueued)
	p.logger.Info().
		Str("entry", entryName).
		Int("damaged_slices", k).
		Int64("slice_size", sliceSize).
		Msg("par2: large repair waiting for the running large repair to finish")
	select {
	case p.largeRepair <- struct{}{}:
		*held = true
		progress.SetPhase(prev)
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting for another large repair to finish: %w", ctx.Err())
	}
}

// releaseLargeRepair frees the gate taken by waitForLargeRepair.
func (p *Par2Repair) releaseLargeRepair() {
	<-p.largeRepair
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

// IsQueued reports whether nzbID currently has a PAR2 pass waiting to run in
// either lane (batch-queued, deferred by a closed repair window, or waiting
// in the URGENT heap).
func (p *Par2Repair) IsQueued(nzbID string) bool {
	if p == nil || nzbID == "" {
		return false
	}
	p.mu.Lock()
	_, q := p.queued[nzbID]
	_, d := p.deferred[nzbID]
	p.mu.Unlock()
	if q || d {
		return true
	}
	p.urgentMu.Lock()
	_, u := p.urgentSet[nzbID]
	p.urgentMu.Unlock()
	return u
}

// IsRunning reports whether a PAR2 pass for nzbID is executing in either
// lane. It used to read a single "active" pointer that every lane's job
// overwrote and cleared on exit, so an URGENT pass never showed as running
// and any two overlapping jobs hid each other.
func (p *Par2Repair) IsRunning(nzbID string) bool {
	if p == nil || nzbID == "" {
		return false
	}
	p.runningMu.Lock()
	_, ok := p.running[nzbID]
	p.runningMu.Unlock()
	return ok
}

// claimRun registers a job for nzbID in p.running, the one place that
// guarantees at most one pass per nzbID. A preempted job still unwinding is
// waited out - it may still be writing patches, since the solve and write
// phases do not check its context. Any other job already registered means
// this run is a duplicate: ok is false and the caller must return without
// touching the handler registry, which the running job owns.
func (p *Par2Repair) claimRun(ctx context.Context, nzbID string, lane repairLane, cancel context.CancelFunc) (job *runningJob, ok bool) {
	for {
		p.runningMu.Lock()
		if p.running == nil {
			p.running = make(map[string]*runningJob)
		}
		cur, busy := p.running[nzbID]
		if !busy {
			job = &runningJob{lane: lane, cancel: cancel, done: make(chan struct{})}
			p.running[nzbID] = job
			p.runningMu.Unlock()
			return job, true
		}
		p.runningMu.Unlock()
		if !cur.preempted.Load() {
			return nil, false
		}
		select {
		case <-cur.done:
		case <-ctx.Done():
			return nil, false
		}
	}
}

// releaseRun deregisters job and wakes anything waiting on it. It returns
// the URGENT requests merged into the job while it ran (see EnqueueUrgent):
// read under the same lock that deregisters it, so a request arriving as
// the job ends is either counted here or finds no job and queues normally.
func (p *Par2Repair) releaseRun(nzbID string, job *runningJob) (merged int, proximity time.Duration) {
	merged, proximity, _ = p.releaseRunNamed(nzbID, job)
	return merged, proximity
}

// releaseRunNamed is releaseRun, also returning the release name the job
// resolved (nzbID if it never did).
func (p *Par2Repair) releaseRunNamed(nzbID string, job *runningJob) (merged int, proximity time.Duration, name string) {
	p.runningMu.Lock()
	if p.running[nzbID] == job {
		delete(p.running, nzbID)
	}
	merged, proximity, name = job.merged, job.mergedProximity, job.entryName
	job.merged = 0
	p.runningMu.Unlock()
	close(job.done)
	if name == "" {
		name = nzbID
	}
	return merged, proximity, name
}

// CancelEntry stops PAR2 work for an entry that is being deleted: a running
// pass is cancelled and marked deleted, so it ends without recording an
// outcome, an attempt or a notification, and an URGENT request still waiting
// on the heap is dropped with its handler claim. It does not wait for the
// pass to unwind: the terminal re-grab calls the delete from inside its own
// pass.
func (p *Par2Repair) CancelEntry(nzbID string) {
	if p == nil || nzbID == "" {
		return
	}
	p.runningMu.Lock()
	job, running := p.running[nzbID]
	if running {
		job.deleted.Store(true)
		if job.cancel != nil {
			job.cancel()
		}
	}
	p.runningMu.Unlock()

	// A RunNow request still waiting for a slot gives up, and releases its
	// own claim. A batch request still in the channel or deferred by a
	// closed repair window is dropped: loop would otherwise run a pass for
	// the deleted entry once it reached it.
	batchDropped := false
	p.mu.Lock()
	if ch, waiting := p.manualWaiting[nzbID]; waiting {
		delete(p.manualWaiting, nzbID)
		close(ch)
	} else if _, q := p.queued[nzbID]; q {
		delete(p.queued, nzbID)
		if p.dropped == nil {
			p.dropped = make(map[string]struct{})
		}
		p.dropped[nzbID] = struct{}{}
		batchDropped = true
	}
	if _, d := p.deferred[nzbID]; d {
		delete(p.deferred, nzbID)
		batchDropped = true
	}
	p.mu.Unlock()

	p.urgentMu.Lock()
	item, queued := p.urgentSet[nzbID]
	if queued {
		heap.Remove(&p.urgentHeap, item.index)
		delete(p.urgentSet, nzbID)
	}
	p.urgentMu.Unlock()
	// A running pass releases its own claim as it ends; a queued request's
	// claim has no pass to release it.
	if (queued || batchDropped) && !running && p.repair != nil && p.repair.handlers != nil {
		p.repair.handlers.ReleasePar2(nzbID)
	}
}

// mergeIntoRunningUrgent records an URGENT request against the URGENT job
// already running for nzbID, if there is one. first reports whether this is
// the first request merged into that job, and name is the job's release
// name (nzbID until the job has resolved it).
func (p *Par2Repair) mergeIntoRunningUrgent(nzbID string, proximity time.Duration) (merged, first bool, name string) {
	p.runningMu.Lock()
	defer p.runningMu.Unlock()
	job, ok := p.running[nzbID]
	if !ok || job.lane != laneUrgent {
		return false, false, nzbID
	}
	if job.merged == 0 || proximity < job.mergedProximity {
		job.mergedProximity = proximity
	}
	job.merged++
	name = job.entryName
	if name == "" {
		name = nzbID
	}
	return true, job.merged == 1, name
}

// setRunningEntryName records the release name job resolved.
func (p *Par2Repair) setRunningEntryName(nzbID string, job *runningJob, name string) {
	p.runningMu.Lock()
	if p.running[nzbID] == job {
		job.entryName = name
	}
	p.runningMu.Unlock()
}

// pendingDeadSegments counts nzbID's recorded dead segments that are not
// patched.
func (p *Par2Repair) pendingDeadSegments(nzbID string) (int, error) {
	if p.pendingDamage != nil {
		return p.pendingDamage(nzbID)
	}
	if p.manager == nil || p.manager.usenet == nil {
		return 0, nil
	}
	pending, err := p.manager.usenet.OverlayPendingRepair(nzbID)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, segs := range pending {
		n += len(segs)
	}
	return n, nil
}

// followUpMerged runs after an URGENT pass has deregistered and released
// its handler claim. A pass reads the overlay's damage once, when it starts;
// requests that arrived while it ran (the read-ahead finding more missing
// articles, its final hand-off) were merged into it rather than each
// starting a pass that claimRun refused and dropped. If damage is still
// recorded now - the pass did not cover what those requests were about, or
// it failed - one more pass is requested through the normal gate, so a
// backoff, a terminal verdict or the repair mode (followUpAllowed) still
// holds it back. One follow-up per pass,
// and only when something asked: a pass nothing interrupted never re-queues
// itself.
func (p *Par2Repair) followUpMerged(nzbID, entryName string, merged int, proximity time.Duration) {
	if merged == 0 {
		return
	}
	if p.ctx != nil && p.ctx.Err() != nil {
		return
	}
	if entryName == "" {
		entryName = nzbID
	}
	requests := logger.Count(merged, "request") + " during the pass"
	left, err := p.pendingDeadSegments(nzbID)
	if err != nil {
		p.logger.Warn().Err(err).Str("entry", entryName).Str("nzb_id", nzbID).Int("requests", merged).Str(par2NoteField, requests).
			Msg("par2 repair: requests arrived during the pass, but the overlay's pending damage could not be read; not re-checking")
		return
	}
	if left == 0 {
		p.logger.Info().Str("entry", entryName).Str("nzb_id", nzbID).Int("requests", merged).Str(par2NoteField, requests).
			Msg("par2 repair: requests that arrived during the pass are covered; no damage left recorded")
		return
	}
	note := requests + " • " + logger.Count(left, "dead segment") + " still recorded"
	ev := p.logger.Info().Str("entry", entryName).Str("nzb_id", nzbID).Int("requests", merged).Int("dead_segments", left).Str(par2NoteField, note)
	if !p.followUpAllowed(nzbID, left) {
		ev.Msg("par2 repair: damage is still recorded after a pass that received more requests; not re-checking, the repair mode does not queue passes for it (run repair now to retry)")
		return
	}
	switch p.enqueueUrgent(nzbID, proximity) {
	case urgentQueued, urgentMerged:
		ev.Msg("par2 repair: damage is still recorded after a pass that received more requests; checking again")
	case urgentRefusedGate:
		ev.Msg("par2 repair: damage is still recorded after the pass, but the entry is backing off or unrepairable; not re-checking now")
	case urgentRefusedRegrab:
		ev.Msg("par2 repair: damage is still recorded after the pass, but a re-grab now owns the entry; not re-checking")
	}
}

// followUpDeferredSets queues the next pass for an entry whose pass repaired
// one PAR2 recovery set and left dead segments another set protects. It runs
// once the pass has released its handler claim, as followUpMerged does, and
// goes through the same gates, the repair mode included.
func (p *Par2Repair) followUpDeferredSets(nzbID, entryName string, deferred int, proximity time.Duration) {
	if p.ctx != nil && p.ctx.Err() != nil {
		return
	}
	if entryName == "" {
		entryName = nzbID
	}
	ev := p.logger.Info().Str("entry", entryName).Str("nzb_id", nzbID).Int("dead_segments", deferred).
		Str(par2NoteField, logger.Count(deferred, "dead segment")+" in another recovery set")
	if !p.followUpAllowed(nzbID, deferred) {
		ev.Msg("par2 repair: one recovery set repaired; the damage another set protects is left, the repair mode does not queue passes for it (run repair now again for the other set)")
		return
	}
	switch p.enqueueUrgent(nzbID, proximity) {
	case urgentQueued, urgentMerged:
		ev.Msg("par2 repair: one recovery set repaired; queued the next pass for the damage another set protects")
	case urgentRefusedGate:
		ev.Msg("par2 repair: one recovery set repaired; the damage another set protects is left, the entry is backing off or unrepairable")
	case urgentRefusedRegrab:
		ev.Msg("par2 repair: one recovery set repaired; the damage another set protects is left, a re-grab now owns the entry")
	}
}

// followUpAllowed applies AutoEnqueue's mode gate to a follow-up pass: a
// follow-up is an automatic pass too, even after a manual "repair now". In
// manual mode it is never queued, and under the threshold mode only when
// deadSegments reaches Par2RepairMinSegments or the release is being
// streamed (see autoEnqueueAllowed). With PAR2 repair switched off nothing
// is queued: a pass that was running when it was switched off must not
// chain another.
func (p *Par2Repair) followUpAllowed(nzbID string, deadSegments int) bool {
	cfg := config.Get().Repair
	if !cfg.Par2RepairEnabled() {
		return false
	}
	return autoEnqueueAllowed(cfg.Par2RepairMode, cfg.Par2RepairMinSegments, deadSegments, p.manager.Streaming(nzbID, ""))
}

// par2SetsDeferredError is runRepair's result when it repaired the PAR2
// recovery set it chose and left dead segments another set protects for the
// next pass. It is classed transient (see classifyPar2Failure).
type par2SetsDeferredError struct {
	dead int
}

// par2Class: the set this pass chose was repaired; the rest waits for the
// next pass.
func (e *par2SetsDeferredError) par2Class() par2Class { return par2Transient }

func (e *par2SetsDeferredError) Error() string {
	return fmt.Sprintf("%d dead segment(s) in another PAR2 recovery set left for the next pass (transient: several recovery sets)", e.dead)
}

// par2DeferredSetsSuccess returns how many dead segments err leaves to
// another PAR2 set when the pass that returned it repaired its own set: 0
// for any other error, and 0 when the pass wrote nothing. A pass that wrote
// nothing keeps the failure and its backoff; queueing it again at once
// would choose the same set and loop.
func par2DeferredSetsSuccess(err error, tally *par2PassTally) int {
	var d *par2SetsDeferredError
	if !errors.As(err, &d) || tally.patched() == 0 {
		return 0
	}
	return d.dead
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
//
// The threshold does not apply while someone is streaming the release (see
// autoEnqueueAllowed).
func (p *Par2Repair) AutoEnqueue(nzbID string, deadSegments int) {
	if p == nil || nzbID == "" {
		return
	}
	cfg := config.Get().Repair
	if !cfg.Par2RepairEnabled() {
		return
	}
	watched := p.manager.Streaming(nzbID, "")
	if !autoEnqueueAllowed(cfg.Par2RepairMode, cfg.Par2RepairMinSegments, deadSegments, watched) {
		// Used to return silently: 53 pads in 5.5 days and no automatic pass,
		// with nothing in the log to say the threshold was why. Once per
		// entry per par2RefusalLogInterval, so a burst of pads is one line.
		if now := time.Now(); cfg.Par2RepairMode == config.Par2RepairModeAutoThreshold {
			if last, ok := p.refusalLogged.Load(nzbID); !ok || now.Sub(last.(time.Time)) >= par2RefusalLogInterval {
				p.refusalLogged.Store(nzbID, now)
				p.logger.Info().Str("entry", nzbID).Int("dead_segments", deadSegments).
					Int("min_segments", cfg.Par2RepairMinSegments).Bool("watched", watched).
					Msg("par2 repair not queued: damage below the automatic repair threshold; left padded")
			}
		}
		return
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

// autoEnqueueAllowed is AutoEnqueue's mode gate. Manual never queues;
// threshold queues from minSegments dead segments - or at once while the
// release is being streamed. The threshold is there to save bandwidth on
// damage nobody is looking at; for a viewer, a pad below it was simply never
// repaired (Salem Lord S01E01, one padded segment on 2026-09-21, never
// queued), while a pass started now can patch the zeros before the player
// reaches them - the mount reads up to read_ahead_size ahead.
func autoEnqueueAllowed(mode config.Par2RepairMode, minSegments, deadSegments int, watched bool) bool {
	switch mode {
	case config.Par2RepairModeManual:
		return false
	case config.Par2RepairModeAutoThreshold:
		return watched || deadSegments >= minSegments
	}
	return true
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
// every other fetch - there is no unthrottled fast path. At most
// par2ManualSlots such passes run at once; a request beyond that waits in
// p.queued (IsQueued reports it) until a slot frees. Returns an error if
// PAR2 repair is disabled outright, or if a pass for this nzbID is already
// queued/deferred/running.
func (p *Par2Repair) RunNow(nzbID string) error {
	_, err := p.runNow(nzbID, false)
	return err
}

// RunNowStatus is RunNow, also reporting whether the request has to wait
// for a manual slot (both are taken) rather than starting at once.
func (p *Par2Repair) RunNowStatus(nzbID string) (waiting bool, err error) {
	return p.runNow(nzbID, false)
}

// runNow is RunNow. fromSweep marks a pass the repair sweep started (see
// runNowAndWait): that pass queues no follow-up for damage it leaves to
// another PAR2 set, since the sweep re-grabs what the pass did not repair.
// waiting reports that no manual slot was free when the request was taken.
func (p *Par2Repair) runNow(nzbID string, fromSweep bool) (waiting bool, err error) {
	if p == nil || nzbID == "" {
		return false, fmt.Errorf("nzbID is required")
	}
	if !config.Get().Repair.Par2RepairEnabled() {
		return false, fmt.Errorf("par2 repair is disabled")
	}
	if p.IsRunning(nzbID) {
		return false, fmt.Errorf("par2 repair already running for this entry")
	}
	// An URGENT job waiting for this nzbID would run a second pass after
	// ours, and its handler claim would be clobbered by the Set below.
	p.urgentMu.Lock()
	_, urgentQueued := p.urgentSet[nzbID]
	p.urgentMu.Unlock()
	if urgentQueued {
		return false, fmt.Errorf("par2 repair already queued for this entry")
	}

	p.mu.Lock()
	_, dupQueued := p.queued[nzbID]
	_, dupDeferred := p.deferred[nzbID]
	if dupQueued || dupDeferred {
		p.mu.Unlock()
		return false, fmt.Errorf("par2 repair already queued for this entry")
	}
	p.queued[nzbID] = struct{}{}
	p.mu.Unlock()

	// Manual override: force-claim the handler registry over whatever it
	// currently holds (including a terminal mark from a prior automatic
	// PAR2 failure) - a user-initiated "repair now" re-evaluates from
	// scratch, same as par2ShouldAutoEnqueue's terminal gate not applying
	// here either. The one exception is a live re-grab: it is about to
	// delete the entry, so a pass would run on a vanishing release.
	if p.repair != nil && p.repair.handlers != nil {
		if !p.repair.handlers.SetUnlessRegrab(nzbID, handlerPar2Running) {
			p.mu.Lock()
			delete(p.queued, nzbID)
			p.mu.Unlock()
			return false, fmt.Errorf("a re-grab is handling this entry")
		}
	}

	cancelled := make(chan struct{})
	p.mu.Lock()
	if p.manualSlots == nil {
		p.manualSlots = make(chan struct{}, par2ManualSlots)
	}
	slots := p.manualSlots
	if p.manualWaiting == nil {
		p.manualWaiting = make(map[string]chan struct{})
	}
	p.manualWaiting[nzbID] = cancelled
	p.mu.Unlock()

	// Take a free slot here, so the caller learns whether the pass starts
	// now or waits. A waiting request's claim is pinned: its timestamp is
	// this call's, and the wait has no bound, so the stale-claim reap could
	// otherwise drop it and let a re-grab take the entry before the pass
	// starts.
	held := false
	select {
	case slots <- struct{}{}:
		held = true
	default:
		if p.repair != nil && p.repair.handlers != nil {
			p.repair.handlers.PinPar2Waiting(nzbID)
		}
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() {
			p.mu.Lock()
			delete(p.queued, nzbID)
			if p.manualWaiting[nzbID] == cancelled {
				delete(p.manualWaiting, nzbID)
			}
			p.mu.Unlock()
		}()
		if !p.waitForManualSlot(nzbID, slots, cancelled, held) {
			// Deleted or shutting down while it waited: no pass runs, so
			// nothing else frees the claim taken above.
			if p.repair != nil && p.repair.handlers != nil {
				p.repair.handlers.ReleasePar2(nzbID)
			}
			return
		}
		defer func() { <-slots }()
		if !held && p.repair != nil && p.repair.handlers != nil {
			// Ends the pin, and restarts the claim's TTL from the pass's
			// start, as an immediate request's claim effectively does.
			p.repair.handlers.EnsurePar2Running(nzbID)
		}
		p.runPass(nzbID, laneBatch, fromSweep)
	}()
	return !held, nil
}

// waitForManualSlot takes one of the slots RunNow's passes share, waiting
// for one to free (held: runNow already took it). It returns false, holding
// no slot, if cancelled is closed (CancelEntry: the entry is being deleted)
// or the worker stops first.
func (p *Par2Repair) waitForManualSlot(nzbID string, slots, cancelled chan struct{}, held bool) bool {
	var stop <-chan struct{}
	if p.ctx != nil {
		stop = p.ctx.Done()
	}
	if !held {
		select {
		case slots <- struct{}{}:
		case <-cancelled:
			return false
		case <-stop:
			return false
		}
	}
	// select picks at random when a slot and a cancel are both ready.
	select {
	case <-cancelled:
	case <-stop:
	default:
		return true
	}
	<-slots
	return false
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

// ManualRepairPossible reports whether a user-initiated pass for nzbID can
// run at all: Availability's structural checks without its coverage
// estimate. That estimate (one slice per recorded-dead segment, against
// filename-derived volume counts) kept the GUI from even starting a pass
// that the heal or partly dead volumes can complete; the pass's own gates
// decide instead. A missing PAR2 record is left to the caller's backfill
// check.
func (p *Par2Repair) ManualRepairPossible(nzbID string) (ok bool, reason string) {
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
	if estimateNeededSlices(pending) == 0 {
		return false, "no damage pending repair"
	}
	return true, ""
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

	available := par2PostedRecoverySlices(nzb.Par2Files)
	if available < needed {
		return false, fmt.Sprintf("only %d recovery slices retained, need at least %d", available, needed)
	}
	return true, ""
}

// errMoreDamageThanRecorded is the terminal error for a damaged set too big to
// repair. It names only the limit that was actually exceeded: too few recovery
// slices retained in the posting, the per-repair limit (the slice cap, or the
// repair memory limit for large slices), or both. The "more damage than
// recorded" prefix is what the untyped fallback in classifyPar2Failure
// matches; the error itself is classed terminal.
func errMoreDamageThanRecorded(damaged int, available uint32, sliceSize int64) error {
	limit := par2.MaxRepairSlicesFor(sliceSize)
	limitText := fmt.Sprintf("the %d-slice repair cap", limit)
	if limit < par2.MaxRepairSlices {
		limitText = fmt.Sprintf("the %d-slice limit for %.1f MiB slices (%d MiB repair memory)",
			limit, float64(sliceSize)/(1<<20), par2.MaxAccumulatorMemory>>20)
	}
	overRetained := uint32(damaged) > available
	overLimit := damaged > limit
	switch {
	case overRetained && overLimit:
		return par2Failf(par2Terminal, "more damage than recorded; %d damaged slices, only %d recovery slices retained (also over %s)", damaged, available, limitText)
	case overLimit:
		return par2Failf(par2Terminal, "more damage than recorded; %d damaged slices is over %s (%d recovery slices retained)", damaged, limitText, available)
	default:
		return par2Failf(par2Terminal, "more damage than recorded; %d damaged slices, only %d recovery slices retained", damaged, available)
	}
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
		// BackfillPar2Refs rebuilds only a record that kept neither list.
		// With one retained (typically the source files of a release posted
		// without PAR2 files) it does nothing and the pass ends "no PAR2 data
		// available": not usable, so the policy re-grabs instead.
		if len(nzb.Par2Source) > 0 || len(nzb.Par2Files) > 0 {
			// Unless the PAR2 files were stored among the posted files (see
			// usenet.RefilePar2Refs) and there is damage for a pass to repair:
			// the pass looks, and a release that has none ends terminal there,
			// which the check above then answers. With nothing pending a pass
			// has nothing to do, as below.
			if usenet.Par2FilesMayBeAmongPosted(nzb) {
				if pending, perr := p.manager.usenet.OverlayPendingRepair(nzbID); perr == nil && estimateNeededSlices(pending) > 0 {
					return true, ""
				}
			}
			return false, "no PAR2 files retained for this release"
		}
		if nzb.Path == "" {
			return false, "no par2 metadata retained and source nzb no longer on disk"
		}
		if _, statErr := os.Stat(nzb.Path); statErr != nil {
			return false, "no par2 metadata retained and source nzb no longer on disk"
		}
		return true, ""
	}
	var pendingSlices uint32
	if pending, perr := p.manager.usenet.OverlayPendingRepair(nzbID); perr == nil {
		pendingSlices = estimateNeededSlices(pending)
	}
	return par2CoverageGate(pendingSlices, func() (bool, string) {
		return p.coverageSufficient(nzbID, nzb)
	})
}

// par2CoverageGate is par2Usable's last step: any pending damage makes PAR2
// usable, and the repair pass itself decides - its gates heal intact dead
// segments first and use partly dead volumes, which the name-only coverage
// estimate cannot see. With nothing pending the estimate answers. (This was
// the "Repair before re-grab" toggle, on by default; it is no longer
// optional.)
func par2CoverageGate(pendingSlices uint32, coverage func() (bool, string)) (bool, string) {
	if pendingSlices > 0 {
		return true, ""
	}
	return coverage()
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
	return int(par2PostedRecoverySlices(nzb.Par2Files))
}

// par2PostedRecoverySlices is the recovery slices files were posted with,
// from names and sizes alone: the named volumes' counts, or for an
// obfuscated posting unnamedRecoveryEstimate's size steps. Unlike summing
// censusPar2Volumes' volumes, it also counts the small unnamed volumes the
// census keeps as index files (see censusUnnamedVolumes).
func par2PostedRecoverySlices(files []storage.Par2FileRef) uint32 {
	vols, _ := censusNamedPar2Volumes(files)
	if len(vols) == 0 {
		return estimateUnnamedRecovery(files)
	}
	var n uint32
	for _, v := range vols {
		n += v.count
	}
	return n
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
		if par2IndexFileTooLarge(f) {
			p.logger.Debug().Str("entry", nzbID).Str("file", f.Name).Int64("size", f.Size).Msg("par2 verify: index file too large to fetch whole")
			continue
		}
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
	// Several recovery sets (a season pack): parse the one protecting this file.
	verifySet, err := par2VerifySet(ctx, sources, nzb.Par2Source, *posted, file)
	if err != nil {
		return false, "", err
	}
	idx, err := par2.ParseIndexSet(ctx, sources, verifySet)
	if err != nil {
		return false, "", fmt.Errorf("parse par2 index: %w", err)
	}
	// Every copy of a file's slice checksums unreadable in the index: the
	// set is damaged, and a pass would fetch further volumes for them first
	// (topUpSliceChecksums). Verify reads only the index, so it says it
	// cannot check rather than vouch for bytes against a damaged set.
	if missing := idx.MissingChecksums(); len(missing) > 0 {
		return false, fmt.Sprintf("cannot verify: PAR2 set has no slice checksums for %d file(s)", len(missing)), nil
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
		return false, Par2VerifyMismatch, nil
	}
	return true, "", nil
}

// par2VerifySet is the recovery set Verify checks posted against: zero when
// the release has one set. With several sets and none tied to this file it
// is an error, as the parse of the mix was before: ParseIndexSet with a zero
// ID now parses the first set it meets, and a same-length file of another
// episode there would match by length alone and report the whole-file MD5
// as a mismatch.
func par2VerifySet(ctx context.Context, sources []par2.Source, postedFiles []storage.PostedFileRef, posted storage.PostedFileRef, file string) ([16]byte, error) {
	var set [16]byte
	if len(posted.Segments) > 0 {
		set = choosePar2Set(ctx, sources, nil, postedFiles,
			map[string][]overlay.DeadSegment{file: {{MessageID: posted.Segments[0].MessageID}}}).setID
	}
	if set != ([16]byte{}) {
		return set, nil
	}
	seen := make(map[[16]byte]bool)
	for _, id := range par2.SourceSetIDs(ctx, sources) {
		if id != ([16]byte{}) {
			seen[id] = true
		}
	}
	// A cancelled walk leaves sources unread: not a reason to call the file
	// untied to any set.
	if err := ctx.Err(); err != nil {
		return set, err
	}
	if len(seen) > 1 {
		return set, fmt.Errorf("parse par2 index: the release carries %d PAR2 recovery sets and none could be tied to this file", len(seen))
	}
	return set, nil
}

// Par2VerifyMismatch is the reason Verify gives when the bytes were checked
// and are wrong. Every other reason with pass=false means the file could not
// be checked at all.
const Par2VerifyMismatch = "whole-file MD5 mismatch"

// EnqueueUrgent schedules nzbID for an immediate URGENT-lane PAR2 repair
// pass, prioritized ahead of any URGENT job with a larger proximity.
// proximity is the playback-time gap between the current playhead and the
// damaged region nzbID protects - callers should re-call as that gap
// narrows (or a closer-in damaged region is discovered) so priority stays
// accurate; the smallest proximity ever reported for a still-pending nzbID
// wins. If a BATCH pass for the same nzbID is currently running, it is
// preempted (cancelled without falling back to legacy repair) so the URGENT
// lane can take over immediately. If an URGENT pass for nzbID is already
// running, the request is merged into it instead: the pass re-checks the
// entry's recorded damage when it ends (see followUpMerged). Safe to call
// from any goroutine; a no-op if p is nil or nzbID is empty.
func (p *Par2Repair) EnqueueUrgent(nzbID string, proximity time.Duration) {
	p.enqueueUrgent(nzbID, proximity)
}

// urgentEnqueueResult is what enqueueUrgent did with a request.
type urgentEnqueueResult int

const (
	urgentQueued        urgentEnqueueResult = iota // on the URGENT heap (new, or already waiting there)
	urgentMerged                                   // an URGENT pass is running; it re-checks when it ends
	urgentRefusedGate                              // disabled, backing off or unrepairable
	urgentRefusedRegrab                            // a re-grab owns the entry
)

// enqueueUrgent is EnqueueUrgent, reporting what happened to the request.
func (p *Par2Repair) enqueueUrgent(nzbID string, proximity time.Duration) urgentEnqueueResult {
	if p == nil || nzbID == "" {
		return urgentRefusedGate
	}
	if proximity < 0 {
		proximity = 0
	}

	// An urgent request must obey the same repairability gate as the batch
	// path: if the entry is marked unrepairable or is still inside its
	// backoff window, do not preempt a running job or queue a new one.
	// Playback padding already covers the viewing experience meanwhile.
	if !p.par2ShouldAutoEnqueue(nzbID) {
		return urgentRefusedGate
	}
	// An URGENT pass is already running for this entry. Queueing another
	// only had a second worker pop it, fail claimRun and drop it - one
	// "already running" line per missing article a read-ahead found, and
	// the request lost. The running pass holds the handler claim; it is
	// told instead, and re-checks the recorded damage when it ends. One
	// line per pass says so.
	if merged, first, name := p.mergeIntoRunningUrgent(nzbID, proximity); merged {
		if first {
			p.logger.Info().Str("entry", name).Str("nzb_id", nzbID).
				Msg("par2 repair: more damage reported while a pass is running; it will be re-checked when the pass ends")
		}
		return urgentMerged
	}
	// Claim the entry for the pass, as Enqueue does: without a claim the
	// sweep's re-grab could delete the entry under a queued or running pass.
	// A live re-grab already owns it: it replaces the release, so a pass
	// would only race its delete.
	if p.repair != nil && p.repair.handlers != nil && !p.repair.handlers.ClaimPar2Queued(nzbID) {
		p.logger.Debug().Str("entry", nzbID).Msg("par2 repair: not queued; a re-grab is already handling this entry")
		return urgentRefusedRegrab
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
		return urgentQueued
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
	return urgentQueued
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
				p.repair.handlers.ReleasePar2(nzbID)
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
			if !p.takeQueued(nzbID) {
				continue
			}
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
			todo := p.takeDeferred(pending)
			pending = nil
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

// takeQueued is loop's bookkeeping for a request it has just taken off the
// channel. It reports false for one CancelEntry dropped, which is skipped. A
// request queued again after the drop has its own place in the channel and
// runs.
func (p *Par2Repair) takeQueued(nzbID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, d := p.dropped[nzbID]; d {
		delete(p.dropped, nzbID)
		return false
	}
	delete(p.queued, nzbID)
	return true
}

// takeDeferred returns the deferred requests still to run, in order, and
// clears them from the deferred set. One CancelEntry removed from the set,
// or a duplicate of one already taken, is left out.
func (p *Par2Repair) takeDeferred(pending []string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	todo := pending[:0:0]
	for _, id := range pending {
		if _, d := p.deferred[id]; !d {
			continue
		}
		delete(p.deferred, id)
		todo = append(todo, id)
	}
	return todo
}

// readyToRun gates the BATCH lane only: enabled, and within the repair
// sweep's off-peak window. The URGENT lane ignores the window entirely (see
// urgentWorker) - it exists specifically to act immediately.
func (p *Par2Repair) readyToRun() bool {
	return config.Get().Repair.Par2RepairEnabled() && p.repair.repairWindowOpen()
}

// par2ErrWithJobCtx attaches the job context's error to a runRepair failure
// that happened after the context ended (deadline, idle watchdog, shutdown).
// Whatever runRepair reported then - a recovery shortfall, no PAR2 file
// fetched, no posted file matched - is a consequence of being cut off, not
// evidence about the data; carrying the context error makes
// classifyPar2Failure back off instead of going terminal (and, on the urgent
// lane, regrabbing).
func par2ErrWithJobCtx(err error, jobCtx context.Context) error {
	if err == nil {
		return nil
	}
	if ctxErr := jobCtx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
		return fmt.Errorf("%w (job context ended: %w)", err, ctxErr)
	}
	return err
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
	p.runPass(nzbID, lane, false)
}

// recordOverlayUnreadable arms the transient backoff for a pass that could
// not read the overlay's pending damage. Without it the exit left no state,
// so every trigger (sweep, playback, manual enqueue) started a pass that
// failed the same way at once. The class is set here, not read from the
// error text, so a path in the message cannot flip it terminal.
func (p *Par2Repair) recordOverlayUnreadable(nzbID string, err error) {
	p.recordPar2Outcome(nzbID, par2Fail(par2Transient, fmt.Errorf("read overlay state: %w", err)), 0)
}

// runPass is runJob. fromSweep marks a pass the repair sweep started: when it
// repairs one PAR2 set and leaves another's damage, it still counts as a
// success but queues no follow-up. The sweep re-grabs the files the pass
// left damaged, and a queued pass would repair the same files alongside
// that re-grab.
func (p *Par2Repair) runPass(nzbID string, lane repairLane, fromSweep bool) {
	base := p.ctx
	if base == nil {
		base = context.Background()
	}
	jobCtx, jobCancel := context.WithCancel(base)
	defer jobCancel()
	// Claimed before anything else - the handler registry, the progress
	// record - so a duplicate pass (RunNow racing the batch loop or an
	// URGENT job) leaves both to the job that owns them.
	job, ok := p.claimRun(base, nzbID, lane, jobCancel)
	if !ok {
		// An URGENT request that was already on the heap when another pass
		// took the entry (it arrived between that pass being popped and
		// registering, or two workers waited out the same preempted batch
		// pass). Hand it back: it merges into the running URGENT pass, or
		// queues again if that pass has just ended. Dropping it here lost
		// the request.
		if lane == laneUrgent && (p.ctx == nil || p.ctx.Err() == nil) {
			res := p.enqueueUrgent(nzbID, 0)
			p.logger.Debug().Str("entry", nzbID).Str("lane", lane.String()).Bool("merged", res == urgentMerged).Bool("requeued", res == urgentQueued).
				Msg("par2 repair: a pass is already running for this entry; request handed back")
			return
		}
		p.logger.Debug().Str("entry", nzbID).Str("lane", lane.String()).
			Msg("par2 repair: a pass is already running for this entry; skipping duplicate")
		return
	}
	// setsDeferred is set when the pass repaired one PAR2 set and left
	// another set's damage: the follow-up for it is queued below, and covers
	// merged requests too. A sweep pass queues none (see runPass).
	setsDeferred := 0
	// Registered first among the job's own defers, so it runs last: after
	// the handler claim below is released, which a follow-up pass needs to
	// take.
	defer func() {
		merged, proximity, name := p.releaseRunNamed(nzbID, job)
		if job.deleted.Load() {
			// The entry is gone: nothing is left to follow up.
			return
		}
		if setsDeferred > 0 && fromSweep {
			p.logger.Info().Str("entry", name).Str("nzb_id", nzbID).Int("dead_segments", setsDeferred).
				Str(par2NoteField, logger.Count(setsDeferred, "dead segment")+" in another recovery set").
				Msg("par2 repair: one recovery set repaired; the sweep handles the damage another set protects")
			return
		}
		if setsDeferred > 0 {
			p.followUpDeferredSets(nzbID, name, setsDeferred, proximity)
			return
		}
		p.followUpMerged(nzbID, name, merged, proximity)
	}()

	start := time.Now()
	progress := p.progress.Start(nzbID, nzbID) // entry name backfilled below once resolved

	// A panic in a pass must not take the process down with it: every PAR2
	// worker (batch loop, urgent lane, RunNow) runs its jobs through here on
	// a bare goroutine, and runRepair is hundreds of lines of index
	// arithmetic over release-supplied data. Registered after the claim and
	// progress record so it runs first: the pass is marked failed and backed
	// off (transient), then the deferred releases below still run. Panics in
	// goroutines runRepair starts itself are not caught here.
	defer func() {
		if r := recover(); r != nil {
			err := par2Failf(par2Transient, "par2 repair panicked: %v (transient: internal error)", r)
			p.logger.Error().Str("entry", nzbID).Str("lane", lane.String()).Interface("panic", r).
				Bytes("stack", debug.Stack()).Msg("par2 repair: pass panicked; recovered")
			progress.SetPhase(Par2PhaseFailed)
			progress.SetLastError(err.Error())
			p.recordPar2Outcome(nzbID, err, 0)
		}
	}()

	if p.repair != nil && p.repair.handlers != nil {
		// Normally already par2_queued (from Enqueue/EnqueueUrgent) or
		// par2_running (RunNow's manual override already Set it). A queued
		// claim reaped while it waited is taken again; a live foreign claim
		// (a re-grab) is left alone.
		p.repair.handlers.EnsurePar2Running(nzbID)
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
			p.repair.handlers.MarkTerminalPar2(nzbID)
		case preempted:
			// Superseded by an urgent-lane job for the same entry, which will
			// claim and release the handler slot itself. Releasing here would
			// briefly hand the entry back to another caller mid-flight.
		default:
			// Only this pass's own (PAR2) claim: a re-grab another caller
			// claimed meanwhile is theirs to release.
			p.repair.handlers.ReleasePar2(nzbID)
		}
	}()

	if p.manager.usenet == nil {
		progress.SetPhase(Par2PhaseFailed)
		progress.SetLastError("usenet client not configured")
		return
	}

	entry, entryErr := p.manager.GetEntry(nzbID)
	if entryErr != nil || entry == nil {
		// A ghost overlay record - the backing entry is already gone
		// (deleted, superseded, re-grabbed under a different nzbID). There
		// is nothing to repair on behalf of, so the pass ends as a deleted
		// one does: no state row, no attempt row, no notification, no
		// follow-up, and the claim released. Recording it left a transient
		// backoff row (the error carried no class) and an attempt for an
		// entry that no longer exists, and nothing removed the row.
		job.deleted.Store(true)
		p.logger.Info().Str("entry", nzbID).Msg("par2 repair: entry no longer exists; nothing to repair")
		progress.SetPhase(Par2PhaseFailed)
		progress.SetLastError("entry no longer exists")
		return
	}
	entryName := entry.Name
	progress.SetEntryName(entryName)

	var releaseBytes int64
	if hdr, herr := p.manager.usenet.GetNZBHeader(nzbID); herr == nil && hdr != nil {
		releaseBytes = hdr.TotalSize
	}
	timeoutCtx, timeoutCancel := context.WithTimeout(jobCtx, par2JobTimeoutFor(releaseBytes))
	defer timeoutCancel()
	if lane == laneUrgent {
		timeoutCtx = nntp.WithPriority(timeoutCtx, nntp.PriorityUrgent)
	}

	// Idle watchdog: cancel a repair that stalls with no forward progress
	// during a network phase. Exits cleanly when the job ends (timeoutCtx is
	// cancelled by the defers above / the job timeout / preemption / shutdown).
	go p.watchIdle(timeoutCtx, jobCancel, progress)

	pending, err := p.manager.usenet.OverlayPendingRepair(nzbID)
	if err != nil {
		// Unknown is not "nothing to do": Completed here made the sweep's
		// PAR2 attempt report success and drop the file from its broken
		// list with nothing repaired.
		p.logger.Warn().Err(err).Str("entry", entryName).Msg("par2 repair: could not read the overlay's pending damage")
		p.recordOverlayUnreadable(nzbID, err)
		progress.SetPhase(Par2PhaseFailed)
		progress.SetLastError("read overlay state: " + err.Error())
		return
	}
	if len(pending) == 0 {
		p.unfailRepaired(nzbID, entryName)
		progress.SetPhase(Par2PhaseCompleted) // nothing pending - not a failure, just nothing to do
		return
	}
	deadSegments := 0
	for _, segs := range pending {
		deadSegments += len(segs)
	}
	// Logged as the pass starts (after claimRun), not when it was queued.
	p.logger.Info().Str("entry", entryName).Str("lane", lane.String()).Int("dead_segments", deadSegments).
		Str(par2NoteField, logger.Count(deadSegments, "dead segment")+" recorded • "+lane.String()).
		Msg("par2 repair started")
	p.setRunningEntryName(nzbID, job, entryName)

	var readBytes int64  // Usenet bytes only - see runRepair's fetch wrapper
	var cacheBytes int64 // bytes sourced from the local DFS cache instead
	var slicesRepaired int
	var deadSlicesDiscovered int // distinct dead slices the repair engine found across all rounds, beyond what the overlay already recorded
	tally := &par2PassTally{recorded: deadSegments}
	err = p.runRepair(timeoutCtx, nzbID, entryName, pending, &readBytes, &cacheBytes, &slicesRepaired, &deadSlicesDiscovered, progress, tally)
	if !job.deleted.Load() && (p.ctx == nil || p.ctx.Err() == nil) {
		// However the pass ended, a file it left with every recorded segment
		// patched is repaired. Only a successful pass cleared the verdict, so
		// a file healed by a pass that then ended terminal or transient kept
		// Failed: its next dead article failed the read instead of padding,
		// and a failed read could poison it in the permanent-failure cache.
		// Done before the terminal re-grab
		// below, which may delete the entry (UnfailRepaired then skips it).
		p.unfailRepaired(nzbID, entryName)
	}
	if deferredDead := par2DeferredSetsSuccess(err, tally); deferredDead > 0 && !job.preempted.Load() && !job.deleted.Load() && (p.ctx == nil || p.ctx.Err() == nil) {
		// The chosen set is repaired; the damage another set protects waits
		// for the next pass, queued as this one ends. Recording this as a
		// failure armed a backoff, kept the repaired files failed and queued
		// nothing.
		setsDeferred = deferredDead
		err = nil
	}
	if err != nil {
		if job.deleted.Load() {
			// The entry was deleted under the pass (CancelEntry). Nothing
			// failed that a backoff, an attempt row or a notification would
			// be about; the claim is released by the default branch above.
			progress.SetPhase(Par2PhaseFailed)
			progress.SetLastError("entry deleted")
			p.logger.Debug().Str("entry", entryName).Msg("par2 repair stopped: entry deleted")
			return
		}
		if job.preempted.Load() {
			preempted = true
			// Queued, not left in the running phase it was cut out of: the
			// urgent pass replaces this record when it starts, but if it never
			// does (PAR2 switched off meanwhile) the record would read as a
			// pass stuck mid-phase for ever.
			progress.SetPhase(Par2PhaseQueued)
			p.logger.Debug().Str("entry", entryName).Msg("par2 repair preempted by urgent lane")
			return
		}
		if p.ctx != nil && p.ctx.Err() != nil {
			// Shutdown (Stop cancelled the root context), not a failure of the
			// pass: recording it would add an attempt and a backoff and send a
			// "repair failed" notification for every deploy. The overlay still
			// holds the damage, so the next start picks the entry up again.
			progress.SetPhase(Par2PhaseQueued)
			p.logger.Debug().Str("entry", entryName).Msg("par2 repair interrupted by shutdown")
			return
		}
		err = par2ErrWithJobCtx(err, timeoutCtx)
		canary := errors.Is(err, par2.ErrChecksumMismatch)
		class := p.recordPar2Outcome(nzbID, err, deadSlicesDiscovered)
		p.logger.Info().Err(err).Str("entry", entryName).Str("lane", lane.String()).Bool("crc_canary", canary).Bool("terminal", class.terminal).
			Str("class", class.name()).
			Int64("cache_bytes", atomic.LoadInt64(&cacheBytes)).Int64("usenet_bytes", atomic.LoadInt64(&readBytes)).
			Func(tally.logFields).
			Dur(par2TookField, time.Since(start)).
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
			ReadBytes:  atomic.LoadInt64(&readBytes),
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
			// Articles confirmed missing on every provider: the posting is
			// dead, and another indexer's NZB for it must not be imported
			// again right after the re-grab (Under Reef S11E06 was, 4 s
			// after its blocklist). Marked before the re-grab goes out.
			if nntp.IsArticleNotFoundError(err) && p.manager.usenet != nil {
				if marked, merr := p.manager.usenet.MarkPostingDeadByNZBID(nzbID); merr != nil || !marked {
					p.logger.Debug().Err(merr).Str("entry", entryName).Msg("par2 repair: posting not marked dead (no persisted identity)")
				}
			}
			if lane == laneUrgent {
				p.regrabOnTerminal(entry, entryName, p.pendingAfterPass(nzbID, entryName, pending))
			}
		}
		return
	}

	p.logger.Info().
		Str("entry", entryName).
		Str("lane", lane.String()).
		Func(tally.logFields).
		Int("slices_rebuilt", slicesRepaired).
		Dur(par2TookField, time.Since(start)).
		Dur("duration", time.Since(start)).
		Int64("cache_bytes", atomic.LoadInt64(&cacheBytes)).
		Int64("usenet_bytes", atomic.LoadInt64(&readBytes)).
		Int("deferred_dead_segments", setsDeferred).
		Msg("par2 repair completed")
	progress.SetPhase(Par2PhaseCompleted)
	p.recordAttempt(&storage.Par2RepairAttempt{
		ID:              uuid.NewString(),
		EntryName:       entryName,
		NzbID:           nzbID,
		StartedAt:       start,
		Duration:        time.Since(start),
		Outcome:         storage.Par2RepairOutcomeCompleted,
		ReadBytes:       atomic.LoadInt64(&readBytes),
		SlicesRepaired:  slicesRepaired,
		SegmentsPatched: tally.patched(),
	})
	p.recordPar2Outcome(nzbID, nil, 0)
	p.notifyCompleted(entryName, tally.patched(), time.Since(start))
}

// par2PassTally is what one pass actually wrote, by how it got the bytes. The
// completion line used to report the number of segments the overlay had
// recorded when the pass started as "patched": a pass that began with one
// recorded segment and rebuilt dozens more it found itself said 1, and so
// did the attempt history and the notification. A nil tally counts nothing.
type par2PassTally struct {
	recorded          int    // dead segments the overlay had recorded when the pass started
	patchedRecorded   int    // of those, written from rebuilt slices
	healed            int    // of those, fetched intact after all and written without parity
	patchedDiscovered int    // found dead by the pass itself, recorded and written
	rounds            int    // full reads of the release the solve needed
	statSweep         string // how the up-front damage check ended
}

func (t *par2PassTally) addPatchedRecorded(n int) {
	if t != nil {
		t.patchedRecorded += n
	}
}

func (t *par2PassTally) addHealed(n int) {
	if t != nil {
		t.healed += n
	}
}

func (t *par2PassTally) addPatchedDiscovered(n int) {
	if t != nil {
		t.patchedDiscovered += n
	}
}

func (t *par2PassTally) setRounds(n int) {
	if t != nil {
		t.rounds = n
	}
}

func (t *par2PassTally) setStatSweep(state string) {
	if t != nil {
		t.statSweep = state
	}
}

// patched is every segment the pass wrote, whichever way.
func (t *par2PassTally) patched() int {
	if t == nil {
		return 0
	}
	return t.patchedRecorded + t.healed + t.patchedDiscovered
}

// note is the tally as the journal's one-line summary. Info and warning
// lines print no other fields there, so without it the journal said only
// that a repair had completed.
func (t *par2PassTally) note() string {
	if t == nil {
		return ""
	}
	parts := []string{fmt.Sprintf("%d of %d recorded patched", t.patchedRecorded+t.healed, t.recorded)}
	if t.healed > 0 {
		parts = append(parts, fmt.Sprintf("%d fetched intact", t.healed))
	}
	if t.patchedDiscovered > 0 {
		parts = append(parts, fmt.Sprintf("%d more found and patched", t.patchedDiscovered))
	}
	if t.rounds > 0 {
		parts = append(parts, logger.Count(t.rounds, "full read"))
	}
	if t.statSweep != "" {
		parts = append(parts, "damage check "+t.statSweep)
	}
	return strings.Join(parts, " • ")
}

// logFields adds the tally to a log line: the total first, then its parts.
func (t *par2PassTally) logFields(e *zerolog.Event) {
	if t == nil {
		return
	}
	e.Str(par2NoteField, t.note())
	e.Int("segments_patched", t.patched()).
		Int("segments_recorded", t.recorded).
		Int("patched_recorded", t.patchedRecorded).
		Int("healed", t.healed).
		Int("patched_discovered", t.patchedDiscovered).
		Int("rounds", t.rounds)
	if t.statSweep != "" {
		e.Str("stat_sweep", t.statSweep)
	}
}

// unfailRepaired clears the Failed verdict of nzbID's files the pass left
// with every recorded segment patched (see Usenet.OverlayUnfailRepaired),
// whether or not the pass succeeded.
// Left Failed, the next escalation for such a file found nothing pending,
// so par2Usable said no and the playback policy re-grabbed it.
func (p *Par2Repair) unfailRepaired(nzbID, entryName string) {
	if p.manager.usenet == nil {
		return
	}
	cleared, err := p.manager.usenet.OverlayUnfailRepaired(nzbID)
	if err != nil {
		p.logger.Warn().Err(err).Str("entry", entryName).Msg("par2 repair: could not clear the failed verdict of repaired files")
		return
	}
	if len(cleared) > 0 {
		p.logger.Info().Str("entry", entryName).Strs("files", cleared).
			Msg("par2 repair: every dead segment is patched; the file is no longer failed")
	}
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

// pendingAfterPass re-reads nzbID's pending damage once a pass has ended.
// runRepair often patches part of the damage before it fails (the heal of
// recorded-dead articles that turn out alive, the placeable segments written
// before an unplaced error), so the map read before the pass can name files
// that have nothing left to repair. The pre-pass map is returned only when
// the overlay can't be read; an empty result means nothing is still damaged.
func (p *Par2Repair) pendingAfterPass(nzbID, entryName string, prePass map[string][]overlay.DeadSegment) map[string][]overlay.DeadSegment {
	after, err := p.manager.usenet.OverlayPendingRepair(nzbID)
	if err != nil {
		p.logger.Debug().Err(err).Str("entry", entryName).
			Msg("par2 repair: could not re-read the overlay's pending damage after the pass; using the damage read before it")
		return prePass
	}
	return after
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

	// A PAR2 verdict is a whole-NZB outcome, so every file that still has
	// damage is re-grabbed - in a season pack each episode is its own Arr
	// file. pending is read after the pass (see pendingAfterPass), so a file
	// the failed pass still patched in full (a healed article, a placed
	// segment) is not in it and keeps its import. This used to take one file
	// in map order and leave the rest padded for the sweep. The bulk form
	// skips the per-entry cooldown, which would refuse every file after the
	// first, and the cache-only warm PAR2 pass, which can't succeed where
	// this full pass just failed; the regrab guard still applies (auto=true).
	if len(pending) == 0 {
		p.logger.Info().Str("entry", entryName).
			Msg("par2 repair: terminal verdict on urgent lane, but the pass patched every recorded dead segment; nothing to re-grab")
		return
	}
	files := make([]string, 0, len(pending))
	for name := range pending {
		files = append(files, name)
	}
	sort.Strings(files)

	// No handlerRegrab claim is taken here: runJob already holds
	// par2_running for this nzbID until it returns, so we are the exclusive
	// handler. That is also why the old RegrabImportGrab call was inert on
	// this path - its own TryAcquire(handlerRegrab) could never win against
	// the running PAR2 job, so it logged "already being handled" and did
	// nothing.
	for _, fileName := range files {
		p.logger.Info().Str("entry", entryName).Str("file", fileName).Int("files", len(files)).
			Msg("par2 repair: terminal verdict on urgent lane; initiating immediate regrab")
		if _, reason, rerr := p.repair.repairPlaybackFileNow(p.ctx, entryName, fileName, true, true); rerr != nil {
			p.logger.Debug().Err(rerr).Str("entry", entryName).Str("file", fileName).Msg("par2 repair: immediate regrab did not proceed")
		} else if reason != "" {
			p.logger.Debug().Str("entry", entryName).Str("file", fileName).Str("reason", reason).Msg("par2 repair: immediate regrab skipped")
		}
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
	// A FileID paired twice (two posted copies of one file, cached before
	// MatchFiles kept one per FileID) is refused, so matching runs again.
	seenFID := make(map[[16]byte]bool, len(cache))
	out := make([]par2.Match, 0, len(cache))
	for _, m := range cache {
		pi, ok := nameToIdx[m.PostedName]
		if !ok || seen[pi] || seenFID[m.FileID] {
			return nil, false
		}
		seenFID[m.FileID] = true
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
//
// A result with an unconfirmed pairing (par2.Match.Unconfirmed) is not
// cached either: the cache keeps no evidence, so a hit would hand runRepair
// the pairing as proven and its patches would skip the confirmation they
// need. Matching runs again on the next pass instead.
func par2MatchToCache(par2Source []storage.PostedFileRef, matches []par2.Match, skipped []par2.MatchSkip) []storage.Par2MatchRef {
	if len(skipped) != 0 || len(matches) != len(par2Source) {
		return nil
	}
	seen := make([]bool, len(par2Source))
	seenFID := make(map[[16]byte]bool, len(matches))
	out := make([]storage.Par2MatchRef, 0, len(matches))
	for _, m := range matches {
		if m.PostedIndex < 0 || m.PostedIndex >= len(par2Source) || seen[m.PostedIndex] || seenFID[m.FileID] || m.Unconfirmed() {
			return nil
		}
		seen[m.PostedIndex] = true
		seenFID[m.FileID] = true
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

		// Only files whose persisted geometry is exact count. On estimated
		// refs an article whose real bytes sit inside one slice can straddle
		// two in the estimate, so its count would be an OVER-estimate - and
		// this gate declares a release unrepairable before fetching any
		// recovery. Skipping keeps the result a lower bound; the in-loop gate
		// judges these files once resolveGeometry has measured them.
		if !segGeometryExact(src.Segments, fd.Length) {
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
func (p *Par2Repair) runRepair(ctx context.Context, nzbID, entryName string, pending map[string][]overlay.DeadSegment, readBytes, cacheBytes *int64, slicesRepaired *int, deadDiscovered *int, progress *par2JobProgressState, tally *par2PassTally) (retErr error) {
	u := p.manager.usenet
	progress.SetPhase(Par2PhaseFetchingRecovery)

	// Held from the first point a large damaged set is known until this pass
	// returns - see waitForLargeRepair.
	var largeHeld bool
	defer func() {
		if largeHeld {
			p.releaseLargeRepair()
		}
	}()

	// fetchPosted wraps every article fetch this pass makes so runJob can
	// report a real (not estimated) read_bytes total in the persisted attempt
	// log - used in place of u.FetchArticle everywhere below. A posted file's
	// articles pass an identity check (see postedArticleMismatch), and the
	// provider that served each is recorded in postedServed (see
	// postedFailover); PAR2 files go through fetch, which has none, and
	// records which provider served each article in prov (see
	// refetchMisservedPar2Articles).
	postedServed := newPar2Provenance()
	fetchPosted := func(fctx context.Context, messageID string, check usenet.ArticleCheck) ([]byte, error) {
		data, host, err := u.FetchArticleCheckedServedBy(fctx, messageID, check)
		if err != nil {
			return nil, err
		}
		postedServed.record(messageID, host, len(data))
		if readBytes != nil {
			n := int64(len(data))
			atomic.AddInt64(readBytes, n)
			progress.AddUsenetBytes(n)
		}
		return data, nil
	}
	// transportFails counts PAR2 article fetches that failed without a 430
	// (a timeout, a reset): each can drop a whole recovery volume for this
	// pass, so a recovery shortfall that follows one says nothing about
	// whether the recovery data exists - see recoveryShortfall.
	var transportFails atomic.Int32
	prov := newPar2Provenance()
	fetch := countPar2TransportFails(ctx, func(fctx context.Context, messageID string) ([]byte, error) {
		data, host, err := u.FetchArticleServedBy(fctx, messageID)
		if err != nil {
			return nil, err
		}
		prov.record(messageID, host, len(data))
		if readBytes != nil {
			n := int64(len(data))
			atomic.AddInt64(readBytes, n)
			progress.AddUsenetBytes(n)
		}
		return data, nil
	}, &transportFails)
	// recoveryShortfall is the "not enough recovery slices" failure for idx,
	// the index the count came from: its skipped packets make it transient.
	recoveryShortfall := func(damaged, got int, idx *par2.Index) error {
		skipped := 0
		if idx != nil {
			skipped = len(idx.SkippedPackets)
		}
		return par2RecoveryShortfall(damaged, got, int(transportFails.Load()), skipped)
	}

	nzb, err := u.GetNZB(nzbID)
	if err != nil {
		return par2Failf(par2Transient, "load NZB record: %w", err)
	}
	if usenet.Par2FilesMayBeAmongPosted(nzb) {
		moved, err := u.RefilePar2Refs(ctx, nzbID)
		if err != nil {
			return par2Failf(par2Transient, "looking for PAR2 files among the posted files did not complete: %w (transient: it can be retried)", err)
		}
		if moved > 0 {
			p.logger.Info().Str("entry", nzbID).Int("par2_files", moved).
				Msg("Found the release's PAR2 files among its posted files by their yEnc names")
			if nzb, err = u.GetNZB(nzbID); err != nil {
				return par2Failf(par2Transient, "reload NZB record after refiling PAR2 files: %w", err)
			}
		}
	}
	if len(nzb.Par2Files) == 0 || len(nzb.Par2Source) == 0 {
		if err := u.BackfillPar2Refs(ctx, nzbID); err != nil {
			if errors.Is(err, usenet.ErrBackfillImpossible) {
				return par2Failf(par2Terminal, "no PAR2 data and backfill failed: %w", err)
			}
			// A read/re-parse/save failure: a later attempt can succeed.
			return par2Failf(par2Transient, "PAR2 refs backfill did not complete: %w (transient: backfill can be retried)", err)
		}
		nzb, err = u.GetNZB(nzbID)
		if err != nil {
			return par2Failf(par2Transient, "reload NZB record after backfill: %w", err)
		}
		if len(nzb.Par2Files) == 0 || len(nzb.Par2Source) == 0 {
			return par2Failf(par2Terminal, "no PAR2 data available")
		}
	}

	// Before anything reads pending by index: the cache source below steers
	// around the dead slots, and every patch is written by index.
	pending = p.moveRelocatedDeadRecords(nzbID, entryName, nzb, pending)

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
	cacheSource := p.newCacheSource(nzbID, entryName, cacheReader, nzb, pending, cacheBytes, progress)

	vols, indexFiles := censusPar2Volumes(nzb.Par2Files)

	// parse parses the fetched PAR2 files, and when packets failed their MD5
	// asks another provider for the articles behind them and parses again
	// (see refetchMisservedPar2Articles). Each article is asked once a pass.
	par2Refs := make(map[string]storage.Par2FileRef, len(nzb.Par2Files))
	for _, f := range nzb.Par2Files {
		par2Refs[f.Name] = f
	}
	refetch := func(fctx context.Context, messageID, exceptHost string) ([]byte, string, error) {
		data, host, err := u.FetchArticleExcept(fctx, messageID, exceptHost)
		if err == nil && readBytes != nil {
			n := int64(len(data))
			atomic.AddInt64(readBytes, n)
			progress.AddUsenetBytes(n)
		}
		return data, host, err
	}
	parse := func(ctx context.Context, srcs []par2.Source, set [16]byte) (*par2.Index, []par2.Source, error) {
		idx, err := par2.ParseIndexSet(ctx, srcs, set)
		if err != nil || len(idx.SkippedPackets) == 0 {
			return idx, srcs, err
		}
		logSkippedPar2Packets(p.logger, entryName, idx)
		next, n := refetchMisservedPar2Articles(ctx, p.logger, idx, srcs, par2Refs, prov, refetch, entryName)
		if n == 0 {
			return idx, srcs, nil
		}
		reparsed, err := par2.ParseIndexSet(ctx, next, set)
		if err != nil {
			return nil, next, err
		}
		p.logger.Info().Str("entry", entryName).Int("articles", n).Int("recovery_before", len(idx.Recovery)).
			Int("recovery_after", len(reparsed.Recovery)).Int("skipped_after", len(reparsed.SkippedPackets)).
			Msg("par2: re-parsed the PAR2 files after another provider served the articles behind bad-checksum packets")
		return reparsed, next, nil
	}

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
	// index is parsed) - catch the hopeless case before fetching any
	// recovery volume. Checked after the (small) index files are fetched, so
	// a dead segment that is really intact can still be healed without
	// parity - see healBeforeRecoveryGate.

	// Fetch every index (non-volume) file, plus enough of the smallest
	// recovery volumes to plausibly cover every dead segment - "fully
	// reading the small vols is acceptable v1" per the design; we don't
	// attempt to skip individual articles within a volume file.
	// indexFetchErr is set when an index file failed for a reason other than
	// a 430: the set it describes may be the one protecting the damage, so
	// the set-dependent failures below are not final - see
	// par2IndexFetchTransient.
	sources, indexFetchErr := fetchPar2IndexFiles(ctx, p.logger, fetch, indexFiles, entryName)

	// A release with several PAR2 sets (one per episode of a season pack):
	// work on the set protecting the damaged files, and leave damage another
	// set protects for the next pass. See choosePar2Set.
	var par2Set [16]byte
	deferredDead := 0
	defer func() {
		// A pass that repaired this set still leaves the other set's damage:
		// report it so it is not recorded as fully repaired. runJob counts
		// the set it repaired as a success and queues the next pass at once
		// (see par2DeferredSetsSuccess).
		if retErr == nil && deferredDead > 0 {
			retErr = &par2SetsDeferredError{dead: deferredDead}
		}
	}()
	applySetChoice := func(c par2SetChoice) error {
		if c.unattributed > 0 {
			return par2FetchShortfall(fmt.Sprintf("the damaged files could not be tied to one of the release's %d PAR2 recovery sets", c.unattributed),
				int(transportFails.Load()))
		}
		deferredDead = c.deferred
		if c.setID == ([16]byte{}) {
			return nil
		}
		par2Set = c.setID
		sources, vols, pending = c.sources, c.vols, c.pending
		available = 0
		for _, v := range vols {
			available += v.count
		}
		p.logger.Info().Str("entry", entryName).Int("volumes", len(vols)).Int("deferred_dead_segments", c.deferred).
			Msg("par2: release carries several recovery sets; repairing the one protecting the damaged files")
		if len(pending) == 0 {
			return par2Failf(par2Transient, "every dead segment is in another PAR2 set (transient: chosen set has none)")
		}
		return nil
	}
	// A damaged file no fetched index names may be in a set posted with no
	// index file, or whose index failed: fetch a volume of each set not seen
	// yet until one names it. Before the choice, so the choice is made over
	// every set and does not narrow the volumes to a set seen so far.
	// nextVolIdx counts the volumes tried, which stay vols[:nextVolIdx].
	var nextVolIdx int
	var searchGone int // volumes in a row the search found gone (430)
	searchUnseenSet := func() error {
		before := transportFails.Load()
		var unseen bool
		var err error
		vols, nextVolIdx, sources, unseen, err = fetchVolumesForUnseenSet(ctx, p.logger, fetch, vols, nextVolIdx, sources,
			nzb.Par2Source, pending, entryName, u.ProcessingMaxConnections(), progress, &searchGone)
		if err != nil {
			return err
		}
		// A volume that failed without a 430 may be of the set looked for:
		// the set-dependent failures below are not final.
		if unseen && indexFetchErr == nil && transportFails.Load() > before {
			indexFetchErr = errors.New("recovery volume failed to fetch without a 430 while looking for the set protecting the damaged files")
		}
		return nil
	}
	if err := searchUnseenSet(); err != nil {
		return err
	}
	if c, next, ok := par2SetAfterVolumes(ctx, sources, vols, nextVolIdx, nzb.Par2Source, pending); ok {
		if err := applySetChoice(c); err != nil {
			return err
		}
		nextVolIdx = next
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
	earlyIdx, earlyErr := par2.ParseIndexSet(ctx, sources, par2Set)
	if earlyErr != nil {
		// A parse cut short by the job's cancel or timeout proves nothing
		// about the release: leave before the gates below read the missing
		// index as "no recovery slices".
		if ctx.Err() != nil {
			return ctx.Err()
		}
		earlyIdx = nil
	}
	// An index file can hold recovery slices too (an obfuscated posting's
	// volumes under par2UnnamedVolumeFloor stay index files, see
	// censusUnnamedVolumes): they are already here, and in no file of vols.
	// Add them, or "no PAR2 recovery volumes retained" goes terminal on data
	// in hand. Slices from a file of vols (a volume the set search fetched)
	// are already in available by name: not again. They are held through
	// the repair on top of the k slices it fetches, so the large-repair
	// gate counts them.
	var searchHeld int
	if earlyIdx != nil {
		volNames := make(map[string]bool, len(vols))
		for _, v := range vols {
			volNames[v.ref.Name] = true
		}
		for _, r := range earlyIdx.Recovery {
			if r.Source >= 0 && r.Source < len(sources) && volNames[sources[r.Source].Name] {
				searchHeld++
			} else {
				available++
			}
		}
	}
	if available == 0 {
		return p.healBeforeRecoveryGate(ctx, nzbID, entryName, nzb, earlyIdx, pending, fetchPosted, cacheSource, tally,
			par2FetchShortfall("no PAR2 recovery volumes retained", int(transportFails.Load())))
	}
	earlyNeeded := 0
	if earlyIdx != nil {
		if earlyK := earlyDamagedSliceCheck(earlyIdx, pending, nzb.Par2Source, nzb.Par2Match, p.logger); earlyK > 0 {
			earlyNeeded = earlyK
			if earlyK > par2.MaxRepairSlicesFor(earlyIdx.SliceSize) || uint32(earlyK) > available {
				p.logger.Info().
					Int("early_k", earlyK).
					Int("max_repair_slices", par2.MaxRepairSlicesFor(earlyIdx.SliceSize)).
					Int64("slice_size", earlyIdx.SliceSize).
					Uint32("available", available).
					Str("entry", entryName).
					Msg("par2: repair provably unavailable before recovery fetch (early arithmetic check)")
				return p.healBeforeRecoveryGate(ctx, nzbID, entryName, nzb, earlyIdx, pending, fetchPosted, cacheSource, tally,
					errMoreDamageThanRecorded(earlyK, available, earlyIdx.SliceSize))
			}
			// Wait here, before the recovery fetch, so a queued large repair
			// doesn't sit on its downloaded volumes while it waits.
			if err := p.waitForLargeRepair(ctx, progress, entryName, earlyK+searchHeld, earlyIdx.SliceSize, &largeHeld); err != nil {
				return err
			}
		}
	}

	needed := firstFetchSlices(estimateNeededSlices(pending), earlyNeeded)
	// fetchedSlices is the name-advertised slice count of the volumes fetched
	// so far. It's only good enough to size and report this first fetch; from
	// here on the code measures recovery coverage by len(idx.Recovery) (the
	// parsed reality), since a fetched-but-mis-served volume inflates this
	// count without contributing any usable slice - see topUpParsedRecovery.
	var fetchedSlices uint32
	nextVolIdx, fetchedSlices, sources, _, err = fetchMoreVolumes(ctx, p.logger, fetch, vols, nextVolIdx, needed, fetchedSlices, sources, entryName, u.ProcessingMaxConnections())
	if err != nil {
		return par2Failf(par2Transient, "fetch recovery volumes: %w", err)
	}
	progress.SetRecoveryVolsFetched(nextVolIdx)
	progress.SetRecoverySlices(int(fetchedSlices), int(needed))
	if len(sources) == 0 {
		return par2FetchShortfall("failed to fetch any PAR2 file", int(transportFails.Load()))
	}

	// The index files showed one set, or none could be chosen from them, but
	// the volumes just fetched may belong to another: an index that did not
	// fetch, or a set posted with no index file. Choose again now that every
	// fetched file's set is visible.
	rechosen := false
	if par2Set == ([16]byte{}) {
		if err := searchUnseenSet(); err != nil {
			return err
		}
		if c, next, ok := par2SetAfterVolumes(ctx, sources, vols, nextVolIdx, nzb.Par2Source, pending); ok {
			if err := applySetChoice(c); err != nil {
				return err
			}
			nextVolIdx = next
			rechosen = par2Set != ([16]byte{})
		}
	}

	idx, sources, err := parse(ctx, sources, par2Set)
	if err != nil {
		return par2IndexFetchTransient(par2IndexParseErr(err), indexFetchErr)
	}
	// applySetChoice counted only the chosen set's named volumes. As at the
	// early gate, recovery slices already in hand (from files with no volume
	// marker) are available too.
	if rechosen && uint32(len(idx.Recovery)) > available {
		available = uint32(len(idx.Recovery))
	}

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
		return p.healBeforeRecoveryGate(ctx, nzbID, entryName, nzb, idx, pending, fetchPosted, cacheSource, tally,
			recoveryShortfall(deadCount, 0, idx))
	}

	// The repair stream checks every intact slice against its IFSC checksum,
	// so a file with no checksums fails the pass part-way through, and every
	// later pass fails the same way on the same volumes. Every PAR2 file of
	// the set carries the packet: fetch further volumes until each file has
	// its checksums or none are left.
	idx, nextVolIdx, sources, err = topUpSliceChecksums(ctx, p.logger, fetch, parse, vols, nextVolIdx, idx, sources, entryName, u.ProcessingMaxConnections(), progress)
	if err != nil {
		return err
	}
	if missing := idx.MissingChecksums(); len(missing) > 0 {
		return par2FetchShortfall(fmt.Sprintf("PAR2 set has no slice checksums for %d file(s)", len(missing)), int(transportFails.Load()))
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
			return par2Failf(par2Terminal, "match posted files: %w", err)
		}
		if ref := par2MatchToCache(nzb.Par2Source, matches, skipped); ref != nil {
			// A cache is only here unused because par2MatchFromCache refused
			// it: replace it rather than redo this matching every pass.
			if serr := u.SaveNZBPar2Match(nzbID, ref, nzb.Par2Match); serr != nil {
				p.logger.Warn().Err(serr).Str("entry", entryName).
					Msg("par2 repair: failed to persist posted-file match cache")
			} else {
				p.logger.Debug().Str("entry", entryName).Int("files", len(ref)).
					Msg("par2 repair: cached posted-file match set for future attempts")
			}
		}
	}
	if len(matches) == 0 {
		if len(skipped) > 0 {
			// Matching skipped files because their bytes failed to fetch:
			// a retry can match them.
			return par2Failf(par2Transient, "no posted file matched the PAR2 recovery set (transient: %d posted file(s) failed to fetch/hash during matching)", len(skipped))
		}
		return par2IndexFetchTransient(par2Failf(par2Terminal, "no posted file matched the PAR2 recovery set"), indexFetchErr)
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

	// An intact article whose body one provider served short or wrong after
	// decode is asked of the others (see postedFailover).
	failover := newPostedFailover(postedServed, func(fctx context.Context, messageID, exceptHost string, check usenet.ArticleCheck) ([]byte, string, error) {
		data, host, err := u.FetchArticleCheckedExcept(fctx, messageID, exceptHost, check)
		if err == nil && readBytes != nil {
			n := int64(len(data))
			atomic.AddInt64(readBytes, n)
			progress.AddUsenetBytes(n)
		}
		return data, host, err
	})
	fetchers := make(map[[16]byte]*postedFileFetcher, len(matches))
	for _, m := range matches {
		file := nzb.Par2Source[m.PostedIndex]
		f := newPostedFileFetcher(ctx, fetchPosted, file, cacheSource, idx.Files[m.FileID].Length, p.logger.With().Str("entry", entryName).Str("file", file.Name).Logger())
		f.failover = failover
		fetchers[m.FileID] = f
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
	msgIDSeg := make(map[string]postedSegPos)
	for _, m := range matches {
		f := fetchers[m.FileID]
		for i, seg := range nzb.Par2Source[m.PostedIndex].Segments {
			msgIDRange[seg.MessageID] = postedRange{fileID: m.FileID, start: f.base[i], end: f.base[i] + f.segSizes[i]}
			msgIDSeg[seg.MessageID] = postedSegPos{fileID: m.FileID, index: i}
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
		return classifyPar2Miss(idx, fileID, nzb.Par2Source, transientUnmatch)
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
	statMissing, statSwept, statExcluded := statPostedFileDamage(statCtx2, p.logger, u.StatSegments, matches, nzb.Par2Source, statControls, entryName, progress)
	statCancel2()
	// A file whose first articles were all 430 is still on estimated
	// boundaries, and none of its dead segments could be placed. Measure
	// it from a later article, skipping those the sweep just found missing,
	// and redo its byte ranges.
	sweptMissing := make(map[string]struct{}, len(statMissing))
	for _, mid := range statMissing {
		sweptMissing[mid] = struct{}{}
	}
	for _, m := range matches {
		f := fetchers[m.FileID]
		if f == nil || !f.resolveGeometryPastDeadHead(sweptMissing) {
			continue
		}
		for i, seg := range nzb.Par2Source[m.PostedIndex].Segments {
			msgIDRange[seg.MessageID] = postedRange{fileID: m.FileID, start: f.base[i], end: f.base[i] + f.segSizes[i]}
		}
	}
	reducedTrust := len(statExcluded) > 0
	statBudget := min(int(available), par2.MaxRepairSlicesFor(idx.SliceSize))
	statMissing, statLeftForSolve := settleStatMisses(ctx, p.logger, entryName, fetchers, msgIDSeg, statMissing, statExcluded,
		max(1, u.ProcessingMaxConnections()), statBudget, progress)
	tally.setStatSweep(statSweepState(statSwept, statExcluded, statLeftForSolve))
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
				_, skipped := transientUnmatch[pn]
				return par2UnmatchedDeadSegmentErr(seg.MessageID, file, pn, isPosted && skipped, indexFetchErr)
			}
			if _, err := idx.DamagedSlices(rng.fileID, rng.start, rng.end); err != nil {
				return par2Failf(par2Terminal, "map dead segment %s to slices: %w", seg.MessageID, err)
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
	progress.SetStatTotal(0) // the article count above is finished; this step has none
	recordedRefs := len(deadRefs)
	deadRefs, err = p.healFetchableDeadSegments(ctx, nzbID, entryName, nzb, idx, fetchers, deadRefs, statMissingSet)
	progress.SetPhase(Par2PhaseFetchingRecovery)
	progress.Touch()
	if err != nil {
		return err
	}
	tally.addHealed(recordedRefs - len(deadRefs))
	// Every recorded dead segment healed. Articles the STAT sweep settled as
	// missing are still gone: returning here reported the release repaired
	// and left them for the reader to pad one at a time. Record each one the
	// reader serves and run the solve for them; the discovered-article step
	// after it patches them. With none served by the reader, there is
	// nothing to repair for playback.
	sweepOnly := false
	var sweptUnplaced error
	if len(deadRefs) == 0 {
		if len(statMissing) == 0 {
			return nil // every dead segment healed; the heal invalidated them
		}
		swept := p.recordSweepMisses(nzbID, entryName, discoveredDeadRefs(nzb, matches, fetchers, msgIDRange, nil))
		if len(swept) == 0 {
			return nil
		}
		var placeable []par2DeadRef
		if placeable, sweptUnplaced = splitPlaceableDeadRefs(fetchers, swept); len(placeable) == 0 {
			return sweptUnplaced
		}
		sweepOnly = true
	}
	// Only segments on measured geometry can be patched (see
	// splitPlaceableDeadRefs). When none can, the solve - a full read of the
	// release - would produce nothing writable; stop before it.
	placeableRefs, unplaced := splitPlaceableDeadRefs(fetchers, deadRefs)
	if len(placeableRefs) == 0 && !sweepOnly {
		return unplaced
	}
	if sweepOnly {
		// Swept articles that cannot be placed stay recorded dead; the pass
		// reports them as it would recorded ones.
		unplaced = sweptUnplaced
	}
	for _, dr := range deadRefs {
		slices, err := idx.DamagedSlices(dr.rng.fileID, dr.rng.start, dr.rng.end)
		if err != nil {
			return par2Failf(par2Terminal, "map dead segment %s to slices: %w", dr.seg.MessageID, err)
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
	statSlicesOf := func(mid string) []int64 {
		rng, ok := msgIDRange[mid]
		if !ok {
			return nil
		}
		slices, derr := idx.DamagedSlices(rng.fileID, rng.start, rng.end)
		if derr != nil {
			return nil
		}
		return slices
	}
	if found, seeded := foldStatSlices(damagedSet, statMissing, statSlicesOf, reducedTrust, statBudget); !seeded {
		p.logger.Warn().Str("entry", entryName).Int("slices", found).Int("recorded_slices", overlayDamaged).Int("budget", statBudget).
			Strs("providers_left_out", statExcluded).
			Str(par2NoteField, fmt.Sprintf("%d more slices on top of %d recorded • budget %d • without %s", found, overlayDamaged, statBudget, strings.Join(statExcluded, ", "))).
			Msg("par2 repair: damage found with a provider left out is over the recovery budget; leaving it for the solve to confirm")
		tally.setStatSweep(statSweepState(statSwept, statExcluded, true))
	}
	if sweepOnly && len(damagedSet) == 0 {
		// Found with a provider left out and over the recovery budget, so
		// not seeded (see foldStatSlices). They are recorded: the next pass
		// takes them as recorded damage.
		return par2Failf(par2Transient, "%d article(s) the STAT sweep found missing were recorded but not repaired (transient: found with %s left out, and over the recovery budget of %d slices)",
			len(statMissing), strings.Join(statExcluded, ", "), statBudget)
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
		return par2Failf(par2Transient, "pre-solve coverage check: %w", cerr)
	}
	// Fold each uncovered file's slices into the damaged set so the solver
	// rebuilds them from parity - whether the miss is transient (its match
	// needed bytes that failed to fetch) or structural (a small file the PAR2
	// set describes but the NZB never posted: an .nfo, an .sfv). Its slices
	// are only reconstructed to run the solve, never patched. Cost: one
	// recovery slice per file slice. Only a file that does not fit the
	// recovery budget still fails the job, and then as the miss's own kind:
	// a transient miss must not become the terminal "more damage than
	// recorded" of the round-loop gate.
	folded, ferr := foldUncoveredFiles(p.logger, entryName, idx, uncov, damagedSet, min(par2.MaxRepairSlicesFor(idx.SliceSize), int(available)), classifyMiss)
	if ferr != nil {
		return ferr
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
	// BODY is gone everywhere), so this is where most of them surface. Bounded by maxIntactRepairRounds full reads plus
	// maxTransportCutRounds reads cut short by a transport error (see par2FailedRounds); a release with damage
	// beyond what the overlay had recorded aborts with a clear terminal
	// reason well before that, rather than spinning.
	var repaired []par2.RepairedSlice
	var damagedPos map[int64]struct{}
	var failedRounds par2FailedRounds
	for round := 0; ; round++ {
		tally.setRounds(round + 1)
		k := len(damaged)
		if k == 0 {
			return par2Failf(par2Transient, "no damaged slices resolved (nothing to repair)")
		}
		if k > par2.MaxRepairSlicesFor(idx.SliceSize) || uint32(k) > available {
			return errMoreDamageThanRecorded(k, available, idx.SliceSize)
		}
		// No early index, or k grew past the large-repair size this round.
		if err := p.waitForLargeRepair(ctx, progress, entryName, k, idx.SliceSize, &largeHeld); err != nil {
			return err
		}

		// Top up recovery slice DATA until we hold at least k PARSED recovery
		// slices - discovering more damage mid-pass can push k past the
		// original name-only estimate that sized the first fetch, and a volume
		// that fetched but was mis-served yields nothing once ParseIndex skips
		// its bad-MD5 packets. See topUpParsedRecovery.
		idx, nextVolIdx, sources, err = topUpParsedRecovery(
			ctx, p.logger, fetch, parse, vols, nextVolIdx, k, idx, sources,
			entryName, u.ProcessingMaxConnections(), progress)
		if err != nil {
			return err
		}
		if k > len(idx.Recovery) {
			return recoveryShortfall(k, len(idx.Recovery), idx)
		}

		// Choose k recovery slices whose solve matrix is invertible for
		// this damage before streaming the release: taking the first k
		// found a singular matrix (possible with gapped exponents) only
		// after the full read, and never tried a spare.
		exps := make([]uint32, len(idx.Recovery))
		for i, ref := range idx.Recovery {
			exps[i] = ref.Exponent
		}
		picked, perr := par2.PickRecovery(damaged, exps, k)
		if perr != nil {
			if nextVolIdx < len(vols) {
				return par2Failf(par2Transient, "%w (transient: more recovery volumes can be fetched)", perr)
			}
			return perr
		}
		chosenRefs := make([]par2.RecoverySliceRef, k)
		for i, j := range picked {
			chosenRefs[i] = idx.Recovery[j]
		}
		recovery := make([]par2.RecoverySlice, k)
		for i, ref := range chosenRefs {
			src := sources[ref.Source].Data
			if ref.Offset+ref.Length > int64(len(src)) {
				return par2Failf(par2Terminal, "recovery slice %d out of range in %s", i, sources[ref.Source].Name)
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
		trackedFetch := func(fctx context.Context, i int64) (data []byte, err error) {
			// The slice source calls this on its own worker goroutines,
			// outside runJob's recover, and ReadSlice does the offset
			// arithmetic over release data that has panicked before on
			// unusual postings. Turn a panic into this slice's error (tagged
			// transient, like runJob's own) so it fails the pass instead of
			// stopping the process.
			defer func() {
				if r := recover(); r != nil {
					p.logger.Error().Str("entry", entryName).Int64("slice", i).Interface("panic", r).
						Bytes("stack", debug.Stack()).Msg("par2 repair: reading an intact slice panicked; recovered")
					data, err = nil, par2Failf(par2Transient, "reading PAR2 slice %d panicked: %v (transient: internal error)", i, r)
				}
			}()
			data, err = jobSource.ReadSlice(fctx, i)
			progress.AddIntactRead(1)
			if atomic.AddInt64(&intactRead, 1) == intactTotal {
				progress.SetPhase(Par2PhaseSolving)
			}
			return data, err
		}
		// Each round's source gets its own context, cancelled as soon as the
		// pass returns. An early-stopped or failed pass otherwise left its
		// workers holding fetched slices (up to 2x the connection count x
		// SliceSize) and their connections until the whole job ended. The
		// workers' fetches run on it too (trackedFetch's fctx), so the cancel
		// aborts the ones still in flight.
		roundCtx, roundCancel := context.WithCancel(ctx)
		// One fetch per article for the whole round, shared by every slice
		// that overlaps it; a failed article is not asked again this round.
		roundArticles := attachPar2RoundArticles(idx, fetchers, u.ProcessingMaxConnections())
		sliceSource := newConcurrentSliceSource(roundCtx, intactOrder, u.ProcessingMaxConnections(), trackedFetch)
		var repairErr error
		// Stop the pass once the unreadable intact slices outnumber the
		// recovery slices left over: the next round would need more than
		// are retained, so the rest of the read only confirms the verdict
		// (Under Reef S11E06 read 2 GB to find 45 against 5 retained).
		spare := min(int(available), par2.MaxRepairSlicesFor(idx.SliceSize)) - k
		var checksumBad []int64 // intact slices whose bytes failed their IFSC this round
		repaired, repairErr = par2.RepairWith(idx, damaged, recovery, sliceSource, par2.RepairOptions{
			MaxUnavailable:     max(0, spare),
			OnChecksumMismatch: func(i int64) { checksumBad = append(checksumBad, i) },
		})
		// Wait for the round's workers before using its result: a worker
		// still inside a fetch adds to readBytes while runJob reads it, can
		// record a dead slice NotFoundIndices below misses, and shares the
		// connection budget with the next round's workers.
		roundCancel()
		sliceSource.Wait()
		roundArticles.detach()
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
		//
		// Only when the job's own ctx has ended: one article's fetch runs
		// under its own par2ArticleFetchTimeout (see fetchSegment), and a
		// stalled article comes back wrapping context.DeadlineExceeded
		// while the job is still live. That is a round cut short by a
		// transport error, counted by par2FailedRounds below like any
		// other timeout.
		if ctx.Err() != nil && (errors.Is(repairErr, context.Canceled) || errors.Is(repairErr, context.DeadlineExceeded)) {
			p.logger.Warn().
				Str("entry", entryName).
				Int("round", round+1).
				Err(repairErr).
				Msg("par2 repair: intact-slice fetch interrupted by context cancellation; treating as transient")
			return repairErr
		}

		notFound := sliceSource.NotFoundIndices()
		newlyDamaged := make([]int64, 0, len(notFound)+len(checksumBad))
		seenNew := make(map[int64]struct{}, len(notFound)+len(checksumBad))
		for _, ni := range append(notFound, checksumBad...) {
			if _, already := damagedPos[ni]; already {
				continue
			}
			if _, dup := seenNew[ni]; dup {
				continue
			}
			seenNew[ni] = struct{}{}
			newlyDamaged = append(newlyDamaged, ni)
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
		kindRound, cut := failedRounds.end(repairErr)
		if stopErr := par2RoundStopError(len(newlyDamaged), kindRound, cut, k, min(int(available), par2.MaxRepairSlicesFor(idx.SliceSize)),
			confirmedMissing, corrupt, shortRead, len(checksumBad), repairErr); stopErr != nil {
			return stopErr
		}
		p.logger.Info().
			Str("entry", entryName).
			Int("newly_damaged", len(newlyDamaged)).
			Int("confirmed_missing", confirmedMissing).
			Int("short_read", shortRead).
			Int("corrupt", corrupt).
			Int("checksum_bad", len(checksumBad)).
			Str("providers", nntp.DescribeOutcomes(nntp.FailoverOutcomes(repairErr))).
			Int("round", round+1).
			Bool("cut_short", cut).
			Bool("stat_sweep_ran", statSwept).
			Str(par2NoteField, fmt.Sprintf("%d more damaged after full read %d", len(newlyDamaged), round+1)).
			Msg("par2 repair: intact slice(s) unreadable; expanding damaged set and retrying")
		damaged = append(damaged, newlyDamaged...)
		sort.Slice(damaged, func(i, j int) bool { return damaged[i] < damaged[j] })
	}
	if slicesRepaired != nil {
		*slicesRepaired = len(repaired)
	}
	progress.SetPhase(Par2PhaseWriting)

	// A file paired with its FileDesc by elimination (or by a differing
	// name) has nothing read behind the pairing. The solve rebuilt the
	// FileDesc's bytes; written over a posted file that is some other file
	// of the same length they would be served as verified data. Its patches
	// wait until a pass reads one of its slices through the pairing.
	unconfirmed := unconfirmedPairings(idx, matches, damagedPos)
	placeableRefs, withheld := withholdUnconfirmedRefs(placeableRefs, unconfirmed)

	repairedByIndex := make(map[int64][]byte, len(repaired))
	for _, rs := range repaired {
		repairedByIndex[rs.Index] = rs.Data
	}

	// Whatever gets patched is invalidated even if a later segment fails
	// here: its stale zero-fill would otherwise outlive the patch.
	var written []par2DeadRef
	defer func() {
		if len(written) > 0 {
			p.invalidateRepairedRanges(nzb, nzbID, entryName, deadRefsByFile(written))
		}
	}()
	// The pass worked out every article's place from the record it loaded at
	// the start. If the record has changed since (a volume reorder), a patch
	// written by those places would land on another article. Nothing is
	// written. The reorder refuses while a pass is running or queued, so
	// this is a second line of defence.
	fresh, err := u.GetNZB(nzbID)
	if err != nil {
		return par2Failf(par2Transient, "reload NZB record before writing patches: %w", err)
	}
	if err := par2RecordMoved(nzb, fresh, placeableRefs); err != nil {
		return par2Failf(par2Transient, "the NZB record changed during the pass, no patch written: %w (transient: this pass's copy of the record is out of date)", err)
	}
	recorded, unwritable, err := p.writeRecordedPatches(nzbID, entryName, nzb, idx, repairedByIndex, placeableRefs)
	written = append(written, recorded...)
	tally.addPatchedRecorded(len(recorded))
	if err != nil {
		return err
	}

	// Articles this pass itself found gone or corrupt everywhere: their
	// slices were just rebuilt, so record and patch them too instead of
	// discarding the work. Best-effort - one that cannot be placed or
	// written is logged and left, never failing a pass that repaired what
	// it was asked to.
	discovered, _ := splitPlaceableDeadRefs(fetchers, discoveredDeadRefs(nzb, matches, fetchers, msgIDRange, deadRefs))
	discovered, withheldDiscovered := withholdUnconfirmedRefs(discovered, unconfirmed)
	withheld = append(withheld, withheldDiscovered...)
	patchedDiscovered := 0
	for _, dr := range discovered {
		start, end, werr := readerPatchWindow(nzb, dr)
		if werr != nil {
			continue
		}
		if err := par2RecordMoved(nzb, fresh, []par2DeadRef{dr}); err != nil {
			p.logger.Warn().Err(err).Str("entry", entryName).Str("file", dr.file).Int("segment", dr.seg.Index).
				Msg("par2 repair: the NZB record changed during the pass; not patching an article it found dead")
			continue
		}
		data, err := extractPostedRange(idx, repairedByIndex, dr.rng.fileID, start, end)
		if err != nil {
			continue // a covering slice was not rebuilt
		}
		if err := u.RecordOverlayDead(nzbID, dr.file, dr.seg.Index, dr.seg.MessageID, dr.seg.Bytes); err != nil {
			p.logger.Warn().Err(err).Str("entry", entryName).Str("file", dr.file).Int("segment", dr.seg.Index).
				Msg("par2 repair: could not record an article the pass found dead")
			continue
		}
		if err := u.OverlayWritePatch(nzbID, dr.file, dr.seg.Index, data); err != nil {
			p.logger.Warn().Err(err).Str("entry", entryName).Str("file", dr.file).Int("segment", dr.seg.Index).
				Msg("par2 repair: could not patch an article the pass found dead")
			continue
		}
		u.ClearFailedFile(nzbID, dr.file)
		written = append(written, dr)
		patchedDiscovered++
	}
	tally.addPatchedDiscovered(patchedDiscovered)
	if patchedDiscovered > 0 {
		p.logger.Info().Str("entry", entryName).Int("articles", patchedDiscovered).
			Str(par2NoteField, logger.Count(patchedDiscovered, "article")).
			Msg("par2 repair: also patched articles the pass found missing or corrupt on every provider")
	}

	// Segments whose boundaries were never measured stay recorded dead, and
	// the pass reports them rather than claiming a full repair. Only when
	// they are the sole damage left does an unwritable segment decide the
	// verdict: the unplaced error keeps its own class (a timed-out geometry
	// probe is retried), and the next pass meets the unwritable one again.
	if unplaced != nil {
		return unplaced
	}
	if len(withheld) > 0 {
		return unconfirmedPairingErr(withheld, matches, nzb)
	}
	return unwritable
}

// unconfirmedPairings returns the FileIDs whose pairing no content backs
// (par2.Match.Unconfirmed) and of which this pass read no slice: every
// slice of the file was in the damaged set and rebuilt from parity. A
// successful par2.RepairWith read every slice outside damaged and checked
// it against its IFSC, so one such slice proves the posted file is the
// FileDesc it was paired with.
func unconfirmedPairings(idx *par2.Index, matches []par2.Match, damaged map[int64]struct{}) map[[16]byte]bool {
	var out map[[16]byte]bool
	for _, m := range matches {
		if !m.Unconfirmed() {
			continue
		}
		fd := idx.Files[m.FileID]
		if fd == nil {
			continue
		}
		slices, err := idx.DamagedSlices(m.FileID, 0, fd.Length)
		confirmed := false
		if err == nil {
			for _, s := range slices {
				if _, dmg := damaged[s]; !dmg {
					confirmed = true
					break
				}
			}
		}
		if !confirmed {
			if out == nil {
				out = make(map[[16]byte]bool)
			}
			out[m.FileID] = true
		}
	}
	return out
}

// withholdUnconfirmedRefs splits off the refs in a file of unconfirmed (see
// unconfirmedPairings), which must not be patched.
func withholdUnconfirmedRefs(refs []par2DeadRef, unconfirmed map[[16]byte]bool) (kept, withheld []par2DeadRef) {
	if len(unconfirmed) == 0 {
		return refs, nil
	}
	kept = refs[:0:0]
	for _, dr := range refs {
		if unconfirmed[dr.rng.fileID] {
			withheld = append(withheld, dr)
			continue
		}
		kept = append(kept, dr)
	}
	return kept, withheld
}

// unconfirmedPairingErr is the pass's error when patches were withheld for
// an unconfirmed pairing. Transient: the segments stay recorded dead, and a
// later pass that reads one slice of the file confirms the pairing.
func unconfirmedPairingErr(withheld []par2DeadRef, matches []par2.Match, nzb *storage.NZB) error {
	fid := withheld[0].rng.fileID
	posted, how := withheld[0].file, "elimination"
	for _, m := range matches {
		if m.FileID != fid {
			continue
		}
		if m.PostedIndex >= 0 && m.PostedIndex < len(nzb.Par2Source) {
			posted = nzb.Par2Source[m.PostedIndex].Name
		}
		if m.Evidence == par2.EvidenceName {
			how = "a name that differs from the PAR2 file's"
		}
		break
	}
	return par2Failf(par2Transient, "%d dead segment(s) left unpatched: posted file %q was paired with its PAR2 file by %s and no slice of it could be read to confirm the pairing (transient: a pass that reads one confirms it)", len(withheld), posted, how)
}

// writeRecordedPatches writes the repaired bytes of each placeable recorded
// dead segment to the overlay, sorting refs by file and segment so every
// pass writes them in the same order. It returns the refs it patched.
//
// A segment whose patch cannot be placed - the NZB record no longer has
// it, its reader slot does not fit the article, a covering slice was not
// rebuilt, or the overlay refuses the length (overlay.ErrPatchGeometry) -
// is skipped, and the rest are still written: the solve has already rebuilt
// their bytes. Those refusals come out the same on every pass, so
// unwritable is classed terminal. Any other
// write error (the entry was deleted, an I/O failure) is returned as err at
// once.
func (p *Par2Repair) writeRecordedPatches(nzbID, entryName string, nzb *storage.NZB, idx *par2.Index, repairedByIndex map[int64][]byte, refs []par2DeadRef) (written []par2DeadRef, unwritable, err error) {
	u := p.manager.usenet
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].file != refs[j].file {
			return refs[i].file < refs[j].file
		}
		return refs[i].seg.Index < refs[j].seg.Index
	})
	skipped := 0
	var first error
	skip := func(dr par2DeadRef, cause error) {
		p.logger.Warn().Err(cause).Str("entry", entryName).Str("file", dr.file).Int("segment", dr.seg.Index).
			Msg("par2 repair: repaired bytes for a dead segment cannot be patched; writing the others")
		skipped++
		if first == nil {
			first = cause
		}
	}
	for _, dr := range refs {
		start, end, werr := readerPatchWindow(nzb, dr)
		if werr != nil {
			skip(dr, fmt.Errorf("patch window for %s segment %d: %w", dr.file, dr.seg.Index, werr))
			continue
		}
		data, xerr := extractPostedRange(idx, repairedByIndex, dr.rng.fileID, start, end)
		if xerr != nil {
			skip(dr, fmt.Errorf("extract repaired bytes for %s: %w", dr.seg.MessageID, xerr))
			continue
		}
		if perr := u.OverlayWritePatch(nzbID, dr.file, dr.seg.Index, data); perr != nil {
			perr = fmt.Errorf("write patch for %s segment %d: %w", dr.file, dr.seg.Index, perr)
			if errors.Is(perr, overlay.ErrPatchGeometry) {
				skip(dr, perr)
				continue
			}
			return written, nil, par2Fail(par2Transient, perr)
		}
		// A prior playback read may have permanently cached this file as
		// failed (see Usenet.shouldPoisonFailedFile) before this repair
		// patched its damage. Un-poison it now so the next read builds a
		// fresh reader against the now-repaired file instead of
		// short-circuiting on a stale cause. Covers both automatic repair
		// and a manual "repair now" (RunNow runs this exact same path).
		u.ClearFailedFile(nzbID, dr.file)
		written = append(written, dr)
	}
	if skipped > 0 {
		unwritable = par2Failf(par2Terminal, "%d dead segment(s) cannot be patched: reader slot and article disagree (first: %w)", skipped, first)
	}
	return written, unwritable, nil
}

// par2RoundShouldStop decides whether runRepair's round loop ends with the
// cause-labelled error instead of folding newly unreadable slices in and
// trying again: nothing new was found, the enlarged damaged set cannot fit
// the recovery budget (retained recovery slices, capped by
// par2.MaxRepairSlicesFor the set's slice size), or the round cap for this
// kind of round is reached. The budget case is the one an early-stopped pass
// always hits. Without it the loop went round again and the next round's
// opening capacity gate returned "more damage than recorded" - terminal - so
// a short-read or checksum-bad pass, which classifyPar2Failure treats as
// suspect, went terminal on its first attempt.
//
// round is the 0-based index of this round among failed rounds of its own
// kind (see par2FailedRounds): a read that ran to the end counts toward
// maxIntactRepairRounds, one cut short by a transport error toward
// maxTransportCutRounds. The reason says which stop fired. Only the two caps
// can stop with the enlarged set still inside the budget.
func par2RoundShouldStop(newlyDamaged, round int, cut bool, k, budget int) (bool, par2RoundStop) {
	switch {
	case newlyDamaged == 0:
		return true, par2RoundStopNothingNew
	case k+newlyDamaged > budget:
		return true, par2RoundStopOverBudget
	case cut && round >= maxTransportCutRounds-1:
		return true, par2RoundStopCutCap
	case !cut && round >= maxIntactRepairRounds-1:
		return true, par2RoundStopCap
	}
	return false, par2RoundContinue
}

// par2RoundStop is why runRepair's round loop ended.
type par2RoundStop int

const (
	par2RoundContinue par2RoundStop = iota
	par2RoundStopNothingNew
	par2RoundStopOverBudget
	par2RoundStopCap
	par2RoundStopCutCap
)

// maxTransportCutRounds bounds the failed rounds whose read was cut short by
// a transport error (see par2RoundCutShort). They do not count toward
// maxIntactRepairRounds: such a round stops at the first slice that times
// out, so the few dead slices it found say nothing about how many more the
// rest of the read would find. Bounded on their own so a dead connection
// cannot loop for ever.
const maxTransportCutRounds = 3

// par2FailedRounds counts runRepair's failed rounds by how their read ended.
type par2FailedRounds struct {
	full, cut int
}

// end records a failed round and returns its 0-based index among rounds of
// the same kind, and whether it was cut short by a transport error.
func (r *par2FailedRounds) end(repairErr error) (round int, cut bool) {
	if par2RoundCutShort(repairErr) {
		r.cut++
		return r.cut - 1, true
	}
	r.full++
	return r.full - 1, false
}

// par2RoundCutShort reports whether par2.RepairWith stopped on an intact
// slice read that failed without a verdict on the slice: not unavailable (a
// 430, a short decode or a corrupt copy, which RepairWith streams past), not
// a checksum abort, and not one of runRepair's own classed errors (a missing
// fetcher, a recovered panic). In practice that is a provider timeout or a
// dropped connection, or one article's own fetch timeout expiring while the
// job is live. A cancelled job never gets here: the round loop returns it
// first.
func par2RoundCutShort(repairErr error) bool {
	if repairErr == nil || errors.Is(repairErr, par2.ErrSliceUnavailable) || errors.Is(repairErr, par2.ErrChecksumMismatch) {
		return false
	}
	var classed par2Classed
	return !errors.As(repairErr, &classed)
}

// par2RoundStopError returns nil when the round loop should fold the newly
// damaged slices in and go round again, and otherwise the error the pass
// ends with. round and cut are as par2FailedRounds.end returns them. A stop
// at either round cap with the enlarged damaged set still inside the budget
// is transient: with providers timing out, each round can be cut short
// before it has read everything, so it finds a few more dead slices each
// time. That says nothing about whether the damage fits, and one more read
// (after the backoff) can repair it. The cause text is kept for the log, but
// repairErr is not wrapped, so a 430 inside it cannot make
// classifyPar2Failure treat the stop as terminal before it sees the tag.
func par2RoundStopError(newlyDamaged, round int, cut bool, k, budget, confirmedMissing, corrupt, shortRead, checksumBad int, repairErr error) error {
	stop, why := par2RoundShouldStop(newlyDamaged, round, cut, k, budget)
	if !stop {
		return nil
	}
	capErr := par2RoundCapError(confirmedMissing, corrupt, shortRead, checksumBad, repairErr)
	switch why {
	case par2RoundStopCap:
		return par2Failf(par2Transient, "%v (transient: stopped after %d reads with the damage still inside the recovery budget, %d of %d slices)",
			capErr, round+1, k+newlyDamaged, budget)
	case par2RoundStopCutCap:
		return par2Failf(par2Transient, "%v (transient: stopped after %d reads cut short by a transport error, with the damage still inside the recovery budget, %d of %d slices)",
			capErr, round+1, k+newlyDamaged, budget)
	}
	return capErr
}

// par2RoundCapError names why intact slices were unreadable when the round
// loop stops, and classes the stop by those causes: a confirmed-dead or
// every-provider-corrupt slice makes it terminal, a short read or bad
// checksum alone suspect. A repairErr classed transient (the stream stopped
// on a retryable error, such as a posted file that failed to match this
// pass) keeps the stop transient. With no cause counted the stop has no
// class of its own, and repairErr decides.
func par2RoundCapError(confirmedMissing, corrupt, shortRead, checksumBad int, repairErr error) error {
	class := par2Suspect
	if confirmedMissing > 0 || corrupt > 0 {
		class = par2Terminal
	}
	var classed par2Classed
	if errors.As(repairErr, &classed) && classed.par2Class() == par2Transient {
		class = par2Transient
	}
	type cause struct {
		n    int
		text string
	}
	var causes []cause
	for _, c := range []cause{
		{confirmedMissing, "confirmed missing across every provider"},
		{corrupt, "corrupt on every provider"},
		{shortRead, "decoded shorter than their recorded size"},
		{checksumBad, "failed their PAR2 checksum"},
	} {
		if c.n > 0 {
			causes = append(causes, c)
		}
	}
	switch len(causes) {
	case 0:
		return fmt.Errorf("repair: %w", repairErr)
	case 1:
		return par2Failf(class, "repair: %d intact slice(s) %s: %w", causes[0].n, causes[0].text, repairErr)
	}
	total := 0
	parts := make([]string, len(causes))
	for i, c := range causes {
		total += c.n
		parts[i] = fmt.Sprintf("%d %s", c.n, c.text)
	}
	return par2Failf(class, "repair: %d intact slice(s) unreadable - %s: %w", total, strings.Join(parts, ", "), repairErr)
}

// deadRefsByFile groups refs back into the file -> dead segments shape
// invalidateRepairedRanges takes.
func deadRefsByFile(refs []par2DeadRef) map[string][]overlay.DeadSegment {
	out := make(map[string][]overlay.DeadSegment)
	for _, dr := range refs {
		out[dr.file] = append(out[dr.file], dr.seg)
	}
	return out
}

// par2DeadRef is one recorded-dead segment and the posted-file byte range it
// covers.
type par2DeadRef struct {
	file string
	seg  overlay.DeadSegment
	rng  postedRange
}

// splitPlaceableDeadRefs separates dead segments whose posted file has
// measured article boundaries - where a patch cut from the repaired slices
// lands on exactly the article's bytes - from the rest. On estimated
// boundaries the cut is shifted by however far the estimate is off, and
// IFSC cannot catch it: it proves the reconstructed slices, not where the
// patch window sits in them. unplaced, when non-nil, explains the rest and
// wraps the geometry probe's error, so a dead head article (430) classifies
// terminal and a timeout backs off.
func splitPlaceableDeadRefs(fetchers map[[16]byte]*postedFileFetcher, deadRefs []par2DeadRef) (placeable []par2DeadRef, unplaced error) {
	var skipped int
	var cause error
	var file string
	for _, dr := range deadRefs {
		// A one-article file has a single boundary, at 0, and its size is
		// the PAR2 file length: nothing to measure.
		if f := fetchers[dr.rng.fileID]; f != nil && (f.exact || (len(f.segs) == 1 && f.trueLen > 0)) {
			placeable = append(placeable, dr)
			continue
		}
		skipped++
		if file == "" {
			file = dr.file
			if f := fetchers[dr.rng.fileID]; f != nil {
				cause = f.geometryErr
			}
		}
	}
	if skipped == 0 {
		return placeable, nil
	}
	if cause == nil {
		cause = errors.New("no article could be measured")
	}
	return placeable, par2Failf(par2Transient, "%d dead segment(s) left unpatched: article boundaries of %q were not measured, so a patch cannot be placed: %w", skipped, file, cause)
}

// recordSweepMisses records in the overlay each article the STAT sweep
// settled as missing that the reader serves (refs from discoveredDeadRefs),
// before a pass with no other damage runs the solve for them. If that solve
// fails, they stay recorded and the next pass takes them as recorded
// damage. Returns the refs it recorded.
func (p *Par2Repair) recordSweepMisses(nzbID, entryName string, refs []par2DeadRef) []par2DeadRef {
	u := p.manager.usenet
	recorded := refs[:0:0]
	for _, dr := range refs {
		if err := u.RecordOverlayDead(nzbID, dr.file, dr.seg.Index, dr.seg.MessageID, dr.seg.Bytes); err != nil {
			p.logger.Warn().Err(err).Str("entry", entryName).Str("file", dr.file).Int("segment", dr.seg.Index).
				Msg("par2 repair: could not record an article the STAT sweep found missing")
			continue
		}
		recorded = append(recorded, dr)
	}
	if len(recorded) > 0 {
		p.logger.Info().Str("entry", entryName).Int("articles", len(recorded)).
			Str(par2NoteField, logger.Count(len(recorded), "article")).
			Msg("par2 repair: recorded damage healed; repairing articles the STAT sweep found missing")
	}
	return recorded
}

// par2HealFlushBytes bounds the verified bytes healFetchableDeadSegments
// holds before writing them: the heal can run on damage beyond the recovery
// budget (healBeforeRecoveryGate), so holding all of it could be gigabytes.
const par2HealFlushBytes = 64 << 20

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
//
// The verified bytes are written in batches, each against the NZB record
// reloaded just before it: a segment whose reader slot moved since the pass
// loaded nzb (a volume reorder) is not written, and the solve's own check
// then ends the pass transient, as it does for its patches.
func (p *Par2Repair) healFetchableDeadSegments(ctx context.Context, nzbID, entryName string, nzb *storage.NZB, idx *par2.Index, fetchers map[[16]byte]*postedFileFetcher, deadRefs []par2DeadRef, statMissing map[string]struct{}) ([]par2DeadRef, error) {
	u := p.manager.usenet
	remaining := deadRefs[:0:0]
	var healedRefs []par2DeadRef
	healed := 0
	type verifiedPatch struct {
		dr   par2DeadRef
		data []byte
	}
	var batch []verifiedPatch
	var batchBytes int
	flush := func() {
		if len(batch) == 0 {
			return
		}
		fresh, ferr := u.GetNZB(nzbID)
		for _, vp := range batch {
			dr := vp.dr
			if ferr != nil {
				p.logger.Warn().Err(ferr).Str("entry", entryName).Str("file", dr.file).Int("segment", dr.seg.Index).
					Msg("par2 repair: could not reload the NZB record before patching a dead segment fetched intact; leaving it to the solve")
				remaining = append(remaining, dr)
				continue
			}
			if err := par2RecordMoved(nzb, fresh, []par2DeadRef{dr}); err != nil {
				p.logger.Warn().Err(err).Str("entry", entryName).Str("file", dr.file).Int("segment", dr.seg.Index).
					Msg("par2 repair: the NZB record changed during the pass; not patching a dead segment fetched intact")
				remaining = append(remaining, dr)
				continue
			}
			if err := u.OverlayWritePatch(nzbID, dr.file, dr.seg.Index, vp.data); err != nil {
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
			healedRefs = append(healedRefs, dr)
			healed++
		}
		batch, batchBytes = nil, 0
	}
	var ctxErr error
	for _, dr := range deadRefs {
		if ctx.Err() != nil {
			ctxErr = ctx.Err()
			break
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
		batch = append(batch, verifiedPatch{dr: dr, data: data})
		if batchBytes += len(data); batchBytes >= par2HealFlushBytes {
			flush()
		}
	}
	// Bytes already verified are written even when ctx ended: the writes
	// are local, and the next pass would fetch them again.
	flush()
	// Invalidated here, not by the caller on success: the solve that follows
	// can still fail, and the healed segments' stale zero-fill in the DFS
	// cache and warm readers would then outlive their patches.
	if len(healedRefs) > 0 {
		p.invalidateRepairedRanges(nzb, nzbID, entryName, deadRefsByFile(healedRefs))
	}
	if healed > 0 {
		p.logger.Info().Str("entry", entryName).Int("healed", healed).Int("remaining", len(remaining)).
			Msg("par2 repair: dead segment(s) fetched intact and verified against PAR2 checksums; patched without parity")
	}
	if ctxErr != nil {
		return nil, ctxErr
	}
	return remaining, nil
}

// errHealNoTieBreak is what healWithoutRecovery's MD5-16k callbacks return:
// with no recovery data in hand the pass fetches nothing to break a length
// tie, so a tied file is simply left unmatched.
var errHealNoTieBreak = errors.New("par2 heal: length tie not broken without recovery data")

// healBeforeRecoveryGate runs when a recovery-capacity gate is about to fail
// the job terminally (no recovery volumes, the early arithmetic check, or no
// recovery slice parsed). Those gates count every recorded-dead segment as
// damage, but playback records a segment dead on one failover pass, and the
// article is often back (Deep in Orbit S02E01: all nine). Healing needs no
// parity - only the index's IFSC - so it is tried first:
//   - every dead segment healed: the job succeeds (nil);
//   - some healed: a transient error, so the next attempt re-evaluates the
//     smaller damage against the same recovery data;
//   - none healed: gateErr, unchanged.
func (p *Par2Repair) healBeforeRecoveryGate(ctx context.Context, nzbID, entryName string, nzb *storage.NZB, idx *par2.Index, pending map[string][]overlay.DeadSegment, fetch postedFetchFunc, cacheSource *cacheSlicedSource, tally *par2PassTally, gateErr error) error {
	healed, total := p.healWithoutRecovery(ctx, nzbID, entryName, nzb, idx, pending, fetch, cacheSource, p.manager.usenet.StatSegments)
	tally.addHealed(healed)
	switch {
	case total > 0 && healed == total:
		p.logger.Info().Str("entry", entryName).Int("healed", healed).AnErr("gate", gateErr).
			Msg("par2 repair: every dead segment healed without recovery data")
		return nil
	case ctx.Err() != nil:
		return ctx.Err() // interrupted mid-heal proves nothing about the recovery data
	case healed > 0:
		// Deliberately not wrapping gateErr: its text carries the terminal
		// substrings this attempt has just disproved for part of the damage.
		p.logger.Info().Str("entry", entryName).Int("healed", healed).Int("dead", total).AnErr("gate", gateErr).
			Msg("par2 repair: healed part of the damage without recovery data; retrying on the rest")
		return par2Failf(par2Transient, "healed %d of %d dead segments without parity; re-evaluating the remaining damage", healed, total)
	default:
		return gateErr
	}
}

// healWithoutRecovery maps pending's dead segments onto idx with no network
// matching - the persisted match cache, or MatchFiles' unique-length and
// unique-name pairings with every tie left unbroken - skips the articles STAT
// confirms gone, and heals the rest through healFetchableDeadSegments. It
// returns how many were healed out of how many were pending; a segment that
// cannot be mapped counts as not healed.
func (p *Par2Repair) healWithoutRecovery(ctx context.Context, nzbID, entryName string, nzb *storage.NZB, idx *par2.Index, pending map[string][]overlay.DeadSegment, fetch postedFetchFunc, cacheSource *cacheSlicedSource, stat func(context.Context, []string) ([]nntp.StatResult, error)) (healed, total int) {
	for _, segs := range pending {
		total += len(segs)
	}
	if idx == nil || nzb == nil || total == 0 || ctx.Err() != nil {
		return 0, total
	}

	matches, ok := par2MatchFromCache(idx, nzb.Par2Source, nzb.Par2Match)
	if !ok {
		posted := make([]par2.PostedFile, len(nzb.Par2Source))
		for i, f := range nzb.Par2Source {
			posted[i] = par2.PostedFile{Name: f.Name, Length: f.Size, MD5_16k: func() ([16]byte, error) {
				return [16]byte{}, errHealNoTieBreak
			}}
		}
		var err error
		if matches, _, err = par2.MatchFiles(idx, posted); err != nil {
			return 0, total
		}
	}
	byPosted := make(map[int][16]byte, len(matches))
	for _, m := range matches {
		byPosted[m.PostedIndex] = m.FileID
	}
	type segPos struct{ posted, seg int }
	where := make(map[string]segPos)
	for pi := range nzb.Par2Source {
		for si, seg := range nzb.Par2Source[pi].Segments {
			where[seg.MessageID] = segPos{pi, si}
		}
	}

	var ids []string
	for _, segs := range pending {
		for _, s := range segs {
			ids = append(ids, s.MessageID)
		}
	}
	missing := make(map[string]struct{})
	if stat != nil {
		statCtx, cancel := context.WithTimeout(ctx, par2RecoveryStatTimeout)
		results, err := stat(statCtx, ids)
		cancel()
		if err == nil {
			for _, r := range results {
				if !r.Available && nntp.IsArticleNotFoundError(r.Error) {
					missing[r.MessageID] = struct{}{}
				}
			}
		}
	}

	fetchers := make(map[[16]byte]*postedFileFetcher)
	var refs []par2DeadRef
	for file, segs := range pending {
		for _, s := range segs {
			if _, gone := missing[s.MessageID]; gone {
				continue
			}
			pos, ok := where[s.MessageID]
			if !ok {
				continue
			}
			fileID, ok := byPosted[pos.posted]
			if !ok {
				continue
			}
			fd := idx.Files[fileID]
			if fd == nil {
				continue
			}
			f := fetchers[fileID]
			if f == nil {
				ref := nzb.Par2Source[pos.posted]
				f = newPostedFileFetcher(ctx, fetch, ref, cacheSource, fd.Length, p.logger.With().Str("entry", entryName).Str("file", ref.Name).Logger())
				f.resolveGeometry()
				// Past a dead head, measure from a later article, skipping
				// those STAT just found missing (as runRepair does after
				// its sweep).
				f.resolveGeometryPastDeadHead(missing)
				fetchers[fileID] = f
			}
			refs = append(refs, par2DeadRef{
				file: file,
				seg:  s,
				rng:  postedRange{fileID: fileID, start: f.base[pos.seg], end: f.base[pos.seg] + f.segSizes[pos.seg]},
			})
		}
	}
	if len(refs) == 0 {
		return 0, total
	}
	remaining, err := p.healFetchableDeadSegments(ctx, nzbID, entryName, nzb, idx, fetchers, refs, nil)
	if err != nil {
		return 0, total
	}
	return len(refs) - len(remaining), total
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
		data, _, err := f.readRange(f.ctx, local*idx.SliceSize, idx.SliceSize, false)
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

// moveRelocatedDeadRecords moves each pending dead record whose article nzb
// now holds at another index of the same file (a volume reorder after the
// damage was recorded) to that index, in the overlay and in the returned
// copy of pending. Left at its old index, the record named a slot holding
// another article, so readerPatchWindow refused it and the pass ended in
// writeRecordedPatches' terminal skip - in the urgent lane, a re-grab. A
// record whose article is nowhere in the file, or in it more than once, is
// left as it was. A failed move is logged and leaves pending unchanged.
func (p *Par2Repair) moveRelocatedDeadRecords(nzbID, entryName string, nzb *storage.NZB, pending map[string][]overlay.DeadSegment) map[string][]overlay.DeadSegment {
	var out map[string][]overlay.DeadSegment
	for name, segs := range pending {
		file := nzb.GetFileByName(name)
		if file == nil {
			continue
		}
		var where map[string]int // message ID → index; -1 when it appears twice
		var moves []overlay.DeadMove
		for _, d := range segs {
			if d.Index >= 0 && d.Index < len(file.Segments) && file.Segments[d.Index].MessageID == d.MessageID {
				continue
			}
			if where == nil {
				where = make(map[string]int, len(file.Segments))
				for i, s := range file.Segments {
					if _, dup := where[s.MessageID]; dup {
						where[s.MessageID] = -1
					} else {
						where[s.MessageID] = i
					}
				}
			}
			if to, ok := where[d.MessageID]; ok && to >= 0 {
				moves = append(moves, overlay.DeadMove{From: d.Index, To: to, MessageID: d.MessageID, Bytes: file.Segments[to].Bytes})
			}
		}
		if len(moves) == 0 {
			continue
		}
		applied, err := p.manager.usenet.OverlayMoveDeadSegments(nzbID, name, moves)
		if err != nil {
			p.logger.Warn().Err(err).Str("entry", entryName).Str("file", name).
				Msg("par2 repair: could not move dead records to where the NZB record now holds their articles")
			continue
		}
		if len(applied) == 0 {
			continue
		}
		byFrom := make(map[int]overlay.DeadMove, len(applied))
		for _, mv := range applied {
			byFrom[mv.From] = mv
		}
		moved := make([]overlay.DeadSegment, 0, len(segs))
		for _, d := range segs {
			if mv, ok := byFrom[d.Index]; ok && mv.MessageID == d.MessageID {
				if mv.Dropped {
					continue
				}
				d.Index = mv.To
				if mv.Bytes > 0 {
					d.Bytes = mv.Bytes
				}
			}
			moved = append(moved, d)
		}
		sort.Slice(moved, func(i, j int) bool { return moved[i].Index < moved[j].Index })
		if out == nil {
			out = make(map[string][]overlay.DeadSegment, len(pending))
			for n, s := range pending {
				out[n] = s
			}
		}
		out[name] = moved
		p.logger.Info().Str("entry", entryName).Str("file", name).Int("records", len(applied)).
			Msg("par2 repair: dead records moved to where the NZB record now holds their articles")
	}
	if out == nil {
		return pending
	}
	return out
}

// par2RecordMoved reports the first of refs whose reader slot in fresh, the
// NZB record as it is now, is not where it was in old, the record the pass
// worked from. A ref with no slot in old is left to the writer, which skips
// it as before.
func par2RecordMoved(old, fresh *storage.NZB, refs []par2DeadRef) error {
	for _, dr := range refs {
		start, end, err := readerPatchWindow(old, dr)
		if err != nil {
			continue
		}
		fs, fe, err := readerPatchWindow(fresh, dr)
		if err != nil {
			return err
		}
		if fs != start || fe != end {
			return fmt.Errorf("%s segment %d now reads [%d,%d) of its article, not [%d,%d)", dr.file, dr.seg.Index, fs, fe, start, end)
		}
	}
	return nil
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
// article-fetch failure). See invalidateRepaired for the order.
func (p *Par2Repair) invalidateRepairedRanges(nzb *storage.NZB, nzbID, entryName string, pending map[string][]overlay.DeadSegment) {
	p.invalidateCachedRanges(nzb, nzbID, entryName, pending)

	// Every pass that writes patches comes through here (the job, and the
	// sweep's warm and cold passes), so this is where a pre-cached episode's
	// readiness row learns its repair landed.
	if p.manager.precache != nil {
		p.manager.precache.OnPar2Repaired(nzbID)
	}
}

// invalidateCachedRanges runs invalidateRepaired against the live usenet
// reader and the DFS mount for pending's segments. It is also used when the
// sweep clears a sticky-failed file whose articles came back: the pads it
// served are cached as zeros, and the clear deletes the only record of them.
func (p *Par2Repair) invalidateCachedRanges(nzb *storage.NZB, nzbID, entryName string, pending map[string][]overlay.DeadSegment) {
	refresh := func(file string, segIdx []int) {
		if p.manager.usenet != nil {
			p.manager.usenet.RefreshRepairedSegments(nzbID, file, segIdx)
		}
	}
	var forget func(file string, off, length int64)
	if fw := p.dfsCacheForgetter(); fw != nil {
		folder := p.dfsCacheFolder(nzbID, entryName)
		forget = func(file string, off, length int64) { fw.ForgetCachedRange(folder, file, off, length) }
	}
	invalidateRepaired(nzb, pending, refresh, forget, func(d time.Duration, f func()) { time.AfterFunc(d, f) })
}

// repairedRangeReforgetDelay is how long after a repair invalidateRepaired
// forgets the patched DFS ranges a second time.
const repairedRangeReforgetDelay = 30 * time.Second

// invalidateRepaired is invalidateRepairedRanges with its seams passed in.
// The order matters:
//  1. refresh the live usenet reader first, so its segment cache stops
//     serving the zero-fill it may hold (the reader is shared with the
//     viewer's stream and with read-ahead, so it is often live; tearing it
//     down only worked while idle).
//  2. then forget the DFS cache ranges, so the next read re-streams them
//     from the reader - which now serves the patch. Forgetting first let a
//     read in between re-stream the stale pad, and the DFS cache persists
//     whatever a stream returns: permanent zeros after a good repair.
//  3. forget them again after repairedRangeReforgetDelay, for a stream that
//     had already read the pad from the reader before step 1 and wrote it
//     after step 2. At worst that costs re-reading one article's range.
//
// forget may be nil (no DFS mount).
func invalidateRepaired(nzb *storage.NZB, pending map[string][]overlay.DeadSegment,
	refresh func(file string, segIdx []int), forget func(file string, off, length int64),
	after func(d time.Duration, f func())) {
	for file, segs := range pending {
		idx := make([]int, 0, len(segs))
		for _, s := range segs {
			idx = append(idx, s.Index)
		}
		refresh(file, idx)
	}
	if forget == nil {
		return
	}
	rngs := buildDeadOutputRanges(nzb, pending)
	forgetAll := func() {
		for file, rs := range rngs {
			for _, r := range rs {
				forget(file, r.start, r.end-r.start)
			}
		}
	}
	forgetAll()
	after(repairedRangeReforgetDelay, forgetAll)
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

// firstFetchSlices sizes the first recovery fetch. An early lower bound from
// the parsed index wins (the top-up loop fetches more if the exact count is
// higher). Without one the dead-article estimate is capped at what a repair
// can use, so a missing index cannot make the pass fetch hundreds of slices.
func firstFetchSlices(estimate uint32, earlyK int) uint32 {
	if earlyK > 0 {
		return uint32(earlyK)
	}
	return min(estimate, uint32(par2.MaxRepairSlices))
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
//
// When no name carries a volume marker (an obfuscated posting), the files
// are split by size instead, as unnamedRecoveryEstimate counts them: a file
// at least twice the smallest and at least par2UnnamedVolumeFloor is a
// recovery volume with an estimated slice count, the rest are index files.
// Census-ed as index files, every one of them was fetched whole before any
// gate - the whole recovery set, several GB for a remux, to repair one
// slice. As volumes they are fetched smallest first and only as far as the
// damage needs.
func censusPar2Volumes(files []storage.Par2FileRef) (vols []par2Volume, indexFiles []storage.Par2FileRef) {
	vols, indexFiles = censusNamedPar2Volumes(files)
	if len(vols) == 0 {
		vols, indexFiles = censusUnnamedVolumes(indexFiles)
	}
	return vols, indexFiles
}

// par2UnnamedVolumeFloor is the smallest unnamed PAR2 file censusPar2Volumes
// takes for a recovery volume. An index file holds only metadata - about 20
// bytes per slice plus a packet per file, a few hundred KiB for an episode -
// so nothing under the floor is fetched as a volume. Without it, a season
// pack whose episode index files differ in size by 2x (a short special
// beside full episodes, or a fixed slice size where the index tracks the
// episode's length) had episode indexes census-ed as volumes: the recovery
// sets they describe were invisible to choosePar2Set, and the pass ended
// "could not be tied". Small volumes kept as index files cost a few MiB of
// fetch and are parsed with the index.
const par2UnnamedVolumeFloor = 4 << 20

// censusUnnamedVolumes moves the files unnamedRecoveryEstimate counts as
// recovery volumes, at or over par2UnnamedVolumeFloor, out of indexFiles,
// each with its share of that estimate (at least one slice). Files under
// twice the smallest stay index files: a season pack's per-episode index
// files are similar sizes. Volumes under the floor stay index files too,
// still counted by the estimate, so the census total plus their slices
// equals it. The counts only size the fetch and the early capacity gate;
// the solve gates on the slices actually parsed.
func censusUnnamedVolumes(indexFiles []storage.Par2FileRef) ([]par2Volume, []storage.Par2FileRef) {
	base, step := unnamedRecoveryGeometry(indexFiles)
	if step == 0 {
		return nil, indexFiles
	}
	var vols []par2Volume
	kept := indexFiles[:0:0]
	for _, f := range indexFiles {
		n := unnamedVolumeSlices(f.Size, base, step)
		if n == 0 || f.Size < par2UnnamedVolumeFloor {
			kept = append(kept, f)
			continue
		}
		if n > int64(^uint32(0)) {
			n = int64(^uint32(0))
		}
		vols = append(vols, par2Volume{ref: f, count: uint32(n)})
	}
	sort.Slice(vols, func(i, j int) bool { return vols[i].ref.Size < vols[j].ref.Size })
	return vols, kept
}

// censusNamedPar2Volumes is censusPar2Volumes without the size-based split
// of an obfuscated posting: every file without a volume marker is an index
// file.
func censusNamedPar2Volumes(files []storage.Par2FileRef) (vols []par2Volume, indexFiles []storage.Par2FileRef) {
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
// any volume whose every article is confirmed missing from the returned list.
// A volume with only some articles gone is kept, behind the whole ones.
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
	// A volume is dropped only when every article is confirmed gone - the
	// rule fetchWholePar2File applies. One with some articles gone still
	// yields every recovery packet the dead ones do not overlap (ParseIndex
	// resyncs past the gap), so it keeps its paper count and moves behind
	// the whole volumes; the solve gates on the slices actually parsed.
	// Dropping it on one 430 under-counted "available", which the capacity
	// gates turn into a terminal verdict.
	dead := make(map[string]struct{})
	partial := make(map[string]struct{})
	for _, v := range candidates {
		gone := 0
		for _, seg := range v.ref.Segments {
			if r, ok := byID[seg.MessageID]; ok && !r.Available && nntp.IsArticleNotFoundError(r.Error) {
				gone++
			}
		}
		switch {
		case gone == 0:
		case gone == len(v.ref.Segments):
			dead[v.ref.Name] = struct{}{}
		default:
			partial[v.ref.Name] = struct{}{}
		}
	}

	var availBefore, availAfter uint32
	for _, v := range vols {
		availBefore += v.count
	}
	if len(partial) > 0 {
		whole := make([]par2Volume, 0, len(vols))
		var damaged []par2Volume
		for _, v := range vols {
			if _, ok := partial[v.ref.Name]; ok {
				damaged = append(damaged, v)
				continue
			}
			whole = append(whole, v)
		}
		vols = append(whole, damaged...)
		logger.Info().Str("entry", entryName).Int("volumes_partial", len(partial)).
			Msg("par2: recovery-volume STAT pre-census found volumes with some articles gone; fetching them last")
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
// A provider that reports one of the controls present is left out and the
// controls are asked again, until the providers still asked all agree the
// controls are missing. The sweep then runs without the ones left out
// (excluded): one provider answering STAT 223 for dead articles used to
// switch the whole sweep off, and the solve then needed a full read of the
// release just to find the damage and a second one to repair it. A miss
// from a sweep with providers left out is not proof on its own - see
// confirmStatMisses.
//
// The bool return is whether the sweep actually completed: false means a
// whole-batch STAT error (the caller falls back to per-round discovery,
// exactly as before this sweep existed), or ctx ending before every article
// was asked about - then the misses confirmed so far are still returned, as
// a lower bound. An individual ambiguous per-article error is simply not
// reported as missing - that segment is left for the round loop, same as any
// transient miss.
//
// progress (nil for none) is told how many articles there are and how many
// have been checked, so the Repair page can show the sweep moving. The
// control probe is not counted.
func statPostedFileDamage(
	ctx context.Context,
	logger zerolog.Logger,
	stat func(context.Context, []string) ([]nntp.StatResult, error),
	matches []par2.Match,
	par2Source []storage.PostedFileRef,
	controls []string,
	entryName string,
	progress *par2JobProgressState,
) (missing []string, completed bool, excluded []string) {
	// Calibrate first. Providers answer STAT 223 for articles whose body is
	// gone everywhere - eweka did so for all 15 dead articles of Game of
	// Castles S08E05, six others for Under Reef S11E06 - which makes a
	// sweep that reports nothing missing worthless. controls are this
	// release's own recorded-dead articles: playback already proved their
	// bodies unfetchable, so STAT must report them missing. A provider
	// that does not is left out and the controls asked again; only when no
	// provider is left whose answer can be believed is the sweep skipped,
	// and the first solve pass finds the damage instead (it collects every
	// unreadable slice).
	for len(controls) > 0 {
		results, err := stat(nntp.WithStatExcludedHosts(ctx, excluded), controls)
		if err != nil {
			logger.Warn().Err(err).Str("entry", entryName).
				Msg("par2: STAT control probe failed; damage will be found per-round instead")
			return nil, false, excluded
		}
		var liar, lyingID, unverifiedID string
		var unverifiedErr error
		present := false
		for _, r := range results {
			switch {
			case r.Available:
				if !present {
					liar, lyingID = r.Host, r.MessageID
				}
				present = true
			case !nntp.IsArticleNotFoundError(r.Error):
				// Could not be asked (a connection error, or nobody left
				// to ask): unknown, not evidence that anyone lied.
				unverifiedID, unverifiedErr = r.MessageID, r.Error
			}
		}
		if !present {
			if unverifiedID != "" {
				logger.Warn().
					AnErr("cause", unverifiedErr).
					Str("entry", entryName).
					Str("message_id", unverifiedID).
					Strs("providers_left_out", excluded).
					Str(par2NoteField, statLeftOutNote("", excluded)).
					Msg("par2: a known-dead article could not be checked on the providers still asked; skipping the STAT damage sweep for this release")
				return nil, false, excluded
			}
			break // every provider still asked agrees the controls are gone
		}
		// The answer names no provider, or names one already left out, or
		// more have lied than a release plausibly has providers: nothing
		// more to narrow down, so the sweep is skipped rather than believed.
		if liar == "" || statHostExcluded(excluded, liar) || len(excluded) >= maxStatExcludedHosts {
			logger.Warn().
				Str("entry", entryName).
				Str("message_id", lyingID).
				Str("provider", liar).
				Strs("providers_left_out", excluded).
				Str(par2NoteField, statLeftOutNote(liar, excluded)).
				Msg("par2: a provider reports a known-dead article as present; skipping the STAT damage sweep for this release")
			return nil, false, excluded
		}
		excluded = append(excluded, liar)
		logger.Warn().
			Str("entry", entryName).
			Str("message_id", lyingID).
			Str("provider", liar).
			Str(par2NoteField, liar).
			Msg("par2: a provider reports a known-dead article as present; its STAT answers are left out of this release's damage sweep")
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
		return nil, true, excluded
	}

	start := time.Now()
	progress.SetStatTotal(len(msgIDs))
	results, err := stat(nntp.WithStatExcludedHosts(nntp.WithStatProgress(ctx, progress.AddStatProbed), excluded), msgIDs)
	if err != nil {
		logger.Warn().Err(err).Str("entry", entryName).
			Msg("par2: posted-file STAT damage sweep failed; damage will be found per-round instead")
		return nil, false, excluded
	}
	unchecked := 0
	for _, r := range results {
		switch {
		case r.Available:
		case nntp.IsArticleNotFoundError(r.Error):
			missing = append(missing, r.MessageID)
		default:
			unchecked++
		}
	}
	// The STAT returns no error when ctx ends part-way: the articles it never
	// reached come back as per-article errors. That is not a finished sweep.
	// The misses already confirmed are kept - each one is a not-found from
	// every provider - but the caller is told the set is incomplete.
	if ctx.Err() != nil {
		logger.Warn().
			Err(ctx.Err()).
			Str("entry", entryName).
			Int("segments_probed", len(msgIDs)).
			Int("segments_missing", len(missing)).
			Int("segments_unchecked", unchecked).
			Int("controls", len(controls)).
			Strs("providers_left_out", excluded).
			Dur("duration", time.Since(start)).
			Dur(par2TookField, time.Since(start)).
			Str(par2NoteField, statSweepNote(len(msgIDs), len(missing), unchecked, excluded)).
			Msg("par2: posted-file STAT damage sweep cut short; the rest of the damage will be found per-round")
		return missing, false, excluded
	}
	logger.Info().
		Str("entry", entryName).
		Int("segments_probed", len(msgIDs)).
		Int("segments_missing", len(missing)).
		Int("segments_unchecked", unchecked).
		Int("controls", len(controls)).
		Strs("providers_left_out", excluded).
		Dur("duration", time.Since(start)).
		Dur(par2TookField, time.Since(start)).
		Str(par2NoteField, statSweepNote(len(msgIDs), len(missing), unchecked, excluded)).
		Msg("par2: posted-file STAT damage sweep complete")
	return missing, true, excluded
}

// seedStatSlices decides whether the damage sweep's extra slices go into
// the damaged set before the first solve. A sweep that asked every provider
// always seeds. One that left providers out seeds only while the recorded
// and extra slices together fit the recovery budget: past it the opening
// capacity gate would fail the job terminally on evidence the round loop
// classifies more carefully.
func seedStatSlices(reducedTrust bool, recordedSlices, extraSlices, budget int) bool {
	return !reducedTrust || recordedSlices+extraSlices <= budget
}

// statHostExcluded reports whether host is already left out.
func statHostExcluded(excluded []string, host string) bool {
	for _, h := range excluded {
		if h == host {
			return true
		}
	}
	return false
}

// The journal's summary fields, under names that functions taking a
// "logger" parameter can still reach.
const (
	par2NoteField = logger.FieldNote
	par2TookField = logger.FieldTook
)

// maxStatExcludedHosts bounds how many providers one damage sweep leaves
// out for misreporting its controls.
const maxStatExcludedHosts = 32

// statSweepState is the up-front damage check's outcome for the pass's
// summary line: whether it finished, which providers it left out, and
// whether what it found was left for the solve to find again (too many
// misses to confirm, or more than the recovery budget).
func statSweepState(completed bool, excluded []string, leftForSolve bool) string {
	state := "incomplete"
	if completed {
		state = "complete"
	}
	if len(excluded) > 0 {
		state += ", without " + strings.Join(excluded, ", ")
	}
	if leftForSolve {
		state += ", misses left for the solve"
	}
	return state
}

// statSweepNote is the damage sweep's result as the journal's summary.
func statSweepNote(checked, missing, unchecked int, excluded []string) string {
	parts := []string{fmt.Sprintf("%d checked", checked), fmt.Sprintf("%d missing", missing)}
	if unchecked > 0 {
		parts = append(parts, fmt.Sprintf("%d could not be checked", unchecked))
	}
	if len(excluded) > 0 {
		parts = append(parts, "without "+strings.Join(excluded, ", "))
	}
	return strings.Join(parts, " • ")
}

// statLeftOutNote names the provider a control line is about and the ones
// already left out, for the journal's summary.
func statLeftOutNote(provider string, excluded []string) string {
	var parts []string
	if provider != "" {
		parts = append(parts, provider)
	}
	if len(excluded) > 0 {
		parts = append(parts, "already without "+strings.Join(excluded, ", "))
	}
	return strings.Join(parts, " • ")
}

// settleStatMisses turns the damage sweep's misses into the set the pass
// acts on, and reports whether they were given up on (leftForSolve).
//
// A sweep that asked every provider is believed: each miss is recorded
// against its file, so the article is patched from the rebuilt slices like
// one the round loop found (see discoveredDeadRefs). It used to be rebuilt
// for the solve and then discarded, and playback failed on it later.
//
// With a provider left out, a miss is only "not found on the providers
// asked": the one left out may hold the article after all. Each is settled
// by fetching its body through the normal failover, every provider included
// - the same evidence the round loop goes on - and only the ones that fail
// as missing or corrupt everywhere are kept (see confirmStatMisses). One
// that turns out fetchable is dropped, so the heal step still tries it.
func settleStatMisses(
	ctx context.Context,
	logger zerolog.Logger,
	entryName string,
	fetchers map[[16]byte]*postedFileFetcher,
	where map[string]postedSegPos,
	missing []string,
	excluded []string,
	concurrency, budget int,
	progress *par2JobProgressState,
) (settled []string, leftForSolve bool) {
	if len(excluded) == 0 {
		markStatMissingBad(fetchers, where, missing)
		return missing, false
	}
	if len(missing) == 0 {
		return nil, false
	}
	limit := statConfirmCap(budget)
	return confirmStatMisses(ctx, logger, entryName, fetchers, where, missing, excluded, concurrency, limit, progress), len(missing) > limit
}

// foldStatSlices adds the slices of the sweep's settled misses to
// damagedSet, so the first solve already knows them. found is how many
// slices they cover beyond those already in the set.
//
// Seeding makes the round loop's opening capacity gate answer for these
// slices, and that gate is terminal. With every provider asked that is
// right. With one left out the evidence is a body fetch, which the round
// loop judges by cause (par2RoundCapError) - so when the seeded set would
// not fit the recovery budget, nothing is added (seeded false) and the
// round loop finds and classifies the damage exactly as if the sweep had
// not run.
func foldStatSlices(damagedSet map[int64]struct{}, missing []string, slicesOf func(messageID string) []int64, reducedTrust bool, budget int) (found int, seeded bool) {
	extra := make(map[int64]struct{})
	for _, mid := range missing {
		for _, s := range slicesOf(mid) {
			if _, known := damagedSet[s]; !known {
				extra[s] = struct{}{}
			}
		}
	}
	if !seedStatSlices(reducedTrust, len(damagedSet), len(extra), budget) {
		return len(extra), false
	}
	for s := range extra {
		damagedSet[s] = struct{}{}
	}
	return len(extra), true
}

// postedSegPos locates one article within the posted file PAR2 protects.
type postedSegPos struct {
	fileID [16]byte
	index  int
}

// markStatMissingBad records articles a full damage sweep found missing on
// every provider as bad on their file's fetcher, the same record a failed
// body fetch leaves, so the pass patches them once their slices are
// rebuilt.
func markStatMissingBad(fetchers map[[16]byte]*postedFileFetcher, where map[string]postedSegPos, missing []string) {
	for _, mid := range missing {
		pos, ok := where[mid]
		if !ok {
			continue
		}
		if f := fetchers[pos.fileID]; f != nil {
			f.badArticles.Store(pos.index, struct{}{})
		}
	}
}

// statConfirmCap is how many sweep misses confirmStatMisses will fetch.
// Past it the release is damaged far beyond what budget recovery slices
// can rebuild, and the solve's early stop reaches that verdict without one
// failing fetch per article.
func statConfirmCap(budget int) int {
	return min(1024, max(64, 8*budget))
}

// confirmStatMisses settles the misses of a damage sweep that ran with
// providers left out. Each candidate's body is fetched through its file's
// fetcher - the normal failover, the left-out providers included, never the
// local cache - and it is kept only when that fails as missing or corrupt
// on every provider, which also records it as bad so the pass patches it
// (see discoveredDeadRefs). One that fetches is alive after all; one that
// fails any other way is left for the round loop to judge. Over limit
// candidates, none is fetched and none kept.
func confirmStatMisses(
	ctx context.Context,
	logger zerolog.Logger,
	entryName string,
	fetchers map[[16]byte]*postedFileFetcher,
	where map[string]postedSegPos,
	candidates []string,
	excluded []string,
	concurrency, limit int,
	progress *par2JobProgressState,
) []string {
	if len(candidates) > limit {
		logger.Warn().Str("entry", entryName).Int("candidates", len(candidates)).Int("limit", limit).
			Strs("providers_left_out", excluded).
			Str(par2NoteField, fmt.Sprintf("%d missing • limit %d • without %s", len(candidates), limit, strings.Join(excluded, ", "))).
			Msg("par2: too many articles missing on the providers asked to confirm one by one; damage will be found per-round instead")
		return nil
	}
	start := time.Now()
	const (
		verdictUnknown = iota
		verdictDead
		verdictAlive
	)
	verdicts := make([]int, len(candidates))
	gp := pool.New().WithMaxGoroutines(max(1, concurrency))
	for i, mid := range candidates {
		pos, ok := where[mid]
		f := fetchers[pos.fileID]
		if !ok || f == nil || pos.index < 0 || pos.index >= len(f.segs) {
			continue
		}
		gp.Go(func() {
			defer progress.Touch()
			if ctx.Err() != nil {
				return
			}
			_, err := f.segmentData(f.ctx, pos.index)
			switch {
			case err == nil:
				verdicts[i] = verdictAlive
			case nntp.IsArticleNotFoundError(err) || nntp.IsCorruptArticleError(err):
				f.badArticles.Store(pos.index, struct{}{})
				verdicts[i] = verdictDead
			}
		})
	}
	gp.Wait()

	var confirmed []string
	alive, unknown := 0, 0
	for i, v := range verdicts {
		switch v {
		case verdictDead:
			confirmed = append(confirmed, candidates[i])
		case verdictAlive:
			alive++
		default:
			unknown++
		}
	}
	logger.Info().
		Str("entry", entryName).
		Int("candidates", len(candidates)).
		Int("confirmed_dead", len(confirmed)).
		Int("alive_after_all", alive).
		Int("undecided", unknown).
		Strs("providers_left_out", excluded).
		Dur("duration", time.Since(start)).
		Dur(par2TookField, time.Since(start)).
		Str(par2NoteField, fmt.Sprintf("%d fetched • %d dead • %d alive after all • %d undecided", len(candidates), len(confirmed), alive, unknown)).
		Msg("par2: articles missing on the providers asked were fetched to confirm")
	return confirmed
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
// error here; the caller turns it into the verdict. parse re-parses (nil:
// plainPar2Parse); runRepair's also asks another provider for the articles
// behind bad-checksum packets.
func topUpParsedRecovery(
	ctx context.Context,
	logger zerolog.Logger,
	fetch articleFetchFunc,
	parse par2ParseFunc,
	vols []par2Volume,
	nextVolIdx int,
	want int,
	idx *par2.Index,
	sources []par2.Source,
	entryName string,
	maxConc int,
	progress *par2JobProgressState,
) (*par2.Index, int, []par2.Source, error) {
	if parse == nil {
		parse = plainPar2Parse
	}
	for want > len(idx.Recovery) {
		var added int
		var err error
		nextVolIdx, _, sources, added, err = fetchMoreVolumes(
			ctx, logger, fetch, vols, nextVolIdx,
			uint32(want), uint32(len(idx.Recovery)), sources, entryName, maxConc)
		if err != nil {
			return idx, nextVolIdx, sources, par2Failf(par2Transient, "fetch recovery volumes: %w", err)
		}
		if progress != nil {
			progress.SetRecoveryVolsFetched(nextVolIdx)
		}
		if added == 0 {
			break
		}
		next, parsed, err := parse(ctx, sources, idx.SetID)
		if err != nil {
			return idx, nextVolIdx, parsed, par2IndexParseErr(err)
		}
		idx, sources = next, parsed
		if progress != nil {
			progress.SetRecoverySlices(len(idx.Recovery), want)
		}
	}
	return idx, nextVolIdx, sources, nil
}

// topUpSliceChecksums fetches recovery volumes, one batch at a time, while a
// file of the set has no slice checksums (idx.MissingChecksums) and volumes
// remain, re-parsing the index after each batch that added a source. Every
// PAR2 file carries the IFSC packets, so any further volume can supply them.
// A batch that adds nothing (its volumes 430'd or failed) does not stop the
// search: nextVolIdx advances past every volume tried, so the loop is bounded
// by len(vols). Files still missing checksums on return are not an error
// here; the caller turns that into the verdict. parse is as for
// topUpParsedRecovery.
func topUpSliceChecksums(
	ctx context.Context,
	logger zerolog.Logger,
	fetch articleFetchFunc,
	parse par2ParseFunc,
	vols []par2Volume,
	nextVolIdx int,
	idx *par2.Index,
	sources []par2.Source,
	entryName string,
	maxConc int,
	progress *par2JobProgressState,
) (*par2.Index, int, []par2.Source, error) {
	if parse == nil {
		parse = plainPar2Parse
	}
	for len(idx.MissingChecksums()) > 0 && nextVolIdx < len(vols) {
		logger.Info().Str("entry", entryName).Int("files", len(idx.MissingChecksums())).
			Msg("par2: fetched PAR2 files hold no slice checksums for some files, fetching another volume")
		var added int
		var err error
		// A shortfall of one slice: the next volume, plus one slack volume.
		nextVolIdx, _, sources, added, err = fetchMoreVolumes(
			ctx, logger, fetch, vols, nextVolIdx, 1, 0, sources, entryName, maxConc)
		if err != nil {
			return idx, nextVolIdx, sources, par2Failf(par2Transient, "fetch recovery volumes: %w", err)
		}
		if progress != nil {
			progress.SetRecoveryVolsFetched(nextVolIdx)
		}
		if added == 0 {
			continue
		}
		next, parsed, err := parse(ctx, sources, idx.SetID)
		if err != nil {
			return idx, nextVolIdx, parsed, par2IndexParseErr(err)
		}
		idx, sources = next, parsed
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
	next, slices, out, added, _, err := fetchVolumeBatch(ctx, logger, fetch, vols, nextVolIdx, needed, fetchedSlices, sources, entryName, maxConc)
	return next, slices, out, added, err
}

// fetchVolumeBatch is fetchMoreVolumes, also returning how many of the
// volumes tried were gone (430).
func fetchVolumeBatch(
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
) (int, uint32, []par2.Source, int, int, error) {
	remaining := vols[nextVolIdx:]
	var shortfall uint32
	if needed > fetchedSlices {
		shortfall = needed - fetchedSlices
	}
	batch := computeRecoveryBatch(remaining, shortfall)
	if len(batch) == 0 {
		return nextVolIdx, fetchedSlices, sources, 0, 0, nil
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

	// A job that ran out of time or was stopped mid-fetch leaves the batch
	// short for a reason that says nothing about the recovery data. Say so,
	// or the caller's "only M recovery slices" shortfall reads as terminal.
	if err := ctx.Err(); err != nil && fetchedSlices < needed {
		return nextVolIdx, fetchedSlices, sources, added, volsNotFound, err
	}
	return nextVolIdx, fetchedSlices, sources, added, volsNotFound, nil
}

// countPar2TransportFails wraps a PAR2 article fetch so that every failure
// other than a 430 adds one to fails while the job context jobCtx is still
// live. The context a fetch is handed is the per-article timeout context
// (see fetchPar2FileConcurrent), so it has always ended by the time a
// timeout returns: testing it instead of jobCtx skipped every timeout as if
// the job had been cancelled, and one slow recovery article made the
// shortfall terminal. Siblings cancelled after a real failure are counted
// too; that is harmless, as the count only needs to be non-zero.
func countPar2TransportFails(jobCtx context.Context, fetch articleFetchFunc, fails *atomic.Int32) articleFetchFunc {
	return func(fctx context.Context, messageID string) ([]byte, error) {
		data, err := fetch(fctx, messageID)
		if err != nil && !nntp.IsArticleNotFoundError(err) && jobCtx.Err() == nil {
			fails.Add(1)
		}
		return data, err
	}
}

// par2MaxIndexFileBytes caps the size of a PAR2 file fetched whole as an
// index file. Index files hold only metadata, a few MiB at most; one this
// large is recovery data the census could not tell apart, and fetching it
// whole happens before any capacity or large-repair gate.
const par2MaxIndexFileBytes = 64 << 20

// par2IndexFileTooLarge reports whether f is over par2MaxIndexFileBytes and
// must not be fetched as an index file; the volumes carry the same metadata.
func par2IndexFileTooLarge(f storage.Par2FileRef) bool {
	return f.Size > par2MaxIndexFileBytes
}

// fetchPar2IndexFiles fetches every index (non-volume) PAR2 file. A file
// that fails, or is over par2MaxIndexFileBytes, is left out: the volumes
// carry the same metadata. indexErr is the first failure that was not a 430
// while ctx was still live (a timeout, a reset): the file may well still be
// on the servers.
func fetchPar2IndexFiles(ctx context.Context, logger zerolog.Logger, fetch articleFetchFunc, indexFiles []storage.Par2FileRef, entryName string) (sources []par2.Source, indexErr error) {
	for _, f := range indexFiles {
		if par2IndexFileTooLarge(f) {
			logger.Info().Str("entry", entryName).Str("file", f.Name).Int64("size", f.Size).
				Msg("par2: index file too large to fetch whole; relying on volume-embedded metadata")
			continue
		}
		data, err := fetchWholePar2File(ctx, fetch, f)
		if err != nil {
			logger.Debug().Err(err).Str("entry", entryName).Str("file", f.Name).Msg("par2: index file fetch failed; relying on volume-embedded metadata")
			if indexErr == nil && !nntp.IsArticleNotFoundError(err) && ctx.Err() == nil {
				indexErr = fmt.Errorf("index file %q failed to fetch without a 430: %v", f.Name, err)
			}
			continue
		}
		sources = append(sources, par2.Source{Name: f.Name, Data: data})
	}
	return sources, indexErr
}

// par2IndexFetchTransient tags err, a failure that depends on which recovery
// set the pass works on, transient when an index file failed to fetch
// without a 430: with that file missing the pass may have picked the wrong
// set, or seen too few sets to pick one. err is formatted with %v so neither
// its class nor a 430 inside it outranks the transient class.
func par2IndexFetchTransient(err, indexErr error) error {
	if err == nil || indexErr == nil {
		return err
	}
	return par2Failf(par2Transient, "%v (transient: PAR2 %v)", err, indexErr)
}

// par2IndexParseErr is a failed PAR2 index parse, terminal: the index or
// the posted file mapping is structurally wrong. A parse that failed on a
// packet MD5 mismatch (see par2.walkPackets) is transient instead: far more
// likely a mis-served or mis-decoded article during the volume fetch, which a
// bare retry fetches again. A cancelled parse stays transient through the
// context check in classifyPar2Failure.
func par2IndexParseErr(err error) error {
	class := par2Terminal
	if strings.Contains(err.Error(), "packet MD5 mismatch") {
		class = par2Transient
	}
	return par2Failf(class, "parse PAR2 index: %w", err)
}

// par2UnmatchedDeadSegmentErr is the failure for dead segment messageID of
// file when it lies in no matched posted file. It is terminal, unless its
// posted file was skipped by matching this pass for a fetch/hash failure
// (postedSkipped) or an index file failed to fetch (indexFetchErr): a retry
// can match it then.
func par2UnmatchedDeadSegmentErr(messageID, file, posted string, postedSkipped bool, indexFetchErr error) error {
	if postedSkipped {
		return par2Failf(par2Transient, "dead segment %s (file %q) is not part of any matched posted file (transient: posted file %q failed to fetch/hash during matching)", messageID, file, posted)
	}
	return par2IndexFetchTransient(par2Failf(par2Terminal, "dead segment %s (file %q) is not part of any matched posted file", messageID, file), indexFetchErr)
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
	exact bool
	// geometryErr is why resolveGeometry could not measure the article
	// boundaries (a 430 only when every probe got one, otherwise the last
	// other error), nil when it did or never ran.
	geometryErr error
	// badArticles holds the segment indexes whose fetch failed as missing
	// (430) or corrupt on every provider during this pass: int -> struct{}.
	badArticles sync.Map
	logger      zerolog.Logger

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

	// round, while a repair round runs, shares this fetcher's articles with
	// the round's other askers: one fetch per article per round, errors
	// kept for the round (see par2RoundArticles). nil outside a round.
	// roundBase is this file's byte position in the round's read order,
	// set by attachPar2RoundArticles before the round's workers start.
	round     atomic.Pointer[par2RoundArticles]
	roundBase int64

	// failover, when set (a repair pass's fetchers), asks another provider
	// for an article whose body one provider served bad after decode: short,
	// or failing a slice's IFSC. nil elsewhere (MD5-16k, heal, warm sweep).
	failover *postedFailover
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
	// parity, inflating k toward the repair cap, and potentially reaching a
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
			// Absorb rounding residual into the last segment. A zero or
			// garbage final bytes= estimate can leave nothing for it; a
			// segment is never empty, so keep at least one byte.
			sizes[len(segs)-1] = max(1, trueLen-bases[len(segs)-1])
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
	// A 430 here makes the unplaced dead segments terminal downstream (see
	// splitPlaceableDeadRefs), so it is reported only when every probe got
	// one. A probe that failed any other way - a timeout, a reset - might
	// measure the article on the next pass, so its error is the one kept and
	// the pass backs off instead of giving up on the release.
	var lastErr, retryableErr error
	for idx := 0; idx < candidates; idx++ {
		seed, size, err := f.measureArticle(idx)
		if err != nil {
			lastErr = err
			if !nntp.IsArticleNotFoundError(err) {
				retryableErr = err
			}
			continue
		}
		f.applyUniformGeometry(seed, size)
		return
	}
	if retryableErr != nil {
		lastErr = retryableErr
	}
	f.geometryErr = lastErr
	f.logger.Warn().Err(lastErr).Int("articles_tried", candidates).
		Msg("par2 geometry: could not measure the article size; keeping estimated segment sizes")
}

// geometryProbeArticles bounds how many leading articles resolveGeometry
// tries before keeping the estimates, and how many later ones
// resolveGeometryPastDeadHead tries after them.
const geometryProbeArticles = 3

// resolveGeometryPastDeadHead measures the geometry of a file whose leading
// probes were all 430 from a later non-final article: each carries its own
// =ypart offset, which measureArticle checks. Articles the STAT sweep found
// missing (sweptMissing) are skipped, so the ones tried are those it saw
// present - or, if the sweep did not get to them, the next in order. Reports
// whether the geometry is now exact. A probe that fails other than with a
// 430 replaces geometryErr, so the unplaced segments back off rather than
// end terminal, as in resolveGeometry.
//
// Same concurrency rule as resolveGeometry: call it before the fetcher is
// shared.
func (f *postedFileFetcher) resolveGeometryPastDeadHead(sweptMissing map[string]struct{}) bool {
	if f.exact || f.fetch == nil || f.geometryErr == nil || !nntp.IsArticleNotFoundError(f.geometryErr) {
		return false
	}
	tried := 0
	var retryableErr error
	lastErr := f.geometryErr
	for idx := geometryProbeArticles; idx < len(f.segs)-1 && tried < geometryProbeArticles; idx++ {
		if _, missing := sweptMissing[f.segs[idx].MessageID]; missing {
			continue
		}
		tried++
		seed, size, err := f.measureArticle(idx)
		if err != nil {
			lastErr = err
			if !nntp.IsArticleNotFoundError(err) {
				retryableErr = err
			}
			continue
		}
		f.applyUniformGeometry(seed, size)
		f.geometryErr = nil
		f.logger.Info().Int("article", idx).Msg("par2 geometry: leading articles are gone; measured the article size from a later one")
		return true
	}
	if tried == 0 {
		return false
	}
	if retryableErr != nil {
		lastErr = retryableErr
	}
	f.geometryErr = lastErr
	f.logger.Warn().Err(lastErr).Int("articles_tried", geometryProbeArticles+tried).
		Msg("par2 geometry: could not measure the article size past the dead leading articles; keeping estimated segment sizes")
	return false
}

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

// segmentData fetches article idx. ctx bounds the fetch: f.ctx (the job's)
// for one-off reads, a repair round's context for the streaming workers so
// a cancelled round aborts its in-flight fetches.
func (f *postedFileFetcher) segmentData(ctx context.Context, idx int) ([]byte, error) {
	if data, ok := f.failover.replaced(f, idx); ok {
		return data, nil
	}
	f.cacheMu.Lock()
	if f.cacheIdx == idx {
		data := f.cacheData
		f.cacheMu.Unlock()
		return data, nil
	}
	f.cacheMu.Unlock()
	if r := f.round.Load(); r != nil {
		return r.segmentData(ctx, f, idx)
	}
	return f.fetchSegment(ctx, idx)
}

// fetchSegment fetches article idx from Usenet and keeps it as the
// fetcher's last-fetched article.
func (f *postedFileFetcher) fetchSegment(ctx context.Context, idx int) ([]byte, error) {
	// Per-segment timeout, independent of how many OTHER segments are being
	// fetched concurrently right now: a single dead/stuck connection fails
	// this one fetch in par2ArticleFetchTimeout, not the whole job's
	// deadline. ctx (the job's own context via par2JobTimeout, or a round's
	// child of it) remains the backstop if it's already closer than that.
	fetchCtx, cancel := context.WithTimeout(ctx, par2ArticleFetchTimeout)
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
	out, _, err := f.readRange(f.ctx, start, length, true)
	return out, err
}

// readRange backs ReadRange, with two extra controls the repair job's
// IFSC-verified cache path needs (see jobSliceSource.ReadSlice):
// allowCache=false forces every byte to come from a fresh Usenet fetch, and
// usedCache reports whether any returned byte was served from the DFS cache
// (so the caller knows whether an IFSC re-check is worth doing). ctx bounds
// the Usenet fetches (see segmentData).
func (f *postedFileFetcher) readRange(ctx context.Context, start, length int64, allowCache bool) (out []byte, usedCache bool, err error) {
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

		data, err := f.segmentData(ctx, segIdx)
		if err != nil {
			if nntp.IsArticleNotFoundError(err) || nntp.IsCorruptArticleError(err) {
				// Gone, or corrupt on every provider: the solve rebuilds its
				// slices, and runRepair patches it (patchDiscoveredDead).
				f.badArticles.Store(segIdx, struct{}{})
			}
			return nil, usedCache, fmt.Errorf("fetch segment %d: %w", segIdx, err)
		}
		avail := int64(len(data)) - withinSeg
		if avail <= 0 {
			// Another provider may hold the whole article.
			if better, ok := f.failover.replace(ctx, f, segIdx, "short body"); ok {
				data = better
				avail = int64(len(data)) - withinSeg
			}
		}
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

// par2Provenance records, for every PAR2 article fetched in one pass, the
// provider that served it and its decoded length. fetchPar2FileConcurrent
// concatenates a file's bodies and leaves out the 430'd ones, so the lengths
// are what maps a byte offset in a fetched PAR2 file back to the article it
// came from. Safe for concurrent use: the volume fetches record from many
// goroutines.
type par2Provenance struct {
	mu      sync.Mutex
	served  map[string]par2ServedArticle // by message ID
	retried map[string]struct{}          // asked of another provider this pass
}

type par2ServedArticle struct {
	host string
	n    int
}

func newPar2Provenance() *par2Provenance {
	return &par2Provenance{served: make(map[string]par2ServedArticle), retried: make(map[string]struct{})}
}

func (pv *par2Provenance) record(messageID, host string, n int) {
	pv.mu.Lock()
	pv.served[messageID] = par2ServedArticle{host: host, n: n}
	pv.mu.Unlock()
}

func (pv *par2Provenance) get(messageID string) (par2ServedArticle, bool) {
	pv.mu.Lock()
	defer pv.mu.Unlock()
	a, ok := pv.served[messageID]
	return a, ok
}

// claimRetry reports whether messageID has not been asked of another
// provider yet this pass, and marks it asked.
func (pv *par2Provenance) claimRetry(messageID string) bool {
	pv.mu.Lock()
	defer pv.mu.Unlock()
	if _, done := pv.retried[messageID]; done {
		return false
	}
	pv.retried[messageID] = struct{}{}
	return true
}

// par2RefetchFunc fetches one article from every provider but exceptHost,
// returning the body and the host that served it (usenet.FetchArticleExcept).
type par2RefetchFunc func(ctx context.Context, messageID, exceptHost string) ([]byte, string, error)

// par2ParseFunc parses sources into the index of set, and returns the
// sources it parsed, which can differ from the ones handed in (see
// refetchMisservedPar2Articles).
type par2ParseFunc func(ctx context.Context, sources []par2.Source, set [16]byte) (*par2.Index, []par2.Source, error)

// plainPar2Parse is a par2ParseFunc that only parses.
func plainPar2Parse(ctx context.Context, sources []par2.Source, set [16]byte) (*par2.Index, []par2.Source, error) {
	idx, err := par2.ParseIndexSet(ctx, sources, set)
	return idx, sources, err
}

// par2ArticleSpan is one article's bytes within a fetched PAR2 file.
type par2ArticleSpan struct {
	id         string
	host       string
	start, end int
}

// par2SourceSpans maps src back to the articles of ref it was concatenated
// from, using the lengths pv recorded. ok is false when the recorded lengths
// do not add up to the source's length: an article left out or fetched in a
// way pv did not see, so no offset can be tied to an article.
func par2SourceSpans(src par2.Source, ref storage.Par2FileRef, pv *par2Provenance) ([]par2ArticleSpan, bool) {
	spans := make([]par2ArticleSpan, 0, len(ref.Segments))
	off := 0
	for _, seg := range ref.Segments {
		a, ok := pv.get(seg.MessageID)
		if !ok {
			continue // 430'd: left out of the concatenation
		}
		spans = append(spans, par2ArticleSpan{id: seg.MessageID, host: a.host, start: off, end: off + a.n})
		off += a.n
	}
	return spans, off == len(src.Data)
}

// par2SkippedPacketRange is the byte range in data of the packet starting at
// offset, by the length its header declares (bytes 8-16, little-endian),
// clamped to data. A header too short or too broken to read gives the header
// alone.
func par2SkippedPacketRange(data []byte, offset int64) (int, int) {
	const headerSize = 64
	start := int(offset)
	if offset < 0 || start >= len(data) {
		return 0, 0
	}
	end := len(data)
	if start+headerSize < end {
		end = start + headerSize
	}
	if start+16 <= len(data) {
		n := binary.LittleEndian.Uint64(data[start+8 : start+16])
		if n >= headerSize && n <= uint64(len(data)-start) {
			end = start + int(n)
		} else if n > uint64(len(data)-start) {
			end = len(data)
		}
	}
	return start, end
}

// refetchMisservedPar2Articles asks another provider, once per article per
// pass, for every PAR2 article overlapping a packet idx skipped for a bad
// MD5: a provider can serve another article's body (or a broken copy) that
// passes yEnc decoding and fails only the packet checksum, which cost the
// packet's whole volume. Excluding the provider that served the bad copy
// (and its backbone) asks only the others; nothing is counted against it,
// so the body_foreign stat keeps counting only identity-check refusals.
// Returns sources with every source that got a different body rebuilt (the
// slice handed in is not modified) and how many articles changed; the
// caller re-parses when that is non-zero. With no other provider, or an
// unmapped source, nothing changes and the shortfall that follows is
// transient (see par2RecoveryShortfall).
func refetchMisservedPar2Articles(ctx context.Context, logger zerolog.Logger, idx *par2.Index, sources []par2.Source,
	files map[string]storage.Par2FileRef, pv *par2Provenance, refetch par2RefetchFunc, entryName string) ([]par2.Source, int) {
	if idx == nil || len(idx.SkippedPackets) == 0 || pv == nil || refetch == nil {
		return sources, 0
	}
	bySource := make(map[int][]par2.SkippedPacket)
	for _, sp := range idx.SkippedPackets {
		if sp.Source >= 0 && sp.Source < len(sources) {
			bySource[sp.Source] = append(bySource[sp.Source], sp)
		}
	}
	srcIdxs := make([]int, 0, len(bySource))
	for si := range bySource {
		srcIdxs = append(srcIdxs, si)
	}
	sort.Ints(srcIdxs)

	out := sources
	changed := 0
	for _, si := range srcIdxs {
		src := sources[si]
		ref, ok := files[src.Name]
		if !ok {
			continue
		}
		spans, ok := par2SourceSpans(src, ref, pv)
		if !ok {
			logger.Debug().Str("entry", entryName).Str("file", src.Name).
				Msg("par2: fetched PAR2 file does not map back to its articles; not asking another provider for the bad packet's articles")
			continue
		}
		replaced := make(map[int][]byte)
		for _, sp := range bySource[si] {
			start, end := par2SkippedPacketRange(src.Data, sp.Offset)
			for i, sn := range spans {
				if sn.end <= start || sn.start >= end || !pv.claimRetry(sn.id) {
					continue
				}
				if ctx.Err() != nil {
					return out, changed
				}
				aCtx, cancel := context.WithTimeout(ctx, par2ArticleFetchTimeout)
				data, host, err := refetch(aCtx, sn.id, sn.host)
				cancel()
				if err != nil {
					logger.Debug().Err(err).Str("entry", entryName).Str("file", src.Name).Str("message_id", sn.id).Str("served_by", sn.host).
						Msg("par2: no other provider served the article behind a bad-checksum PAR2 packet")
					continue
				}
				if bytes.Equal(data, src.Data[sn.start:sn.end]) {
					continue // the same copy elsewhere too
				}
				pv.record(sn.id, host, len(data))
				replaced[i] = data
				logger.Info().Str("entry", entryName).Str("file", src.Name).Str("message_id", sn.id).
					Str("served_by", sn.host).Str("refetched_from", host).
					Msg("par2: another provider served a different body for the article behind a bad-checksum PAR2 packet")
			}
		}
		if len(replaced) == 0 {
			continue
		}
		data := make([]byte, 0, len(src.Data))
		for i, sn := range spans {
			if b, ok := replaced[i]; ok {
				data = append(data, b...)
				continue
			}
			data = append(data, src.Data[sn.start:sn.end]...)
		}
		if changed == 0 {
			out = append([]par2.Source(nil), sources...)
		}
		out[si] = par2.Source{Name: src.Name, Data: data}
		changed += len(replaced)
	}
	return out, changed
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
// structural/terminal or transient/retryable, and the error is classed to
// match; the "(transient: ...)" tag and the "no posted-file fetcher for
// file" prefix stay in the text for the log.
func missingFetcherErr(fileID [16]byte, classifyMiss func(fileID [16]byte) (reason string, terminal bool)) error {
	reason, terminal := "", true
	if classifyMiss != nil {
		reason, terminal = classifyMiss(fileID)
	}
	if terminal {
		if reason != "" {
			return par2Failf(par2Terminal, "no posted-file fetcher for file %x (%s)", fileID, reason)
		}
		return par2Failf(par2Terminal, "no posted-file fetcher for file %x", fileID)
	}
	return par2Failf(par2Transient, "no posted-file fetcher for file %x (transient: %s)", fileID, reason)
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

// ReadSlice reads intact slice globalIdx; ctx bounds its Usenet fetches (a
// repair round's context, so a cancelled round aborts them).
func (s *jobSliceSource) ReadSlice(ctx context.Context, globalIdx int64) ([]byte, error) {
	fileID, local, err := s.idx.SliceLocation(globalIdx)
	if err != nil {
		return nil, err
	}
	f, ok := s.fetchers[fileID]
	if !ok {
		return nil, missingFetcherErr(fileID, s.classifyMiss)
	}
	start := local * s.idx.SliceSize
	data, usedCache, err := f.readRange(ctx, start, s.idx.SliceSize, true)
	if err != nil {
		return nil, err
	}
	if !usedCache {
		return s.otherCopyIfBad(ctx, f, globalIdx, start, data), nil
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
		fresh, _, ferr := f.readRange(ctx, start, s.idx.SliceSize, false)
		if ferr != nil {
			return nil, ferr
		}
		fresh = s.otherCopyIfBad(ctx, f, globalIdx, start, fresh)
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

// otherCopyIfBad returns data, slice globalIdx just read from Usenet, or
// the slice read again after another provider served the articles under it,
// when data fails its IFSC and the other copy passes. One provider can hold
// a body that passes the identity check and decodes cleanly but is not the
// posted bytes (a twin file's same part); par2.RepairWith would only count
// the slice unavailable, and the next round asks the same provider first
// again. Only with exact article boundaries: estimated ones put every
// slice off its IFSC, and asking for each would cost an article per slice.
func (s *jobSliceSource) otherCopyIfBad(ctx context.Context, f *postedFileFetcher, globalIdx, start int64, data []byte) []byte {
	if f.failover == nil || !f.exact {
		return data
	}
	if ok, err := s.idx.VerifySliceChecksum(globalIdx, data); err != nil || ok {
		return data
	}
	// The slice's IFSC does not say which of its articles is bad. Another
	// slice's failover may already have replaced it; else the articles are
	// asked for one at a time, until the slice passes.
	reread := func() ([]byte, bool) {
		again, _, err := f.readRange(ctx, start, s.idx.SliceSize, false)
		if err != nil {
			return nil, false
		}
		ok, verr := s.idx.VerifySliceChecksum(globalIdx, again)
		return again, verr == nil && ok
	}
	end := min(start+s.idx.SliceSize, f.length)
	var toAsk []int
	replacedAny := false
	for pos := start; pos < end; {
		segIdx, err := f.segmentFor(pos)
		if err != nil {
			break
		}
		if _, ok := f.failover.replaced(f, segIdx); ok {
			replacedAny = true
		} else {
			toAsk = append(toAsk, segIdx)
		}
		next := f.base[segIdx] + f.segSizes[segIdx]
		if next <= pos {
			break
		}
		pos = next
	}
	if replacedAny {
		if again, ok := reread(); ok {
			return again
		}
	}
	for _, segIdx := range toAsk {
		if _, ok := f.failover.replace(ctx, f, segIdx, "slice failed its IFSC"); !ok {
			continue
		}
		if again, ok := reread(); ok {
			return again
		}
	}
	return data
}

// postedFailoverMax caps how many posted-file articles one pass asks
// another provider for. Exact boundaries keep the count to the articles
// that are really bad; the cap bounds a provider that mis-serves a whole
// file.
const postedFailoverMax = 16

// postedFailover asks another provider, once per article slot per pass, for
// a posted-file article whose body the first provider served bad after
// decode (short, or failing a slice's IFSC), and holds the other copy for
// the rest of the pass, every round, in place of the first. Shared by the
// pass's fetchers; safe for concurrent use.
type postedFailover struct {
	served  *par2Provenance // the provider that served each posted article
	refetch func(ctx context.Context, messageID, exceptHost string, check usenet.ArticleCheck) ([]byte, string, error)

	mu      sync.Mutex
	entries map[par2RoundArticleKey]*postedFailoverEntry
}

type postedFailoverEntry struct {
	done chan struct{} // closed once data is set
	data []byte        // the other provider's copy, nil when there is none
}

func newPostedFailover(served *par2Provenance, refetch func(ctx context.Context, messageID, exceptHost string, check usenet.ArticleCheck) ([]byte, string, error)) *postedFailover {
	return &postedFailover{served: served, refetch: refetch, entries: make(map[par2RoundArticleKey]*postedFailoverEntry)}
}

// replaced returns the other provider's copy of article idx of f, when one
// has been fetched. Nil-safe.
func (fo *postedFailover) replaced(f *postedFileFetcher, idx int) ([]byte, bool) {
	if fo == nil {
		return nil, false
	}
	fo.mu.Lock()
	e := fo.entries[par2RoundArticleKey{f: f, idx: idx}]
	fo.mu.Unlock()
	if e == nil {
		return nil, false
	}
	select {
	case <-e.done:
		return e.data, e.data != nil
	default:
		return nil, false
	}
}

// replace returns another provider's copy of article idx of f: one already
// fetched, a fetch in flight (waited for), or a new fetch from every
// provider but the one that served the bad body. A copy must pass the
// identity check and decode to the article's full size. false when there
// is none: the article was asked already, the cap is reached, no other
// provider holds it, or the serving provider is not known (a cache read).
// Nil-safe.
func (fo *postedFailover) replace(ctx context.Context, f *postedFileFetcher, idx int, why string) ([]byte, bool) {
	if fo == nil || !f.exact {
		return nil, false
	}
	key := par2RoundArticleKey{f: f, idx: idx}
	fo.mu.Lock()
	if e, ok := fo.entries[key]; ok {
		fo.mu.Unlock()
		select {
		case <-e.done:
			return e.data, e.data != nil
		case <-ctx.Done():
			return nil, false
		}
	}
	if len(fo.entries) >= postedFailoverMax {
		fo.mu.Unlock()
		return nil, false
	}
	e := &postedFailoverEntry{done: make(chan struct{})}
	fo.entries[key] = e
	fo.mu.Unlock()

	messageID := f.segs[idx].MessageID
	first, known := fo.served.get(messageID)
	var data []byte
	var host string
	var err error
	if known {
		fctx, cancel := context.WithTimeout(ctx, par2ArticleFetchTimeout)
		data, host, err = fo.refetch(fctx, messageID, first.host, f.identityCheck(idx))
		cancel()
	}
	fo.mu.Lock()
	switch {
	case !known:
	case err != nil:
		f.logger.Debug().Err(err).Int("segment", idx).Str("first_provider", first.host).Str("reason", why).
			Msg("par2 repair: no other news server served a good copy of an intact article")
		if ctx.Err() != nil {
			// Cut off by the round, not answered: a later asker may try.
			delete(fo.entries, key)
		}
	case int64(len(data)) < f.segSizes[idx]:
		f.logger.Debug().Int("segment", idx).Str("provider", host).Int("bytes", len(data)).Int64("want", f.segSizes[idx]).
			Msg("par2 repair: another news server's copy of an intact article is short too")
	default:
		e.data = data
		f.logger.Info().Int("segment", idx).Str("first_provider", first.host).Str("provider", host).Str("reason", why).
			Msg("par2 repair: asked another news server for an intact article; its copy is used for the rest of the pass")
	}
	fo.mu.Unlock()
	close(e.done)
	return e.data, e.data != nil
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

// estimateUnnamedRecovery estimates the recovery slices held by PAR2 files
// whose names carry no volume marker (an obfuscated posting), for the
// network-free coverage check; censusPar2Volumes splits such a posting the
// same way. The smallest file is the index; a file at least twice its size is a recovery
// volume, holding (size - index) / step slices, where step is the smallest
// such difference - a posting's smallest volume holds one slice. Files under
// twice the index size are other index files (one set per episode in a
// season pack), not volumes. 0 when nothing qualifies. runRepair counts the
// real slices once it has parsed them.
func estimateUnnamedRecovery(files []storage.Par2FileRef) uint32 {
	n, _ := unnamedRecoveryEstimate(files)
	return n
}

// unnamedRecoveryEstimate is estimateUnnamedRecovery that also returns the
// size step it counted in: one recovery packet, slice plus packet overhead.
func unnamedRecoveryEstimate(files []storage.Par2FileRef) (uint32, int64) {
	base, step := unnamedRecoveryGeometry(files)
	if step == 0 {
		return 0, 0
	}
	var n int64
	for _, f := range files {
		n += unnamedVolumeSlices(f.Size, base, step)
	}
	if n > int64(^uint32(0)) {
		n = int64(^uint32(0))
	}
	return uint32(n), step
}

// unnamedRecoveryGeometry returns the smallest file size (the index) and the
// size step of one recovery slice for a set of unnamed PAR2 files; step is 0
// when no file is at least twice the smallest.
func unnamedRecoveryGeometry(files []storage.Par2FileRef) (base, step int64) {
	base = -1
	for _, f := range files {
		if f.Size > 0 && (base < 0 || f.Size < base) {
			base = f.Size
		}
	}
	if base <= 0 {
		return 0, 0
	}
	for _, f := range files {
		if f.Size >= 2*base {
			if d := f.Size - base; step == 0 || d < step {
				step = d
			}
		}
	}
	return base, step
}

// unnamedVolumeSlices is the estimated recovery slices in an unnamed PAR2
// file of the given size: 0 for an index file (under twice base), else at
// least 1.
func unnamedVolumeSlices(size, base, step int64) int64 {
	if step <= 0 || size < 2*base {
		return 0
	}
	return max((size-base+step/2)/step, 1)
}
