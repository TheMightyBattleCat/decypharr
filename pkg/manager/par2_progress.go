package manager

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
)

// Par2JobPhase is a PAR2 repair job's current stage, for live progress
// reporting.
type Par2JobPhase string

const (
	Par2PhaseQueued           Par2JobPhase = "queued"
	Par2PhaseFetchingRecovery Par2JobPhase = "fetching_recovery"
	Par2PhaseStreamingIntact  Par2JobPhase = "streaming_intact"
	Par2PhaseSolving          Par2JobPhase = "solving"
	Par2PhaseWriting          Par2JobPhase = "writing"
	Par2PhaseCompleted        Par2JobPhase = "completed"
	Par2PhaseFailed           Par2JobPhase = "failed"
)

// Par2JobProgress is an immutable snapshot of one PAR2 repair job's live
// progress, for JSON serving - see par2JobProgressState.Snapshot. Distinct
// from storage.Par2RepairAttempt (the persisted, terminal-outcome history
// record): this exists only in memory, for whichever job(s) have run this
// process, and is gone on restart - it exists purely so a stall is visible
// within seconds, not just at completion.
type Par2JobProgress struct {
	NzbID     string       `json:"nzb_id"`
	EntryName string       `json:"entry_name"`
	Phase     Par2JobPhase `json:"phase"`
	StartedAt time.Time    `json:"started_at"`
	UpdatedAt time.Time    `json:"updated_at"`

	RecoveryVolsFetched   int `json:"recovery_vols_fetched"`
	RecoverySlicesFetched int `json:"recovery_slices_fetched"`
	RecoverySlicesNeeded  int `json:"recovery_slices_needed"`

	IntactSlicesRead  int `json:"intact_slices_read"`
	IntactSlicesTotal int `json:"intact_slices_total"`

	CacheBytes  int64 `json:"cache_bytes"`
	UsenetBytes int64 `json:"usenet_bytes"`

	LastError string `json:"last_error,omitempty"`
}

// par2JobProgressState is the mutable, concurrency-safe live state behind
// one Par2JobProgress. The hot-path counters (bumped from many concurrent
// slice-fetch goroutines - see concurrentSliceSource) are plain atomics, so
// updating them never blocks a fetch; the rarely-changed fields (phase,
// last error) sit behind a small mutex. Snapshot copies everything into a
// plain, race-free value for JSON responses.
type par2JobProgressState struct {
	nzbID     string
	entryName string
	startedAt time.Time
	updatedAt atomic.Int64 // unix nano

	recoveryVolsFetched   atomic.Int64
	recoverySlicesFetched atomic.Int64
	recoverySlicesNeeded  atomic.Int64
	intactSlicesRead      atomic.Int64
	intactSlicesTotal     atomic.Int64
	cacheBytes            atomic.Int64
	usenetBytes           atomic.Int64

	mu        sync.RWMutex
	phase     Par2JobPhase
	lastError string
}

func newPar2JobProgressState(nzbID, entryName string) *par2JobProgressState {
	s := &par2JobProgressState{
		nzbID: nzbID, entryName: entryName,
		startedAt: time.Now(),
		phase:     Par2PhaseQueued,
	}
	s.touch()
	return s
}

func (s *par2JobProgressState) touch() {
	s.updatedAt.Store(time.Now().UnixNano())
}

// Every setter below is a nil-safe no-op on a nil receiver: callers thread
// *par2JobProgressState through plain fields (e.g. cacheSlicedSource.progress)
// that unit tests and some construction paths deliberately leave nil rather
// than always requiring a live tracked job.
func (s *par2JobProgressState) SetPhase(phase Par2JobPhase) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.phase = phase
	s.mu.Unlock()
	s.touch()
}

func (s *par2JobProgressState) SetEntryName(name string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.entryName = name
	s.mu.Unlock()
	s.touch()
}

func (s *par2JobProgressState) SetLastError(err string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.lastError = err
	s.mu.Unlock()
	s.touch()
}

func (s *par2JobProgressState) SetRecoveryVolsFetched(n int) {
	if s == nil {
		return
	}
	s.recoveryVolsFetched.Store(int64(n))
	s.touch()
}

func (s *par2JobProgressState) SetRecoverySlices(fetched, needed int) {
	if s == nil {
		return
	}
	s.recoverySlicesFetched.Store(int64(fetched))
	s.recoverySlicesNeeded.Store(int64(needed))
	s.touch()
}

func (s *par2JobProgressState) SetIntactTotal(n int) {
	if s == nil {
		return
	}
	s.intactSlicesTotal.Store(int64(n))
	s.touch()
}

func (s *par2JobProgressState) AddIntactRead(n int) {
	if s == nil || n == 0 {
		return
	}
	s.intactSlicesRead.Add(int64(n))
	s.touch()
}

func (s *par2JobProgressState) AddCacheBytes(n int64) {
	if s == nil || n == 0 {
		return
	}
	s.cacheBytes.Add(n)
	s.touch()
}

func (s *par2JobProgressState) AddUsenetBytes(n int64) {
	if s == nil || n == 0 {
		return
	}
	s.usenetBytes.Add(n)
	s.touch()
}

// Snapshot returns a plain, immutable copy safe to hand to a JSON encoder
// or read after the job has moved on. Nil-safe: a nil receiver returns the
// zero value.
func (s *par2JobProgressState) Snapshot() Par2JobProgress {
	if s == nil {
		return Par2JobProgress{}
	}
	s.mu.RLock()
	phase, entryName, lastErr := s.phase, s.entryName, s.lastError
	s.mu.RUnlock()
	return Par2JobProgress{
		NzbID:     s.nzbID,
		EntryName: entryName,
		Phase:     phase,
		StartedAt: s.startedAt,
		UpdatedAt: time.Unix(0, s.updatedAt.Load()),

		RecoveryVolsFetched:   int(s.recoveryVolsFetched.Load()),
		RecoverySlicesFetched: int(s.recoverySlicesFetched.Load()),
		RecoverySlicesNeeded:  int(s.recoverySlicesNeeded.Load()),

		IntactSlicesRead:  int(s.intactSlicesRead.Load()),
		IntactSlicesTotal: int(s.intactSlicesTotal.Load()),

		CacheBytes:  s.cacheBytes.Load(),
		UsenetBytes: s.usenetBytes.Load(),

		LastError: lastErr,
	}
}

// par2ProgressTracker holds live progress for recently-started jobs, keyed
// by nzbID - a small map rather than a single slot, since a manual RunNow
// can run concurrently with the worker's own queue-driven job for a
// DIFFERENT nzbID (the worker only serializes jobs for the SAME nzbID via
// the queued/deferred/active bookkeeping in Par2Repair itself). Entries are
// simply overwritten by the next job for that nzbID; nothing prunes old
// ones proactively (bounded in practice - one entry per nzbID that has ever
// had a PAR2 pass this process, a small set), so a caller can still see the
// last completed/failed job's final state, not just active ones.
type par2ProgressTracker struct {
	jobs *xsync.Map[string, *par2JobProgressState]
}

// newPar2ProgressTracker builds an empty tracker - xsync.Map's zero value
// is NOT usable (it panics on first Store), so a par2ProgressTracker must
// always come from here, never a bare struct literal.
func newPar2ProgressTracker() par2ProgressTracker {
	return par2ProgressTracker{jobs: xsync.NewMap[string, *par2JobProgressState]()}
}

// Start begins tracking a new job for nzbID, replacing any previous state
// for it.
func (t *par2ProgressTracker) Start(nzbID, entryName string) *par2JobProgressState {
	s := newPar2JobProgressState(nzbID, entryName)
	t.jobs.Store(nzbID, s)
	return s
}

// Get returns the live or most-recently-finished progress state for nzbID,
// if any job has run for it this process.
func (t *par2ProgressTracker) Get(nzbID string) (*par2JobProgressState, bool) {
	if t.jobs == nil {
		return nil, false
	}
	return t.jobs.Load(nzbID)
}
