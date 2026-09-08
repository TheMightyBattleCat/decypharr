package manager

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/sourcegraph/conc/pool"

	"github.com/sirrobot01/decypharr/internal/nntp"
)

// sliceFetchResult is one concurrent worker's outcome for one global PAR2
// slice index.
type sliceFetchResult struct {
	idx  int64
	data []byte
	err  error
}

// deadCause records WHY a slice's backing bytes could not be read. Both
// causes are unrecoverable at this position and both are folded into the
// damaged set identically - the distinction is not about what to do, it is
// about what is true, because the two have opposite root causes and the same
// remedy would be wrong for one of them:
//
//   - causeConfirmedMissing: a hard 430 across every provider. The article is
//     genuinely gone from usenet. Provider-side damage.
//   - causeShortRead: the article EXISTS (it STATs fine) and simply decoded
//     to fewer bytes than our persisted segment geometry claimed. That is a
//     statement about our own arithmetic, not about the posting - an
//     over-estimated segment size manufactures this on wholly intact data.
//
// Reporting both as "confirmed missing across every provider" is not merely
// imprecise, it actively misleads: it makes a geometry bug indistinguishable
// from provider damage in the logs, and lets an inflated damaged set push k
// past the recovery budget into a terminal "unrepairable" verdict on a
// release that is fine.
type deadCause uint8

const (
	causeConfirmedMissing deadCause = iota
	causeShortRead
)

// concurrentSliceSource wraps a per-slice fetch function with a bounded
// pool of concurrent fetchers - mirroring pkg/usenet/download.go's model
// (pool.New().WithContext(ctx).WithMaxGoroutines(N) plus a bounded result
// channel for backpressure) - so par2.Repair's sequential ReadSlice calls
// are served by fetches that already started, or already finished, well
// before they're needed, instead of one strictly-sequential fetch at a
// time. A single stalled connection now only holds up its own goroutine's
// segment, not every other intact slice queued behind it: with N workers
// in flight, one stuck fetch costs at most one of N concurrent slots for
// up to its own per-segment timeout, not the whole job's deadline.
//
// Memory-bounded: at most 2x maxConcurrency slices are ever buffered ahead
// of the consumer (the channel's capacity), never the whole recovery set -
// par2.Repair's own "never hold more than one slice at a time" streaming
// design is preserved at that layer; this only widens the window from
// exactly 1 to a small, fixed multiple of the connection limit, matching
// the concurrency the normal download path already uses.
type concurrentSliceSource struct {
	ctx      context.Context // the pool's context; ReadSlice consults ctx.Err() so a cancelled/timed-out job surfaces as a wrapped context error, not a bare "pool closed early"
	resultCh chan sliceFetchResult
	pending  map[int64]sliceFetchResult // out-of-order results not yet consumed - read/written only from ReadSlice's single caller goroutine

	// notFoundMu guards notFound - written concurrently by every worker in
	// run(), read by NotFoundIndices after the pool has drained (or
	// mid-flight, best-effort). See NotFoundIndices.
	notFoundMu sync.Mutex
	notFound   map[int64]deadCause
}

// newConcurrentSliceSource launches maxConcurrency worker goroutines that
// fetch every index in order via fetchOne, in parallel, feeding results
// back through a bounded channel. order must list every global slice index
// par2.Repair.ReadSlice will be called with, in the exact sequence it will
// call them (ascending, skipping already-damaged indices) - see runRepair,
// which derives this from the same idx.NumSlices()/damaged set par2.Repair
// itself uses internally, so the two can never disagree.
func newConcurrentSliceSource(ctx context.Context, order []int64, maxConcurrency int, fetchOne func(globalIdx int64) ([]byte, error)) *concurrentSliceSource {
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}
	s := &concurrentSliceSource{
		ctx:      ctx,
		resultCh: make(chan sliceFetchResult, maxConcurrency*2),
		pending:  make(map[int64]sliceFetchResult),
		notFound: make(map[int64]deadCause),
	}
	go s.run(ctx, order, maxConcurrency, fetchOne)
	return s
}

func (s *concurrentSliceSource) run(ctx context.Context, order []int64, maxConcurrency int, fetchOne func(int64) ([]byte, error)) {
	p := pool.New().WithContext(ctx).WithMaxGoroutines(maxConcurrency)
	for _, idx := range order {
		p.Go(func(ctx context.Context) error {
			data, err := fetchOne(idx)
			if err != nil {
				// This slice's backing data is unreadable at this position:
				// either a hard 430 confirmed across every provider
				// (ExecuteWithFailover already exhausted them all before
				// returning it), or a backing article that decoded too short to
				// serve the bytes the slice needs (ErrSegmentShort). Either way
				// it's not a transient hiccup - retrying the same fetch yields
				// the same result - so both are folded into the damaged set for
				// recovery-slice reconstruction. The CAUSE is retained because
				// the two mean opposite things about where the fault lies; see
				// deadCause. Recorded regardless of whether ReadSlice ever gets
				// asked for this exact index: par2.Repair aborts on the FIRST
				// error it sees, so a later index's failure here would otherwise
				// be silently lost - see runRepair's retry loop, which
				// reclassifies every index collected here into the damaged set
				// at once.
				//
				// The 430 test comes first: readRange wraps a fetch failure as
				// "fetch segment N: <err>" and a short decode as
				// "segment N: ErrSegmentShort", so the two are disjoint in
				// practice, but a genuinely-missing article is the stronger
				// claim and should win any overlap.
				switch {
				case nntp.IsArticleNotFoundError(err):
					s.notFoundMu.Lock()
					s.notFound[idx] = causeConfirmedMissing
					s.notFoundMu.Unlock()
				case errors.Is(err, ErrSegmentShort):
					s.notFoundMu.Lock()
					s.notFound[idx] = causeShortRead
					s.notFoundMu.Unlock()
				}
			}
			select {
			case s.resultCh <- sliceFetchResult{idx: idx, data: data, err: err}:
			case <-ctx.Done():
			}
			return nil // never abort the pool early - ReadSlice is what surfaces the real error, to the exact caller expecting it
		})
	}
	_ = p.Wait()
	close(s.resultCh)
}

// NotFoundIndices returns every global slice index whose fetch has failed
// with a confirmed article-not-found (across every provider) so far. Safe
// to call at any time, including while workers are still running (the
// returned set just may not be complete yet) - runRepair's retry loop
// calls it only after par2.Repair has already returned, by which point
// every worker that was going to report anything has either finished or
// been abandoned by the pool's context cancellation.
func (s *concurrentSliceSource) NotFoundIndices() []int64 {
	s.notFoundMu.Lock()
	defer s.notFoundMu.Unlock()
	out := make([]int64, 0, len(s.notFound))
	for idx := range s.notFound {
		out = append(out, idx)
	}
	return out
}

// DeadCauseCounts breaks the NotFoundIndices set down by why each slice was
// unreadable: articles confirmed gone from every provider, versus articles
// that exist but decoded shorter than our persisted segment geometry claimed.
// Same locking and same best-effort snapshot semantics as NotFoundIndices.
//
// The split exists so a caller can say which it saw. A nonzero shortRead is a
// signal about THIS code's arithmetic, not about the posting: it means a
// segment size we persisted overstates what the article really decodes to (see
// exactSegGeometry), and the slices counted here may be wholly intact data
// being reconstructed from parity for no reason.
func (s *concurrentSliceSource) DeadCauseCounts() (confirmedMissing, shortRead int) {
	s.notFoundMu.Lock()
	defer s.notFoundMu.Unlock()
	for _, cause := range s.notFound {
		if cause == causeShortRead {
			shortRead++
			continue
		}
		confirmedMissing++
	}
	return confirmedMissing, shortRead
}

// ReadSlice implements par2.SliceSource, satisfying the "read exactly once
// per intact slice, in ascending order" contract Repair depends on - the
// concurrency happens entirely underneath, in the worker pool started by
// newConcurrentSliceSource. Callers must request exactly the sequence
// passed as order; any other index blocks forever waiting for a result
// that will never arrive.
func (s *concurrentSliceSource) ReadSlice(globalIdx int64) ([]byte, error) {
	if r, ok := s.pending[globalIdx]; ok {
		delete(s.pending, globalIdx)
		return r.data, r.err
	}
	for r := range s.resultCh {
		if r.idx == globalIdx {
			return r.data, r.err
		}
		s.pending[r.idx] = r
	}
	// The channel drained without ever yielding globalIdx. The common cause
	// is context cancellation: run()'s workers take the <-ctx.Done() branch
	// on their result send, so an idle-timeout / job-deadline / preemption /
	// shutdown drops in-flight results on the floor, run() closes resultCh,
	// and we land here. Surface that as a wrapped context error so
	// runRepair's retry loop (and the backoff classifier downstream) can see
	// errors.Is(err, context.Canceled/DeadlineExceeded) and treat the repair
	// as transiently interrupted rather than terminally failed.
	if err := s.ctx.Err(); err != nil {
		return nil, fmt.Errorf("par2: fetch for slice %d interrupted: %w", globalIdx, err)
	}
	return nil, fmt.Errorf("par2: no fetch result for slice %d (worker pool closed early)", globalIdx)
}
