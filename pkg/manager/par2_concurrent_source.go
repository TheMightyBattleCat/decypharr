package manager

import (
	"context"
	"fmt"

	"github.com/sourcegraph/conc/pool"
)

// sliceFetchResult is one concurrent worker's outcome for one global PAR2
// slice index.
type sliceFetchResult struct {
	idx  int64
	data []byte
	err  error
}

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
	resultCh chan sliceFetchResult
	pending  map[int64]sliceFetchResult // out-of-order results not yet consumed - read/written only from ReadSlice's single caller goroutine
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
		resultCh: make(chan sliceFetchResult, maxConcurrency*2),
		pending:  make(map[int64]sliceFetchResult),
	}
	go s.run(ctx, order, maxConcurrency, fetchOne)
	return s
}

func (s *concurrentSliceSource) run(ctx context.Context, order []int64, maxConcurrency int, fetchOne func(int64) ([]byte, error)) {
	p := pool.New().WithContext(ctx).WithMaxGoroutines(maxConcurrency)
	for _, idx := range order {
		p.Go(func(ctx context.Context) error {
			data, err := fetchOne(idx)
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
	return nil, fmt.Errorf("par2: no fetch result for slice %d (worker pool closed early)", globalIdx)
}
