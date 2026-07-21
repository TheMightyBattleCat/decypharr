package manager

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/nntp"
)

func TestConcurrentSliceSourceReturnsResultsForCorrectIndex(t *testing.T) {
	order := []int64{0, 1, 2, 3, 4}
	fetchOne := func(idx int64) ([]byte, error) {
		return []byte{byte(idx)}, nil
	}
	src := newConcurrentSliceSource(context.Background(), order, 3, fetchOne)

	for _, idx := range order {
		data, err := src.ReadSlice(idx)
		if err != nil {
			t.Fatalf("ReadSlice(%d): unexpected error: %v", idx, err)
		}
		if len(data) != 1 || data[0] != byte(idx) {
			t.Errorf("ReadSlice(%d) = %v, want [%d]", idx, data, idx)
		}
	}
}

func TestConcurrentSliceSourceHandlesOutOfOrderCompletion(t *testing.T) {
	// Slice 0 takes much longer than the rest, so workers 1..4 finish and
	// land in the resultCh/pending buffer well before ReadSlice(0) is ever
	// asked for - proving the out-of-order buffering path works, not just
	// the happy "everything already matches" case.
	order := []int64{0, 1, 2, 3, 4}
	fetchOne := func(idx int64) ([]byte, error) {
		if idx == 0 {
			time.Sleep(50 * time.Millisecond)
		}
		return []byte{byte(idx)}, nil
	}
	src := newConcurrentSliceSource(context.Background(), order, 5, fetchOne)

	// Give the fast workers a head start so their results are sitting in
	// resultCh/pending before we ever call ReadSlice at all.
	time.Sleep(20 * time.Millisecond)

	for _, idx := range order {
		data, err := src.ReadSlice(idx)
		if err != nil {
			t.Fatalf("ReadSlice(%d): unexpected error: %v", idx, err)
		}
		if data[0] != byte(idx) {
			t.Errorf("ReadSlice(%d) = %v, want [%d]", idx, data, idx)
		}
	}
}

func TestConcurrentSliceSourcePropagatesTheRightError(t *testing.T) {
	order := []int64{0, 1, 2}
	boom := errors.New("boom on 1")
	fetchOne := func(idx int64) ([]byte, error) {
		if idx == 1 {
			return nil, boom
		}
		return []byte{byte(idx)}, nil
	}
	src := newConcurrentSliceSource(context.Background(), order, 3, fetchOne)

	if _, err := src.ReadSlice(0); err != nil {
		t.Fatalf("ReadSlice(0): unexpected error: %v", err)
	}
	if _, err := src.ReadSlice(1); !errors.Is(err, boom) {
		t.Fatalf("ReadSlice(1) error = %v, want %v", err, boom)
	}
	if _, err := src.ReadSlice(2); err != nil {
		t.Fatalf("ReadSlice(2): unexpected error: %v", err)
	}
}

func TestConcurrentSliceSourceBoundsConcurrency(t *testing.T) {
	const maxConcurrency = 4
	order := make([]int64, 40)
	for i := range order {
		order[i] = int64(i)
	}

	var inFlight, peak atomic.Int32
	var mu sync.Mutex
	fetchOne := func(idx int64) ([]byte, error) {
		n := inFlight.Add(1)
		mu.Lock()
		if int32(n) > peak.Load() {
			peak.Store(n)
		}
		mu.Unlock()
		time.Sleep(2 * time.Millisecond)
		inFlight.Add(-1)
		return []byte{byte(idx)}, nil
	}
	src := newConcurrentSliceSource(context.Background(), order, maxConcurrency, fetchOne)

	for _, idx := range order {
		if _, err := src.ReadSlice(idx); err != nil {
			t.Fatalf("ReadSlice(%d): unexpected error: %v", idx, err)
		}
	}

	if got := peak.Load(); got > maxConcurrency {
		t.Errorf("peak concurrent fetches = %d, want <= %d", got, maxConcurrency)
	}
	if got := peak.Load(); got < 2 {
		t.Errorf("peak concurrent fetches = %d, want > 1 (fetches should genuinely overlap, not run one at a time)", got)
	}
}

func TestConcurrentSliceSourceTracksNotFoundIndices(t *testing.T) {
	order := []int64{0, 1, 2, 3}
	notFoundErr := &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: "no such article"}
	fetchOne := func(idx int64) ([]byte, error) {
		if idx == 1 || idx == 3 {
			return nil, notFoundErr
		}
		return []byte{byte(idx)}, nil
	}
	src := newConcurrentSliceSource(context.Background(), order, 4, fetchOne)

	for _, idx := range order {
		_, _ = src.ReadSlice(idx) // drain everything so all workers have reported in
	}

	got := src.NotFoundIndices()
	want := map[int64]bool{1: true, 3: true}
	if len(got) != len(want) {
		t.Fatalf("NotFoundIndices() = %v, want exactly %v", got, want)
	}
	for _, idx := range got {
		if !want[idx] {
			t.Errorf("NotFoundIndices() contains unexpected index %d", idx)
		}
	}
}

func TestConcurrentSliceSourceDoesNotTrackOtherErrorsAsNotFound(t *testing.T) {
	order := []int64{0, 1}
	timeoutErr := &nntp.Error{Type: nntp.ErrorTypeTimeout, Code: 0, Message: "deadline exceeded"}
	fetchOne := func(idx int64) ([]byte, error) {
		if idx == 1 {
			return nil, timeoutErr
		}
		return []byte{0}, nil
	}
	src := newConcurrentSliceSource(context.Background(), order, 2, fetchOne)
	for _, idx := range order {
		_, _ = src.ReadSlice(idx)
	}
	if got := src.NotFoundIndices(); len(got) != 0 {
		t.Errorf("NotFoundIndices() = %v, want empty - a timeout is not a confirmed not-found", got)
	}
}
