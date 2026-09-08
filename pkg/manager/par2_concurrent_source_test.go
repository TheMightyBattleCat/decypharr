package manager

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

func TestConcurrentSliceSourceTracksShortSegmentAsNotFound(t *testing.T) {
	order := []int64{0, 1, 2}
	fetchOne := func(idx int64) ([]byte, error) {
		if idx == 1 {
			// Wrapped exactly as postedFileFetcher.ReadRange surfaces it.
			return nil, fmt.Errorf("segment %d: %w", 7, ErrSegmentShort)
		}
		return []byte{byte(idx)}, nil
	}
	src := newConcurrentSliceSource(context.Background(), order, 3, fetchOne)
	for _, idx := range order {
		_, _ = src.ReadSlice(idx)
	}
	got := src.NotFoundIndices()
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("NotFoundIndices() = %v, want [1] - a truncated backing article is unrecoverable data, like a 430", got)
	}
}

// A short read and a hard 430 both land in the damaged set, but they mean
// opposite things about where the fault lies - a 430 is provider damage, a
// short read is a claim about our own persisted segment geometry. Reporting
// both as "confirmed missing across every provider" made a geometry bug on
// intact data indistinguishable from a dead posting, in the logs and in the
// terminal classifier.
func TestConcurrentSliceSourceSplitsDeadCauses(t *testing.T) {
	order := []int64{0, 1, 2, 3}
	notFoundErr := &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: "no such article"}
	fetchOne := func(idx int64) ([]byte, error) {
		switch idx {
		case 1:
			return nil, notFoundErr
		case 2:
			return nil, fmt.Errorf("segment %d: %w", 7, ErrSegmentShort)
		case 3:
			// A transient timeout is neither: it must not be counted at all.
			return nil, context.DeadlineExceeded
		}
		return []byte{byte(idx)}, nil
	}
	src := newConcurrentSliceSource(context.Background(), order, 4, fetchOne)
	for _, idx := range order {
		_, _ = src.ReadSlice(idx)
	}

	confirmedMissing, shortRead := src.DeadCauseCounts()
	if confirmedMissing != 1 {
		t.Errorf("confirmedMissing = %d, want 1 (only the 430)", confirmedMissing)
	}
	if shortRead != 1 {
		t.Errorf("shortRead = %d, want 1 (only the ErrSegmentShort)", shortRead)
	}
	if total := len(src.NotFoundIndices()); total != 2 {
		t.Errorf("NotFoundIndices() has %d entries, want 2 - the timeout must not be folded in", total)
	}
}

// TestConcurrentSliceSourceWrapsContextErrorWhenCancelled covers the real
// cause of the "worker pool closed early" report: run()'s workers take the
// <-ctx.Done() branch on their result send when the job's context is
// cancelled (idle-timeout / deadline / preemption / shutdown), so their
// results never reach resultCh, run() closes the channel, and ReadSlice
// drains it without a match. That must surface as a wrapped context error so
// errors.Is(err, context.Canceled/DeadlineExceeded) works for the retry
// loop and the backoff classifier downstream - not a bare synthetic string
// that erases the cause.
func TestConcurrentSliceSourceWrapsContextErrorWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Struct built directly so the "workers dropped their results, run()
	// closed the channel" state is deterministic - racing a real cancel
	// against the send select is inherently flaky.
	s := &concurrentSliceSource{
		ctx:      ctx,
		resultCh: make(chan sliceFetchResult),
		pending:  make(map[int64]sliceFetchResult),
		notFound: make(map[int64]deadCause),
	}
	close(s.resultCh)

	_, err := s.ReadSlice(7)
	if err == nil {
		t.Fatal("ReadSlice(7) returned nil error, want a wrapped context error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadSlice(7) error = %v, want errors.Is(..., context.Canceled)", err)
	}
	if !strings.Contains(err.Error(), "7") {
		t.Errorf("ReadSlice(7) error = %q, want it to name slice 7", err)
	}
}

// TestConcurrentSliceSourceReportsPoolClosedWhenNotCancelled is the
// theoretical fallthrough: the channel drained without the requested index
// but the context is still live (a genuine internal invariant break, e.g. a
// caller requesting an index that was never in order). That must keep the
// original diagnostic string and must NOT masquerade as a context error.
func TestConcurrentSliceSourceReportsPoolClosedWhenNotCancelled(t *testing.T) {
	s := &concurrentSliceSource{
		ctx:      context.Background(),
		resultCh: make(chan sliceFetchResult),
		pending:  make(map[int64]sliceFetchResult),
		notFound: make(map[int64]deadCause),
	}
	close(s.resultCh)

	_, err := s.ReadSlice(2)
	if err == nil {
		t.Fatal("ReadSlice(2) returned nil error, want the pool-closed diagnostic")
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ReadSlice(2) error = %v, must not be a context error when ctx is live", err)
	}
	if !strings.Contains(err.Error(), "worker pool closed early") {
		t.Errorf("ReadSlice(2) error = %q, want the original 'worker pool closed early' diagnostic", err)
	}
}

// TestConcurrentSliceSourceContextErrorIsClassifiedTransient walks the wrapped
// chain end to end - concurrentSliceSource.ReadSlice wrap, then par2.Repair's
// own "read intact slice %d: %w" wrap (pkg/usenet/par2/repair.go) - and
// confirms the backoff classifier still sees it as transient. This is the
// misclassification the fix targets: a cut-off intact-slice fetch must back
// off and retry, never mark the entry permanently unrepairable.
func TestConcurrentSliceSourceContextErrorIsClassifiedTransient(t *testing.T) {
	readErr := fmt.Errorf("par2: fetch for slice %d interrupted: %w", int64(7), context.DeadlineExceeded)
	repairErr := fmt.Errorf("par2: read intact slice %d: %w", int64(7), readErr)

	if !errors.Is(repairErr, context.DeadlineExceeded) {
		t.Fatalf("wrapped repair error lost the context cause: %v", repairErr)
	}
	if class := classifyPar2Failure(repairErr); class.terminal {
		t.Fatalf("classifyPar2Failure(interrupted repair).terminal = true (reason %q), want transient", class.reason)
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
