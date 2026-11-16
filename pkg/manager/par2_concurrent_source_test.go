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
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
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
	order := []int64{0, 1, 2, 3, 4}
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
		case 4:
			return nil, corruptErr
		}
		return []byte{byte(idx)}, nil
	}
	src := newConcurrentSliceSource(context.Background(), order, 4, fetchOne)
	for _, idx := range order {
		_, _ = src.ReadSlice(idx)
	}

	confirmedMissing, shortRead, corrupt := src.DeadCauseCounts()
	wantCorrupt, wantTotal := 1, 3
	if corrupt != wantCorrupt {
		t.Errorf("corrupt = %d, want %d (only the every-provider CRC failure)", corrupt, wantCorrupt)
	}
	if confirmedMissing != 1 {
		t.Errorf("confirmedMissing = %d, want 1 (only the 430)", confirmedMissing)
	}
	if shortRead != 1 {
		t.Errorf("shortRead = %d, want 1 (only the ErrSegmentShort)", shortRead)
	}
	if total := len(src.NotFoundIndices()); total != wantTotal {
		t.Errorf("NotFoundIndices() has %d entries, want %d - the timeout must not be folded in", total, wantTotal)
	}
	// Each unreadable slice is marked so par2.Repair finishes the pass.
	for _, idx := range []int64{1, 2} {
		if _, err := newConcurrentSliceSource(context.Background(), []int64{idx}, 1, fetchOne).ReadSlice(idx); !errors.Is(err, par2.ErrSliceUnavailable) {
			t.Errorf("slice %d: %v, want ErrSliceUnavailable", idx, err)
		}
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

// Cancelling a source whose consumer has stopped reading (an early-stopped
// pass) stops the fetches. fetchOne takes no context, so without the checks
// in run() the unblocked workers read the rest of the release.
func TestConcurrentSliceSourceStopsFetchingWhenCancelled(t *testing.T) {
	const n = 2000
	order := make([]int64, n)
	for i := range order {
		order[i] = int64(i)
	}
	var calls atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	src := newConcurrentSliceSource(ctx, order, 4, func(i int64) ([]byte, error) {
		calls.Add(1)
		time.Sleep(time.Millisecond)
		return []byte{byte(i)}, nil
	})
	for i := int64(0); i < 5; i++ {
		if _, err := src.ReadSlice(i); err != nil {
			t.Fatalf("ReadSlice(%d): %v", i, err)
		}
	}
	cancel()
	time.Sleep(200 * time.Millisecond)
	settled := calls.Load()
	time.Sleep(200 * time.Millisecond)
	if after := calls.Load(); after != settled || after > 100 {
		t.Fatalf("fetches after cancel: %d then %d of %d, want them stopped", settled, after, n)
	}
}

// One slow article must not let the other workers run ahead: with slice 0
// held back (a fetch waiting on failover while the rest come from the DFS
// cache), only slices inside the read-ahead window may be fetched, and
// ReadSlice may park no more than that window. Before the window was bounded
// by position, 3999 of 4000 slices were fetched and parked behind slice 0.
func TestConcurrentSliceSourceBoundsReadAheadBehindSlowSlice(t *testing.T) {
	const (
		total       = 4000
		concurrency = 8
		window      = 2 * concurrency
	)
	order := make([]int64, total)
	for i := range order {
		order[i] = int64(i)
	}
	release := make(chan struct{})
	var startedBeforeRelease atomic.Int64
	var released atomic.Bool
	fetch := func(i int64) ([]byte, error) {
		if !released.Load() {
			startedBeforeRelease.Add(1)
		}
		if i == 0 {
			<-release
		}
		return []byte{byte(i)}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := newConcurrentSliceSource(ctx, order, concurrency, fetch)

	go func() {
		time.Sleep(300 * time.Millisecond)
		released.Store(true)
		close(release)
	}()
	if _, err := src.ReadSlice(0); err != nil {
		t.Fatal(err)
	}
	if parked := len(src.pending); parked > window {
		t.Errorf("ReadSlice parked %d slices while slice 0 was outstanding, want at most the window of %d", parked, window)
	}
	if started := startedBeforeRelease.Load(); started > window {
		t.Errorf("%d fetches started while slice 0 was outstanding, want at most the window of %d", started, window)
	}

	// The window moves with the consumer: every slice still arrives, in
	// order, with its own data.
	for _, idx := range order[1:] {
		data, err := src.ReadSlice(idx)
		if err != nil {
			t.Fatalf("ReadSlice(%d): %v", idx, err)
		}
		if len(data) != 1 || data[0] != byte(idx) {
			t.Fatalf("ReadSlice(%d) = %v, want [%d]", idx, data, byte(idx))
		}
	}
}

// Error results hand back their read-ahead slot like good ones. A pass that
// carries on past unreadable slices (par2.ErrSliceUnavailable) must not hang
// once more of them than the window have been read.
func TestConcurrentSliceSourceFailingSlicesDoNotStallTheWindow(t *testing.T) {
	const (
		total       = 200
		concurrency = 2 // window of 4, far fewer than the failing slices
	)
	order := make([]int64, total)
	for i := range order {
		order[i] = int64(i)
	}
	fetch := func(i int64) ([]byte, error) {
		if i < 50 || i%3 == 0 {
			return nil, &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: "no such article"}
		}
		return []byte{byte(i)}, nil
	}
	src := newConcurrentSliceSource(context.Background(), order, concurrency, fetch)

	done := make(chan error, 1)
	go func() {
		for _, idx := range order {
			_, err := src.ReadSlice(idx)
			wantFail := idx < 50 || idx%3 == 0
			if wantFail && !errors.Is(err, par2.ErrSliceUnavailable) {
				done <- fmt.Errorf("ReadSlice(%d) error = %v, want ErrSliceUnavailable", idx, err)
				return
			}
			if !wantFail && err != nil {
				done <- fmt.Errorf("ReadSlice(%d): %v", idx, err)
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reading through failing slices hung: an error result did not give back its read-ahead slot")
	}
}

// A pass that stops reading early leaves the read-ahead window full, so run()
// is waiting for a slot. Cancelling the context must free that wait, close
// the result channel, and let ReadSlice report the cancellation promptly.
func TestConcurrentSliceSourceCancelReleasesAFullWindow(t *testing.T) {
	const total = 1000
	order := make([]int64, total)
	for i := range order {
		order[i] = int64(i)
	}
	ctx, cancel := context.WithCancel(context.Background())
	src := newConcurrentSliceSource(ctx, order, 2, func(i int64) ([]byte, error) {
		return []byte{byte(i)}, nil
	})
	for i := int64(0); i < 3; i++ {
		if _, err := src.ReadSlice(i); err != nil {
			t.Fatalf("ReadSlice(%d): %v", i, err)
		}
	}
	// Let the workers fill the window so run() is blocked on a slot.
	time.Sleep(50 * time.Millisecond)
	cancel()

	closed := make(chan struct{})
	go func() {
		// Drain without consuming slots; ends when run() closes the channel.
		for range src.resultCh {
		}
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("result channel not closed after cancel: run() is still waiting for a read-ahead slot")
	}

	errc := make(chan error, 1)
	go func() {
		_, err := src.ReadSlice(3)
		errc <- err
	}()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ReadSlice after cancel = %v, want a wrapped context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReadSlice after cancel did not return")
	}
}
