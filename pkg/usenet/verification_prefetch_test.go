package usenet

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakePrefetchReader records FetchRange calls for verificationPrefetch tests.
type fakePrefetchReader struct {
	mu    sync.Mutex
	calls []fetchCall
	block chan struct{} // if non-nil, FetchRange blocks on it before returning
	err   error         // returned by FetchRange once calls have been recorded
}

type fetchCall struct {
	off, length int64
}

func (f *fakePrefetchReader) ReadAt(p []byte, off int64) (int, error) { return len(p), nil }

func (f *fakePrefetchReader) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	return len(p), nil
}

func (f *fakePrefetchReader) Prefetch(ctx context.Context, off, length int64) {}

func (f *fakePrefetchReader) FetchRange(ctx context.Context, off, length int64, concurrency int) error {
	f.mu.Lock()
	f.calls = append(f.calls, fetchCall{off, length})
	block := f.block
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return f.err
}

func (f *fakePrefetchReader) fetchedTo(base int64) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var max int64
	for _, c := range f.calls {
		if end := c.off - base + c.length; end > max {
			max = end
		}
	}
	return max
}

func (f *fakePrefetchReader) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func TestVerificationPrefetch_StopsAtTotal(t *testing.T) {
	u := &Usenet{maxConnections: 15}
	r := &fakePrefetchReader{}
	const base, total = int64(1 << 20), int64(20 << 20)

	m := &meteredReader{}
	m.consumed.Store(total) // read already finished: prefetch should fill to total and stop

	done := make(chan struct{})
	go func() { defer close(done); u.verificationPrefetch(context.Background(), r, base, total, m) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("verificationPrefetch did not return")
	}

	if got := r.fetchedTo(base); got != total {
		t.Fatalf("fetched to %d, want %d", got, total)
	}
	r.mu.Lock()
	for _, c := range r.calls {
		if c.off-base+c.length > total {
			t.Fatalf("FetchRange %+v overshoots total %d (base %d)", c, total, base)
		}
	}
	r.mu.Unlock()
}

func TestVerificationPrefetch_RampsWithConsumed(t *testing.T) {
	u := &Usenet{maxConnections: 15}
	r := &fakePrefetchReader{}
	const base, total = int64(0), int64(1 << 30)

	m := &meteredReader{} // consumed stays 0

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); u.verificationPrefetch(ctx, r, base, total, m) }()

	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	// consumed never crossed the ramp threshold, so nothing should have been
	// fetched despite total being 1 GiB.
	if n := r.callCount(); n != 0 {
		t.Fatalf("prefetched %d chunks with consumed=0, want 0", n)
	}
}

func TestVerificationPrefetch_RespectsAheadWindow(t *testing.T) {
	u := &Usenet{maxConnections: 15}
	r := &fakePrefetchReader{}
	const base, total = int64(0), int64(1 << 30)

	m := &meteredReader{}
	m.consumed.Store(verificationPrefetchChunk) // just past the ramp gate

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); u.verificationPrefetch(ctx, r, base, total, m) }()

	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	// ahead == consumed == 8 MiB, so fetchedTo should settle around
	// consumed+ahead and not run away toward total.
	if got := r.fetchedTo(base); got > 4*verificationPrefetchChunk {
		t.Fatalf("fetched to %d, expected it to stay near the ahead window (~%d)", got, 2*verificationPrefetchChunk)
	}
}

func TestVerificationPrefetch_ExitsOnCancelWhileFetching(t *testing.T) {
	u := &Usenet{maxConnections: 15}
	r := &fakePrefetchReader{block: make(chan struct{})}
	const base, total = int64(0), int64(1 << 30)

	m := &meteredReader{}
	m.consumed.Store(verificationPrefetchAhead)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); u.verificationPrefetch(ctx, r, base, total, m) }()

	// let it enter a blocked FetchRange
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("verificationPrefetch did not exit after ctx cancel during FetchRange")
	}
}
