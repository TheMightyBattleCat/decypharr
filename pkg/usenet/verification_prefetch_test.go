package usenet

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeWindowedReader simulates reader.StreamingReader.FetchRangeWindowed at
// segment granularity so verificationPrefetch's horizon function can be tested
// without a real NNTP-backed reader.
type fakeWindowedReader struct {
	mu        sync.Mutex
	fetchedTo int64 // highest segment-end offset (relative to base) fetched so far
	calls     int
	block     chan struct{} // if non-nil, each "segment fetch" waits on it
}

const fakeSegSize = 1 << 20 // 1 MiB

func (f *fakeWindowedReader) ReadAt(p []byte, off int64) (int, error) { return len(p), nil }
func (f *fakeWindowedReader) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	return len(p), nil
}
func (f *fakeWindowedReader) Prefetch(ctx context.Context, off, length int64) {}
func (f *fakeWindowedReader) FetchRange(ctx context.Context, off, length int64, concurrency int) error {
	return nil
}

func (f *fakeWindowedReader) FetchRangeWindowed(ctx context.Context, base, total int64, concurrency int, horizon func() int64) error {
	f.mu.Lock()
	f.calls++
	block := f.block
	f.mu.Unlock()

	var rel int64
	for rel < total {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		for rel > horizon() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		if block != nil {
			select {
			case <-block:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		next := rel + fakeSegSize
		if next > total {
			next = total
		}
		f.mu.Lock()
		f.fetchedTo = next
		f.mu.Unlock()
		rel = next
	}
	return nil
}

func (f *fakeWindowedReader) fetched() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fetchedTo
}

func (f *fakeWindowedReader) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestVerificationPrefetch_StopsAtTotal(t *testing.T) {
	u := &Usenet{maxConnections: 15, verificationConnections: 32}
	r := &fakeWindowedReader{}
	const base, total = int64(1 << 20), int64(20 << 20)

	m := &meteredReader{}
	m.consumed.Store(total) // read already finished: fill to total and stop

	done := make(chan struct{})
	go func() { defer close(done); u.verificationPrefetch(context.Background(), r, base, total, m) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("verificationPrefetch did not return")
	}

	if got := r.fetched(); got != total {
		t.Fatalf("fetched to %d, want %d (no overshoot, no shortfall)", got, total)
	}
}

func TestVerificationPrefetch_RampHoldsUntilConsumed(t *testing.T) {
	u := &Usenet{maxConnections: 15, verificationConnections: 32}
	r := &fakeWindowedReader{}
	const base, total = int64(0), int64(1 << 30)

	m := &meteredReader{} // consumed stays 0 - below the ramp

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); u.verificationPrefetch(ctx, r, base, total, m) }()

	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	if got := r.fetched(); got != 0 {
		t.Fatalf("prefetched %d bytes with consumed below the ramp, want 0", got)
	}
	if r.callCount() != 1 {
		t.Fatalf("FetchRangeWindowed called %d times, want exactly 1", r.callCount())
	}
}

func TestVerificationPrefetch_StaysWithinAheadWindow(t *testing.T) {
	u := &Usenet{maxConnections: 15, verificationConnections: 32}
	r := &fakeWindowedReader{}
	const base, total = int64(0), int64(1 << 30)

	m := &meteredReader{}
	m.consumed.Store(verificationPrefetchRamp) // just past the ramp, and not advancing

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); u.verificationPrefetch(ctx, r, base, total, m) }()

	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done

	// horizon == consumed + ahead; with consumed frozen at the ramp the fetch
	// must settle around that and never run away toward the 1 GiB total.
	want := int64(verificationPrefetchRamp + verificationPrefetchAhead)
	if got := r.fetched(); got > want+fakeSegSize {
		t.Fatalf("fetched to %d, expected it to stop near the ahead window (~%d)", got, want)
	}
}

func TestVerificationPrefetch_ExitsOnCancelWhileFetching(t *testing.T) {
	u := &Usenet{maxConnections: 15, verificationConnections: 32}
	r := &fakeWindowedReader{block: make(chan struct{})}
	const base, total = int64(0), int64(1 << 30)

	m := &meteredReader{}
	m.consumed.Store(verificationPrefetchAhead)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); u.verificationPrefetch(ctx, r, base, total, m) }()

	time.Sleep(100 * time.Millisecond) // let it block inside a "segment fetch"
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("verificationPrefetch did not exit after ctx cancel during a fetch")
	}
}
