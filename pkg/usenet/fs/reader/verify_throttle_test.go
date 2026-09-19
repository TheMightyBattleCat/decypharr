package reader

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
)

func never() bool { return false }

// throttleTestReader is a reader on a stall board with a short poll, plus a
// function that marks a stall on another reader of the board.
func throttleTestReader(t *testing.T, yieldFor time.Duration) (*StreamingReader, func()) {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	b := newStallBoard(yieldFor, 10*time.Millisecond)
	sr := &StreamingReader{stalls: b, ctx: context.Background(), logger: zerolog.Nop()}
	other := &StreamingReader{stalls: b}
	return sr, func() { other.NotePlaybackWait(playbackStallAfter, time.Now()) }
}

func yieldingCtx() context.Context {
	return ContextForYieldingVerification(ContextWithoutPadding(context.Background()))
}

// A yielding verification read (the repair sweep's) runs verifyStallWorkers
// wide while another file's playback stalls: a worker below that claims at
// once, one at or above it waits until the stall ages out of the window.
func TestWaitVerifyThrottleNarrowsDuringStall(t *testing.T) {
	sr, stall := throttleTestReader(t, 300*time.Millisecond)
	stall()
	var since atomic.Int64

	start := time.Now()
	if !sr.waitVerifyThrottle(yieldingCtx(), verifyStallWorkers-1, 32, &since, never) || time.Since(start) > 50*time.Millisecond {
		t.Fatal("a worker below verifyStallWorkers waited during a stall")
	}
	start = time.Now()
	if !sr.waitVerifyThrottle(yieldingCtx(), verifyStallWorkers, 32, &since, never) {
		t.Fatal("held worker gave up")
	}
	if waited := time.Since(start); waited < 200*time.Millisecond {
		t.Fatalf("worker %d waited %v during a stall, want until the 300 ms window passed", verifyStallWorkers, waited)
	}
	if since.Load() != 0 {
		t.Error("throttle start not cleared once the stall aged out")
	}
}

// An import's check (a verification read without the yield mark), and a
// yielding read with no stall anywhere, keep every worker.
func TestWaitVerifyThrottleOnlyYieldingReadsDuringStalls(t *testing.T) {
	sr, stall := throttleTestReader(t, time.Minute)
	var since atomic.Int64

	if !sr.waitVerifyThrottle(yieldingCtx(), 20, 32, &since, never) {
		t.Fatal("no stall anywhere, yet the worker was held")
	}
	stall()
	done := make(chan bool, 1)
	go func() {
		done <- sr.waitVerifyThrottle(ContextWithoutPadding(context.Background()), 20, 32, &since, never)
	}()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("unmarked verification read returned false")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("an import's verification read was throttled")
	}
}

// Cancelling the read releases a held worker, and so does running out of
// segments to claim.
func TestWaitVerifyThrottleReleases(t *testing.T) {
	sr, stall := throttleTestReader(t, time.Minute)
	stall()
	var since atomic.Int64

	ctx, cancel := context.WithCancel(yieldingCtx())
	done := make(chan bool, 1)
	go func() { done <- sr.waitVerifyThrottle(ctx, 10, 32, &since, never) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("cancelled wait returned true")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled wait did not return")
	}

	var out atomic.Bool
	go func() { done <- sr.waitVerifyThrottle(yieldingCtx(), 10, 32, &since, out.Load) }()
	time.Sleep(50 * time.Millisecond)
	out.Store(true)
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("exhausted wait returned false")
		}
	case <-time.After(time.Second):
		t.Fatal("held worker kept waiting with nothing left to claim")
	}
}

// End to end: a 32-wide windowed prefetch under a stall that never clears
// drains its range on the verifyStallWorkers workers and returns.
func TestFetchRangeWindowedNarrowedStillDrains(t *testing.T) {
	const segBytes = 1 << 20
	sr := newWindowedTestReader(t, 40, segBytes)
	defer sr.Close()
	b := newStallBoard(time.Minute, 10*time.Millisecond)
	sr.stalls = b
	(&StreamingReader{stalls: b}).NotePlaybackWait(playbackStallAfter, time.Now())

	done := make(chan error, 1)
	go func() {
		done <- sr.FetchRangeWindowed(yieldingCtx(), 0, 40*segBytes, 32, func() int64 { return 40 * segBytes })
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("FetchRangeWindowed returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("narrowed prefetch never returned under a standing stall")
	}
}
