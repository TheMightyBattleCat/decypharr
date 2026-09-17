package reader

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// yieldTestReaders builds two readers over already-cached segments sharing a
// stall board with a short yield window, so FetchRange on either does nothing
// but its dispatch and yield logic.
func yieldTestReaders(t *testing.T, yieldFor time.Duration) (burst, played *StreamingReader, board *stallBoard) {
	t.Helper()
	board = newStallBoard(yieldFor, 10*time.Millisecond)
	burst = newWindowedTestReader(t, 20, 1<<16)
	played = newWindowedTestReader(t, 20, 1<<16)
	burst.stalls = board
	played.stalls = board
	t.Cleanup(func() {
		_ = burst.Close()
		_ = played.Close()
	})
	return burst, played, board
}

// fetchRangeTook runs FetchRange over the whole burst reader and reports how
// long it took, failing if it doesn't finish within limit.
func fetchRangeTook(t *testing.T, ctx context.Context, sr *StreamingReader, limit time.Duration) (time.Duration, error) {
	t.Helper()
	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- sr.FetchRange(ctx, 0, sr.Size(), 4) }()
	select {
	case err := <-done:
		return time.Since(start), err
	case <-time.After(limit):
		t.Fatalf("FetchRange still running after %v", limit)
		return 0, nil
	}
}

func TestStallBoardStalledOther(t *testing.T) {
	b := newStallBoard(time.Minute, time.Second)
	a, other := &StreamingReader{}, &StreamingReader{}
	now := time.Now()

	if b.stalledOther(a, now) {
		t.Fatal("stall reported on an empty board")
	}
	b.note(a, now)
	if b.stalledOther(a, now) {
		t.Error("a reader's own stall made it yield")
	}
	if !b.stalledOther(other, now.Add(30*time.Second)) {
		t.Error("another reader's stall 30s ago (window 1m) not reported")
	}
	if b.stalledOther(other, now.Add(time.Minute)) {
		t.Error("a stall a full window old still reported")
	}

	// A look inside the window of a newer stall drops the expired one.
	b.note(other, now.Add(50*time.Second))
	if !b.stalledOther(a, now.Add(70*time.Second)) {
		t.Error("other's stall 20s ago not reported")
	}
	b.mu.Lock()
	_, kept := b.last[a]
	n := len(b.last)
	b.mu.Unlock()
	if kept || n != 1 {
		t.Errorf("board holds %d stalls (a's expired one kept: %v), want only other's", n, kept)
	}
}

func TestNotePlaybackReadThreshold(t *testing.T) {
	b := newStallBoard(time.Minute, time.Second)
	sr := &StreamingReader{stalls: b}
	other := &StreamingReader{stalls: b}
	now := time.Now()

	sr.notePlaybackRead(playbackStallAfter-time.Millisecond, now)
	if sr.lastPlaybackRead.Load() != now.UnixNano() {
		t.Error("a quick playback read did not mark the reader as being played")
	}
	if b.stalledOther(other, now) {
		t.Error("a read under playbackStallAfter counted as a stall")
	}
	sr.notePlaybackRead(playbackStallAfter, now)
	if !b.stalledOther(other, now) {
		t.Error("a read of playbackStallAfter did not count as a stall")
	}
}

// Only a client stream's read marks the reader as being played: not a
// background read (durable persist, precache), and not a verification read
// even when it came through the stream path.
func TestReadAtMarksPlaybackOnly(t *testing.T) {
	// The test readers have no bytes on disk: past the segment check a read
	// fails and resets its segments, so each read gets a fresh reader.
	for _, tc := range []struct {
		name string
		ctx  func(context.Context) context.Context
		want bool
	}{
		{"background", func(ctx context.Context) context.Context { return ctx }, false},
		{"verification", func(ctx context.Context) context.Context { return ContextForPlayback(ContextWithoutPadding(ctx)) }, false},
		{"playback", ContextForPlayback, true},
	} {
		sr := newWindowedTestReader(t, 4, 1<<16)
		sr.stalls = newStallBoard(time.Minute, time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, _ = sr.ReadAtContext(tc.ctx(ctx), make([]byte, 4096), 0)
		cancel()
		_ = sr.Close()
		if got := sr.lastPlaybackRead.Load() != 0; got != tc.want {
			t.Errorf("%s read marked the reader as being played = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// PrecacheYieldToPlayback=false turns the pause off.
func TestFetchRangeYieldSwitchedOff(t *testing.T) {
	burst, played, board := yieldTestReaders(t, time.Minute)
	board.note(played, time.Now())
	off := false
	cfg := config.Get()
	cfg.Precache.PrecacheYieldToPlayback = &off
	defer func() { cfg.Precache.PrecacheYieldToPlayback = nil }()

	if took, err := fetchRangeTook(t, context.Background(), burst, 2*time.Second); err != nil || took > 200*time.Millisecond {
		t.Fatalf("switched off: FetchRange took %v (err %v), want no pause", took, err)
	}
}

// A burst pauses while another file's playback has stalled within the window,
// and finishes once the window passes without a new stall.
func TestFetchRangeYieldsToStalledPlaybackElsewhere(t *testing.T) {
	const window = 300 * time.Millisecond
	burst, played, board := yieldTestReaders(t, window)
	board.note(played, time.Now())

	took, err := fetchRangeTook(t, context.Background(), burst, 5*time.Second)
	if err != nil {
		t.Fatalf("FetchRange: %v", err)
	}
	if took < window-50*time.Millisecond {
		t.Fatalf("FetchRange finished in %v while another reader stalled %v ago", took, window)
	}
}

func TestFetchRangeNoYieldWithoutOtherStall(t *testing.T) {
	burst, _, board := yieldTestReaders(t, time.Minute)

	if took, err := fetchRangeTook(t, context.Background(), burst, 2*time.Second); err != nil || took > 200*time.Millisecond {
		t.Fatalf("no stalls: FetchRange took %v (err %v), want no pause", took, err)
	}

	// Its own playback stalling: read-ahead on the file being played keeps going.
	board.note(burst, time.Now())
	if took, err := fetchRangeTook(t, context.Background(), burst, 2*time.Second); err != nil || took > 200*time.Millisecond {
		t.Fatalf("own stall: FetchRange took %v (err %v), want no pause", took, err)
	}
}

// A file that is itself being played doesn't yield to another file's stall.
func TestFetchRangeBeingPlayedDoesNotYield(t *testing.T) {
	burst, played, board := yieldTestReaders(t, time.Minute)
	now := time.Now()
	board.note(played, now)
	burst.lastPlaybackRead.Store(now.UnixNano())

	if took, err := fetchRangeTook(t, context.Background(), burst, 2*time.Second); err != nil || took > 200*time.Millisecond {
		t.Fatalf("FetchRange on a file being played took %v (err %v), want no pause", took, err)
	}
}

func TestFetchRangeYieldHonoursContext(t *testing.T) {
	burst, played, board := yieldTestReaders(t, time.Minute)
	board.note(played, time.Now())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	took, err := fetchRangeTook(t, ctx, burst, 2*time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("FetchRange error = %v, want the caller's deadline", err)
	}
	if took > time.Second {
		t.Fatalf("FetchRange took %v to notice its deadline while paused", took)
	}
}
