package reader

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/nntp"
)

// Playback keeps escalating a segment that only ever times out into the
// confirmed-missing path (so it is padded and repaired), but a verification
// read never turns timeouts into a 430: that would trip its DeadSegmentSignal
// and mark the posting dead with no provider having said so.
func TestEscalateTimeoutStreak(t *testing.T) {
	newFetcher := func() *SegmentFetcher {
		return &SegmentFetcher{timeoutStreak: make(map[int]int), logger: zerolog.Nop(), ctx: context.Background()}
	}
	timeoutErr := &nntp.Error{Type: nntp.ErrorTypeTimeout, Message: "i/o timeout"}

	t.Run("playback escalates on the third timeout", func(t *testing.T) {
		sf := newFetcher()
		ctx := ContextForPlayback(context.Background())
		for i := 1; i < maxDownloadTimeoutStreak; i++ {
			if got := sf.escalateTimeoutStreak(ctx, 7, timeoutErr, time.Minute); got != error(timeoutErr) {
				t.Fatalf("timeout %d escalated early: %v", i, got)
			}
		}
		if got := sf.escalateTimeoutStreak(ctx, 7, context.DeadlineExceeded, time.Minute); !nntp.IsArticleNotFoundError(got) {
			t.Fatalf("third timeout = %v, want the confirmed-missing error", got)
		}
	})

	t.Run("verification read keeps the timeout", func(t *testing.T) {
		sf := newFetcher()
		vctx := ContextWithoutPadding(context.Background())
		for i := 1; i <= maxDownloadTimeoutStreak+2; i++ {
			if got := sf.escalateTimeoutStreak(vctx, 7, timeoutErr, time.Minute); nntp.IsArticleNotFoundError(got) {
				t.Fatalf("verification timeout %d became a 430: %v", i, got)
			}
		}
		// The streak still counted: the next playback read escalates.
		if got := sf.escalateTimeoutStreak(ContextForPlayback(context.Background()), 7, timeoutErr, time.Minute); !nntp.IsArticleNotFoundError(got) {
			t.Fatalf("playback after verification timeouts = %v, want escalation", got)
		}
	})

	t.Run("a caller that walked away is not counted", func(t *testing.T) {
		sf := newFetcher()
		gone, cancel := context.WithCancel(context.Background())
		cancel()
		for i := 0; i < maxDownloadTimeoutStreak+1; i++ {
			sf.escalateTimeoutStreak(gone, 7, context.DeadlineExceeded, time.Minute)
		}
		if n := sf.noteDownloadTimeout(7); n != 1 {
			t.Fatalf("streak after cancelled callers = %d, want nothing counted", n-1)
		}
	})

	t.Run("other errors pass through", func(t *testing.T) {
		sf := newFetcher()
		other := &nntp.Error{Type: nntp.ErrorTypeConnection, Message: "reset"}
		if got := sf.escalateTimeoutStreak(context.Background(), 7, other, time.Minute); got != error(other) {
			t.Fatalf("got %v", got)
		}
	})
}
