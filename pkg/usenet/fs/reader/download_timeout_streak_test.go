package reader

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/sirrobot01/decypharr/internal/nntp"
)

// "The fetch ran out of time" reaches doFetch in two different shapes, and the
// first cut of the escalation matched only one of them - which is why it never
// fired once in production. Both must be recognised, and a caller that simply
// walked away must NOT be.
func TestFetchTimeoutClassification(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		wantCount bool
	}{
		{
			name:      "DownloadTimeout expiry (article trickling under the idle deadline)",
			err:       context.DeadlineExceeded,
			wantCount: true,
		},
		{
			name:      "StreamBodyTimeout expiry (connection fully idle)",
			err:       &nntp.Error{Type: nntp.ErrorTypeTimeout, Message: "read timeout"},
			wantCount: true,
		},
		{
			name:      "wrapped nntp timeout still counts",
			err:       fmt.Errorf("fetch segment: %w", &nntp.Error{Type: nntp.ErrorTypeTimeout}),
			wantCount: true,
		},
		{
			name:      "article-not-found is the 430 path, not a timeout",
			err:       &nntp.Error{Type: nntp.ErrorTypeArticleNotFound},
			wantCount: false,
		},
		{
			name:      "caller cancellation says nothing about the article",
			err:       context.Canceled,
			wantCount: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Mirrors the guard in doFetch.
			got := errors.Is(tc.err, context.DeadlineExceeded) || nntp.IsTimeoutError(tc.err)
			if got != tc.wantCount {
				t.Fatalf("counts-as-timeout = %v, want %v (err %T: %v)", got, tc.wantCount, tc.err, tc.err)
			}
		})
	}
}

// The streak counter is what decides whether a segment that keeps exhausting
// DownloadTimeout is escalated to the confirmed-missing path (pad + queue a
// repair) or retried again. Two properties matter, and getting either wrong
// re-introduces a bug:
//
//   - It must reach maxDownloadTimeoutStreak on CONSECUTIVE timeouts, so a
//     genuinely unfetchable article stops being retried forever (the Grant
//     S04E10 segment 139 case: a 55-minute loop that never reached repair).
//   - It must reset on any success, so a merely slow segment - one that times
//     out once or twice and then lands - is never marked permanently dead.
//     A live read at 150MB in that same file took 90.7s and then succeeded;
//     that segment must never be escalated.
func TestDownloadTimeoutStreak(t *testing.T) {
	newFetcher := func() *SegmentFetcher {
		return &SegmentFetcher{timeoutStreak: make(map[int]int)}
	}

	t.Run("consecutive timeouts reach the escalation threshold", func(t *testing.T) {
		sf := newFetcher()
		var last int
		for i := 1; i <= maxDownloadTimeoutStreak; i++ {
			last = sf.noteDownloadTimeout(7)
			if last != i {
				t.Fatalf("noteDownloadTimeout call %d = %d, want %d", i, last, i)
			}
		}
		if last < maxDownloadTimeoutStreak {
			t.Fatalf("streak = %d, want >= %d (escalation threshold)", last, maxDownloadTimeoutStreak)
		}
	})

	t.Run("a success resets the streak", func(t *testing.T) {
		sf := newFetcher()
		for range maxDownloadTimeoutStreak - 1 {
			sf.noteDownloadTimeout(7)
		}
		// The segment finally downloads.
		sf.clearDownloadTimeout(7)

		// A later timeout must start counting from scratch, so a segment that
		// intermittently succeeds can never accumulate its way to a permanent
		// verdict.
		if got := sf.noteDownloadTimeout(7); got != 1 {
			t.Fatalf("after clear, noteDownloadTimeout = %d, want 1", got)
		}
	})

	t.Run("streaks are tracked per segment", func(t *testing.T) {
		sf := newFetcher()
		sf.noteDownloadTimeout(1)
		sf.noteDownloadTimeout(1)
		if got := sf.noteDownloadTimeout(2); got != 1 {
			t.Fatalf("segment 2 streak = %d, want 1 (must not inherit segment 1's)", got)
		}
		// Clearing one segment must not disturb another's count.
		sf.clearDownloadTimeout(2)
		if got := sf.noteDownloadTimeout(1); got != 3 {
			t.Fatalf("segment 1 streak = %d, want 3 after clearing segment 2", got)
		}
	})

	t.Run("clearing an unknown segment is a no-op", func(t *testing.T) {
		sf := newFetcher()
		sf.clearDownloadTimeout(99) // must not panic or create an entry
		if got := sf.noteDownloadTimeout(99); got != 1 {
			t.Fatalf("noteDownloadTimeout = %d, want 1", got)
		}
	})
}
