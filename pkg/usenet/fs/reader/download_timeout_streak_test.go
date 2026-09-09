package reader

import (
	"testing"
)

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
