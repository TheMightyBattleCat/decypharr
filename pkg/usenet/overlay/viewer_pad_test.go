package overlay

import "testing"

// Past the configured caps, a viewer's read keeps padding (so they keep
// watching while the repair runs), the file is still recorded failed (so
// repair policy re-grabs it), and non-viewer reads still fail.
func TestDecideForViewerPadsPastCaps(t *testing.T) {
	s := newTestStore(t)
	s.SetPolicy(Policy{MaxRunSegments: 4, MaxTotalSegments: 2, MaxByteRatio: 1})
	var notified int
	s.SetFailedNotifier(func(string, string) { notified++ })

	const size, total = 1 << 30, 1000
	for _, seg := range []int{100, 200} {
		if d, v := s.DecideForViewer("n", "m.mkv", seg, "", 1000, size, total); d != DecisionPad || v != VerdictDegraded {
			t.Fatalf("seg %d within caps: %v/%v, want pad/degraded", seg, d, v)
		}
	}
	// Third dead segment: past MaxTotalSegments.
	if d, v := s.DecideForViewer("n", "m.mkv", 300, "", 1000, size, total); d != DecisionPad || v != VerdictFailed {
		t.Fatalf("viewer past caps: %v/%v, want pad/failed", d, v)
	}
	if d, v := s.DecideForViewer("n", "m.mkv", 400, "", 1000, size, total); d != DecisionPad || v != VerdictFailed {
		t.Fatalf("viewer on a failed file: %v/%v, want pad/failed", d, v)
	}
	if notified != 1 {
		t.Fatalf("failed notifier fired %d times, want once (on the transition)", notified)
	}
	if d, _ := s.Decide("n", "m.mkv", 500, "", 1000, size, total); d != DecisionFail {
		t.Fatalf("non-viewer read on a failed file: %v, want fail", d)
	}
}

func TestDecideForViewerStopsAtCeilings(t *testing.T) {
	s := newTestStore(t)
	s.SetPolicy(Policy{MaxRunSegments: 4, MaxTotalSegments: 1, MaxByteRatio: 0.01})
	const total = 1000

	// A tenth of the file padded is the viewer ceiling.
	const size = 100_000
	if d, _ := s.DecideForViewer("n", "m.mkv", 100, "", 9_000, size, total); d != DecisionPad {
		t.Fatalf("9%% of the file: %v, want pad", d)
	}
	if d, _ := s.DecideForViewer("n", "m.mkv", 200, "", 2_000, size, total); d != DecisionFail {
		t.Fatalf("11%% of the file: %v, want fail", d)
	}

	// Header-region damage still fails for a viewer.
	s2 := newTestStore(t)
	s2.SetPolicy(Policy{MaxRunSegments: 4, MaxTotalSegments: 1, MaxByteRatio: 1})
	if d, _ := s2.DecideForViewer("n", "m.mkv", 0, "", 10, 1<<30, total); d != DecisionFail {
		t.Fatalf("header damage for a viewer: %v, want fail", d)
	}
}
