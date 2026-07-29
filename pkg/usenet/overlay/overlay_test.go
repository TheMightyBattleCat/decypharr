package overlay

import (
	"sort"
	"testing"

	"github.com/rs/zerolog"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s
}

func TestIsVideoContainer(t *testing.T) {
	cases := map[string]bool{
		"Movie.2024.mkv":  true,
		"Movie.2024.MP4":  true,
		"show.s01e01.avi": true,
		"video.ts":        true,
		"video.m2ts":      true,
		"video.mov":       true,
		"video.wmv":       true,
		"readme.nfo":      false,
		"archive.rar":     false,
		"noext":           false,
	}
	for name, want := range cases {
		if got := IsVideoContainer(name); got != want {
			t.Errorf("IsVideoContainer(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestDecidePadsWithinCapsAndPersists(t *testing.T) {
	s := newTestStore(t)
	const nzbID, file = "nzb-1", "movie.mkv"
	const fileSize = int64(1_000_000_000) // 1GB, so byte-ratio cap is not the limiter here

	decision, verdict := s.Decide(nzbID, file, 10, "<msg10>", 700_000, fileSize, 0)
	if decision != DecisionPad {
		t.Fatalf("Decide = %v, want DecisionPad", decision)
	}
	if verdict != VerdictDegraded {
		t.Fatalf("verdict = %v, want VerdictDegraded", verdict)
	}

	// Re-deciding the same already-padded segment should return the same
	// decision without erroring, and without needing to re-run the caps math.
	decision2, verdict2 := s.Decide(nzbID, file, 10, "<msg10>", 700_000, fileSize, 0)
	if decision2 != DecisionPad || verdict2 != VerdictDegraded {
		t.Fatalf("re-decide = (%v, %v), want (DecisionPad, VerdictDegraded)", decision2, verdict2)
	}

	if got := s.Verdict(nzbID, file); got != VerdictDegraded {
		t.Fatalf("Store.Verdict = %v, want VerdictDegraded", got)
	}
}

func TestDecideFailsNonVideoContainer(t *testing.T) {
	s := newTestStore(t)
	decision, verdict := s.Decide("nzb-1", "release.nfo", 0, "<msg0>", 1000, 1_000_000, 0)
	if decision != DecisionFail {
		t.Fatalf("Decide = %v, want DecisionFail for non-video-container file", decision)
	}
	if verdict != VerdictFailed {
		t.Fatalf("verdict = %v, want VerdictFailed", verdict)
	}
}

func TestDecideFailsBeyondRunCap(t *testing.T) {
	s := newTestStore(t)
	const nzbID, file = "nzb-1", "movie.mkv"
	const fileSize = int64(1_000_000_000)

	// DefaultPolicy().MaxRunSegments consecutive dead segments should still pad...
	maxRun := DefaultPolicy().MaxRunSegments
	for i := 0; i < maxRun; i++ {
		decision, _ := s.Decide(nzbID, file, i, "<msg>", 1000, fileSize, 0)
		if decision != DecisionPad {
			t.Fatalf("segment %d: Decide = %v, want DecisionPad", i, decision)
		}
	}

	// ...but one more, extending the same contiguous run, should fail.
	decision, verdict := s.Decide(nzbID, file, maxRun, "<msg>", 1000, fileSize, 0)
	if decision != DecisionFail {
		t.Fatalf("Decide = %v, want DecisionFail once the run exceeds the cap", decision)
	}
	if verdict != VerdictFailed {
		t.Fatalf("verdict = %v, want VerdictFailed", verdict)
	}

	// The verdict is sticky: even a fresh, non-contiguous segment on this
	// file must fail now.
	decision, verdict = s.Decide(nzbID, file, 1000, "<msg>", 1000, fileSize, 0)
	if decision != DecisionFail || verdict != VerdictFailed {
		t.Fatalf("Decide after failed verdict = (%v, %v), want (DecisionFail, VerdictFailed)", decision, verdict)
	}
}

func TestDecideFailsBeyondByteRatioCap(t *testing.T) {
	s := newTestStore(t)
	const nzbID, file = "nzb-1", "movie.mkv"
	const fileSize = int64(1000)

	// 2% of 1000 bytes is 20 bytes; a single 21-byte dead segment already
	// exceeds the ratio cap.
	decision, verdict := s.Decide(nzbID, file, 0, "<msg>", 21, fileSize, 0)
	if decision != DecisionFail {
		t.Fatalf("Decide = %v, want DecisionFail once pad bytes exceed the ratio cap", decision)
	}
	if verdict != VerdictFailed {
		t.Fatalf("verdict = %v, want VerdictFailed", verdict)
	}
}

func TestWritePatchAndPatchBytesRoundtrip(t *testing.T) {
	s := newTestStore(t)
	const nzbID, file = "nzb-1", "movie.mkv"

	if _, ok := s.PatchBytes(nzbID, file, 5); ok {
		t.Fatalf("PatchBytes found a patch before one was written")
	}

	want := []byte("recovered-bytes")
	if err := s.WritePatch(nzbID, file, 5, want); err != nil {
		t.Fatalf("WritePatch: %v", err)
	}

	got, ok := s.PatchBytes(nzbID, file, 5)
	if !ok {
		t.Fatalf("PatchBytes did not find the written patch")
	}
	if string(got) != string(want) {
		t.Fatalf("PatchBytes = %q, want %q", got, want)
	}

	if got := s.Verdict(nzbID, file); got != VerdictClean {
		t.Fatalf("Verdict after the only dead segment is patched = %v, want VerdictClean", got)
	}
}

func TestWritePatchClearsDegradedVerdictButNotFailed(t *testing.T) {
	s := newTestStore(t)
	const nzbID, file = "nzb-1", "movie.mkv"

	if _, verdict := s.Decide(nzbID, file, 0, "<msg>", 1000, 1_000_000_000, 0); verdict != VerdictDegraded {
		t.Fatalf("setup: verdict = %v, want VerdictDegraded", verdict)
	}
	if err := s.WritePatch(nzbID, file, 0, []byte("fixed")); err != nil {
		t.Fatalf("WritePatch: %v", err)
	}
	if got := s.Verdict(nzbID, file); got != VerdictClean {
		t.Fatalf("Verdict after patching the only dead segment = %v, want VerdictClean", got)
	}

	// Once a file has failed, patching a segment must not resurrect it back
	// to degraded/clean - the legacy repair path may already be in flight.
	s2 := newTestStore(t)
	s2.Decide(nzbID, "release.nfo", 0, "<msg>", 1000, 1_000_000, 0) // non-video -> immediate fail
	if err := s2.WritePatch(nzbID, "release.nfo", 0, []byte("fixed")); err != nil {
		t.Fatalf("WritePatch: %v", err)
	}
	if got := s2.Verdict(nzbID, "release.nfo"); got != VerdictFailed {
		t.Fatalf("Verdict after patching a segment on an already-failed file = %v, want VerdictFailed (sticky)", got)
	}
}

func TestRecordDeadIsIdempotent(t *testing.T) {
	s := newTestStore(t)
	const nzbID, file = "nzb-1", "movie.mkv"

	if err := s.RecordDead(nzbID, file, 3, "<msg3>", 500); err != nil {
		t.Fatalf("RecordDead: %v", err)
	}
	if err := s.RecordDead(nzbID, file, 3, "<msg3-different>", 999); err != nil {
		t.Fatalf("RecordDead (second call): %v", err)
	}

	m, err := s.loadManifestLocked(nzbID)
	if err != nil {
		t.Fatalf("loadManifestLocked: %v", err)
	}
	fe := m.Files[file]
	if fe == nil || len(fe.DeadSegments) != 1 {
		t.Fatalf("expected exactly one dead segment recorded, got %+v", fe)
	}
	if fe.DeadSegments[0].MessageID != "<msg3>" {
		t.Fatalf("RecordDead overwrote an existing record; MessageID = %q, want %q", fe.DeadSegments[0].MessageID, "<msg3>")
	}
}

func TestRecordDeadUpdatesVerdictImmediately(t *testing.T) {
	s := newTestStore(t)
	const nzbID, file = "nzb-1", "movie.mkv"

	// Before any damage is recorded, the file reads as clean.
	if got := s.Verdict(nzbID, file); got != VerdictClean {
		t.Fatalf("Verdict before any RecordDead = %v, want VerdictClean", got)
	}

	// RecordDead is used by the background sweep and the import-availability
	// gate, neither of which goes through Decide - so it must recompute the
	// verdict itself instead of leaving a stale VerdictClean sitting next to
	// a non-empty DeadSegments list.
	if err := s.RecordDead(nzbID, file, 3, "<msg3>", 500); err != nil {
		t.Fatalf("RecordDead: %v", err)
	}
	if got := s.Verdict(nzbID, file); got == VerdictClean {
		t.Fatalf("Verdict after RecordDead = %v, want non-clean (verdict must reflect the recorded dead segment)", got)
	}
	if got := s.Verdict(nzbID, file); got != VerdictDegraded {
		t.Fatalf("Verdict after a single RecordDead = %v, want VerdictDegraded", got)
	}
}

func TestRecordDeadDoesNotUnfailAStickyFailedVerdict(t *testing.T) {
	s := newTestStore(t)
	const nzbID, file = "nzb-1", "release.nfo" // non-video container -> Decide fails it immediately

	if _, verdict := s.Decide(nzbID, file, 0, "<msg0>", 1000, 1_000_000, 0); verdict != VerdictFailed {
		t.Fatalf("setup: verdict = %v, want VerdictFailed", verdict)
	}

	if err := s.RecordDead(nzbID, file, 1, "<msg1>", 500); err != nil {
		t.Fatalf("RecordDead: %v", err)
	}
	if got := s.Verdict(nzbID, file); got != VerdictFailed {
		t.Fatalf("Verdict after RecordDead on an already-failed file = %v, want VerdictFailed (sticky)", got)
	}
}

func TestDeleteEntryRemovesManifestAndPatches(t *testing.T) {
	s := newTestStore(t)
	const nzbID, file = "nzb-1", "movie.mkv"

	if err := s.WritePatch(nzbID, file, 0, []byte("data")); err != nil {
		t.Fatalf("WritePatch: %v", err)
	}
	if err := s.DeleteEntry(nzbID); err != nil {
		t.Fatalf("DeleteEntry: %v", err)
	}
	if got := s.Verdict(nzbID, file); got != VerdictClean {
		t.Fatalf("Verdict after DeleteEntry = %v, want VerdictClean (nothing recorded)", got)
	}
	if _, ok := s.PatchBytes(nzbID, file, 0); ok {
		t.Fatalf("PatchBytes still found a patch after DeleteEntry")
	}
}

func TestHandleNilSafety(t *testing.T) {
	var s *Store
	h := s.Handle("nzb-1")
	if h != nil {
		t.Fatalf("Handle on a nil Store should be nil")
	}
	// Every Handle method must tolerate a nil receiver without panicking.
	if _, ok := h.PatchBytes("f", 0); ok {
		t.Fatalf("nil Handle.PatchBytes should report no patch")
	}
	if decision, verdict := h.Decide("f", 0, "<m>", 10, 100, 0); decision != DecisionFail || verdict != VerdictFailed {
		t.Fatalf("nil Handle.Decide = (%v, %v), want (DecisionFail, VerdictFailed)", decision, verdict)
	}
	if h.ShouldLogPad("f", 0) {
		t.Fatalf("nil Handle.ShouldLogPad should be false")
	}
	h.EnqueueRepair() // must not panic
	if err := h.RecordDead("f", 0, "<m>", 10); err == nil {
		t.Fatalf("nil Handle.RecordDead should return an error, not silently succeed")
	}
}

func TestShouldLogPadOncePerProcess(t *testing.T) {
	s := newTestStore(t)
	if !s.ShouldLogPad("nzb-1", "movie.mkv", 0) {
		t.Fatalf("first ShouldLogPad call should be true")
	}
	if s.ShouldLogPad("nzb-1", "movie.mkv", 0) {
		t.Fatalf("second ShouldLogPad call for the same (entry, file, segment) should be false")
	}
	if !s.ShouldLogPad("nzb-1", "movie.mkv", 1) {
		t.Fatalf("ShouldLogPad for a different segment should be true")
	}
}

// TestScreenProjectionMatchesRealDecide is the anti-drift guard for the
// read-only damage screen (usenet.OverlayScreenFile): it must project the
// exact same verdict WithinPadCaps would produce if Decide actually processed
// the same segments for real. Both call WithinPadCaps directly, so this
// mainly guards against a future edit to Decide (or the screen) that stops
// routing through it - if that ever happens, this test starts failing.
func TestScreenProjectionMatchesRealDecide(t *testing.T) {
	const file = "movie.mkv"
	policy := DefaultPolicy()

	cases := []struct {
		name string
		// already holds the segments already recorded (as Decide would leave
		// them); newlyMissing holds the screen's full-STAT discoveries not
		// yet recorded anywhere.
		already      []DeadSegment
		newlyMissing []DeadSegment
		fileSize     int64
		wantVerdict  Verdict
	}{
		{
			name:         "stays within caps",
			already:      []DeadSegment{{Index: 0, Bytes: 100, Status: StatusDead}, {Index: 1, Bytes: 100, Status: StatusDead}},
			newlyMissing: []DeadSegment{{Index: 5, Bytes: 100, Status: StatusDead}, {Index: 6, Bytes: 100, Status: StatusDead}},
			fileSize:     10_000_000,
			wantVerdict:  VerdictDegraded,
		},
		{
			name:         "run cap exceeded by newly-discovered segments",
			already:      []DeadSegment{{Index: 10, Bytes: 100, Status: StatusDead}},
			newlyMissing: []DeadSegment{{Index: 11, Bytes: 100, Status: StatusDead}, {Index: 12, Bytes: 100, Status: StatusDead}, {Index: 13, Bytes: 100, Status: StatusDead}, {Index: 14, Bytes: 100, Status: StatusDead}},
			fileSize:     10_000_000,
			wantVerdict:  VerdictFailed,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The screen's projection: a hypothetical FileEntry combining
			// already-recorded segments with the newly-discovered ones,
			// evaluated WITHOUT persisting anything.
			hypothetical := &FileEntry{DeadSegments: append(append([]DeadSegment{}, tc.already...), tc.newlyMissing...)}
			_, projectedOK := WithinPadCaps(hypothetical, tc.fileSize, policy, 0)
			projectedVerdict := VerdictFailed
			if projectedOK {
				projectedVerdict = VerdictDegraded
			}
			if projectedVerdict != tc.wantVerdict {
				t.Fatalf("projected verdict = %v, want %v", projectedVerdict, tc.wantVerdict)
			}

			// The real path: feed the exact same final segment set through
			// an actual Store.Decide sequence, in index order, exactly as
			// live playback failures would arrive - one Decide call per
			// segment, already-recorded ones first.
			s := newTestStore(t)
			s.SetPolicy(policy)
			all := append(append([]DeadSegment{}, tc.already...), tc.newlyMissing...)
			sort.Slice(all, func(i, j int) bool { return all[i].Index < all[j].Index })
			var gotVerdict Verdict
			for _, seg := range all {
				_, gotVerdict = s.Decide("nzb-1", file, seg.Index, "<msg>", seg.Bytes, tc.fileSize, 0)
			}
			if gotVerdict != tc.wantVerdict {
				t.Fatalf("real Decide sequence verdict = %v, want %v", gotVerdict, tc.wantVerdict)
			}
		})
	}
}

func TestDecideFailsHeaderRegionDamage(t *testing.T) {
	s := newTestStore(t)
	const nzbID, file = "nzb-1", "movie.mkv"

	// Segment 10 sits inside the first 1% of a 5000-segment file (indices
	// 0-49), so it must fail outright even though every other cap is
	// nowhere close to being hit.
	decision, verdict := s.Decide(nzbID, file, 10, "<m>", 1000, 1_000_000_000, 5000)
	if decision != DecisionFail {
		t.Fatalf("Decide = %v, want DecisionFail for a dead segment in the header region", decision)
	}
	if verdict != VerdictFailed {
		t.Fatalf("verdict = %v, want VerdictFailed", verdict)
	}
}

func TestDecidePadsJustPastHeaderRegion(t *testing.T) {
	s := newTestStore(t)
	const nzbID, file = "nzb-1", "movie.mkv"

	// Segment 50 is the first index outside the header region (0-49) of a
	// 5000-segment file, so it pads normally.
	decision, verdict := s.Decide(nzbID, file, 50, "<m>", 1000, 1_000_000_000, 5000)
	if decision != DecisionPad {
		t.Fatalf("Decide = %v, want DecisionPad just past the header region", decision)
	}
	if verdict != VerdictDegraded {
		t.Fatalf("verdict = %v, want VerdictDegraded", verdict)
	}
}

func TestDecideUnknownTotalSegmentsSkipsHeaderCheck(t *testing.T) {
	s := newTestStore(t)
	const nzbID, file = "nzb-1", "movie.mkv"

	// totalSegments = 0 means "unknown" - the header-region check must be
	// disabled rather than treating index 0 as always in-region.
	decision, verdict := s.Decide(nzbID, file, 3, "<m>", 1000, 1_000_000_000, 0)
	if decision != DecisionPad {
		t.Fatalf("Decide = %v, want DecisionPad when totalSegments is unknown", decision)
	}
	if verdict != VerdictDegraded {
		t.Fatalf("verdict = %v, want VerdictDegraded", verdict)
	}
}

func TestDecideHeaderDamageSelfHealsPreviouslyPadded(t *testing.T) {
	s := newTestStore(t)
	const nzbID, file = "nzb-1", "movie.mkv"

	// First decided without knowing totalSegments - pads.
	decision, verdict := s.Decide(nzbID, file, 3, "<m>", 1000, 1_000_000_000, 0)
	if decision != DecisionPad || verdict != VerdictDegraded {
		t.Fatalf("setup Decide = (%v, %v), want (DecisionPad, VerdictDegraded)", decision, verdict)
	}

	// Replayed later with totalSegments known: the same segment now falls
	// in the header region, so it must fail instead of honoring the earlier
	// pad decision - proving a previously-padded segment re-evaluates
	// through the caps/positional check rather than short-circuiting.
	decision, verdict = s.Decide(nzbID, file, 3, "<m>", 1000, 1_000_000_000, 5000)
	if decision != DecisionFail {
		t.Fatalf("Decide = %v, want DecisionFail once header damage is detected on replay", decision)
	}
	if verdict != VerdictFailed {
		t.Fatalf("verdict = %v, want VerdictFailed", verdict)
	}
}
