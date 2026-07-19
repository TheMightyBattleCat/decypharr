package overlay

import (
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

	decision, verdict := s.Decide(nzbID, file, 10, "<msg10>", 700_000, fileSize)
	if decision != DecisionPad {
		t.Fatalf("Decide = %v, want DecisionPad", decision)
	}
	if verdict != VerdictDegraded {
		t.Fatalf("verdict = %v, want VerdictDegraded", verdict)
	}

	// Re-deciding the same already-padded segment should return the same
	// decision without erroring, and without needing to re-run the caps math.
	decision2, verdict2 := s.Decide(nzbID, file, 10, "<msg10>", 700_000, fileSize)
	if decision2 != DecisionPad || verdict2 != VerdictDegraded {
		t.Fatalf("re-decide = (%v, %v), want (DecisionPad, VerdictDegraded)", decision2, verdict2)
	}

	if got := s.Verdict(nzbID, file); got != VerdictDegraded {
		t.Fatalf("Store.Verdict = %v, want VerdictDegraded", got)
	}
}

func TestDecideFailsNonVideoContainer(t *testing.T) {
	s := newTestStore(t)
	decision, verdict := s.Decide("nzb-1", "release.nfo", 0, "<msg0>", 1000, 1_000_000)
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

	// maxPadRunSegments consecutive dead segments should still pad...
	for i := 0; i < maxPadRunSegments; i++ {
		decision, _ := s.Decide(nzbID, file, i, "<msg>", 1000, fileSize)
		if decision != DecisionPad {
			t.Fatalf("segment %d: Decide = %v, want DecisionPad", i, decision)
		}
	}

	// ...but one more, extending the same contiguous run, should fail.
	decision, verdict := s.Decide(nzbID, file, maxPadRunSegments, "<msg>", 1000, fileSize)
	if decision != DecisionFail {
		t.Fatalf("Decide = %v, want DecisionFail once the run exceeds the cap", decision)
	}
	if verdict != VerdictFailed {
		t.Fatalf("verdict = %v, want VerdictFailed", verdict)
	}

	// The verdict is sticky: even a fresh, non-contiguous segment on this
	// file must fail now.
	decision, verdict = s.Decide(nzbID, file, 1000, "<msg>", 1000, fileSize)
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
	decision, verdict := s.Decide(nzbID, file, 0, "<msg>", 21, fileSize)
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

	if _, verdict := s.Decide(nzbID, file, 0, "<msg>", 1000, 1_000_000_000); verdict != VerdictDegraded {
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
	s2.Decide(nzbID, "release.nfo", 0, "<msg>", 1000, 1_000_000) // non-video -> immediate fail
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
	if decision, verdict := h.Decide("f", 0, "<m>", 10, 100); decision != DecisionFail || verdict != VerdictFailed {
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
