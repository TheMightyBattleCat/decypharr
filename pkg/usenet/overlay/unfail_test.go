package overlay

import (
	"bytes"
	"reflect"
	"testing"
)

// failFile drives file past tiny caps with dead segments 0 and 2, so its
// verdict is Failed with two dead records of 1024 bytes each.
func failFile(t *testing.T, s *Store, nzbID, file string) {
	t.Helper()
	s.SetPolicy(Policy{MaxRunSegments: 1, MaxTotalSegments: 1, MaxByteRatio: 1.0})
	_, _ = s.Decide(nzbID, file, 0, "<a@test>", 1024, 1<<20, 0)
	if _, v := s.Decide(nzbID, file, 2, "<b@test>", 1024, 1<<20, 0); v != VerdictFailed {
		t.Fatalf("setup: verdict = %v, want failed", v)
	}
}

// A file PAR2 fully repaired used to stay Failed (recomputeVerdictLocked
// keeps it), which made the playback policy re-grab it.
func TestUnfailRepairedClearsFullyPatchedFile(t *testing.T) {
	s := newTestStore(t)
	const nzbID = "nzb-1"
	failFile(t, s, nzbID, "done.mkv")
	failFile(t, s, nzbID, "half.mkv")

	patch := bytes.Repeat([]byte{7}, 1024)
	for _, seg := range []int{0, 2} {
		if err := s.WritePatch(nzbID, "done.mkv", seg, patch); err != nil {
			t.Fatalf("WritePatch done.mkv/%d: %v", seg, err)
		}
	}
	if err := s.WritePatch(nzbID, "half.mkv", 0, patch); err != nil {
		t.Fatalf("WritePatch half.mkv/0: %v", err)
	}
	if v := s.Verdict(nzbID, "done.mkv"); v != VerdictFailed {
		t.Fatalf("after patching, verdict = %v, want failed (sticky through WritePatch)", v)
	}

	cleared, err := s.UnfailRepaired(nzbID)
	if err != nil {
		t.Fatalf("UnfailRepaired: %v", err)
	}
	if !reflect.DeepEqual(cleared, []string{"done.mkv"}) {
		t.Fatalf("cleared = %v, want [done.mkv]", cleared)
	}
	if v := s.Verdict(nzbID, "done.mkv"); v != VerdictClean {
		t.Fatalf("done.mkv verdict = %v, want clean", v)
	}
	if v := s.Verdict(nzbID, "half.mkv"); v != VerdictFailed {
		t.Fatalf("half.mkv verdict = %v, want failed (segment 2 still dead)", v)
	}
	if got := s.PatchedSegments(nzbID, "done.mkv"); !reflect.DeepEqual(got, []int{0, 2}) {
		t.Fatalf("patched segments = %v, want [0 2] (patches must survive)", got)
	}
	if data, ok := s.PatchBytes(nzbID, "done.mkv", 2); !ok || !bytes.Equal(data, patch) {
		t.Fatalf("patch bytes for segment 2 lost")
	}

	again, err := s.UnfailRepaired(nzbID)
	if err != nil || len(again) != 0 {
		t.Fatalf("second UnfailRepaired = %v, %v; want nothing", again, err)
	}
}

// A rejected (torn-down) entry must not have its manifest re-created.
func TestUnfailRepairedSkipsRejected(t *testing.T) {
	s := newTestStore(t)
	s.MarkRejected("gone")
	cleared, err := s.UnfailRepaired("gone")
	if err != nil || len(cleared) != 0 {
		t.Fatalf("UnfailRepaired on rejected = %v, %v", cleared, err)
	}
	if s.EntryExists("gone") {
		t.Fatalf("UnfailRepaired re-created a rejected entry's directory")
	}
}
