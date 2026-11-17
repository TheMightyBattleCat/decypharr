package overlay

import (
	"bytes"
	"testing"
)

// A read that got its 430 before a repair wrote the patch asks Decide after
// it. Decide must leave the record patched and say "serve the patch", not
// mark it padded: PatchBytes serves only patched records, so the flip would
// serve zeros for a segment PAR2 repaired and queue it for repair again.
func TestDecideAfterWritePatchLeavesRecordPatched(t *testing.T) {
	for _, viewer := range []bool{false, true} {
		s := newTestStore(t)
		const id, file = "nzb1", "movie.mkv"
		if err := s.RecordDead(id, file, 500, "<m>", 1000); err != nil {
			t.Fatal(err)
		}
		want := bytes.Repeat([]byte{0xAB}, 1000)
		if err := s.WritePatch(id, file, 500, want); err != nil {
			t.Fatal(err)
		}
		decide := s.Decide
		if viewer {
			decide = s.DecideForViewer
		}
		d, v := decide(id, file, 500, "<m>", 1000, 1000*1000, 1000)
		if d != DecisionPatch {
			t.Fatalf("viewer=%v: decision %v, want DecisionPatch", viewer, d)
		}
		if v != VerdictClean {
			t.Fatalf("viewer=%v: verdict %s, want clean (the only dead segment is patched)", viewer, v)
		}
		got, ok := s.PatchBytes(id, file, 500)
		if !ok || !bytes.Equal(got, want) {
			t.Fatalf("viewer=%v: patch readable after Decide=%v, want the written patch", viewer, ok)
		}
		m, err := s.GetManifest(id)
		if err != nil {
			t.Fatal(err)
		}
		if st := m.Files[file].DeadSegments[0].Status; st != StatusPatched {
			t.Fatalf("viewer=%v: status %s after Decide, want patched", viewer, st)
		}
		pending, _ := s.PendingRepair(id)
		if n := len(pending[file]); n != 0 {
			t.Fatalf("viewer=%v: %d segment(s) pending repair after Decide, want 0", viewer, n)
		}
	}
}
