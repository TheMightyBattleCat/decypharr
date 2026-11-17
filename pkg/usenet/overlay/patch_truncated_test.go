package overlay

import (
	"bytes"
	"os"
	"testing"
)

// A patch file cut short on disk (a crash after the rename but before the
// data reached the disk) must not be served under a "patched" record.
// PatchBytes must refuse it and flip the record back to dead, so the file
// reads as damaged and the segment is queued for repair again.
func TestPatchBytesRefusesTruncatedPatchAndMarksDead(t *testing.T) {
	for _, size := range []int64{0, 400} {
		s := newTestStore(t)
		const id, file = "nzb1", "movie.mkv"
		if err := s.RecordDead(id, file, 7, "<m>", 1000); err != nil {
			t.Fatal(err)
		}
		if err := s.WritePatch(id, file, 7, bytes.Repeat([]byte{0xAB}, 1000)); err != nil {
			t.Fatal(err)
		}
		path, err := s.patchPath(id, file, 7)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Truncate(path, size); err != nil {
			t.Fatal(err)
		}

		if data, ok := s.PatchBytes(id, file, 7); ok {
			t.Fatalf("size=%d: PatchBytes served a %d-byte patch for a 1000-byte segment", size, len(data))
		}
		m, err := s.GetManifest(id)
		if err != nil {
			t.Fatal(err)
		}
		fe := m.Files[file]
		if st := fe.DeadSegments[0].Status; st != StatusDead {
			t.Fatalf("size=%d: status %s after a truncated patch was found, want dead", size, st)
		}
		if fe.Verdict != VerdictDegraded {
			t.Fatalf("size=%d: verdict %s, want degraded", size, fe.Verdict)
		}
		if got := s.PatchedSegments(id, file); len(got) != 0 {
			t.Fatalf("size=%d: PatchedSegments = %v, want none", size, got)
		}
		pending, _ := s.PendingRepair(id)
		if n := len(pending[file]); n != 1 {
			t.Fatalf("size=%d: %d segment(s) pending repair, want 1", size, n)
		}

		// A fresh repair writes a full patch again and it is served.
		want := bytes.Repeat([]byte{0xCD}, 1000)
		if err := s.WritePatch(id, file, 7, want); err != nil {
			t.Fatal(err)
		}
		if got, ok := s.PatchBytes(id, file, 7); !ok || !bytes.Equal(got, want) {
			t.Fatalf("size=%d: re-written patch not served (ok=%v, %d bytes)", size, ok, len(got))
		}
	}
}

// A patch whose length matches its record is still served unchanged.
func TestPatchBytesServesIntactPatch(t *testing.T) {
	s := newTestStore(t)
	const id, file = "nzb1", "movie.mkv"
	want := bytes.Repeat([]byte{0x11}, 512)
	if err := s.WritePatch(id, file, 3, want); err != nil {
		t.Fatal(err)
	}
	got, ok := s.PatchBytes(id, file, 3)
	if !ok || !bytes.Equal(got, want) {
		t.Fatalf("intact patch: ok=%v, %d bytes, want the written 512", ok, len(got))
	}
}
