package overlay

import "testing"

// Every manifest write and every removal of the entry directory gives the
// entry a new generation; reads leave it alone, and other entries keep
// theirs. Callers cache manifest-derived data on it (pad marks).
func TestManifestGenerationChangesOnEveryWrite(t *testing.T) {
	s := newTestStore(t)
	const id = "nzb-gen"
	if g := s.ManifestGeneration(id); g != 0 {
		t.Fatalf("fresh store generation = %d, want 0", g)
	}
	last := uint64(0)
	step := func(name string, fn func() error) {
		t.Helper()
		if err := fn(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		g := s.ManifestGeneration(id)
		if g == last {
			t.Fatalf("%s left the generation at %d", name, g)
		}
		last = g
	}
	step("RecordDead", func() error { return s.RecordDead(id, "a.mkv", 1, "<a1>", 10) })
	step("RecordDead b", func() error { return s.RecordDead(id, "b.mkv", 1, "<b1>", 10) })
	step("WritePatch", func() error { return s.WritePatch(id, "a.mkv", 1, make([]byte, 10)) })
	step("SetCoverageFraction", func() error { return s.SetCoverageFraction(id, "b.mkv", 0.5) })

	if _, err := s.GetManifest(id); err != nil {
		t.Fatal(err)
	}
	if g := s.ManifestGeneration(id); g != last {
		t.Fatalf("GetManifest changed the generation %d -> %d", last, g)
	}
	other := s.ManifestGeneration("nzb-other")
	if err := s.RecordDead("nzb-other", "c.mkv", 1, "<c1>", 10); err != nil {
		t.Fatal(err)
	}
	if s.ManifestGeneration(id) != last || s.ManifestGeneration("nzb-other") == other {
		t.Fatal("a write to another entry moved this entry's generation, or not its own")
	}

	step("DeleteFile (not last)", func() error { _, err := s.DeleteFile(id, "a.mkv"); return err })
	step("DeleteFile (last)", func() error { _, err := s.DeleteFile(id, "b.mkv"); return err })
	step("RecordDead again", func() error { return s.RecordDead(id, "a.mkv", 2, "<a2>", 10) })
	step("ClearFileDamage (last)", func() error { _, err := s.ClearFileDamage(id, "a.mkv"); return err })
	step("RecordDead once more", func() error { return s.RecordDead(id, "a.mkv", 3, "<a3>", 10) })
	step("DeleteEntry", func() error { return s.DeleteEntry(id) })
	if last == 0 {
		t.Fatal("generation back at 0 after a removal")
	}
}
