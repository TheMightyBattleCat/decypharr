package par2

import (
	"errors"
	"testing"
)

// Each pass of MatchFiles tags its pairing with what made it, and only the
// pairings no content backs report Unconfirmed.
func TestMatchFilesEvidence(t *testing.T) {
	dead := func() ([16]byte, error) { return [16]byte{}, errors.New("430 no such article") }

	t.Run("length-unique and md5 tie-break", func(t *testing.T) {
		idx := syntheticIndex(t, 4096, map[[16]byte]*FileDesc{
			fid(1): {FileID: fid(1), Length: 1000, MD5_16k: [16]byte{0xA1}, Name: "a.nfo"},
			fid(2): {FileID: fid(2), Length: 9000, MD5_16k: [16]byte{0xB2}, Name: "b.rar"},
			fid(3): {FileID: fid(3), Length: 9000, MD5_16k: [16]byte{0xC3}, Name: "c.rar"},
		}, [][16]byte{fid(1), fid(2), fid(3)})
		posted := []PostedFile{
			{Name: "x1", Length: 1000},
			{Name: "x2", Length: 9000, MD5_16k: func() ([16]byte, error) { return [16]byte{0xC3}, nil }},
			{Name: "x3", Length: 9000, MD5_16k: func() ([16]byte, error) { return [16]byte{0xB2}, nil }},
		}
		got := evidenceByPosted(t, idx, posted)
		want := map[int]MatchEvidence{0: EvidenceLengthUnique, 1: EvidenceMD5, 2: EvidenceMD5}
		assertEvidence(t, got, want, nil)
	})

	t.Run("residual md5", func(t *testing.T) {
		idx := syntheticIndex(t, 4096, map[[16]byte]*FileDesc{
			fid(1): {FileID: fid(1), Length: 5000, MD5_16k: [16]byte{0xA1}, Name: "a.rar"},
		}, [][16]byte{fid(1)})
		posted := []PostedFile{
			{Name: "obf", Length: 4990, MD5_16k: func() ([16]byte, error) { return [16]byte{0xA1}, nil }},
		}
		assertEvidence(t, evidenceByPosted(t, idx, posted), map[int]MatchEvidence{0: EvidenceMD5}, nil)
	})

	t.Run("elimination is unconfirmed", func(t *testing.T) {
		idx, hashOf := threeEqualVolumes()
		posted := []PostedFile{
			{Name: "obf-a", Length: 50 << 20, MD5_16k: hashOf(1)},
			{Name: "obf-b", Length: 50 << 20, MD5_16k: hashOf(2)},
			{Name: "obf-x", Length: 50 << 20, MD5_16k: dead},
		}
		assertEvidence(t, evidenceByPosted(t, idx, posted),
			map[int]MatchEvidence{0: EvidenceMD5, 1: EvidenceMD5, 2: EvidenceElimination}, map[int]bool{2: true})
	})

	t.Run("name pass, same name is confirmed", func(t *testing.T) {
		idx := syntheticIndex(t, 4096, map[[16]byte]*FileDesc{
			fid(1): {FileID: fid(1), Length: 5000, MD5_16k: [16]byte{0xA1}, Name: "show.part10.rar"},
		}, [][16]byte{fid(1)})
		posted := []PostedFile{{Name: "show.part10.rar", Length: 4990, MD5_16k: dead}}
		assertEvidence(t, evidenceByPosted(t, idx, posted), map[int]MatchEvidence{0: EvidenceName}, nil)
	})

	t.Run("name pass, differing name is unconfirmed", func(t *testing.T) {
		idx := syntheticIndex(t, 4096, map[[16]byte]*FileDesc{
			fid(1): {FileID: fid(1), Length: 5000, MD5_16k: [16]byte{0xA1}, Name: "Show.Part10.rar"},
		}, [][16]byte{fid(1)})
		posted := []PostedFile{{Name: "show.part10.rar", Length: 4990, MD5_16k: dead}}
		assertEvidence(t, evidenceByPosted(t, idx, posted), map[int]MatchEvidence{0: EvidenceName}, map[int]bool{0: true})
	})

	t.Run("cache-rebuilt match is confirmed", func(t *testing.T) {
		if (Match{PostedIndex: 0, FileID: fid(1), NameMismatch: true}).Unconfirmed() {
			t.Fatal("a match with no Evidence (rebuilt from the cache) reports Unconfirmed")
		}
	})
}

func evidenceByPosted(t *testing.T, idx *Index, posted []PostedFile) map[int]Match {
	t.Helper()
	matches, skipped, err := MatchFiles(idx, posted)
	if err != nil || len(skipped) != 0 {
		t.Fatalf("MatchFiles: err=%v skipped=%+v", err, skipped)
	}
	out := make(map[int]Match, len(matches))
	for _, m := range matches {
		out[m.PostedIndex] = m
	}
	return out
}

func assertEvidence(t *testing.T, got map[int]Match, want map[int]MatchEvidence, unconfirmed map[int]bool) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("matches = %+v, want %d", got, len(want))
	}
	for pi, ev := range want {
		m, ok := got[pi]
		if !ok {
			t.Fatalf("posted %d unmatched, want evidence %q", pi, ev)
		}
		if m.Evidence != ev {
			t.Errorf("posted %d evidence %q, want %q", pi, m.Evidence, ev)
		}
		if m.Unconfirmed() != unconfirmed[pi] {
			t.Errorf("posted %d Unconfirmed() = %v, want %v", pi, m.Unconfirmed(), unconfirmed[pi])
		}
	}
}
