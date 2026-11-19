package manager

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// The match cache holds no evidence, so a hit is trusted as proven. A
// result with a pairing nothing read backs is not cached; one whose
// pairings are all content-backed still is.
func TestPar2MatchToCacheRefusesAnUnconfirmedPairing(t *testing.T) {
	src := []storage.PostedFileRef{{Name: "a.rar"}, {Name: "b.rar"}}
	idA, idB := [16]byte{1}, [16]byte{2}
	for _, tc := range []struct {
		name   string
		b      par2.Match
		cached bool
	}{
		{"elimination", par2.Match{PostedIndex: 1, FileID: idB, Evidence: par2.EvidenceElimination}, false},
		{"name, differing", par2.Match{PostedIndex: 1, FileID: idB, Evidence: par2.EvidenceName, NameMismatch: true}, false},
		{"name, same", par2.Match{PostedIndex: 1, FileID: idB, Evidence: par2.EvidenceName}, true},
		{"md5, differing name", par2.Match{PostedIndex: 1, FileID: idB, Evidence: par2.EvidenceMD5, NameMismatch: true}, true},
		{"length-unique", par2.Match{PostedIndex: 1, FileID: idB, Evidence: par2.EvidenceLengthUnique}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := par2MatchToCache(src, []par2.Match{{PostedIndex: 0, FileID: idA, Evidence: par2.EvidenceMD5}, tc.b}, nil)
			if (got != nil) != tc.cached {
				t.Errorf("cache = %+v, want cached=%v", got, tc.cached)
			}
		})
	}
}

// A file paired by elimination is confirmed once any of its slices was
// read in the pass (outside the damaged set); with every slice damaged it
// is not, and only its refs are withheld.
func TestPar2UnconfirmedPairingsNeedOneReadSlice(t *testing.T) {
	// Slice size 100: a.rar is slices 0-2, b.rar slices 3-5.
	idx, ids := buildTestIndexN(t, 100, []earlyFileSpec{{"a.rar", 250}, {"b.rar", 250}})
	idA, idB := ids["a.rar"], ids["b.rar"]
	matches := []par2.Match{
		{PostedIndex: 0, FileID: idA, Evidence: par2.EvidenceMD5},
		{PostedIndex: 1, FileID: idB, Evidence: par2.EvidenceElimination},
	}
	set := func(s ...int64) map[int64]struct{} {
		m := make(map[int64]struct{}, len(s))
		for _, v := range s {
			m[v] = struct{}{}
		}
		return m
	}

	if got := unconfirmedPairings(idx, matches, set(0, 1, 2, 3, 4)); len(got) != 0 {
		t.Errorf("b.rar slice 5 was read, got unconfirmed %v, want none", got)
	}
	got := unconfirmedPairings(idx, matches, set(1, 3, 4, 5))
	if len(got) != 1 || !got[idB] {
		t.Fatalf("every b.rar slice damaged: unconfirmed %v, want only b.rar", got)
	}

	refs := []par2DeadRef{
		{file: "a.rar", rng: postedRange{fileID: idA}},
		{file: "b.rar", rng: postedRange{fileID: idB}},
		{file: "b.rar", rng: postedRange{fileID: idB}},
	}
	kept, withheld := withholdUnconfirmedRefs(refs, got)
	if len(kept) != 1 || kept[0].file != "a.rar" || len(withheld) != 2 {
		t.Errorf("kept %+v withheld %+v, want a.rar kept and both b.rar refs withheld", kept, withheld)
	}
	if kept, withheld := withholdUnconfirmedRefs(refs, nil); len(kept) != 3 || len(withheld) != 0 {
		t.Errorf("nothing unconfirmed: kept %d withheld %d, want 3 and 0", len(kept), len(withheld))
	}
}
