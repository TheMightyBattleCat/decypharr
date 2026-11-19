package par2

import (
	"errors"
	"testing"
)

// threeEqualVolumes is a PAR2 set of three volumes v1..v3 of one length,
// with MD5-16k 0x11..0x13, and a hasher returning the MD5-16k of vN.
func threeEqualVolumes() (*Index, func(n byte) func() ([16]byte, error)) {
	const length = 50 << 20
	idx := &Index{SliceSize: 1 << 20, Files: map[[16]byte]*FileDesc{}}
	for i := byte(1); i <= 3; i++ {
		idx.Files[fid(i)] = &FileDesc{FileID: fid(i), MD5_16k: [16]byte{0x10 + i}, Length: length, Name: "v" + string('0'+i)}
		idx.FileOrder = append(idx.FileOrder, fid(i))
	}
	hashOf := func(n byte) func() ([16]byte, error) {
		return func() ([16]byte, error) { return [16]byte{0x10 + n}, nil }
	}
	return idx, hashOf
}

// The NZB carries v1, v2 and a second copy of v2; v3 itself was never
// posted. The copy hashes as v2, which is already claimed. That hash is
// evidence it is not v3, so the last-one-standing pass must not pair it
// with v3: every read through that pairing would fail its IFSC.
func TestMatchFilesLastOneStandingSkipsAHashedFile(t *testing.T) {
	idx, hashOf := threeEqualVolumes()
	posted := []PostedFile{
		{Name: "obf-a", Length: 50 << 20, MD5_16k: hashOf(1)},
		{Name: "obf-b", Length: 50 << 20, MD5_16k: hashOf(2)},
		{Name: "obf-c", Length: 50 << 20, MD5_16k: hashOf(2)}, // second copy of v2
	}
	matches, skipped, err := MatchFiles(idx, posted)
	if err != nil {
		t.Fatalf("MatchFiles: %v", err)
	}
	if len(skipped) != 0 {
		t.Fatalf("skipped = %+v, want none (every file hashed)", skipped)
	}
	want := map[int][16]byte{0: fid(1), 1: fid(2)}
	if len(matches) != len(want) {
		t.Fatalf("matches = %+v, want only obf-a -> v1 and obf-b -> v2", matches)
	}
	for _, m := range matches {
		if m.FileID == fid(3) {
			t.Fatalf("posted %q paired with v3, which the NZB does not carry; its MD5-16k is v2's", posted[m.PostedIndex].Name)
		}
		if w, ok := want[m.PostedIndex]; !ok || w != m.FileID {
			t.Fatalf("posted %q paired with %q, want %v", posted[m.PostedIndex].Name, idx.Files[m.FileID].Name, want)
		}
	}
}

// The same set, but the odd posted file cannot be hashed (its first
// article is dead). Nothing speaks against the pairing, so the deduction
// still pairs it with v3: a tied volume with a dead first article is the
// case the pass exists for.
func TestMatchFilesLastOneStandingKeepsAnUnhashedFile(t *testing.T) {
	idx, hashOf := threeEqualVolumes()
	dead := func() ([16]byte, error) { return [16]byte{}, errors.New("430 no such article") }
	posted := []PostedFile{
		{Name: "obf-a", Length: 50 << 20, MD5_16k: hashOf(1)},
		{Name: "obf-b", Length: 50 << 20, MD5_16k: hashOf(2)},
		{Name: "obf-x", Length: 50 << 20, MD5_16k: dead},
	}
	matches, skipped, err := MatchFiles(idx, posted)
	if err != nil {
		t.Fatalf("MatchFiles: %v", err)
	}
	if len(skipped) != 0 {
		t.Fatalf("skipped = %+v, want none once the deduction paired obf-x", skipped)
	}
	paired := false
	for _, m := range matches {
		if m.PostedIndex == 2 {
			if m.FileID != fid(3) {
				t.Fatalf("obf-x paired with %q, want v3", idx.Files[m.FileID].Name)
			}
			paired = true
		}
	}
	if !paired || len(matches) != 3 {
		t.Fatalf("matches = %+v, want all three, obf-x -> v3", matches)
	}
}
