package par2

import (
	"reflect"
	"testing"
)

// syntheticIndex builds a minimal, valid *Index by hand (bypassing packet
// parsing) for tests that only care about the mapping/matching logic.
func syntheticIndex(t *testing.T, sliceSize int64, files map[[16]byte]*FileDesc, order [][16]byte) *Index {
	t.Helper()
	idx := &Index{
		SliceSize: sliceSize,
		FileOrder: order,
		Files:     files,
		Slices:    make(map[[16]byte][]SliceChecksum),
	}
	if err := idx.finalize(); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	return idx
}

func fid(b byte) [16]byte {
	var id [16]byte
	id[0] = b
	return id
}

func TestSliceBaseAndDamagedSlices(t *testing.T) {
	fA, fB, fC := fid(1), fid(2), fid(3)
	files := map[[16]byte]*FileDesc{
		fA: {FileID: fA, Length: 10000}, // ceil(10000/4096) = 3 slices
		fB: {FileID: fB, Length: 4096},  // exactly 1 slice
		fC: {FileID: fC, Length: 1},     // 1 slice (padded)
	}
	idx := syntheticIndex(t, 4096, files, [][16]byte{fA, fB, fC})

	cases := []struct {
		id   [16]byte
		want int64
	}{
		{fA, 0},
		{fB, 3},
		{fC, 4},
	}
	for _, c := range cases {
		got, err := idx.SliceBase(c.id)
		if err != nil {
			t.Fatalf("SliceBase(%x): %v", c.id, err)
		}
		if got != c.want {
			t.Errorf("SliceBase(%x) = %d, want %d", c.id, got, c.want)
		}
	}
	if idx.NumSlices() != 5 {
		t.Fatalf("NumSlices() = %d, want 5", idx.NumSlices())
	}

	// A byte range entirely inside the first slice of fA.
	dmg, err := idx.DamagedSlices(fA, 0, 100)
	if err != nil {
		t.Fatalf("DamagedSlices: %v", err)
	}
	if !reflect.DeepEqual(dmg, []int64{0}) {
		t.Errorf("DamagedSlices(fA, 0, 100) = %v, want [0]", dmg)
	}

	// A byte range spanning slices 1 and 2 of fA (global indices 1, 2) -
	// partially covering slice 2 still marks it fully damaged.
	dmg, err = idx.DamagedSlices(fA, 4096+10, 8192+50)
	if err != nil {
		t.Fatalf("DamagedSlices: %v", err)
	}
	if !reflect.DeepEqual(dmg, []int64{1, 2}) {
		t.Errorf("DamagedSlices spanning slices = %v, want [1 2]", dmg)
	}

	// fB's only slice is global index 3.
	dmg, err = idx.DamagedSlices(fB, 0, 4096)
	if err != nil {
		t.Fatalf("DamagedSlices: %v", err)
	}
	if !reflect.DeepEqual(dmg, []int64{3}) {
		t.Errorf("DamagedSlices(fB) = %v, want [3]", dmg)
	}
}

func TestSliceLocationRoundTrip(t *testing.T) {
	fA, fB := fid(1), fid(2)
	files := map[[16]byte]*FileDesc{
		fA: {FileID: fA, Length: 9000},
		fB: {FileID: fB, Length: 4000},
	}
	idx := syntheticIndex(t, 4096, files, [][16]byte{fA, fB})
	// fA: ceil(9000/4096) = 3 slices (global 0,1,2); fB: 1 slice (global 3).
	tests := []struct {
		global    int64
		wantFile  [16]byte
		wantLocal int64
	}{
		{0, fA, 0},
		{2, fA, 2},
		{3, fB, 0},
	}
	for _, tc := range tests {
		gotFile, gotLocal, err := idx.sliceLocation(tc.global)
		if err != nil {
			t.Fatalf("sliceLocation(%d): %v", tc.global, err)
		}
		if gotFile != tc.wantFile || gotLocal != tc.wantLocal {
			t.Errorf("sliceLocation(%d) = (%x, %d), want (%x, %d)", tc.global, gotFile, gotLocal, tc.wantFile, tc.wantLocal)
		}
	}
	if _, _, err := idx.sliceLocation(4); err == nil {
		t.Fatalf("sliceLocation(4) should error (out of range), got none")
	}
}

func TestMatchFilesUnambiguousLength(t *testing.T) {
	fA, fB := fid(1), fid(2)
	files := map[[16]byte]*FileDesc{
		fA: {FileID: fA, Length: 1000, Name: "a.rar"},
		fB: {FileID: fB, Length: 2000, Name: "b.rar"},
	}
	idx := syntheticIndex(t, 4096, files, [][16]byte{fA, fB})

	posted := []PostedFile{
		{Name: "a.rar", Length: 1000},
		{Name: "b.rar", Length: 2000},
	}
	matches, err := MatchFiles(idx, posted)
	if err != nil {
		t.Fatalf("MatchFiles: %v", err)
	}
	if len(matches) != 2 {
		t.Fatalf("MatchFiles returned %d matches, want 2: %+v", len(matches), matches)
	}
	byPosted := make(map[int][16]byte)
	for _, m := range matches {
		byPosted[m.PostedIndex] = m.FileID
		if m.NameMismatch {
			t.Errorf("unexpected NameMismatch for posted index %d", m.PostedIndex)
		}
	}
	if byPosted[0] != fA || byPosted[1] != fB {
		t.Fatalf("matches = %v, want {0:fA, 1:fB}", byPosted)
	}
}

func TestMatchFilesTieBrokenByMD5_16k(t *testing.T) {
	fA, fB := fid(1), fid(2)
	md5A := [16]byte{0xAA}
	md5B := [16]byte{0xBB}
	files := map[[16]byte]*FileDesc{
		fA: {FileID: fA, Length: 5000, MD5_16k: md5A, Name: "renamed-a.rar"},
		fB: {FileID: fB, Length: 5000, MD5_16k: md5B, Name: "b.rar"},
	}
	idx := syntheticIndex(t, 4096, files, [][16]byte{fA, fB})

	posted := []PostedFile{
		{Name: "a.rar", Length: 5000, MD5_16k: func() ([16]byte, error) { return md5A, nil }},
		{Name: "b.rar", Length: 5000, MD5_16k: func() ([16]byte, error) { return md5B, nil }},
	}
	matches, err := MatchFiles(idx, posted)
	if err != nil {
		t.Fatalf("MatchFiles: %v", err)
	}
	if len(matches) != 2 {
		t.Fatalf("MatchFiles returned %d matches, want 2: %+v", len(matches), matches)
	}
	byPosted := make(map[int]Match)
	for _, m := range matches {
		byPosted[m.PostedIndex] = m
	}
	if byPosted[0].FileID != fA {
		t.Errorf("posted[0] matched %x, want fA (%x)", byPosted[0].FileID, fA)
	}
	if !byPosted[0].NameMismatch {
		t.Errorf("posted[0] should report NameMismatch (a.rar vs renamed-a.rar)")
	}
	if byPosted[1].FileID != fB {
		t.Errorf("posted[1] matched %x, want fB (%x)", byPosted[1].FileID, fB)
	}
	if byPosted[1].NameMismatch {
		t.Errorf("posted[1] should not report NameMismatch")
	}
}

func TestMatchFilesNoMatchIsNotAnError(t *testing.T) {
	fA := fid(1)
	files := map[[16]byte]*FileDesc{fA: {FileID: fA, Length: 1000, Name: "a.rar"}}
	idx := syntheticIndex(t, 4096, files, [][16]byte{fA})

	posted := []PostedFile{{Name: "unrelated.nfo", Length: 999}}
	matches, err := MatchFiles(idx, posted)
	if err != nil {
		t.Fatalf("MatchFiles: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("MatchFiles = %v, want no matches", matches)
	}
}
