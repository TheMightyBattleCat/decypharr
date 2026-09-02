package par2

import (
	"errors"
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
	matches, skipped, err := MatchFiles(idx, posted)
	if err != nil {
		t.Fatalf("MatchFiles: %v", err)
	}
	if len(skipped) != 0 {
		t.Fatalf("MatchFiles skipped %d files, want 0: %+v", len(skipped), skipped)
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
	matches, skipped, err := MatchFiles(idx, posted)
	if err != nil {
		t.Fatalf("MatchFiles: %v", err)
	}
	if len(skipped) != 0 {
		t.Fatalf("MatchFiles skipped %d files, want 0: %+v", len(skipped), skipped)
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
	matches, skipped, err := MatchFiles(idx, posted)
	if err != nil {
		t.Fatalf("MatchFiles: %v", err)
	}
	if len(skipped) != 0 {
		t.Fatalf("MatchFiles skipped %d files, want 0: %+v", len(skipped), skipped)
	}
	if len(matches) != 0 {
		t.Fatalf("MatchFiles = %v, want no matches", matches)
	}
}

func TestMatchFilesResidualMD5MatchesLengthMiss(t *testing.T) {
	fidExact := [16]byte{1}
	fidTail := [16]byte{2}
	md5Exact := [16]byte{0xAA}
	md5Tail := [16]byte{0xBB}
	idx := &Index{
		Files: map[[16]byte]*FileDesc{
			fidExact: {FileID: fidExact, MD5_16k: md5Exact, Length: 1000, Name: "vol.001"},
			fidTail:  {FileID: fidTail, MD5_16k: md5Tail, Length: 331614074, Name: "vol.010"},
		},
		FileOrder: [][16]byte{fidExact, fidTail},
	}
	posted := []PostedFile{
		{Name: "vol.001", Length: 1000, MD5_16k: func() ([16]byte, error) { return md5Exact, nil }},
		{Name: "vol.010", Length: 331600499, MD5_16k: func() ([16]byte, error) { return md5Tail, nil }},
	}
	matches, skipped, err := MatchFiles(idx, posted)
	if err != nil {
		t.Fatalf("MatchFiles: %v", err)
	}
	if len(skipped) != 0 {
		t.Fatalf("MatchFiles skipped %d files, want 0: %+v", len(skipped), skipped)
	}
	got := map[int][16]byte{}
	for _, m := range matches {
		got[m.PostedIndex] = m.FileID
	}
	if got[0] != fidExact {
		t.Errorf("posted 0 (exact length) FileID = %x, want %x", got[0], fidExact)
	}
	if got[1] != fidTail {
		t.Errorf("posted 1 (tail volume) FileID = %x, want %x - residual MD5-16k pass did not match the length-miss tail volume", got[1], fidTail)
	}
}

func TestMatchFilesResidualNoMatchOnMD5Mismatch(t *testing.T) {
	fidExact := [16]byte{1}
	fidTail := [16]byte{2}
	idx := &Index{
		Files: map[[16]byte]*FileDesc{
			fidExact: {FileID: fidExact, MD5_16k: [16]byte{0xAA}, Length: 1000, Name: "vol.001"},
			fidTail:  {FileID: fidTail, MD5_16k: [16]byte{0xBB}, Length: 331614074, Name: "vol.010"},
		},
		FileOrder: [][16]byte{fidExact, fidTail},
	}
	posted := []PostedFile{
		{Name: "vol.001", Length: 1000, MD5_16k: func() ([16]byte, error) { return [16]byte{0xAA}, nil }},
		{Name: "vol.010", Length: 331600499, MD5_16k: func() ([16]byte, error) { return [16]byte{0xCC}, nil }},
	}
	matches, skipped, err := MatchFiles(idx, posted)
	if err != nil {
		t.Fatalf("MatchFiles: %v", err)
	}
	if len(skipped) != 0 {
		t.Fatalf("MatchFiles skipped %d files, want 0: %+v", len(skipped), skipped)
	}
	for _, m := range matches {
		if m.PostedIndex == 1 {
			t.Errorf("posted 1 must stay unmatched (no equal MD5-16k) but matched FileID %x", m.FileID)
		}
	}
}

// Three same-length files: two are matched by MD5-16k, and the third has a
// dead tie-break article. The last-one-standing pass pairs the sole
// remaining posted file with the sole remaining FileDesc.
func TestMatchFilesLastOneStandingResolvesDeadTieBreak(t *testing.T) {
	fA, fB, fC := fid(1), fid(2), fid(3)
	md5A, md5B, md5C := [16]byte{0xAA}, [16]byte{0xBB}, [16]byte{0xCC}
	files := map[[16]byte]*FileDesc{
		fA: {FileID: fA, Length: 5000, MD5_16k: md5A, Name: "a.rar"},
		fB: {FileID: fB, Length: 5000, MD5_16k: md5B, Name: "b.rar"},
		fC: {FileID: fC, Length: 5000, MD5_16k: md5C, Name: "c.rar"},
	}
	idx := syntheticIndex(t, 4096, files, [][16]byte{fA, fB, fC})

	posted := []PostedFile{
		{Name: "a.rar", Length: 5000, MD5_16k: func() ([16]byte, error) { return md5A, nil }},
		{Name: "b.rar", Length: 5000, MD5_16k: func() ([16]byte, error) { return md5B, nil }},
		{Name: "c.rar", Length: 5000, MD5_16k: func() ([16]byte, error) { return [16]byte{}, errors.New("nntp: 430 no such article") }},
	}
	matches, skipped, err := MatchFiles(idx, posted)
	if err != nil {
		t.Fatalf("MatchFiles: %v", err)
	}
	if len(skipped) != 0 {
		t.Fatalf("skipped = %+v, want 0", skipped)
	}
	got := map[int][16]byte{}
	for _, m := range matches {
		got[m.PostedIndex] = m.FileID
	}
	if got[0] != fA || got[1] != fB || got[2] != fC {
		t.Fatalf("matches = %v, want {0:fA, 1:fB, 2:fC}", got)
	}
}

// Two unmatched FileDescs and two unmatched posted files at the same length:
// the pairing is genuinely ambiguous, so the deduction must not fire.
func TestMatchFilesLastOneStandingStaysAmbiguous(t *testing.T) {
	fA, fB := fid(1), fid(2)
	files := map[[16]byte]*FileDesc{
		fA: {FileID: fA, Length: 5000, MD5_16k: [16]byte{0xAA}, Name: "a.rar"},
		fB: {FileID: fB, Length: 5000, MD5_16k: [16]byte{0xBB}, Name: "b.rar"},
	}
	idx := syntheticIndex(t, 4096, files, [][16]byte{fA, fB})

	// nil MD5_16k on both: no tie-break, no residual, both stay unmatched.
	posted := []PostedFile{
		{Name: "a.rar", Length: 5000},
		{Name: "b.rar", Length: 5000},
	}
	matches, skipped, err := MatchFiles(idx, posted)
	if err != nil {
		t.Fatalf("MatchFiles: %v", err)
	}
	if len(skipped) != 0 {
		t.Fatalf("skipped = %+v, want 0", skipped)
	}
	if len(matches) != 0 {
		t.Fatalf("matches = %+v, want 0 - 2x2 length group is ambiguous", matches)
	}
}

// A length group with one unmatched FileDesc and no posted files at all
// must not panic or mismatch.
func TestMatchFilesLastOneStandingNoPostedAtLength(t *testing.T) {
	fA, fB := fid(1), fid(2)
	files := map[[16]byte]*FileDesc{
		fA: {FileID: fA, Length: 1000, MD5_16k: [16]byte{0xAA}, Name: "a.rar"},
		fB: {FileID: fB, Length: 9999, MD5_16k: [16]byte{0xBB}, Name: "b.rar"},
	}
	idx := syntheticIndex(t, 4096, files, [][16]byte{fA, fB})

	posted := []PostedFile{
		{Name: "a.rar", Length: 1000, MD5_16k: func() ([16]byte, error) { return [16]byte{0xAA}, nil }},
	}
	matches, skipped, err := MatchFiles(idx, posted)
	if err != nil {
		t.Fatalf("MatchFiles: %v", err)
	}
	if len(skipped) != 0 {
		t.Fatalf("skipped = %+v, want 0", skipped)
	}
	if len(matches) != 1 || matches[0].PostedIndex != 0 || matches[0].FileID != fA {
		t.Fatalf("matches = %+v, want {0 -> fA}", matches)
	}
}

// When every file is resolved by exact length, the deduction pass changes
// nothing.
func TestMatchFilesLastOneStandingNoOpWhenAllMatched(t *testing.T) {
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
	matches, skipped, err := MatchFiles(idx, posted)
	if err != nil {
		t.Fatalf("MatchFiles: %v", err)
	}
	if len(skipped) != 0 || len(matches) != 2 {
		t.Fatalf("matches = %+v, skipped = %+v, want 2 matches / 0 skipped", matches, skipped)
	}
}

// A single posted file's MD5-16k fetch erroring out during tie-break, when
// two same-length files remain: fA matches by MD5, and the last-one-standing
// deduction then pairs the only unmatched posted file with the only
// unmatched FileDesc at that length - no fetch needed, no skip recorded.
func TestMatchFilesTieBreakFetchErrorResolvedByDeduction(t *testing.T) {
	fA, fB := fid(1), fid(2)
	md5A := [16]byte{0xAA}
	md5B := [16]byte{0xBB}
	files := map[[16]byte]*FileDesc{
		fA: {FileID: fA, Length: 5000, MD5_16k: md5A, Name: "a.rar"},
		fB: {FileID: fB, Length: 5000, MD5_16k: md5B, Name: "b.rar"},
	}
	idx := syntheticIndex(t, 4096, files, [][16]byte{fA, fB})

	wantErr := errors.New("nntp: connection reset")
	posted := []PostedFile{
		{Name: "a.rar", Length: 5000, MD5_16k: func() ([16]byte, error) { return md5A, nil }},
		{Name: "b.rar", Length: 5000, MD5_16k: func() ([16]byte, error) { return [16]byte{}, wantErr }},
	}
	matches, skipped, err := MatchFiles(idx, posted)
	if err != nil {
		t.Fatalf("MatchFiles returned hard error, want nil: %v", err)
	}
	if len(skipped) != 0 {
		t.Fatalf("skipped = %+v, want 0 - deduction should resolve the tie", skipped)
	}
	got := map[int][16]byte{}
	for _, m := range matches {
		got[m.PostedIndex] = m.FileID
	}
	if got[0] != fA || got[1] != fB {
		t.Fatalf("matches = %v, want {0:fA, 1:fB}", got)
	}
}

// With three same-length files and the tie-break fetch failing for two of
// them, the deduction stays ambiguous (2 unmatched FileDescs, 2 unmatched
// posted files) and both misses are reported as transient skips.
func TestMatchFilesPartialOnTieBreakFetchError(t *testing.T) {
	fA, fB, fC := fid(1), fid(2), fid(3)
	md5A := [16]byte{0xAA}
	files := map[[16]byte]*FileDesc{
		fA: {FileID: fA, Length: 5000, MD5_16k: md5A, Name: "a.rar"},
		fB: {FileID: fB, Length: 5000, MD5_16k: [16]byte{0xBB}, Name: "b.rar"},
		fC: {FileID: fC, Length: 5000, MD5_16k: [16]byte{0xCC}, Name: "c.rar"},
	}
	idx := syntheticIndex(t, 4096, files, [][16]byte{fA, fB, fC})

	wantErr := errors.New("nntp: connection reset")
	posted := []PostedFile{
		{Name: "a.rar", Length: 5000, MD5_16k: func() ([16]byte, error) { return md5A, nil }},
		{Name: "b.rar", Length: 5000, MD5_16k: func() ([16]byte, error) { return [16]byte{}, wantErr }},
		{Name: "c.rar", Length: 5000, MD5_16k: func() ([16]byte, error) { return [16]byte{}, wantErr }},
	}
	matches, skipped, err := MatchFiles(idx, posted)
	if err != nil {
		t.Fatalf("MatchFiles returned hard error, want nil: %v", err)
	}
	if len(matches) != 1 || matches[0].PostedIndex != 0 || matches[0].FileID != fA {
		t.Fatalf("matches = %+v, want exactly {posted 0 -> fA}", matches)
	}
	skippedIdx := map[int]bool{}
	for _, s := range skipped {
		skippedIdx[s.PostedIndex] = true
		if !errors.Is(s.Err, wantErr) {
			t.Errorf("skipped entry for %d: Err = %v, want it to wrap %v", s.PostedIndex, s.Err, wantErr)
		}
	}
	if len(skipped) != 2 || !skippedIdx[1] || !skippedIdx[2] {
		t.Fatalf("skipped = %+v, want entries for posted indexes 1 and 2", skipped)
	}
}

// The same fetch error, but this time the residual MD5-16k pass is the one
// that hits it (the file's length missed every bucket). Still best-effort,
// still reported as a skip.
func TestMatchFilesPartialOnResidualFetchError(t *testing.T) {
	fExact, fTail := fid(1), fid(2)
	idx := &Index{
		Files: map[[16]byte]*FileDesc{
			fExact: {FileID: fExact, MD5_16k: [16]byte{0xAA}, Length: 1000, Name: "vol.001"},
			fTail:  {FileID: fTail, MD5_16k: [16]byte{0xBB}, Length: 331614074, Name: "vol.010"},
		},
		FileOrder: [][16]byte{fExact, fTail},
	}
	wantErr := errors.New("nntp: timeout")
	posted := []PostedFile{
		{Name: "vol.001", Length: 1000, MD5_16k: func() ([16]byte, error) { return [16]byte{0xAA}, nil }},
		{Name: "vol.010", Length: 331600499, MD5_16k: func() ([16]byte, error) { return [16]byte{}, wantErr }},
	}
	matches, skipped, err := MatchFiles(idx, posted)
	if err != nil {
		t.Fatalf("MatchFiles returned hard error, want nil: %v", err)
	}
	if len(matches) != 1 || matches[0].PostedIndex != 0 {
		t.Fatalf("matches = %+v, want exactly {posted 0 -> fExact}", matches)
	}
	if len(skipped) != 1 || skipped[0].PostedIndex != 1 || !errors.Is(skipped[0].Err, wantErr) {
		t.Fatalf("skipped = %+v, want one entry for posted index 1 wrapping %v", skipped, wantErr)
	}
}
