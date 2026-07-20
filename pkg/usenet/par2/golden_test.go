package par2

import (
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// loadFixtureSources reads every .par2 file in testdata/par2 (the index plus
// every recovery volume gen.sh produced) as ParseIndex Sources.
func loadFixtureSources(t *testing.T) []Source {
	t.Helper()
	dir := filepath.Join("testdata", "par2")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read testdata dir: %v", err)
	}
	var sources []Source
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".par2" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		sources = append(sources, Source{Name: e.Name(), Data: data})
	}
	if len(sources) == 0 {
		t.Fatalf("no .par2 fixtures found in %s - run gen.sh", dir)
	}
	return sources
}

func loadFixtureFile(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "par2", name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return data
}

func TestParseIndexAgainstFixtures(t *testing.T) {
	idx, err := ParseIndex(loadFixtureSources(t))
	if err != nil {
		t.Fatalf("ParseIndex: %v", err)
	}
	if idx.SliceSize != 4096 {
		t.Errorf("SliceSize = %d, want 4096", idx.SliceSize)
	}
	// file1=40000 (10 slices), file2=35000 (9 slices), file3=25000 (7 slices).
	if idx.NumSlices() != 26 {
		t.Errorf("NumSlices() = %d, want 26", idx.NumSlices())
	}
	if len(idx.FileOrder) != 3 {
		t.Fatalf("FileOrder has %d entries, want 3", len(idx.FileOrder))
	}
	if len(idx.Recovery) != 5 {
		t.Errorf("len(Recovery) = %d, want 5 (20%% of 26 slices)", len(idx.Recovery))
	}
	for _, id := range idx.FileOrder {
		if _, ok := idx.Files[id]; !ok {
			t.Errorf("FileOrder references %x with no FileDesc", id)
		}
		if _, ok := idx.Slices[id]; !ok {
			t.Errorf("no IFSC checksums recorded for file %x", id)
		}
	}
}

// fixtureSliceSource resolves global slice indices to bytes from the
// original (undamaged) fixture files, for feeding to Repair as the "intact"
// source. It panics if asked for a slice index the test marked damaged -
// Repair must never actually need to read one.
type fixtureSliceSource struct {
	idx     *Index
	damaged map[int64]struct{}
	byFile  map[[16]byte][]byte
}

func (s *fixtureSliceSource) ReadSlice(globalIdx int64) ([]byte, error) {
	if _, dmg := s.damaged[globalIdx]; dmg {
		return nil, fmt.Errorf("test bug: Repair asked for damaged slice %d - it must never do this", globalIdx)
	}
	fileID, local, err := s.idx.sliceLocation(globalIdx)
	if err != nil {
		return nil, err
	}
	content := s.byFile[fileID]
	start := local * s.idx.SliceSize
	buf := make([]byte, s.idx.SliceSize)
	n := copy(buf, content[start:min(int64(len(content)), start+s.idx.SliceSize)])
	_ = n // remaining bytes stay zero - the required end-of-file padding
	return buf, nil
}

// goldenFixture bundles the parsed index and matched, original file content
// shared by every golden-repair-style test below.
type goldenFixture struct {
	sources      []Source
	idx          *Index
	byFile       map[[16]byte][]byte
	fileIDByName map[string][16]byte
}

func loadGoldenFixture(t *testing.T) *goldenFixture {
	t.Helper()
	sources := loadFixtureSources(t)
	idx, err := ParseIndex(sources)
	if err != nil {
		t.Fatalf("ParseIndex: %v", err)
	}

	originals := map[string][]byte{
		"file1.bin": loadFixtureFile(t, "file1.bin"),
		"file2.bin": loadFixtureFile(t, "file2.bin"),
		"file3.bin": loadFixtureFile(t, "file3.bin"),
	}

	posted := make([]PostedFile, 0, len(originals))
	postedNames := make([]string, 0, len(originals))
	for name, content := range originals {
		content := content
		posted = append(posted, PostedFile{
			Name:   name,
			Length: int64(len(content)),
			MD5_16k: func() ([16]byte, error) {
				n := len(content)
				if n > 16384 {
					n = 16384
				}
				return md5.Sum(content[:n]), nil
			},
		})
		postedNames = append(postedNames, name)
	}

	matches, err := MatchFiles(idx, posted)
	if err != nil {
		t.Fatalf("MatchFiles: %v", err)
	}
	if len(matches) != 3 {
		t.Fatalf("MatchFiles found %d matches, want 3: %+v", len(matches), matches)
	}

	byFile := make(map[[16]byte][]byte)
	fileIDByName := make(map[string][16]byte)
	for _, m := range matches {
		if m.NameMismatch {
			t.Errorf("unexpected NameMismatch for %s", postedNames[m.PostedIndex])
		}
		name := postedNames[m.PostedIndex]
		byFile[m.FileID] = originals[name]
		fileIDByName[name] = m.FileID
	}

	return &goldenFixture{sources: sources, idx: idx, byFile: byFile, fileIDByName: fileIDByName}
}

// assertRepairMatchesOriginal runs Repair for damagedList (which must not
// exceed the fixture's available recovery slices) and byte-compares every
// reconstructed, trimmed slice against the real original file content.
func (gf *goldenFixture) assertRepairMatchesOriginal(t *testing.T, damagedList []int64) {
	t.Helper()
	idx := gf.idx

	if len(damagedList) > len(idx.Recovery) {
		t.Fatalf("test wants to damage %d slices but only %d recovery slices are available", len(damagedList), len(idx.Recovery))
	}

	damagedSet := make(map[int64]struct{}, len(damagedList))
	for _, d := range damagedList {
		damagedSet[d] = struct{}{}
	}

	recovery := make([]RecoverySlice, len(damagedList))
	for i, ref := range idx.Recovery[:len(damagedList)] {
		src := gf.sources[ref.Source].Data
		recovery[i] = RecoverySlice{
			Exponent: ref.Exponent,
			Data:     append([]byte(nil), src[ref.Offset:ref.Offset+ref.Length]...),
		}
	}

	sliceSource := &fixtureSliceSource{idx: idx, damaged: damagedSet, byFile: gf.byFile}

	repaired, err := Repair(idx, damagedList, recovery, sliceSource)
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if len(repaired) != len(damagedList) {
		t.Fatalf("Repair returned %d slices, want %d", len(repaired), len(damagedList))
	}

	for _, rs := range repaired {
		fileID, local, err := idx.sliceLocation(rs.Index)
		if err != nil {
			t.Fatalf("sliceLocation(%d): %v", rs.Index, err)
		}
		trimmed, err := idx.TrimSlice(rs.Index, rs.Data)
		if err != nil {
			t.Fatalf("TrimSlice(%d): %v", rs.Index, err)
		}

		original := gf.byFile[fileID]
		start := local * idx.SliceSize
		end := min(start+int64(len(trimmed)), int64(len(original)))
		want := original[start:end]

		if len(trimmed) != len(want) {
			t.Fatalf("slice %d: reconstructed length %d, want %d", rs.Index, len(trimmed), len(want))
		}
		for i := range want {
			if trimmed[i] != want[i] {
				t.Fatalf("slice %d: byte %d = %#x, want %#x (reconstructed data does not match the original file)", rs.Index, i, trimmed[i], want[i])
			}
		}
	}
}

// TestGoldenRepair is the fixture-driven golden test: parse the real PAR2
// set generated by gen.sh, treat a handful of slices spanning every file as
// "lost" (zeroed in memory, and never read by Repair), reconstruct them via
// the engine, and byte-compare the result against the real original files.
func TestGoldenRepair(t *testing.T) {
	gf := loadGoldenFixture(t)

	// Damage one slice from each of the three files (global indices depend
	// on FileOrder, which is content-hash-derived, not the file names - so
	// resolve via SliceBase rather than assuming an order).
	var damagedList []int64
	for _, name := range []string{"file1.bin", "file2.bin", "file3.bin"} {
		base, err := gf.idx.SliceBase(gf.fileIDByName[name])
		if err != nil {
			t.Fatalf("SliceBase(%s): %v", name, err)
		}
		// The second slice of each file (index base+1) - away from both the
		// first and (padded) final slice, to also exercise a fully-interior
		// slice at least once.
		damagedList = append(damagedList, base+1)
	}
	gf.assertRepairMatchesOriginal(t, damagedList)
}

// TestGoldenRepairFinalPaddedSlice specifically damages each file's LAST
// (zero-padded) slice, exercising Index.TrimSlice's padding removal on a
// reconstructed (not just a directly-read) slice.
func TestGoldenRepairFinalPaddedSlice(t *testing.T) {
	gf := loadGoldenFixture(t)

	var damagedList []int64
	for _, name := range []string{"file1.bin", "file2.bin"} { // 2 files - all 5 recovery slices allow up to 5, use 2 to leave headroom
		id := gf.fileIDByName[name]
		fd := gf.idx.Files[id]
		base, err := gf.idx.SliceBase(id)
		if err != nil {
			t.Fatalf("SliceBase(%s): %v", name, err)
		}
		lastLocal := ceilDiv(fd.Length, gf.idx.SliceSize) - 1
		damagedList = append(damagedList, base+lastLocal)
	}
	gf.assertRepairMatchesOriginal(t, damagedList)
}

// TestGoldenRepairAllRecoverySlices uses every available recovery slice
// (k=5, this fixture's maximum) at once, spread across all three files.
func TestGoldenRepairAllRecoverySlices(t *testing.T) {
	gf := loadGoldenFixture(t)

	var damagedList []int64
	offsets := []int64{0, 1, 0, 1, 2}
	names := []string{"file1.bin", "file1.bin", "file2.bin", "file2.bin", "file3.bin"}
	for i, name := range names {
		base, err := gf.idx.SliceBase(gf.fileIDByName[name])
		if err != nil {
			t.Fatalf("SliceBase(%s): %v", name, err)
		}
		damagedList = append(damagedList, base+offsets[i])
	}
	gf.assertRepairMatchesOriginal(t, damagedList)
}
