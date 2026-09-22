package par2

import (
	"crypto/md5"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

// TestParseIndexSkipsBadPacketButStillBuilds corrupts one packet's body in a
// single source so its packet MD5 fails, and asserts ParseIndex skips it,
// records it in SkippedPackets, and still assembles a structurally complete
// index from the duplicate Main/FileDesc/IFSC packets in the other sources.
func TestParseIndexSkipsBadPacketButStillBuilds(t *testing.T) {
	sources := loadFixtureSources(t)
	clean, err := ParseIndex(sources)
	if err != nil {
		t.Fatalf("baseline ParseIndex: %v", err)
	}

	// Locate the first packet of the first source and flip a body byte.
	var badOffset int64 = -1
	_ = walkPackets(sources[0].Data, nil, func(h packetHeader, packet []byte, offset int64) error {
		if badOffset < 0 {
			badOffset = offset
		}
		return nil
	})
	if badOffset < 0 {
		t.Fatal("fixture source 0 has no parseable packet")
	}
	mutated := append([]byte(nil), sources[0].Data...)
	mutated[badOffset+int64(packetHeaderSize)+4] ^= 0xFF // a body byte, past the header
	sources[0].Data = mutated

	idx, err := ParseIndex(sources)
	if err != nil {
		t.Fatalf("ParseIndex with one bad packet: %v", err)
	}
	if len(idx.SkippedPackets) == 0 {
		t.Fatal("SkippedPackets is empty; the bad packet was not recorded")
	}
	if idx.SkippedPackets[0].Source != 0 || idx.SkippedPackets[0].Offset != badOffset {
		t.Errorf("SkippedPackets[0] = %+v, want Source 0, Offset %d", idx.SkippedPackets[0], badOffset)
	}
	if idx.SliceSize != clean.SliceSize || idx.NumSlices() != clean.NumSlices() || len(idx.FileOrder) != len(clean.FileOrder) {
		t.Errorf("index from a partial source set differs from clean: SliceSize %d/%d, NumSlices %d/%d, files %d/%d",
			idx.SliceSize, clean.SliceSize, idx.NumSlices(), clean.NumSlices(), len(idx.FileOrder), len(clean.FileOrder))
	}
	for _, id := range idx.FileOrder {
		if _, ok := idx.Files[id]; !ok {
			t.Errorf("FileOrder references %x with no FileDesc after skip", id)
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

	matches, skipped, err := MatchFiles(idx, posted)
	if err != nil {
		t.Fatalf("MatchFiles: %v", err)
	}
	if len(skipped) != 0 {
		t.Fatalf("MatchFiles skipped %d files, want 0: %+v", len(skipped), skipped)
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

// unavailableSliceSource is fixtureSliceSource with some intact slices
// unreadable, the way a dead article makes them.
type unavailableSliceSource struct {
	*fixtureSliceSource
	gone  map[int64]struct{}
	reads map[int64]int
}

func (s *unavailableSliceSource) ReadSlice(globalIdx int64) ([]byte, error) {
	s.reads[globalIdx]++
	if _, gone := s.gone[globalIdx]; gone {
		return nil, fmt.Errorf("%w: article gone", ErrSliceUnavailable)
	}
	return s.fixtureSliceSource.ReadSlice(globalIdx)
}

// Unavailable intact slices do not stop the pass: every one is found in it,
// the error names them all at once, and a retry with them added to the
// damaged set repairs the lot. Tale of Castles S08E05 (2026-09-22) found one
// dead intact slice per full re-read until the round cap made it terminal.
func TestGoldenRepairCollectsEveryUnavailableSlice(t *testing.T) {
	gf := loadGoldenFixture(t)
	base1, _ := gf.idx.SliceBase(gf.fileIDByName["file1.bin"])
	base2, _ := gf.idx.SliceBase(gf.fileIDByName["file2.bin"])
	damaged := []int64{base1 + 1}
	gone := map[int64]struct{}{base1 + 2: {}, base2 + 1: {}}

	recovery := make([]RecoverySlice, len(damaged))
	for i, ref := range gf.idx.Recovery[:len(damaged)] {
		recovery[i] = RecoverySlice{Exponent: ref.Exponent, Data: gf.sources[ref.Source].Data[ref.Offset : ref.Offset+ref.Length]}
	}
	src := &unavailableSliceSource{
		fixtureSliceSource: &fixtureSliceSource{idx: gf.idx, damaged: map[int64]struct{}{damaged[0]: {}}, byFile: gf.byFile},
		gone:               gone,
		reads:              map[int64]int{},
	}
	_, err := Repair(gf.idx, damaged, recovery, src)
	if !errors.Is(err, ErrSliceUnavailable) {
		t.Fatalf("Repair: %v, want ErrSliceUnavailable", err)
	}
	if !strings.HasPrefix(err.Error(), "2 intact slice(s) unavailable") {
		t.Fatalf("Repair error %q does not count both unavailable slices", err)
	}
	for s := int64(0); s < gf.idx.NumSlices(); s++ {
		if s == damaged[0] {
			continue
		}
		if src.reads[s] != 1 {
			t.Fatalf("slice %d read %d times, want every intact slice read once in the pass", s, src.reads[s])
		}
	}
	gf.assertRepairMatchesOriginal(t, []int64{base1 + 1, base1 + 2, base2 + 1})
}

// With a spare-recovery budget, a pass stops once the unavailable slices
// exceed it rather than reading the rest of the release.
func TestGoldenRepairStopsWhenUnavailableExceedsBudget(t *testing.T) {
	gf := loadGoldenFixture(t)
	base1, _ := gf.idx.SliceBase(gf.fileIDByName["file1.bin"])
	damaged := []int64{base1}
	gone := map[int64]struct{}{base1 + 1: {}, base1 + 2: {}}
	recovery := []RecoverySlice{{Exponent: gf.idx.Recovery[0].Exponent, Data: gf.sources[gf.idx.Recovery[0].Source].Data[gf.idx.Recovery[0].Offset : gf.idx.Recovery[0].Offset+gf.idx.Recovery[0].Length]}}
	src := &unavailableSliceSource{
		fixtureSliceSource: &fixtureSliceSource{idx: gf.idx, damaged: map[int64]struct{}{base1: {}}, byFile: gf.byFile},
		gone:               gone,
		reads:              map[int64]int{},
	}
	_, err := RepairWith(gf.idx, damaged, recovery, src, RepairOptions{MaxUnavailable: 1})
	if !errors.Is(err, ErrSliceUnavailable) || !strings.Contains(err.Error(), "pass stopped") {
		t.Fatalf("RepairWith: %v, want a stopped pass", err)
	}
	read := 0
	for _, n := range src.reads {
		read += n
	}
	if total := int(gf.idx.NumSlices()) - 1; read >= total {
		t.Fatalf("read %d of %d intact slices, want the pass stopped early", read, total)
	}
}

// corruptSliceSource serves some intact slices with a flipped byte, the way
// a mis-served article that passes its own yEnc CRC would.
type corruptSliceSource struct {
	*fixtureSliceSource
	bad map[int64]struct{}
}

func (s *corruptSliceSource) ReadSlice(globalIdx int64) ([]byte, error) {
	data, err := s.fixtureSliceSource.ReadSlice(globalIdx)
	if err != nil {
		return nil, err
	}
	if _, bad := s.bad[globalIdx]; bad {
		data = append([]byte(nil), data...)
		data[0] ^= 0xff
	}
	return data, nil
}

// An intact slice failing its IFSC checksum is reported and skipped, not
// accumulated. Accumulating it guaranteed the solve failed its own IFSC check
// after a full read, which was then classified terminal; skipped, it is
// reconstructed from parity on the next round like an unreadable slice.
func TestGoldenRepairReportsChecksumMismatchedIntactSlice(t *testing.T) {
	gf := loadGoldenFixture(t)
	base1, _ := gf.idx.SliceBase(gf.fileIDByName["file1.bin"])
	base2, _ := gf.idx.SliceBase(gf.fileIDByName["file2.bin"])
	damaged := []int64{base1 + 1}
	bad := base2 + 1

	recovery := []RecoverySlice{{Exponent: gf.idx.Recovery[0].Exponent, Data: gf.sources[gf.idx.Recovery[0].Source].Data[gf.idx.Recovery[0].Offset : gf.idx.Recovery[0].Offset+gf.idx.Recovery[0].Length]}}
	src := &corruptSliceSource{
		fixtureSliceSource: &fixtureSliceSource{idx: gf.idx, damaged: map[int64]struct{}{damaged[0]: {}}, byFile: gf.byFile},
		bad:                map[int64]struct{}{bad: {}},
	}
	var reported []int64
	_, err := RepairWith(gf.idx, damaged, recovery, src, RepairOptions{MaxUnavailable: -1, OnChecksumMismatch: func(i int64) { reported = append(reported, i) }})
	if errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("RepairWith: %v - a single bad intact slice must not end as a checksum failure", err)
	}
	if !errors.Is(err, ErrSliceUnavailable) {
		t.Fatalf("RepairWith: %v, want ErrSliceUnavailable", err)
	}
	if len(reported) != 1 || reported[0] != bad {
		t.Fatalf("reported %v, want [%d]", reported, bad)
	}
	gf.assertRepairMatchesOriginal(t, []int64{base1 + 1, bad})
}
