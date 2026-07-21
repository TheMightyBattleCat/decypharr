package manager

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

func TestCensusPar2Volumes(t *testing.T) {
	files := []storage.Par2FileRef{
		{Name: "release.par2", Size: 1000},
		{Name: "release.vol1+2.par2", Size: 8000},
		{Name: "release.vol0+1.par2", Size: 4000},
		{Name: "release.vol3+4.par2", Size: 16000},
		{Name: "release.VOL7+1.PAR2", Size: 4200}, // case-insensitive
		{Name: "release.nfo", Size: 500},          // not PAR2-vol-named, not an index either - still "other"
	}

	vols, indexFiles := censusPar2Volumes(files)

	if len(vols) != 4 {
		t.Fatalf("censusPar2Volumes found %d vols, want 4: %+v", len(vols), vols)
	}
	// Smallest file first: vol0+1(4000) < VOL7+1(4200) < vol1+2(8000) < vol3+4(16000).
	wantOrder := []string{"release.vol0+1.par2", "release.VOL7+1.PAR2", "release.vol1+2.par2", "release.vol3+4.par2"}
	for i, want := range wantOrder {
		if vols[i].ref.Name != want {
			t.Errorf("vols[%d].ref.Name = %q, want %q", i, vols[i].ref.Name, want)
		}
	}
	if vols[0].start != 0 || vols[0].count != 1 {
		t.Errorf("vols[0] (vol0+1) start/count = %d/%d, want 0/1", vols[0].start, vols[0].count)
	}
	if vols[2].start != 1 || vols[2].count != 2 {
		t.Errorf("vols[2] (vol1+2) start/count = %d/%d, want 1/2", vols[2].start, vols[2].count)
	}

	if len(indexFiles) != 2 {
		t.Fatalf("censusPar2Volumes found %d index/other files, want 2: %+v", len(indexFiles), indexFiles)
	}
}

// TestCensusPar2VolumesHyphenConvention covers MultiPar/par2j's
// "volSTART-END" (inclusive end, not a count) naming - found live against a
// real Usenet release during validation. A naive "+"-only regex fails to
// match these at all, silently reporting zero recovery volumes available
// for a release that is fully PAR2-protected.
func TestCensusPar2VolumesHyphenConvention(t *testing.T) {
	files := []storage.Par2FileRef{
		{Name: "release.par2", Size: 4854},               // index file, no vol pattern
		{Name: "release.vol00-01.par2", Size: 34622143},  // start=0, end=1 -> count=2
		{Name: "release.vol01-03.par2", Size: 69241648},  // start=1, end=3 -> count=3
		{Name: "release.VOL03-07.PAR2", Size: 138482016}, // start=3, end=7 -> count=5, case-insensitive
	}

	vols, indexFiles := censusPar2Volumes(files)

	if len(vols) != 3 {
		t.Fatalf("censusPar2Volumes found %d vols, want 3: %+v", len(vols), vols)
	}
	if len(indexFiles) != 1 || indexFiles[0].Name != "release.par2" {
		t.Fatalf("indexFiles = %+v, want just release.par2", indexFiles)
	}

	byName := make(map[string]par2Volume, len(vols))
	for _, v := range vols {
		byName[v.ref.Name] = v
	}

	cases := []struct {
		name         string
		start, count uint32
	}{
		{"release.vol00-01.par2", 0, 2},
		{"release.vol01-03.par2", 1, 3},
		{"release.VOL03-07.PAR2", 3, 5},
	}
	for _, c := range cases {
		v, ok := byName[c.name]
		if !ok {
			t.Fatalf("censusPar2Volumes did not classify %q as a volume", c.name)
		}
		if v.start != c.start || v.count != c.count {
			t.Errorf("%s: start/count = %d/%d, want %d/%d", c.name, v.start, v.count, c.start, c.count)
		}
	}
}

// fakeFetcher returns fixed bytes per message ID and counts how many times
// each ID was actually fetched, so tests can assert on caching behavior.
type fakeFetcher struct {
	data  map[string][]byte
	calls map[string]int
	err   error
}

func newFakeFetcher(data map[string][]byte) *fakeFetcher {
	return &fakeFetcher{data: data, calls: make(map[string]int)}
}

func (f *fakeFetcher) fetch(_ context.Context, messageID string) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.calls[messageID]++
	data, ok := f.data[messageID]
	if !ok {
		return nil, errors.New("fakeFetcher: unknown message id " + messageID)
	}
	return data, nil
}

func repeatByte(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func TestPostedFileFetcherReadRangeSingleSegment(t *testing.T) {
	fetcher := newFakeFetcher(map[string][]byte{
		"<seg1>": repeatByte(0xAA, 1000),
	})
	file := storage.PostedFileRef{
		Name: "f", Size: 1000,
		Segments: []storage.Par2SegmentRef{{MessageID: "<seg1>", Bytes: 1000}},
	}
	pf := newPostedFileFetcher(context.Background(), fetcher.fetch, file, nil)

	got, err := pf.ReadRange(100, 200)
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if len(got) != 200 {
		t.Fatalf("ReadRange returned %d bytes, want 200", len(got))
	}
	for i, b := range got {
		if b != 0xAA {
			t.Fatalf("byte %d = %#x, want 0xAA", i, b)
		}
	}
}

func TestPostedFileFetcherReadRangeCrossesSegments(t *testing.T) {
	fetcher := newFakeFetcher(map[string][]byte{
		"<seg1>": repeatByte(0x11, 100),
		"<seg2>": repeatByte(0x22, 100),
	})
	file := storage.PostedFileRef{
		Name: "f", Size: 200,
		Segments: []storage.Par2SegmentRef{
			{MessageID: "<seg1>", Bytes: 100},
			{MessageID: "<seg2>", Bytes: 100},
		},
	}
	pf := newPostedFileFetcher(context.Background(), fetcher.fetch, file, nil)

	got, err := pf.ReadRange(90, 20) // 10 bytes from seg1, 10 from seg2
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	for i := 0; i < 10; i++ {
		if got[i] != 0x11 {
			t.Errorf("byte %d = %#x, want 0x11 (from seg1)", i, got[i])
		}
	}
	for i := 10; i < 20; i++ {
		if got[i] != 0x22 {
			t.Errorf("byte %d = %#x, want 0x22 (from seg2)", i, got[i])
		}
	}
}

func TestPostedFileFetcherReadRangeZeroPadsPastEOF(t *testing.T) {
	fetcher := newFakeFetcher(map[string][]byte{
		"<seg1>": repeatByte(0x55, 100),
	})
	// File's real length (90) is shorter than the segment's decoded bytes
	// (100) - simulates reading a slice that overlaps the file's padded tail.
	file := storage.PostedFileRef{
		Name: "f", Size: 90,
		Segments: []storage.Par2SegmentRef{{MessageID: "<seg1>", Bytes: 100}},
	}
	pf := newPostedFileFetcher(context.Background(), fetcher.fetch, file, nil)

	got, err := pf.ReadRange(0, 100) // slice size 100, file only has 90 real bytes
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	for i := 0; i < 90; i++ {
		if got[i] != 0x55 {
			t.Errorf("byte %d = %#x, want 0x55", i, got[i])
		}
	}
	for i := 90; i < 100; i++ {
		if got[i] != 0 {
			t.Errorf("byte %d = %#x, want 0 (past EOF padding)", i, got[i])
		}
	}
}

func TestPostedFileFetcherCachesLastSegment(t *testing.T) {
	fetcher := newFakeFetcher(map[string][]byte{
		"<seg1>": repeatByte(0x01, 100),
		"<seg2>": repeatByte(0x02, 100),
	})
	file := storage.PostedFileRef{
		Name: "f", Size: 200,
		Segments: []storage.Par2SegmentRef{
			{MessageID: "<seg1>", Bytes: 100},
			{MessageID: "<seg2>", Bytes: 100},
		},
	}
	pf := newPostedFileFetcher(context.Background(), fetcher.fetch, file, nil)

	// Read from seg1 three times in a row - should only fetch it once.
	for i := 0; i < 3; i++ {
		if _, err := pf.ReadRange(0, 10); err != nil {
			t.Fatalf("ReadRange: %v", err)
		}
	}
	if fetcher.calls["<seg1>"] != 1 {
		t.Errorf("seg1 fetched %d times, want 1 (should be cached)", fetcher.calls["<seg1>"])
	}

	// Now read from seg2 - seg1 falls out of the (single-entry) cache.
	if _, err := pf.ReadRange(100, 10); err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if fetcher.calls["<seg2>"] != 1 {
		t.Errorf("seg2 fetched %d times, want 1", fetcher.calls["<seg2>"])
	}

	// Re-reading seg1 now must re-fetch it (cache holds only the last one).
	if _, err := pf.ReadRange(0, 10); err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if fetcher.calls["<seg1>"] != 2 {
		t.Errorf("seg1 re-fetched %d times after cache eviction, want 2", fetcher.calls["<seg1>"])
	}
}

// --- Tests requiring a real *par2.Index, built from hand-assembled packets ---

func buildTestPar2Packet(t *testing.T, setID [16]byte, typ [16]byte, body []byte) []byte {
	t.Helper()
	for len(body)%4 != 0 {
		body = append(body, 0)
	}
	length := 64 + len(body)
	pkt := make([]byte, length)
	copy(pkt[0:8], []byte("PAR2\x00PKT"))
	binary.LittleEndian.PutUint64(pkt[8:16], uint64(length))
	copy(pkt[32:48], setID[:])
	copy(pkt[48:64], typ[:])
	copy(pkt[64:], body)
	sum := md5.Sum(pkt[32:])
	copy(pkt[16:32], sum[:])
	return pkt
}

var (
	testMainType     = [16]byte{'P', 'A', 'R', ' ', '2', '.', '0', 0, 'M', 'a', 'i', 'n', 0, 0, 0, 0}
	testFileDescType = [16]byte{'P', 'A', 'R', ' ', '2', '.', '0', 0, 'F', 'i', 'l', 'e', 'D', 'e', 's', 'c'}
)

// buildTestIndex assembles a minimal, valid two-file PAR2 index (Main +
// FileDesc packets only, no IFSC) for testing manager-side glue logic that
// doesn't touch checksum verification.
func buildTestIndex(t *testing.T, sliceSize uint64, fileALen, fileBLen uint64) (idx *par2.Index, fileA, fileB [16]byte) {
	t.Helper()
	var setID [16]byte
	setID[0] = 0x42

	fileA = [16]byte{0x01}
	fileB = [16]byte{0x02}

	mainBody := make([]byte, 12+32)
	binary.LittleEndian.PutUint64(mainBody[0:8], sliceSize)
	binary.LittleEndian.PutUint32(mainBody[8:12], 2)
	copy(mainBody[12:28], fileA[:])
	copy(mainBody[28:44], fileB[:])
	mainPkt := buildTestPar2Packet(t, setID, testMainType, mainBody)

	fdBody := func(id [16]byte, length uint64, name string) []byte {
		b := make([]byte, 56+len(name))
		copy(b[0:16], id[:])
		binary.LittleEndian.PutUint64(b[48:56], length)
		copy(b[56:], name)
		return b
	}
	fdAPkt := buildTestPar2Packet(t, setID, testFileDescType, fdBody(fileA, fileALen, "a.rar"))
	fdBPkt := buildTestPar2Packet(t, setID, testFileDescType, fdBody(fileB, fileBLen, "b.rar"))

	idx, err := par2.ParseIndex([]par2.Source{
		{Name: "test.par2", Data: append(append(mainPkt, fdAPkt...), fdBPkt...)},
	})
	if err != nil {
		t.Fatalf("ParseIndex: %v", err)
	}
	return idx, fileA, fileB
}

func TestExtractPostedRangeSingleSlice(t *testing.T) {
	idx, fileA, _ := buildTestIndex(t, 100, 250, 300)

	repaired := map[int64][]byte{
		0: repeatByte(0xAB, 100),
	}
	got, err := extractPostedRange(idx, repaired, fileA, 10, 60)
	if err != nil {
		t.Fatalf("extractPostedRange: %v", err)
	}
	if len(got) != 50 {
		t.Fatalf("len = %d, want 50", len(got))
	}
	for _, b := range got {
		if b != 0xAB {
			t.Fatalf("byte = %#x, want 0xAB", b)
		}
	}
}

func TestExtractPostedRangeSpansMultipleSlices(t *testing.T) {
	// fileA is 250 bytes at slice size 100 -> 3 slices (global 0,1,2).
	idx, fileA, _ := buildTestIndex(t, 100, 250, 300)

	repaired := map[int64][]byte{
		0: repeatByte(0x01, 100),
		1: repeatByte(0x02, 100),
	}
	// Range [90, 150) spans the tail of slice 0 and the head of slice 1.
	got, err := extractPostedRange(idx, repaired, fileA, 90, 150)
	if err != nil {
		t.Fatalf("extractPostedRange: %v", err)
	}
	if len(got) != 60 {
		t.Fatalf("len = %d, want 60", len(got))
	}
	for i := 0; i < 10; i++ {
		if got[i] != 0x01 {
			t.Errorf("byte %d = %#x, want 0x01 (from slice 0)", i, got[i])
		}
	}
	for i := 10; i < 60; i++ {
		if got[i] != 0x02 {
			t.Errorf("byte %d = %#x, want 0x02 (from slice 1)", i, got[i])
		}
	}
}

func TestExtractPostedRangeOnSecondFileUsesCorrectBase(t *testing.T) {
	// fileA: 250 bytes -> 3 slices (global 0,1,2). fileB starts at global 3.
	idx, _, fileB := buildTestIndex(t, 100, 250, 150)

	base, err := idx.SliceBase(fileB)
	if err != nil {
		t.Fatalf("SliceBase: %v", err)
	}
	if base != 3 {
		t.Fatalf("SliceBase(fileB) = %d, want 3", base)
	}

	repaired := map[int64][]byte{
		3: repeatByte(0x99, 100),
	}
	got, err := extractPostedRange(idx, repaired, fileB, 0, 50)
	if err != nil {
		t.Fatalf("extractPostedRange: %v", err)
	}
	for _, b := range got {
		if b != 0x99 {
			t.Fatalf("byte = %#x, want 0x99", b)
		}
	}
}

func TestJobSliceSourceReadSlice(t *testing.T) {
	idx, fileA, fileB := buildTestIndex(t, 100, 250, 150)

	fetcherA := newFakeFetcher(map[string][]byte{"<a1>": repeatByte(0x0A, 250)})
	fetcherB := newFakeFetcher(map[string][]byte{"<b1>": repeatByte(0x0B, 150)})

	fileARef := storage.PostedFileRef{Name: "a.rar", Size: 250, Segments: []storage.Par2SegmentRef{{MessageID: "<a1>", Bytes: 250}}}
	fileBRef := storage.PostedFileRef{Name: "b.rar", Size: 150, Segments: []storage.Par2SegmentRef{{MessageID: "<b1>", Bytes: 150}}}

	src := &jobSliceSource{
		idx: idx,
		fetchers: map[[16]byte]*postedFileFetcher{
			fileA: newPostedFileFetcher(context.Background(), fetcherA.fetch, fileARef, nil),
			fileB: newPostedFileFetcher(context.Background(), fetcherB.fetch, fileBRef, nil),
		},
	}

	// Global slice 0 belongs to fileA.
	data, err := src.ReadSlice(0)
	if err != nil {
		t.Fatalf("ReadSlice(0): %v", err)
	}
	if data[0] != 0x0A {
		t.Errorf("ReadSlice(0)[0] = %#x, want 0x0A (fileA)", data[0])
	}

	// Global slice 3 is fileB's first slice (fileA occupies 0,1,2).
	data, err = src.ReadSlice(3)
	if err != nil {
		t.Fatalf("ReadSlice(3): %v", err)
	}
	if data[0] != 0x0B {
		t.Errorf("ReadSlice(3)[0] = %#x, want 0x0B (fileB)", data[0])
	}
}
