package manager

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

var testIFSCType = [16]byte{'P', 'A', 'R', ' ', '2', '.', '0', 0, 'I', 'F', 'S', 'C', 0, 0, 0, 0}

// buildSingleFileIndexWithIFSC assembles a one-file PAR2 index (Main +
// FileDesc + IFSC) whose IFSC checksums are computed from sliceData - one
// entry per slice, each already padded to sliceSize.
func buildSingleFileIndexWithIFSC(t *testing.T, sliceSize int, fileID [16]byte, name string, sliceData [][]byte) *par2.Index {
	t.Helper()
	var setID [16]byte
	setID[0] = 0x42

	total := 0
	for _, s := range sliceData {
		if len(s) != sliceSize {
			t.Fatalf("slice data must be exactly sliceSize (%d), got %d", sliceSize, len(s))
		}
		total += sliceSize
	}

	mainBody := make([]byte, 12+16)
	binary.LittleEndian.PutUint64(mainBody[0:8], uint64(sliceSize))
	binary.LittleEndian.PutUint32(mainBody[8:12], 1)
	copy(mainBody[12:28], fileID[:])
	mainPkt := buildTestPar2Packet(t, setID, testMainType, mainBody)

	fdBody := make([]byte, 56+len(name))
	copy(fdBody[0:16], fileID[:])
	binary.LittleEndian.PutUint64(fdBody[48:56], uint64(total))
	copy(fdBody[56:], name)
	fdPkt := buildTestPar2Packet(t, setID, testFileDescType, fdBody)

	ifscBody := make([]byte, 16+20*len(sliceData))
	copy(ifscBody[0:16], fileID[:])
	for i, s := range sliceData {
		off := 16 + i*20
		sum := md5.Sum(s)
		copy(ifscBody[off:off+16], sum[:])
		binary.LittleEndian.PutUint32(ifscBody[off+16:off+20], crc32.ChecksumIEEE(s))
	}
	ifscPkt := buildTestPar2Packet(t, setID, testIFSCType, ifscBody)

	data := append(append(append([]byte{}, mainPkt...), fdPkt...), ifscPkt...)
	idx, err := par2.ParseIndex([]par2.Source{{Name: "test.par2", Data: data}})
	if err != nil {
		t.Fatalf("ParseIndex: %v", err)
	}
	return idx
}

// stubCacheReader serves bytes for a file from a fixed in-memory copy,
// honoring the requested offset - unlike fakeCacheReader it lets a test
// script both a correct and a stale (zero-fill) cache copy.
type stubCacheReader struct {
	content map[string][]byte
	calls   int
}

func (r *stubCacheReader) PeekCachedRange(_, filename string, p []byte, off int64) bool {
	r.calls++
	buf, ok := r.content[filename]
	if !ok || off < 0 || off+int64(len(p)) > int64(len(buf)) {
		return false
	}
	copy(p, buf[off:off+int64(len(p))])
	return true
}

// jobSourceForIFSC wires a jobSliceSource around one posted file "a.rar" of
// two 100-byte slices, with the given cache content and NNTP article bytes.
func jobSourceForIFSC(t *testing.T, cacheContent map[string][]byte, article []byte) (*jobSliceSource, *fakeFetcher, *stubCacheReader) {
	t.Helper()
	const sliceSize = 100
	fileID := [16]byte{0x01}
	slice0 := repeatByte(0x11, sliceSize)
	slice1 := repeatByte(0x22, sliceSize)
	idx := buildSingleFileIndexWithIFSC(t, sliceSize, fileID, "a.rar", [][]byte{slice0, slice1})

	reader := &stubCacheReader{content: cacheContent}
	cacheSrc := &cacheSlicedSource{
		reader:    reader,
		entryName: "Some.Release",
		byMessageID: map[string][]cacheMapEntry{
			"<a1>": {{fileName: "a.rar", dataStart: 0, dataEnd: 200, outputStart: 0}},
		},
		deadRanges: map[string][]outputByteRange{},
	}

	fetcher := newFakeFetcher(map[string][]byte{"<a1>": article})
	fileRef := storage.PostedFileRef{Name: "a.rar", Size: 200, Segments: []storage.Par2SegmentRef{{MessageID: "<a1>", Bytes: 200}}}
	pf := newPostedFileFetcher(context.Background(), uncheckedPosted(fetcher.fetch), fileRef, cacheSrc, 200, zerolog.Nop())

	return &jobSliceSource{
		idx:      idx,
		fetchers: map[[16]byte]*postedFileFetcher{fileID: pf},
		logger:   zerolog.Nop(),
	}, fetcher, reader
}

func TestJobSliceSourceCacheHitPassesIFSC(t *testing.T) {
	good := append(repeatByte(0x11, 100), repeatByte(0x22, 100)...)
	src, fetcher, reader := jobSourceForIFSC(t, map[string][]byte{"a.rar": good}, good)

	data, err := src.ReadSlice(0)
	if err != nil {
		t.Fatalf("ReadSlice(0): %v", err)
	}
	for i, b := range data {
		if b != 0x11 {
			t.Fatalf("byte %d = %#x, want 0x11 (cached, IFSC-verified)", i, b)
		}
	}
	if reader.calls == 0 {
		t.Errorf("expected the cache to be consulted")
	}
	if fetcher.calls["<a1>"] != 0 {
		t.Errorf("cache hit verified clean must not refetch, got %d fetches", fetcher.calls["<a1>"])
	}
}

func TestJobSliceSourceStaleZeroCacheFailsIFSCAndRefetches(t *testing.T) {
	stale := make([]byte, 200) // whole file zero-padded in the cache
	good := append(repeatByte(0x11, 100), repeatByte(0x22, 100)...)
	src, fetcher, _ := jobSourceForIFSC(t, map[string][]byte{"a.rar": stale}, good)

	data, err := src.ReadSlice(0)
	if err != nil {
		t.Fatalf("ReadSlice(0): %v", err)
	}
	for i, b := range data {
		if b != 0x11 {
			t.Fatalf("byte %d = %#x, want 0x11 - stale cache slice should have been refetched from NNTP", i, b)
		}
	}
	if fetcher.calls["<a1>"] != 1 {
		t.Errorf("stale cache slice must refetch exactly once, got %d", fetcher.calls["<a1>"])
	}
}

func TestJobSliceSourcePartialZeroCacheFailsIFSCAndRefetches(t *testing.T) {
	// First half of slice 0 real, second half zero-fill (edge of a padded
	// segment) - still an IFSC mismatch, still refetched.
	partial := make([]byte, 200)
	copy(partial, repeatByte(0x11, 50))
	copy(partial[100:], repeatByte(0x22, 100))
	good := append(repeatByte(0x11, 100), repeatByte(0x22, 100)...)
	src, fetcher, _ := jobSourceForIFSC(t, map[string][]byte{"a.rar": partial}, good)

	data, err := src.ReadSlice(0)
	if err != nil {
		t.Fatalf("ReadSlice(0): %v", err)
	}
	for i, b := range data {
		if b != 0x11 {
			t.Fatalf("byte %d = %#x, want 0x11 after refetch", i, b)
		}
	}
	if fetcher.calls["<a1>"] != 1 {
		t.Errorf("partial-zero cache slice must refetch once, got %d", fetcher.calls["<a1>"])
	}
}

func TestJobSliceSourceCacheMissUsesNNTPUnchanged(t *testing.T) {
	good := append(repeatByte(0x11, 100), repeatByte(0x22, 100)...)
	// No cache content for "a.rar" -> stubCacheReader misses.
	src, fetcher, reader := jobSourceForIFSC(t, map[string][]byte{}, good)

	data, err := src.ReadSlice(1)
	if err != nil {
		t.Fatalf("ReadSlice(1): %v", err)
	}
	for i, b := range data {
		if b != 0x22 {
			t.Fatalf("byte %d = %#x, want 0x22 (fetched)", i, b)
		}
	}
	if reader.calls == 0 {
		t.Errorf("cache should still have been consulted (and missed)")
	}
	if fetcher.calls["<a1>"] != 1 {
		t.Errorf("cache miss should fetch once, got %d", fetcher.calls["<a1>"])
	}
}
