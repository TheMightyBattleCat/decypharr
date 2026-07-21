package manager

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// fakeCacheReader is a scriptable dfsCacheRangeReader: present reports true
// for any (entryName, filename, off, len) tuple recorded in it, regardless
// of what other logic decides - used to prove the dead-range exclusion is
// checked BEFORE ever asking the cache, not merely that a real cache
// happens to also refuse to serve padding.
type fakeCacheReader struct {
	present map[string]bool // key: filename, value: whether ANY read for it succeeds
	calls   int
}

func (f *fakeCacheReader) PeekCachedRange(entryName, filename string, p []byte, off int64) bool {
	f.calls++
	if !f.present[filename] {
		return false
	}
	for i := range p {
		p[i] = 0xCA
	}
	return true
}

func TestBuildCacheSegmentMapIndexesOnlyStoredFiles(t *testing.T) {
	nzb := &storage.NZB{
		Files: []storage.NZBFile{
			{
				Name:     "movie.mkv",
				IsStored: true,
				Segments: []storage.NZBSegment{
					{MessageID: "<a>", StartOffset: 0, EndOffset: 99, SegmentDataStart: 32, Bytes: 100},
					{MessageID: "<b>", StartOffset: 100, EndOffset: 199, SegmentDataStart: 0, Bytes: 100},
				},
			},
			{
				Name:     "compressed.mkv",
				IsStored: false, // compressed - never mappable
				Segments: []storage.NZBSegment{
					{MessageID: "<c>", StartOffset: 0, EndOffset: 99, SegmentDataStart: 0, Bytes: 100},
				},
			},
			{
				Name:      "deleted.mkv",
				IsStored:  true,
				IsDeleted: true, // removed - must not be indexed
				Segments: []storage.NZBSegment{
					{MessageID: "<d>", StartOffset: 0, EndOffset: 99, SegmentDataStart: 0, Bytes: 100},
				},
			},
		},
	}

	m := buildCacheSegmentMap(nzb)

	if _, ok := m["<a>"]; !ok {
		t.Errorf("expected <a> (stored file segment) to be indexed")
	}
	if _, ok := m["<b>"]; !ok {
		t.Errorf("expected <b> (stored file segment) to be indexed")
	}
	if _, ok := m["<c>"]; ok {
		t.Errorf("compressed file's segment <c> must not be indexed - no direct byte mapping exists")
	}
	if _, ok := m["<d>"]; ok {
		t.Errorf("deleted file's segment <d> must not be indexed")
	}

	entry := m["<a>"][0]
	if entry.fileName != "movie.mkv" || entry.dataStart != 32 || entry.dataEnd != 132 || entry.outputStart != 0 {
		t.Errorf("m[<a>][0] = %+v, want {movie.mkv 32 132 0}", entry)
	}
}

func TestBuildDeadOutputRangesMapsByFileSegmentIndex(t *testing.T) {
	nzb := &storage.NZB{
		Files: []storage.NZBFile{
			{
				Name: "movie.mkv",
				Segments: []storage.NZBSegment{
					{MessageID: "<a>", StartOffset: 0, EndOffset: 99},
					{MessageID: "<b>", StartOffset: 100, EndOffset: 199},
					{MessageID: "<c>", StartOffset: 200, EndOffset: 299},
				},
			},
		},
	}
	pending := map[string][]overlay.DeadSegment{
		"movie.mkv": {
			{Index: 1, Status: overlay.StatusPadded}, // segment <b>, bytes [100,200)
		},
	}

	dead := buildDeadOutputRanges(nzb, pending)
	rs, ok := dead["movie.mkv"]
	if !ok || len(rs) != 1 {
		t.Fatalf("dead[movie.mkv] = %+v, want exactly one range", rs)
	}
	if rs[0].start != 100 || rs[0].end != 200 {
		t.Errorf("dead range = [%d,%d), want [100,200)", rs[0].start, rs[0].end)
	}
}

func TestBuildDeadOutputRangesSkipsOutOfBoundsIndex(t *testing.T) {
	nzb := &storage.NZB{
		Files: []storage.NZBFile{
			{Name: "movie.mkv", Segments: []storage.NZBSegment{{MessageID: "<a>", StartOffset: 0, EndOffset: 99}}},
		},
	}
	pending := map[string][]overlay.DeadSegment{
		"movie.mkv": {{Index: 5}}, // out of range - must be skipped, not panic
	}
	dead := buildDeadOutputRanges(nzb, pending)
	if len(dead["movie.mkv"]) != 0 {
		t.Errorf("expected no dead ranges for an out-of-bounds index, got %+v", dead["movie.mkv"])
	}
}

// baseCacheSource builds a cacheSlicedSource over one message <a> mapped
// entirely (article-local [0,100)) to movie.mkv's output range [1000,1100),
// with the given dead ranges and cache-present set.
func baseCacheSource(t *testing.T, deadRanges []outputByteRange, cachedFiles map[string]bool) (*cacheSlicedSource, *fakeCacheReader) {
	t.Helper()
	reader := &fakeCacheReader{present: cachedFiles}
	src := &cacheSlicedSource{
		reader:    reader,
		entryName: "Some.Release",
		byMessageID: map[string][]cacheMapEntry{
			"<a>": {{fileName: "movie.mkv", dataStart: 0, dataEnd: 100, outputStart: 1000}},
		},
		deadRanges: map[string][]outputByteRange{"movie.mkv": deadRanges},
	}
	return src, reader
}

func TestReadCachedResolvesFromCacheWhenMappedAndPresentAndNotDead(t *testing.T) {
	src, reader := baseCacheSource(t, nil, map[string]bool{"movie.mkv": true})

	data, ok := src.readCached("<a>", 10, 20) // article-local [10,30) -> output [1010,1030)
	if !ok {
		t.Fatalf("readCached: want ok=true (mapped, cached, not dead)")
	}
	if len(data) != 20 {
		t.Fatalf("readCached returned %d bytes, want 20", len(data))
	}
	for i, b := range data {
		if b != 0xCA {
			t.Fatalf("byte %d = %#x, want 0xCA (from fake cache)", i, b)
		}
	}
	if reader.calls != 1 {
		t.Errorf("expected exactly one cache read, got %d", reader.calls)
	}
}

func TestReadCachedNeverResolvesADeadRangeEvenWhenCachePresent(t *testing.T) {
	// Output range [1010,1030) (article-local [10,30)) overlaps the dead
	// range [1015,1020) - must refuse regardless of the fake cache reader
	// unconditionally reporting "present" for this file.
	src, reader := baseCacheSource(t, []outputByteRange{{start: 1015, end: 1020}}, map[string]bool{"movie.mkv": true})

	if _, ok := src.readCached("<a>", 10, 20); ok {
		t.Fatalf("readCached: want ok=false - the requested range overlaps a dead/padded segment")
	}
	if reader.calls != 0 {
		t.Errorf("must not even ask the cache once a dead-range overlap is found, got %d calls", reader.calls)
	}
}

func TestReadCachedFallsThroughWhenNotYetCached(t *testing.T) {
	src, _ := baseCacheSource(t, nil, map[string]bool{"movie.mkv": false})

	if _, ok := src.readCached("<a>", 10, 20); ok {
		t.Fatalf("readCached: want ok=false - not present in the cache")
	}
}

func TestReadCachedFallsThroughWhenNoMapping(t *testing.T) {
	src, reader := baseCacheSource(t, nil, map[string]bool{"movie.mkv": true})

	// <z> has no entry in byMessageID at all - e.g. a compressed archive
	// member, or an article this repair pass never indexed.
	if _, ok := src.readCached("<z>", 0, 10); ok {
		t.Fatalf("readCached: want ok=false - no cache mapping for this message id")
	}
	if reader.calls != 0 {
		t.Errorf("must not touch the cache reader with no mapping, got %d calls", reader.calls)
	}
}

func TestReadCachedFallsThroughWhenRangeSpansIntoUnmappedRegion(t *testing.T) {
	// Mapping only covers article-local [0,100) (e.g. a RAR header precedes
	// it in a fuller article); a request reaching past that - [90,110) -
	// isn't FULLY covered and must fall through to a real fetch rather than
	// silently return a partial/wrong slice.
	src, reader := baseCacheSource(t, nil, map[string]bool{"movie.mkv": true})

	if _, ok := src.readCached("<a>", 90, 20); ok {
		t.Fatalf("readCached: want ok=false - requested range isn't fully covered by the mapping")
	}
	if reader.calls != 0 {
		t.Errorf("must not touch the cache reader for a partially-mapped range, got %d calls", reader.calls)
	}
}

func TestReadCachedNilSourceAlwaysMisses(t *testing.T) {
	var src *cacheSlicedSource
	if _, ok := src.readCached("<a>", 0, 10); ok {
		t.Fatalf("nil *cacheSlicedSource.readCached must always miss")
	}

	src = &cacheSlicedSource{reader: nil}
	if _, ok := src.readCached("<a>", 0, 10); ok {
		t.Fatalf("readCached with a nil reader must always miss")
	}
}

func TestReadCachedIncrementsCacheByteCounter(t *testing.T) {
	src, _ := baseCacheSource(t, nil, map[string]bool{"movie.mkv": true})
	var counted int64
	src.cacheBytes = &counted

	if _, ok := src.readCached("<a>", 0, 42); !ok {
		t.Fatalf("readCached: want ok=true")
	}
	if counted != 42 {
		t.Errorf("cacheBytes counter = %d, want 42", counted)
	}
}
