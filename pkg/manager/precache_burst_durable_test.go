package manager

import (
	"context"
	"fmt"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

const burstSegSize = 100

// scratchSource is a burstSource whose reader keeps only the last `capacity`
// segments it fetched - the usenet reader's scratch SegmentCache in
// miniature. A read-back of an evicted segment downloads it again, as the
// real reader does.
type scratchSource struct {
	nzb       *storage.NZB
	pending   map[string][]overlay.DeadSegment
	capacity  int
	lru       []int // oldest first
	downloads map[int]int
	readBacks map[int]int
	released  map[int]int // segment -> times released from the scratch cache
}

func newScratchSource(filename string, segments, capacity int) *scratchSource {
	f := storage.NZBFile{Name: filename}
	for i := range segments {
		f.Segments = append(f.Segments, storage.NZBSegment{
			Number:      i,
			StartOffset: int64(i * burstSegSize),
			EndOffset:   int64((i+1)*burstSegSize - 1),
		})
	}
	return &scratchSource{
		nzb:       &storage.NZB{Files: []storage.NZBFile{f}},
		capacity:  capacity,
		downloads: map[int]int{},
		readBacks: map[int]int{},
	}
}

func (s *scratchSource) fetch(idx int) {
	for i, v := range s.lru {
		if v == idx {
			s.lru = append(s.lru[:i], s.lru[i+1:]...)
			s.lru = append(s.lru, idx)
			return
		}
	}
	s.downloads[idx]++
	s.lru = append(s.lru, idx)
	if len(s.lru) > s.capacity {
		s.lru = s.lru[1:]
	}
}

func (s *scratchSource) GetNZB(string) (*storage.NZB, error) { return s.nzb, nil }

func (s *scratchSource) OverlayPendingRepair(string) (map[string][]overlay.DeadSegment, error) {
	return s.pending, nil
}

func (s *scratchSource) ReadCachedAt(_ context.Context, _, _ string, p []byte, off int64) (int, error) {
	idx := int(off / burstSegSize)
	s.readBacks[idx]++
	s.fetch(idx)
	for i := range p {
		p[i] = byte(idx + 1)
	}
	return len(p), nil
}

// ReleaseCached drops the segments wholly inside [off, off+length), as the
// real reader does.
func (s *scratchSource) ReleaseCached(_, _ string, off, length int64) int {
	if s.released == nil {
		s.released = map[int]int{}
	}
	n := 0
	for idx := int((off + burstSegSize - 1) / burstSegSize); int64((idx+1)*burstSegSize) <= off+length; idx++ {
		for i, v := range s.lru {
			if v == idx {
				s.lru = append(s.lru[:i], s.lru[i+1:]...)
				s.released[idx]++
				n++
				break
			}
		}
	}
	return n
}

func (s *scratchSource) ReadAhead(ctx context.Context, nzoID, filename string, from int64, concurrency int) error {
	return s.ReadAheadRange(ctx, nzoID, filename, from, -1, concurrency)
}

func (s *scratchSource) ReadAheadRange(_ context.Context, _, _ string, off, length int64, _ int) error {
	size := int64(len(s.nzb.Files[0].Segments) * burstSegSize)
	if length < 0 {
		length = size - off
	}
	for idx := int(off / burstSegSize); int64(idx*burstSegSize) < off+length; idx++ {
		s.fetch(idx)
	}
	return nil
}

func (s *scratchSource) totalDownloads() int {
	n := 0
	for _, c := range s.downloads {
		n += c
	}
	return n
}

// durableStore is the DFS cache: a dfsCacheRangeWriter and a
// dfsCacheRangePresence over the same bytes, answering presence byte by byte
// for exactly the range asked about, as the real range tracker does.
type durableStore struct {
	have   map[int64]bool // byte offset -> durably cached
	writes map[int64]int  // write offset -> writes
}

func newDurableStore() *durableStore {
	return &durableStore{have: map[int64]bool{}, writes: map[int64]int{}}
}

func (d *durableStore) seed(off, length int64) {
	for b := off; b < off+length; b++ {
		d.have[b] = true
	}
}

func (d *durableStore) WriteCachedRange(_, _ string, _ int64, p []byte, off int64) error {
	d.writes[off]++
	d.seed(off, int64(len(p)))
	return nil
}

func (d *durableStore) HasCachedRange(_, _ string, off, length int64) bool {
	for b := off; b < off+length; b++ {
		if !d.have[b] {
			return false
		}
	}
	return true
}

// holes lists the byte ranges of [0, size) not durably cached.
func (d *durableStore) holes(size int64) [][2]int64 {
	var out [][2]int64
	for b := int64(0); b < size; b++ {
		if d.have[b] {
			continue
		}
		if n := len(out); n > 0 && out[n-1][1] == b {
			out[n-1][1] = b + 1
		} else {
			out = append(out, [2]int64{b, b + 1})
		}
	}
	return out
}

// burstChunk cuts through segments (burstSegSize 100), as a chunk of tens of
// megabytes cuts through ~700 KB articles. burstChunks ends each chunk on
// the segment boundary after the cut, so the chunks really fetched are
// alignedChunk long.
const (
	burstChunk   = 250
	alignedChunk = 300
)

// Chunk by chunk, every segment is downloaded once and persisted from the
// scratch cache, the whole file ends up durable, and a segment straddling a
// chunk boundary is written once. Fetching the whole file first and
// persisting after - the order before - downloads a file larger than the
// scratch cache twice.
func TestBurstChunksPersistWithoutRefetch(t *testing.T) {
	const filename = "episode.mkv"
	const size = 10 * burstSegSize
	src := newScratchSource(filename, 10, 4)
	store := newDurableStore()

	res, err := burstChunks(context.Background(), src, store, store, "Entry", "hash", filename,
		0, size, 4, burstChunk, zerolog.Nop(), burstOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if got := src.totalDownloads(); got != 10 {
		t.Fatalf("downloads = %d, want 10 (one per segment): %v", got, src.downloads)
	}
	if h := store.holes(size); len(h) != 0 {
		t.Fatalf("durable holes %v, want none", h)
	}
	for off, n := range store.writes {
		if n != 1 {
			t.Fatalf("segment at %d written %d times, want once", off, n)
		}
	}
	if res.persist.segmentsWritten != 10 || res.fetched != size || res.skipped != 0 {
		t.Fatalf("written %d fetched %d skipped %d, want 10, %d, 0", res.persist.segmentsWritten, res.fetched, res.skipped, size)
	}

	// The old order, for contrast. The walk from the start evicts the tail
	// the burst left behind before reaching it, so every segment comes down
	// twice.
	old := newScratchSource(filename, 10, 3)
	if err := old.ReadAhead(context.Background(), "hash", filename, 0, 4); err != nil {
		t.Fatal(err)
	}
	persistDurableRanges(context.Background(), old, newDurableStore(), "Entry", "hash", filename, size, zerolog.Nop())
	if got := old.totalDownloads(); got != 20 {
		t.Fatalf("fetch-then-persist downloads = %d, want 20 (every segment twice)", got)
	}
}

// A chunk the durable cache already holds is not fetched, and nothing in it
// is written again.
func TestBurstChunksSkipDurableChunks(t *testing.T) {
	const filename = "episode.mkv"
	const size = 10 * burstSegSize
	src := newScratchSource(filename, 10, 4)
	store := newDurableStore()
	store.seed(200, 400) // segments 2-5; covers the chunk [300, 600)

	res, err := burstChunks(context.Background(), src, store, store, "Entry", "hash", filename,
		0, size, 4, burstChunk, zerolog.Nop(), burstOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if res.skipped != alignedChunk || res.fetched != size-alignedChunk {
		t.Fatalf("skipped %d fetched %d, want %d and %d", res.skipped, res.fetched, alignedChunk, size-alignedChunk)
	}
	for _, seg := range []int{3, 4, 5} { // the skipped chunk
		if src.downloads[seg] != 0 {
			t.Fatalf("segment %d, inside the durable chunk, was downloaded", seg)
		}
	}
	for seg := 2; seg < 6; seg++ {
		if store.writes[int64(seg*burstSegSize)] != 0 || src.readBacks[seg] != 0 {
			t.Fatalf("durable segment %d read back or written again", seg)
		}
	}
	if h := store.holes(size); len(h) != 0 {
		t.Fatalf("durable holes %v, want none", h)
	}
}

// The durable cache holds a chunk's segments except the tail of its last
// one. The chunk is fetched, since it is not all there, but only that
// segment is read back and written: the others are found durable and left.
func TestBurstChunksPartlyDurableSegmentIsCompleted(t *testing.T) {
	const filename = "episode.mkv"
	const size = 10 * burstSegSize
	src := newScratchSource(filename, 10, 4)
	store := newDurableStore()
	store.seed(0, burstChunk) // segments 0, 1 and the head of 2 [200, 250)

	res, err := burstChunks(context.Background(), src, store, store, "Entry", "hash", filename,
		0, size, 4, burstChunk, zerolog.Nop(), burstOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if res.skipped != 0 {
		t.Fatalf("skipped %d, want 0: the first chunk lacks the tail of segment 2", res.skipped)
	}
	if h := store.holes(size); len(h) != 0 {
		t.Fatalf("durable holes %v, want none", h)
	}
	if store.writes[2*burstSegSize] != 1 {
		t.Fatalf("partly durable segment 2 written %d times, want once", store.writes[2*burstSegSize])
	}
	for _, seg := range []int{0, 1} {
		if store.writes[int64(seg*burstSegSize)] != 0 || src.readBacks[seg] != 0 {
			t.Fatalf("durable segment %d read back or written again", seg)
		}
	}
}

// Chunks end on segment boundaries, so no segment is fetched with one chunk,
// released, and fetched again with the next - and every segment made
// durable is released from the scratch cache exactly once.
func TestBurstChunksReleaseEachDurableSegmentOnce(t *testing.T) {
	const filename = "episode.mkv"
	const size = 10 * burstSegSize
	src := newScratchSource(filename, 10, 4)
	store := newDurableStore()

	res, err := burstChunks(context.Background(), src, store, store, "Entry", "hash", filename,
		0, size, 4, burstChunk, zerolog.Nop(), burstOpts{})
	if err != nil {
		t.Fatal(err)
	}
	for seg := range 10 {
		if src.downloads[seg] != 1 {
			t.Fatalf("segment %d downloaded %d times, want once", seg, src.downloads[seg])
		}
		if src.released[seg] != 1 {
			t.Fatalf("segment %d released %d times, want once", seg, src.released[seg])
		}
	}
	if len(src.lru) != 0 {
		t.Fatalf("scratch cache still holds segments %v after the burst", src.lru)
	}
	if res.persist.released != 10 {
		t.Fatalf("persist stats count %d released, want 10", res.persist.released)
	}
}

// A segment held for repair is not durable, so it is not released either:
// it stays in the scratch cache, and the durable segments either side of it
// are released as two runs.
func TestBurstChunksPendingRepairNotReleased(t *testing.T) {
	const filename = "episode.mkv"
	const size = 6 * burstSegSize
	src := newScratchSource(filename, 6, 6)
	src.pending = map[string][]overlay.DeadSegment{filename: {{Index: 1}}}
	store := newDurableStore()

	if _, err := burstChunks(context.Background(), src, store, store, "Entry", "hash", filename,
		0, size, 4, burstChunk, zerolog.Nop(), burstOpts{}); err != nil {
		t.Fatal(err)
	}
	if src.released[1] != 0 {
		t.Fatal("the segment held for repair was released from the scratch cache")
	}
	if len(src.lru) != 1 || src.lru[0] != 1 {
		t.Fatalf("scratch cache holds %v, want only the pending segment 1", src.lru)
	}
	for _, seg := range []int{0, 2, 3, 4, 5} {
		if src.released[seg] != 1 {
			t.Fatalf("durable segment %d released %d times, want once", seg, src.released[seg])
		}
	}
}

// A segment held for repair - possibly zero-filled in the scratch cache - is
// fetched with its chunk but never persisted.
func TestBurstChunksPendingRepairNotPersisted(t *testing.T) {
	const filename = "episode.mkv"
	const size = 6 * burstSegSize
	src := newScratchSource(filename, 6, 4)
	src.pending = map[string][]overlay.DeadSegment{filename: {{Index: 2}}} // straddles the first boundary
	store := newDurableStore()

	res, err := burstChunks(context.Background(), src, store, store, "Entry", "hash", filename,
		0, size, 4, burstChunk, zerolog.Nop(), burstOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if store.writes[2*burstSegSize] != 0 || src.readBacks[2] != 0 {
		t.Fatal("segment pending repair was read back or persisted")
	}
	if h := store.holes(size); len(h) != 1 || h[0] != [2]int64{200, 300} {
		t.Fatalf("durable holes %v, want only the pending segment [200 300]", h)
	}
	if res.persist.segmentsWritten != 5 {
		t.Fatalf("persisted %d segments, want 5", res.persist.segmentsWritten)
	}
}

// A burst from mid-file persists the segment `from` falls inside and nothing
// before it - the earlier segments are the playback's business, and reading
// them back would download them.
func TestBurstChunksFromMidSegment(t *testing.T) {
	const filename = "episode.mkv"
	const size = 8 * burstSegSize
	src := newScratchSource(filename, 8, 4)
	store := newDurableStore()

	from := int64(2*burstSegSize + 50)
	if _, err := burstChunks(context.Background(), src, store, store, "Entry", "hash", filename,
		from, size, 4, burstChunk, zerolog.Nop(), burstOpts{}); err != nil {
		t.Fatal(err)
	}
	for seg := range 2 {
		if src.downloads[seg] != 0 || src.readBacks[seg] != 0 {
			t.Fatalf("segment %d before `from` was downloaded or read back", seg)
		}
	}
	if h := store.holes(size); len(h) != 1 || h[0] != [2]int64{0, 2 * burstSegSize} {
		t.Fatalf("durable holes %v, want only what precedes from's segment [0 200]", h)
	}
	if got := src.totalDownloads(); got != 6 {
		t.Fatalf("downloads = %d, want 6", got)
	}
}

// The whole-file persist pass (after a repair) skips what the durable cache
// already holds, so it reads back only the repaired segment.
func TestPersistDurableSpanSkipsCachedSegments(t *testing.T) {
	const filename = "episode.mkv"
	src := newScratchSource(filename, 5, 2)
	store := newDurableStore()
	store.seed(0, 3*burstSegSize)
	store.seed(4*burstSegSize, burstSegSize)
	persistDurableSpan(context.Background(), src, store, store, "Entry", "hash", filename, 5*burstSegSize, 0, 5*burstSegSize, zerolog.Nop())
	if len(src.readBacks) != 1 || src.readBacks[3] != 1 {
		t.Fatalf("read-backs = %v, want only segment 3", src.readBacks)
	}
	if fmt.Sprint(store.writes) != fmt.Sprint(map[int64]int{300: 1}) {
		t.Fatalf("writes = %v, want only offset 300", store.writes)
	}
}
