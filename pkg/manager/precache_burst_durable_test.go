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
// dfsCacheRangePresence over the same bytes.
type durableStore struct {
	have   map[int64]bool // segment start offset -> durably cached
	writes map[int64]int
}

func newDurableStore() *durableStore {
	return &durableStore{have: map[int64]bool{}, writes: map[int64]int{}}
}

func (d *durableStore) WriteCachedRange(_, _ string, _ int64, p []byte, off int64) error {
	d.writes[off]++
	d.have[off] = true
	return nil
}

func (d *durableStore) HasCachedRange(_, _ string, off, length int64) bool {
	for seg := off - off%burstSegSize; seg < off+length; seg += burstSegSize {
		if !d.have[seg] {
			return false
		}
	}
	return true
}

// Chunk by chunk, every segment is downloaded once and persisted from the
// scratch cache. Fetching the whole file first and persisting after - the
// order before - downloads a file larger than the scratch cache twice.
func TestBurstChunksPersistWithoutRefetch(t *testing.T) {
	const filename = "episode.mkv"
	src := newScratchSource(filename, 10, 3)
	store := newDurableStore()

	res, err := burstChunks(context.Background(), src, store, store, "Entry", "hash", filename,
		0, 10*burstSegSize, 4, 2*burstSegSize, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if got := src.totalDownloads(); got != 10 {
		t.Fatalf("downloads = %d, want 10 (one per segment): %v", got, src.downloads)
	}
	if len(store.writes) != 10 || res.persist.segmentsWritten != 10 {
		t.Fatalf("persisted %d segments (%d written), want 10", len(store.writes), res.persist.segmentsWritten)
	}
	if res.fetched != 10*burstSegSize || res.skipped != 0 {
		t.Fatalf("fetched %d skipped %d, want %d and 0", res.fetched, res.skipped, 10*burstSegSize)
	}

	// The old order, for contrast.
	old := newScratchSource(filename, 10, 3)
	if err := old.ReadAhead(context.Background(), "hash", filename, 0, 4); err != nil {
		t.Fatal(err)
	}
	persistDurableRanges(context.Background(), old, newDurableStore(), "Entry", "hash", filename, 10*burstSegSize, zerolog.Nop())
	// The walk from the start evicts the tail the burst left behind before
	// reaching it, so every segment comes down twice.
	if got := old.totalDownloads(); got != 20 {
		t.Fatalf("fetch-then-persist downloads = %d, want 20 (every segment twice)", got)
	}
}

// A chunk the durable cache already holds is neither fetched nor read back.
func TestBurstChunksSkipDurableChunks(t *testing.T) {
	const filename = "episode.mkv"
	src := newScratchSource(filename, 10, 3)
	store := newDurableStore()
	for seg := 2; seg < 6; seg++ {
		store.have[int64(seg*burstSegSize)] = true
	}

	res, err := burstChunks(context.Background(), src, store, store, "Entry", "hash", filename,
		0, 10*burstSegSize, 4, 2*burstSegSize, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	for seg := 2; seg < 6; seg++ {
		if src.downloads[seg] != 0 || src.readBacks[seg] != 0 || store.writes[int64(seg*burstSegSize)] != 0 {
			t.Fatalf("durable segment %d touched: downloads %d, read-backs %d, writes %d",
				seg, src.downloads[seg], src.readBacks[seg], store.writes[int64(seg*burstSegSize)])
		}
	}
	if res.skipped != 4*burstSegSize || res.fetched != 6*burstSegSize {
		t.Fatalf("skipped %d fetched %d, want %d and %d", res.skipped, res.fetched, 4*burstSegSize, 6*burstSegSize)
	}
	if got := src.totalDownloads(); got != 6 {
		t.Fatalf("downloads = %d, want 6", got)
	}
}

// A segment held for repair - possibly zero-filled in the scratch cache - is
// fetched with its chunk but never persisted.
func TestBurstChunksPendingRepairNotPersisted(t *testing.T) {
	const filename = "episode.mkv"
	src := newScratchSource(filename, 6, 3)
	src.pending = map[string][]overlay.DeadSegment{filename: {{Index: 3}}}
	store := newDurableStore()

	res, err := burstChunks(context.Background(), src, store, store, "Entry", "hash", filename,
		0, 6*burstSegSize, 4, 2*burstSegSize, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if store.writes[3*burstSegSize] != 0 || src.readBacks[3] != 0 {
		t.Fatal("segment pending repair was read back or persisted")
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
	src := newScratchSource(filename, 8, 3)
	store := newDurableStore()

	from := int64(2*burstSegSize + 50)
	if _, err := burstChunks(context.Background(), src, store, store, "Entry", "hash", filename,
		from, 8*burstSegSize, 4, 2*burstSegSize, zerolog.Nop()); err != nil {
		t.Fatal(err)
	}
	for seg := range 2 {
		if src.downloads[seg] != 0 || src.readBacks[seg] != 0 {
			t.Fatalf("segment %d before `from` was downloaded or read back", seg)
		}
	}
	for seg := 2; seg < 8; seg++ {
		if store.writes[int64(seg*burstSegSize)] != 1 {
			t.Fatalf("segment %d persisted %d times, want 1", seg, store.writes[int64(seg*burstSegSize)])
		}
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
	for seg := range 5 {
		if seg != 3 {
			store.have[int64(seg*burstSegSize)] = true
		}
	}
	persistDurableSpan(context.Background(), src, store, store, "Entry", "hash", filename, 5*burstSegSize, 0, 5*burstSegSize, zerolog.Nop())
	if len(src.readBacks) != 1 || src.readBacks[3] != 1 {
		t.Fatalf("read-backs = %v, want only segment 3", src.readBacks)
	}
	if fmt.Sprint(store.writes) != fmt.Sprint(map[int64]int{300: 1}) {
		t.Fatalf("writes = %v, want only offset 300", store.writes)
	}
}
