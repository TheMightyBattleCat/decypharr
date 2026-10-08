package reader

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/nntp"
)

// scratchSegBytes is a typical article: not a multiple of the buffer's 1 MB
// blocks, so neighbouring segments share blocks.
const scratchSegBytes = 716_800

// newScratchTestReader is a reader over nSegs empty segments with a real
// scratch cache: a real buffer over a real file under a temp directory. Its
// news client has no providers, so anything that tries to download fails and
// is counted in the reader's DownloadErrors.
func newScratchTestReader(t *testing.T, nSegs int, opts ...Option) *StreamingReader {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	// The forced-missing set is read once per test binary, by whichever test
	// fetches first. These tests can reach the fetch path, so they must not
	// be the ones to fix it empty for the tests that rely on it.
	t.Setenv("DECYPHARR_FORCE_MISSING_SEGMENTS", "<dead-normal-read@test>,<dead-verification-read@test>")

	segments := make([]SegmentMeta, nSegs)
	for i := range segments {
		segments[i] = SegmentMeta{MessageID: fmt.Sprintf("<scratch-%d@test>", i), Number: i + 1, Bytes: scratchSegBytes}
	}
	opts = append([]Option{
		WithMaxConnections(4),
		WithDiskPath(t.TempDir()),
		WithLogger(zerolog.Nop()),
	}, opts...)
	sr, err := NewStreamingReader(context.Background(), &nntp.Client{}, segments, opts...)
	if err != nil {
		t.Fatalf("NewStreamingReader: %v", err)
	}
	t.Cleanup(func() { _ = sr.Close() })
	return sr
}

func scratchSegData(i int) []byte {
	return bytes.Repeat([]byte{byte(i%251 + 1)}, scratchSegBytes)
}

// store puts segment i into the scratch cache the way a completed download
// does: through the cache's stream writer, then Finalize.
func store(t *testing.T, sr *StreamingReader, i int) {
	t.Helper()
	w := sr.cache.StreamWriter(i)
	if w == nil {
		t.Fatalf("no stream writer for segment %d", i)
	}
	if _, err := w.Write(scratchSegData(i)); err != nil {
		t.Fatalf("store segment %d: %v", i, err)
	}
	w.Finalize()
}

// scratchFileAllocated is how much of the reader's scratch file the
// filesystem has given blocks to.
func scratchFileAllocated(t *testing.T, sr *StreamingReader) int64 {
	t.Helper()
	fi, err := os.Stat(filepath.Join(sr.cache.diskPath, "segments.bin"))
	if err != nil {
		t.Fatal(err)
	}
	return fi.Sys().(*syscall.Stat_t).Blocks * 512
}

// diskWrites is how many bytes the usenet pool has written to scratch files
// so far, by either route.
func diskWrites() int64 {
	st := usenetBufferPool().Stats()
	return st.FlushedBytes + st.WriteThroughBytes
}

// The whole point of the change: a pre-cache burst through the real scratch
// cache - fetch a chunk, read it back, release it, for a file several times
// the RAM a stream gets - writes nothing to the scratch file. It is run
// beside a full read-ahead window of the stream being played, held in the
// same cache, which is the case that used to send every burst to disk: a
// 96 MB chunk on top of that window in 64 MB of RAM.
func TestBurstThroughScratchCacheNeverTouchesDisk(t *testing.T) {
	const (
		window     = 64 << 20 // the largest read-ahead window in normal use
		windowSegs = window/scratchSegBytes + 1
		burstSegs  = 520 // ~355 MB, close to three times the stream's RAM
	)
	sr := newScratchTestReader(t, windowSegs+burstSegs)
	ctx := ContextForCopyOut(context.Background())
	before := diskWrites()

	// The playing stream's read-ahead window: fetched, not yet consumed.
	for i := range windowSegs {
		store(t, sr, i)
	}

	got := make([]byte, scratchSegBytes)
	chunkSegs := BurstChunkBytes / scratchSegBytes
	for first := windowSegs; first < windowSegs+burstSegs; first += chunkSegs {
		last := min(first+chunkSegs, windowSegs+burstSegs)
		for i := first; i < last; i++ {
			store(t, sr, i)
		}
		for i := first; i < last; i++ {
			n, err := sr.ReadAtContext(ctx, got, sr.cache.SegmentOffset(i))
			if err != nil || n != scratchSegBytes || !bytes.Equal(got, scratchSegData(i)) {
				t.Fatalf("segment %d read back: n=%d err=%v intact=%v", i, n, err, bytes.Equal(got, scratchSegData(i)))
			}
		}
		lo, hi := sr.cache.SegmentOffset(first), sr.cache.SegmentOffset(last)
		if released := sr.Release(lo, hi-lo); released != last-first {
			t.Fatalf("chunk at segment %d: released %d segments, want %d", first, released, last-first)
		}
	}

	if wrote := diskWrites() - before; wrote != 0 {
		t.Fatalf("%d bytes written to the scratch file, want 0", wrote)
	}
	if n := scratchFileAllocated(t, sr); n != 0 {
		t.Fatalf("scratch file has %d bytes allocated, want 0", n)
	}
	// The window is untouched and still readable.
	for i := range windowSegs {
		if sr.cache.GetState(i) != StateOnDisk {
			t.Fatalf("read-ahead window segment %d lost its place: state %v", i, sr.cache.GetState(i))
		}
	}
	if _, err := sr.ReadAtContext(ctx, got, 0); err != nil || !bytes.Equal(got, scratchSegData(0)) {
		t.Fatalf("read-ahead window segment 0: err=%v intact=%v", err, bytes.Equal(got, scratchSegData(0)))
	}
	if errs := sr.stats.DownloadErrors.Load(); errs != 0 {
		t.Fatalf("%d downloads were attempted; the read-back must not start any", errs)
	}
}

// The sizes above are not a coincidence the next edit can break silently:
// one burst chunk, the largest read-ahead window in normal use and a margin
// for segments in flight have to fit in the RAM a stream gets.
func TestBurstChunkFitsBesideAReadAheadWindow(t *testing.T) {
	const (
		window = 64 << 20
		margin = 16 << 20
	)
	if BurstChunkBytes+window+margin > bufferMemorySize {
		t.Fatalf("burst chunk %d MB + read-ahead window %d MB + margin %d MB exceeds the %d MB a stream's scratch cache keeps in RAM",
			BurstChunkBytes>>20, window>>20, margin>>20, bufferMemorySize>>20)
	}
}

// Release drops only what the caller can have made durable: segments wholly
// inside the range that are cached, not in use, and hold the article's own
// bytes.
func TestReleaseLeavesWhatItMustNot(t *testing.T) {
	sr := newScratchTestReader(t, 12)
	for i := range 10 {
		store(t, sr, i)
	}
	sr.cache.PinRange(4, 4)         // a read in progress
	sr.fetcher.setPadded(5, true)   // zero-fill for a dead article
	sr.fetcher.MarkPatched(6)       // a PAR2 patch
	if !sr.cache.MarkFetching(10) { // a download in progress
		t.Fatal("setup: segment 10 should be free to fetch")
	}

	// From the middle of segment 1 to the middle of segment 9.
	lo := sr.cache.SegmentOffset(1) + 100
	hi := sr.cache.SegmentOffset(9) + 100
	released := sr.Release(lo, hi-lo)

	want := map[int]SegmentState{
		0:  StateOnDisk, // outside the range
		1:  StateOnDisk, // straddles its start
		2:  StateEmpty,
		3:  StateEmpty,
		4:  StateOnDisk, // pinned
		5:  StateOnDisk, // padded
		6:  StateOnDisk, // patched
		7:  StateEmpty,
		8:  StateEmpty,
		9:  StateOnDisk, // straddles its end
		10: StateFetching,
		11: StateEmpty, // never fetched
	}
	for i, state := range want {
		if got := sr.cache.GetState(i); got != state {
			t.Errorf("segment %d: state %v, want %v", i, got, state)
		}
	}
	if released != 4 {
		t.Errorf("released %d segments, want 4", released)
	}

	// The neighbours of what was released share blocks with it and must
	// still read back whole.
	got := make([]byte, scratchSegBytes)
	for _, i := range []int{1, 4, 5, 6, 9} {
		if n, ok := sr.cache.ReadInto(i, got); !ok || n != scratchSegBytes || !bytes.Equal(got, scratchSegData(i)) {
			t.Errorf("segment %d next to a released one: ok=%v n=%d intact=%v", i, ok, n, bytes.Equal(got, scratchSegData(i)))
		}
	}
}

// A released segment is simply gone: a reader waiting on it is woken, and
// storing it again serves the new bytes.
func TestReleasedSegmentCanBeFetchedAgain(t *testing.T) {
	sr := newScratchTestReader(t, 4)
	for i := range 4 {
		store(t, sr, i)
	}
	off := sr.cache.SegmentOffset(1)
	if released := sr.Release(off, 2*scratchSegBytes); released != 2 {
		t.Fatalf("released %d, want 2", released)
	}
	if _, ok := sr.cache.Get(1); ok {
		t.Fatal("released segment 1 still reads from the cache")
	}

	store(t, sr, 1)
	got := make([]byte, scratchSegBytes)
	if n, err := sr.ReadAtContext(ContextForCopyOut(context.Background()), got, off); err != nil || n != scratchSegBytes || !bytes.Equal(got, scratchSegData(1)) {
		t.Fatalf("segment 1 after being fetched again: n=%d err=%v intact=%v", n, err, bytes.Equal(got, scratchSegData(1)))
	}
	// Releasing nothing, or a range past the file, is harmless.
	if sr.Release(off, 0) != 0 || sr.Release(1<<40, 100) != 0 || sr.Release(-5, 100) != 0 {
		t.Fatal("an empty or out-of-range release dropped something")
	}
}

// prefetchQueuedAny reports whether any segment is marked as queued for
// prefetch. The mark is set as a read queues it, before the read returns.
func prefetchQueuedAny(sf *SegmentFetcher) bool {
	for i := range sf.prefetchQueued {
		if sf.prefetchQueued[i].Load() != 0 {
			return true
		}
	}
	return false
}

// A copy-out read does not start prefetch beyond what it reads, and does not
// move the consumed mark; an ordinary read of the same bytes does both.
//
// A queued prefetch is always visible: its segment is marked queued before
// the read returns, and the mark is cleared only after a worker has tried
// the download, which with no providers fails and is counted. So "no mark,
// then no failed download" means none was ever queued.
func TestCopyOutReadStartsNoPrefetch(t *testing.T) {
	read := func(ctx context.Context) (prefetched bool, sr *StreamingReader) {
		sr = newScratchTestReader(t, 40, WithPrefetchAhead(16))
		store(t, sr, 0)
		got := make([]byte, scratchSegBytes)
		if _, err := sr.ReadAtContext(ctx, got, 0); err != nil {
			t.Fatalf("read: %v", err)
		}
		marked := prefetchQueuedAny(sr.fetcher)
		return marked || sr.stats.DownloadErrors.Load() > 0, sr
	}

	prefetched, sr := read(context.Background())
	if !prefetched {
		t.Fatal("an ordinary read queued no prefetch: the test proves nothing")
	}
	if sr.cache.maxConsumedOff.Load() == 0 {
		t.Fatal("an ordinary read did not move the consumed mark: the test proves nothing")
	}

	prefetched, sr = read(ContextForCopyOut(context.Background()))
	if prefetched {
		t.Fatal("a copy-out read queued prefetch beyond what it read")
	}
	if got := sr.cache.maxConsumedOff.Load(); got != 0 {
		t.Fatalf("a copy-out read moved the consumed mark to %d", got)
	}
}

// Reads, releases and re-fetches of the same segments at once: a read either
// fails or returns exactly the right bytes, never a punched hole read back
// as zeros. The file is larger than the RAM the cache keeps, so much of it
// is on the scratch file, where the buffer reads without a lock.
func TestReleaseRacingReadsNeverServesWrongBytes(t *testing.T) {
	const nSegs = 260 // ~178 MB against 128 MB of RAM
	sr := newScratchTestReader(t, nSegs)
	for i := range nSegs {
		store(t, sr, i)
	}
	if diskWrites() == 0 {
		t.Log("nothing was written through; the lock-free disk path is not exercised")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	copyCtx := ContextForCopyOut(ctx)
	var wg sync.WaitGroup
	var reads, wrong atomic.Int64
	report := make(chan string, 1)

	for r := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(r + 1)))
			buf := make([]byte, 3*scratchSegBytes)
			for ctx.Err() == nil {
				seg := rng.Intn(nSegs - 3)
				off := sr.cache.SegmentOffset(seg) + int64(rng.Intn(scratchSegBytes))
				p := buf[:1+rng.Intn(2*scratchSegBytes)]
				n, err := sr.ReadAtContext(copyCtx, p, off)
				if err != nil {
					continue // released under it and not re-fetchable here: fine
				}
				reads.Add(1)
				for i := 0; i < n; i++ {
					want := byte((int((off+int64(i))/scratchSegBytes))%251 + 1)
					if p[i] != want {
						wrong.Add(1)
						select {
						case report <- fmt.Sprintf("read at %d len %d: byte %d is %#x, want %#x", off, n, i, p[i], want):
						default:
						}
						break
					}
				}
			}
		}()
	}
	wg.Add(1)
	go func() { // the releaser
		defer wg.Done()
		rng := rand.New(rand.NewSource(99))
		for ctx.Err() == nil {
			seg := rng.Intn(nSegs - 8)
			sr.Release(sr.cache.SegmentOffset(seg), int64(1+rng.Intn(8))*scratchSegBytes)
		}
	}()
	wg.Add(1)
	go func() { // downloads landing in released slots
		defer wg.Done()
		rng := rand.New(rand.NewSource(7))
		for ctx.Err() == nil {
			seg := rng.Intn(nSegs)
			if !sr.cache.MarkFetching(seg) {
				continue
			}
			w := sr.cache.StreamWriter(seg)
			_, _ = w.Write(scratchSegData(seg))
			w.Finalize()
		}
	}()
	wg.Wait()

	if n := wrong.Load(); n != 0 {
		t.Fatalf("%d reads returned wrong bytes; first: %s", n, <-report)
	}
	if reads.Load() == 0 {
		t.Fatal("no read succeeded: the test proves nothing")
	}
}

// Closing a reader deletes its scratch file, so nothing still in RAM is
// written to it first.
func TestClosingAReaderWritesNothingToItsScratchFile(t *testing.T) {
	sr := newScratchTestReader(t, 40)
	for i := range 30 { // ~20 MB, all of it dirty in RAM
		store(t, sr, i)
	}
	before := diskWrites()
	dir := sr.cache.diskPath
	if err := sr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if wrote := diskWrites() - before; wrote != 0 {
		t.Fatalf("closing the reader wrote %d bytes to a file it then deleted", wrote)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("scratch directory still there after Close: %v", err)
	}
}
