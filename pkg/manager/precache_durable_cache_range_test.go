package manager

import (
	"context"
	"fmt"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// fakeNZBSource is a scriptable precacheNZBSource - no real NNTP client or
// overlay store required.
type fakeNZBSource struct {
	nzb        *storage.NZB
	nzbErr     error
	pending    map[string][]overlay.DeadSegment
	pendingErr error
	reads      map[int64][]byte // offset -> bytes ReadCachedAt returns
	readCalls  map[int64]int    // offset -> number of ReadCachedAt calls, for asserting a range was never touched
}

func (f *fakeNZBSource) GetNZB(id string) (*storage.NZB, error) { return f.nzb, f.nzbErr }

func (f *fakeNZBSource) OverlayPendingRepair(nzoID string) (map[string][]overlay.DeadSegment, error) {
	return f.pending, f.pendingErr
}

func (f *fakeNZBSource) ReadCachedAt(ctx context.Context, nzoID, filename string, p []byte, off int64) (int, error) {
	if f.readCalls == nil {
		f.readCalls = map[int64]int{}
	}
	f.readCalls[off]++
	data, ok := f.reads[off]
	if !ok {
		return 0, fmt.Errorf("no data configured for offset %d", off)
	}
	return copy(p, data), nil
}

// threeSegmentNZB builds a 300-byte file with three 100-byte segments:
// [0,100), [100,200), [200,300) - segment 1 is left for the caller to mark
// dead/clean as needed.
func threeSegmentNZB(filename string) *storage.NZB {
	return &storage.NZB{
		Files: []storage.NZBFile{
			{
				Name: filename,
				Segments: []storage.NZBSegment{
					{Number: 0, StartOffset: 0, EndOffset: 99},
					{Number: 1, StartOffset: 100, EndOffset: 199},
					{Number: 2, StartOffset: 200, EndOffset: 299},
				},
			},
		},
	}
}

func bytesOf(n int, b byte) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

// TestPersistDurableRangesWritesOnlyCleanSegments proves Step 2's core
// contract: a file with mixed clean+damaged segments persists only the
// clean ones durably - the damaged segment is never read, never written,
// and therefore never readable back as healthy.
func TestPersistDurableRangesWritesOnlyCleanSegments(t *testing.T) {
	const filename = "episode.mkv"
	src := &fakeNZBSource{
		nzb:     threeSegmentNZB(filename),
		pending: map[string][]overlay.DeadSegment{filename: {{Index: 1}}}, // segment 1 (bytes [100,200)) is damaged
		reads: map[int64][]byte{
			0:   bytesOf(100, 0xAA),
			200: bytesOf(100, 0xBB),
		},
	}
	writer := &recordingWriter{}

	persistDurableRanges(context.Background(), src, writer, "Some.Release", "infohash1", filename, 300, zerolog.Nop())

	if got := src.readCalls[100]; got != 0 {
		t.Fatalf("ReadCachedAt was called %d times for the damaged segment's offset (100), want 0", got)
	}
	if len(writer.writes) != 2 {
		t.Fatalf("writer recorded %d writes, want 2 (clean segments only): %+v", len(writer.writes), writer.writes)
	}
	byOff := map[int64]fakeWrite{}
	for _, w := range writer.writes {
		byOff[w.off] = w
	}
	if w, ok := byOff[0]; !ok || w.fileSize != 300 || string(w.p) != string(bytesOf(100, 0xAA)) {
		t.Fatalf("missing/incorrect write for clean segment 0: %+v", byOff[0])
	}
	if w, ok := byOff[200]; !ok || w.fileSize != 300 || string(w.p) != string(bytesOf(100, 0xBB)) {
		t.Fatalf("missing/incorrect write for clean segment 2: %+v", byOff[200])
	}
	if _, ok := byOff[100]; ok {
		t.Fatalf("damaged segment (offset 100) was durably written - a padded/degraded range must never be persisted as clean")
	}
}

// TestPersistDurableRangesAllCleanWritesEverySegment is the inverse sanity
// check: with nothing pending, every segment is written.
func TestPersistDurableRangesAllCleanWritesEverySegment(t *testing.T) {
	const filename = "episode.mkv"
	src := &fakeNZBSource{
		nzb: threeSegmentNZB(filename),
		reads: map[int64][]byte{
			0:   bytesOf(100, 1),
			100: bytesOf(100, 2),
			200: bytesOf(100, 3),
		},
	}
	writer := &recordingWriter{}

	persistDurableRanges(context.Background(), src, writer, "Some.Release", "infohash1", filename, 300, zerolog.Nop())

	if len(writer.writes) != 3 {
		t.Fatalf("writer recorded %d writes, want 3 (all clean)", len(writer.writes))
	}
}

// TestPersistDurableRangesSecondCallPicksUpRepairedSegment proves the
// PENDING-REPAIR -> REPAIRED transition: calling persistDurableRanges again
// after the overlay no longer reports a segment as pending persists it,
// with no separate code path or on-disk marker - the same function just
// sees a different pending set.
func TestPersistDurableRangesSecondCallPicksUpRepairedSegment(t *testing.T) {
	const filename = "episode.mkv"
	src := &fakeNZBSource{
		nzb:     threeSegmentNZB(filename),
		pending: map[string][]overlay.DeadSegment{filename: {{Index: 1}}},
		reads: map[int64][]byte{
			0:   bytesOf(100, 1),
			200: bytesOf(100, 3),
		},
	}
	writer := &recordingWriter{}
	persistDurableRanges(context.Background(), src, writer, "Some.Release", "infohash1", filename, 300, zerolog.Nop())
	if len(writer.writes) != 2 {
		t.Fatalf("first call: writer recorded %d writes, want 2", len(writer.writes))
	}

	// Repair "landed": segment 1 is no longer pending, and its real bytes
	// are now available to read back.
	src.pending = nil
	src.reads[100] = bytesOf(100, 2)

	persistDurableRanges(context.Background(), src, writer, "Some.Release", "infohash1", filename, 300, zerolog.Nop())
	// persistDurableRanges itself doesn't dedupe against what's already
	// durable - that's WriteAtNoOverwrite's job downstream of the seam (see
	// TestWriteCachedRangeSkipsAlreadyPresentBytes) - so re-writing the
	// already-clean segments 0/2 on this second call is expected; what
	// matters is that the previously-dead segment 1 is now included at all.
	found := false
	for _, w := range writer.writes {
		if w.off == 100 {
			found = true
			if string(w.p) != string(bytesOf(100, 2)) {
				t.Fatalf("repaired segment write = %q, want the post-repair bytes", w.p)
			}
		}
	}
	if !found {
		t.Fatalf("expected a write for the now-repaired segment at offset 100")
	}
}

// TestPersistDurableRangesNoOpsWithoutWriter proves the DFS write seam being
// unavailable (nil writer - rclone mode, no mount, mount not ready) is a
// clean no-op, not a panic or a wasted read.
func TestPersistDurableRangesNoOpsWithoutWriter(t *testing.T) {
	const filename = "episode.mkv"
	src := &fakeNZBSource{nzb: threeSegmentNZB(filename)}
	persistDurableRanges(context.Background(), src, nil, "Some.Release", "infohash1", filename, 300, zerolog.Nop())
	if len(src.readCalls) != 0 {
		t.Fatalf("expected no reads when writer is nil, got %v", src.readCalls)
	}
}

// TestPersistDurableRangesAbortsOnCancelledContext proves an expired/cancelled
// ctx (the playhead moved on before the walk finished) stops the segment walk
// cleanly instead of burning read budget on bytes nobody's waiting for.
func TestPersistDurableRangesAbortsOnCancelledContext(t *testing.T) {
	const filename = "episode.mkv"
	src := &fakeNZBSource{
		nzb: threeSegmentNZB(filename),
		reads: map[int64][]byte{
			0:   bytesOf(100, 1),
			100: bytesOf(100, 2),
			200: bytesOf(100, 3),
		},
	}
	writer := &recordingWriter{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already done before the first segment

	persistDurableRanges(ctx, src, writer, "Some.Release", "infohash1", filename, 300, zerolog.Nop())

	if len(src.readCalls) != 0 {
		t.Fatalf("expected no reads after a cancelled ctx, got %v", src.readCalls)
	}
	if len(writer.writes) != 0 {
		t.Fatalf("expected no writes after a cancelled ctx, got %+v", writer.writes)
	}
}

// TestPersistDurableRangesAbortMidWalkKeepsEarlierWrites proves the abort is a
// clean stop, not a rollback: segments persisted before the ctx expired stay
// durably written; the rest is left for a later pass.
func TestPersistDurableRangesAbortMidWalkKeepsEarlierWrites(t *testing.T) {
	const filename = "episode.mkv"
	ctx, cancel := context.WithCancel(context.Background())
	src := &cancellingNZBSource{
		fakeNZBSource: fakeNZBSource{
			nzb: threeSegmentNZB(filename),
			reads: map[int64][]byte{
				0:   bytesOf(100, 1),
				100: bytesOf(100, 2),
				200: bytesOf(100, 3),
			},
		},
		cancelAfterOffset: 0, // cancel once segment 0 has been read
		cancel:            cancel,
	}
	writer := &recordingWriter{}

	persistDurableRanges(ctx, src, writer, "Some.Release", "infohash1", filename, 300, zerolog.Nop())

	if len(writer.writes) != 1 || writer.writes[0].off != 0 {
		t.Fatalf("writes = %+v, want exactly the pre-cancel write at offset 0", writer.writes)
	}
	if _, touched := src.readCalls[200]; touched {
		t.Fatalf("segment 2 was read after the ctx was cancelled")
	}
}

// cancellingNZBSource cancels the run's context right after ReadCachedAt is
// called for a given offset, so a test can observe a mid-walk abort.
type cancellingNZBSource struct {
	fakeNZBSource
	cancelAfterOffset int64
	cancel            context.CancelFunc
}

func (c *cancellingNZBSource) ReadCachedAt(ctx context.Context, nzoID, filename string, p []byte, off int64) (int, error) {
	n, err := c.fakeNZBSource.ReadCachedAt(ctx, nzoID, filename, p, off)
	if off == c.cancelAfterOffset {
		c.cancel()
	}
	return n, err
}

// TestPersistDurableRangesSkipsSegmentReadFailure proves a read failure for
// one segment doesn't abort the rest - best-effort, matching the rest of
// precache's philosophy.
func TestPersistDurableRangesSkipsSegmentReadFailure(t *testing.T) {
	const filename = "episode.mkv"
	src := &fakeNZBSource{
		nzb: threeSegmentNZB(filename),
		reads: map[int64][]byte{
			// offset 0 deliberately unconfigured -> ReadCachedAt errors
			200: bytesOf(100, 9),
		},
	}
	writer := &recordingWriter{}
	persistDurableRanges(context.Background(), src, writer, "Some.Release", "infohash1", filename, 300, zerolog.Nop())

	if len(writer.writes) != 1 || writer.writes[0].off != 200 {
		t.Fatalf("writes = %+v, want exactly one write at offset 200 (segment 0's read failure must not block segment 2)", writer.writes)
	}
}
