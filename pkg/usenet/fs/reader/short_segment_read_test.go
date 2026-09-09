package reader

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
)

// newShortSegmentCache builds a 4-segment cache whose middle segment is
// stored SHORT of its slot - the state a truncated article body leaves behind
// once bufferStreamWriter.Finalize commits it (Finalize commits on
// written > 0 without checking that it filled maxBytes).
func newShortSegmentCache(t *testing.T, slot int, shortLen int) *SegmentCache {
	t.Helper()
	config.SetConfigPath(t.TempDir())

	segments := make([]SegmentMeta, 4)
	for i := range segments {
		segments[i] = SegmentMeta{MessageID: fmt.Sprintf("<seg-%d@test>", i), Number: i + 1, Bytes: int64(slot)}
	}

	cfg := DefaultConfig()
	cfg.DiskPath = t.TempDir()

	cache, err := NewSegmentCache(t.Context(), segments, cfg, &ReaderStats{}, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewSegmentCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	fill := func(idx int, n int, b byte) {
		data := make([]byte, n)
		for i := range data {
			data[i] = b
		}
		if err := cache.Put(idx, data); err != nil {
			t.Fatalf("Put(%d): %v", idx, err)
		}
	}

	fill(0, slot, 0xAA)     // healthy
	fill(1, shortLen, 0xBB) // TRUNCATED - short of its slot
	fill(2, slot, 0xCC)     // healthy, sits after the hole
	fill(3, slot, 0xDD)     // healthy

	return cache
}

// A segment stored shorter than its slot leaves a hole. readFromCache must
// stop at that hole and return the contiguous prefix, because its `totalRead`
// is a plain sum: if it kept going, segment 2's bytes would land at their own
// outOffset and the returned count would imply a contiguous run over a buffer
// with a stale gap in the middle of it. Every io.ReaderAt caller reads n as
// "the first n bytes are valid".
func TestReadFromCacheStopsAtShortSegment(t *testing.T) {
	const slot, shortLen = 1000, 400
	cache := newShortSegmentCache(t, slot, shortLen)

	sr := &StreamingReader{
		cache:     cache,
		config:    DefaultConfig(),
		totalSize: cache.segOffsets[len(cache.segOffsets)-1],
		segCount:  4,
		logger:    zerolog.Nop(),
		stats:     &ReaderStats{},
	}

	// Span segments 0..2, straddling the truncated one in the middle.
	p := make([]byte, 3*slot)
	n, err := sr.readFromCache(t.Context(), p, 0, 0, 2)

	// Contiguous prefix: all of segment 0 plus the surviving part of segment 1.
	if want := slot + shortLen; n != want {
		t.Errorf("n = %d, want %d (contiguous prefix, not the gapped sum %d)",
			n, want, slot+shortLen+slot)
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("err = %v, want io.ErrUnexpectedEOF", err)
	}

	// Everything the call reported is genuinely the bytes it read.
	for i := 0; i < slot; i++ {
		if p[i] != 0xAA {
			t.Fatalf("p[%d] = %#x, want 0xAA (segment 0)", i, p[i])
		}
	}
	for i := slot; i < slot+shortLen; i++ {
		if p[i] != 0xBB {
			t.Fatalf("p[%d] = %#x, want 0xBB (short segment 1)", i, p[i])
		}
	}

	// The regression this guards: segment 2's bytes must NOT have been written
	// past the hole. If they were, the caller would get n=1400 describing a
	// buffer that also holds live data at 2000..3000 with a stale gap between.
	for i := 2 * slot; i < 3*slot; i++ {
		if p[i] == 0xCC {
			t.Fatalf("p[%d] holds segment 2's data past the hole - readFromCache "+
				"kept filling after a short segment", i)
		}
	}
}

// The healthy path must be untouched: a full span of intact segments still
// reads contiguously with no error.
func TestReadFromCacheHealthySpanUnaffected(t *testing.T) {
	const slot = 1000
	cache := newShortSegmentCache(t, slot, slot) // segment 1 stored FULL

	sr := &StreamingReader{
		cache:     cache,
		config:    DefaultConfig(),
		totalSize: cache.segOffsets[len(cache.segOffsets)-1],
		segCount:  4,
		logger:    zerolog.Nop(),
		stats:     &ReaderStats{},
	}

	p := make([]byte, 3*slot)
	n, err := sr.readFromCache(t.Context(), p, 0, 0, 2)
	if err != nil {
		t.Fatalf("healthy span returned %v, want nil", err)
	}
	if n != 3*slot {
		t.Fatalf("n = %d, want %d", n, 3*slot)
	}
	if p[0] != 0xAA || p[slot] != 0xBB || p[2*slot] != 0xCC {
		t.Fatalf("healthy span did not assemble contiguously")
	}
}

// A read that starts exactly at a truncated segment's stored end has no bytes
// to give. That must not look like a clean EOF: (0, nil) is indistinguishable
// from end-of-file to a caller, which is how a hole turns into silent
// truncation instead of a re-read.
func TestReadFromCacheAtTruncationPointIsNotSilentEOF(t *testing.T) {
	const slot, shortLen = 1000, 400
	cache := newShortSegmentCache(t, slot, shortLen)

	sr := &StreamingReader{
		cache:     cache,
		config:    DefaultConfig(),
		totalSize: cache.segOffsets[len(cache.segOffsets)-1],
		segCount:  4,
		logger:    zerolog.Nop(),
		stats:     &ReaderStats{},
	}

	// Start inside segment 1, exactly where its stored bytes run out.
	off := int64(slot + shortLen)
	p := make([]byte, slot)
	n, err := sr.readFromCache(t.Context(), p, off, 1, 1)

	if n != 0 {
		t.Fatalf("n = %d, want 0 at the truncation point", n)
	}
	if err == nil {
		t.Fatal("got (0, nil) at a truncation point - a caller cannot tell that " +
			"from a clean EOF, so the hole becomes silent truncation")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("err = %v, want io.ErrUnexpectedEOF", err)
	}
}
