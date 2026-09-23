package reader

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// A reader that padded a dead segment keeps serving the zero-fill after a
// PAR2 repair lands, because a cache hit never consults the overlay patch.
// RefetchSegments must make the next read go back through the fetch path,
// which fails on the dead article and serves the patch - and must leave the
// reader's other cached segments alone.
func TestRefetchSegmentsServesPatchAfterPad(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	// forceMissingMessageIDs is read once per test binary, so reuse the IDs
	// TestDoFetchPaddingIsSuppressedForVerificationReads forces (whichever
	// test fetches first fixes the set) - this one on its own overlay store.
	const msgIDDead = "<dead-normal-read@test>"
	t.Setenv("DECYPHARR_FORCE_MISSING_SEGMENTS", msgIDDead+",<dead-verification-read@test>")

	segments := make([]SegmentMeta, 200)
	for i := range segments {
		segments[i] = SegmentMeta{MessageID: fmt.Sprintf("<refetch-filler-%d@test>", i), Number: i + 1, Bytes: 1024}
	}
	segments[120].MessageID = msgIDDead

	store, err := overlay.NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("overlay.NewStore: %v", err)
	}
	store.SetPolicy(overlay.Policy{MaxRunSegments: 10, MaxTotalSegments: 10, MaxByteRatio: 1.0})

	cfg := DefaultConfig()
	cfg.DiskPath = t.TempDir()
	cfg.Overlay = store.Handle("nzb-1")
	cfg.OverlayFile = "movie.mkv"

	ctx := context.Background()
	stats := &ReaderStats{}
	cache, err := NewSegmentCache(ctx, segments, cfg, stats, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewSegmentCache: %v", err)
	}
	defer cache.Close()
	sf := NewSegmentFetcher(ctx, nil, cache, cfg, stats, zerolog.Nop())
	defer sf.Close()
	sr := &StreamingReader{cache: cache, fetcher: sf}

	if err := sf.Fetch(ctx, 120); err != nil {
		t.Fatalf("Fetch(dead) = %v, want nil (padded)", err)
	}
	if data, ok := cache.Get(120); !ok || !bytes.Equal(data, make([]byte, len(data))) {
		t.Fatalf("setup: segment 120 should hold zero-fill, got ok=%v", ok)
	}
	// An unrelated cached segment, which the refresh must keep.
	if err := cache.Put(10, bytes.Repeat([]byte{7}, 1024)); err != nil {
		t.Fatal(err)
	}

	patch := bytes.Repeat([]byte{0xAB}, int(cache.SegmentDataSize(120)))
	if err := store.WritePatch("nzb-1", "movie.mkv", 120, patch); err != nil {
		t.Fatalf("WritePatch: %v", err)
	}
	// The repair has landed, but the reader still serves its zero-fill.
	if data, _ := cache.Get(120); !bytes.Equal(data, make([]byte, len(data))) {
		t.Fatal("setup: expected the stale pad to survive the patch write")
	}

	sr.RefetchSegments([]int{120})
	if got := cache.GetState(120); got != StateEmpty {
		t.Fatalf("segment 120 state after refresh = %v, want StateEmpty", got)
	}
	if got := cache.GetState(10); got != StateOnDisk {
		t.Fatalf("unrelated segment 10 state after refresh = %v, want StateOnDisk", got)
	}

	if err := sf.Fetch(ctx, 120); err != nil {
		t.Fatalf("Fetch after refresh = %v, want nil (patch)", err)
	}
	if data, ok := cache.Get(120); !ok || !bytes.Equal(data, patch) {
		t.Fatalf("segment 120 after refresh: ok=%v, want the patch bytes", ok)
	}
}
