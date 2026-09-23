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

// newPatchFirstFixture builds a cache+fetcher over 200 segments with a real
// overlay store and a nil NNTP client: any fetch that reaches a provider
// panics, so a passing Fetch proves the patch was served without one.
func newPatchFirstFixture(t *testing.T) (*overlay.Store, *SegmentCache, Config) {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	segments := make([]SegmentMeta, 200)
	for i := range segments {
		segments[i] = SegmentMeta{MessageID: fmt.Sprintf("<patch-first-%d@test>", i), Number: i + 1, Bytes: 1024}
	}
	store, err := overlay.NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("overlay.NewStore: %v", err)
	}
	cfg := DefaultConfig()
	cfg.DiskPath = t.TempDir()
	cfg.Overlay = store.Handle("nzb-1")
	cfg.OverlayFile = "movie.mkv"
	cache, err := NewSegmentCache(context.Background(), segments, cfg, &ReaderStats{}, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewSegmentCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return store, cache, cfg
}

// A reader built after a repair serves the patch without asking a provider.
func TestFetchServesExistingPatchWithoutProviders(t *testing.T) {
	store, cache, cfg := newPatchFirstFixture(t)
	patch := bytes.Repeat([]byte{0xCD}, int(cache.SegmentDataSize(50)))
	if err := store.WritePatch("nzb-1", "movie.mkv", 50, patch); err != nil {
		t.Fatalf("WritePatch: %v", err)
	}

	sf := NewSegmentFetcher(context.Background(), nil, cache, cfg, &ReaderStats{}, zerolog.Nop())
	defer sf.Close()
	if err := sf.Fetch(context.Background(), 50); err != nil {
		t.Fatalf("Fetch = %v, want the patch", err)
	}
	if data, ok := cache.Get(50); !ok || !bytes.Equal(data, patch) {
		t.Fatalf("segment 50: ok=%v, want the patch bytes", ok)
	}
}

// A repair that lands on a live reader: RefetchSegments marks the segment
// patched, so the next read is the patch, not a round of 430s.
func TestRefetchSegmentsThenFetchSkipsProviders(t *testing.T) {
	store, cache, cfg := newPatchFirstFixture(t)
	sf := NewSegmentFetcher(context.Background(), nil, cache, cfg, &ReaderStats{}, zerolog.Nop())
	defer sf.Close()
	sr := &StreamingReader{cache: cache, fetcher: sf}

	if err := cache.Put(70, make([]byte, cache.SegmentDataSize(70))); err != nil { // the old pad
		t.Fatal(err)
	}
	patch := bytes.Repeat([]byte{0xEF}, int(cache.SegmentDataSize(70)))
	if err := store.WritePatch("nzb-1", "movie.mkv", 70, patch); err != nil {
		t.Fatalf("WritePatch: %v", err)
	}
	sr.RefetchSegments([]int{70})

	if err := sf.Fetch(context.Background(), 70); err != nil {
		t.Fatalf("Fetch after refresh = %v, want the patch", err)
	}
	if data, ok := cache.Get(70); !ok || !bytes.Equal(data, patch) {
		t.Fatalf("segment 70: ok=%v, want the patch bytes", ok)
	}
}
