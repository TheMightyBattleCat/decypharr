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

// refetchTestReader builds a StreamingReader (cache + fetcher) over 200 1 KiB
// segments with a real overlay store and no NNTP client. Prefetch is off, so
// nothing reaches a provider.
func refetchTestReader(t *testing.T, nzbID string, deadIdx int, deadID string) (*StreamingReader, *overlay.Store) {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	segments := make([]SegmentMeta, 200)
	for i := range segments {
		segments[i] = SegmentMeta{MessageID: fmt.Sprintf("<refetch-%s-%d@test>", nzbID, i), Number: i + 1, Bytes: 1024}
	}
	segments[deadIdx].MessageID = deadID

	store, err := overlay.NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("overlay.NewStore: %v", err)
	}
	store.SetPolicy(overlay.Policy{MaxRunSegments: 10, MaxTotalSegments: 10, MaxByteRatio: 1.0})

	cfg := DefaultConfig()
	cfg.DiskPath = t.TempDir()
	cfg.PrefetchAhead = 0
	cfg.Overlay = store.Handle(nzbID)
	cfg.OverlayFile = "movie.mkv"

	ctx, cancel := context.WithCancel(context.Background())
	stats := &ReaderStats{}
	cache, err := NewSegmentCache(ctx, segments, cfg, stats, zerolog.Nop())
	if err != nil {
		cancel()
		t.Fatalf("NewSegmentCache: %v", err)
	}
	sf := NewSegmentFetcher(ctx, nil, cache, cfg, stats, zerolog.Nop())
	sr := &StreamingReader{
		cache:     cache,
		fetcher:   sf,
		config:    cfg,
		totalSize: cache.TotalSize(),
		segCount:  cache.SegmentCount(),
		ctx:       ctx,
		cancel:    cancel,
		logger:    zerolog.Nop(),
		stats:     stats,
	}
	t.Cleanup(func() { _ = sr.Close() })
	return sr, store
}

// A PAR2 repair landing on a live reader whose dead segment a background
// read left Failed must make that segment serve the patch - to the viewer
// and to the pre-cache's persist walk. RefetchSegments used to clear only the
// error and leave the slot Failed, so every later read got "segment N failed".
func TestRefetchAfterRepairServesPatchForFailedSlot(t *testing.T) {
	const deadID = "<dead-normal-read@test>" // forceMissingMessageIDs is read once per test binary; reuse the shared IDs
	t.Setenv("DECYPHARR_FORCE_MISSING_SEGMENTS", deadID+",<dead-verification-read@test>")
	sr, store := refetchTestReader(t, "nzb-refetch", 120, deadID)

	// A pre-cache burst hits the dead article: recorded, never zero-filled,
	// the slot left Failed with a 430.
	if err := sr.fetcher.Fetch(ContextForBurstDownload(context.Background()), 120); err == nil {
		t.Fatal("setup: burst read of a dead segment succeeded")
	}
	if got := sr.cache.GetState(120); got != StateFailed {
		t.Fatalf("setup: state after burst = %v, want Failed", got)
	}

	// The repair lands: patch written, then the live reader is refreshed.
	patch := bytes.Repeat([]byte{0xAB}, int(sr.cache.SegmentDataSize(120)))
	if err := store.WritePatch("nzb-refetch", "movie.mkv", 120, patch); err != nil {
		t.Fatalf("WritePatch: %v", err)
	}
	sr.RefetchSegments([]int{120})
	if got := sr.cache.GetState(120); got == StateFailed {
		t.Fatalf("slot still Failed after RefetchSegments (err=%v)", sr.cache.GetError(120))
	}

	p := make([]byte, 1024)
	if n, err := sr.ReadAtContext(ContextForBurstDownload(context.Background()), p, 120*1024); err != nil || !bytes.Equal(p[:n], patch) {
		t.Errorf("persist-walk (burst ctx) read: n=%d err=%v, want the patch", n, err)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		n, err := sr.ReadAtContext(ContextForPlayback(context.Background()), p, 120*1024)
		if err != nil || n != len(p) || !bytes.Equal(p, patch) {
			t.Errorf("playback read #%d: n=%d err=%v state=%v, want the patch", attempt, n, err, sr.cache.GetState(120))
		}
	}
}

// Without a refresh, a burst read of the Failed slot still gets its cached
// 430 (the refresh is what makes the patch reachable for non-playback reads).
func TestFailedSlotWithoutRefreshKeepsCachedError(t *testing.T) {
	const deadID = "<dead-normal-read@test>"
	t.Setenv("DECYPHARR_FORCE_MISSING_SEGMENTS", deadID+",<dead-verification-read@test>")
	sr, store := refetchTestReader(t, "nzb-refetch-ctl", 120, deadID)

	if err := sr.fetcher.Fetch(ContextForBurstDownload(context.Background()), 120); err == nil {
		t.Fatal("setup: burst read of a dead segment succeeded")
	}
	patch := bytes.Repeat([]byte{0xAB}, int(sr.cache.SegmentDataSize(120)))
	if err := store.WritePatch("nzb-refetch-ctl", "movie.mkv", 120, patch); err != nil {
		t.Fatalf("WritePatch: %v", err)
	}
	p := make([]byte, 1024)
	if _, err := sr.ReadAtContext(ContextForBurstDownload(context.Background()), p, 120*1024); err == nil {
		t.Fatal("expected the cached 430 without a refresh")
	}
}

// A Failed slot with no recorded cause is refetched, never reported as a
// successful fetch.
func TestFetchFailedSlotWithoutErrorRefetches(t *testing.T) {
	const deadID = "<dead-normal-read@test>"
	t.Setenv("DECYPHARR_FORCE_MISSING_SEGMENTS", deadID+",<dead-verification-read@test>")
	sr, store := refetchTestReader(t, "nzb-refetch-nil", 120, deadID)

	patch := bytes.Repeat([]byte{0xCD}, int(sr.cache.SegmentDataSize(120)))
	if err := store.WritePatch("nzb-refetch-nil", "movie.mkv", 120, patch); err != nil {
		t.Fatalf("WritePatch: %v", err)
	}
	sr.fetcher.MarkPatched(120)
	sr.cache.states[120].Store(uint32(StateFailed))
	sr.cache.errors[120].Store(nil)

	if err := sr.fetcher.Fetch(ContextForBurstDownload(context.Background()), 120); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if got := sr.cache.GetState(120); got != StateOnDisk {
		t.Fatalf("state after Fetch = %v, want OnDisk (the patch)", got)
	}
}
