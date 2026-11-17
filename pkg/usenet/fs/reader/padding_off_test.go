package reader

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// paddingOffReader builds a reader over 200 segments with a real overlay
// store and playback padding switched off. Segment deadIdx carries deadID,
// which the test makes confirmed-missing through
// DECYPHARR_FORCE_MISSING_SEGMENTS. The NNTP client is nil, so a fetch that
// reaches a provider panics. A non-nil patch is written for deadIdx before
// the reader is built (a reader opened after a repair).
func paddingOffReader(t *testing.T, nzbID string, deadIdx int, deadID string, patch []byte) (*StreamingReader, *overlay.Store) {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	segments := make([]SegmentMeta, 200)
	for i := range segments {
		segments[i] = SegmentMeta{MessageID: fmt.Sprintf("<padding-off-%s-%d@test>", nzbID, i), Number: i + 1, Bytes: 1024}
	}
	segments[deadIdx].MessageID = deadID

	store, err := overlay.NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("overlay.NewStore: %v", err)
	}
	store.SetPolicy(overlay.Policy{MaxRunSegments: 10, MaxTotalSegments: 10, MaxByteRatio: 1.0})
	if patch != nil {
		if err := store.WritePatch(nzbID, "movie.mkv", deadIdx, patch); err != nil {
			t.Fatalf("WritePatch: %v", err)
		}
	}

	cfg := DefaultConfig()
	cfg.DiskPath = t.TempDir()
	cfg.PrefetchAhead = 0
	for _, opt := range []Option{WithOverlay(store.Handle(nzbID), "movie.mkv"), WithPadding(false)} {
		opt(&cfg)
	}

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

// With playback padding off, a segment PAR2 has patched is read as the
// patch, both when the repair landed before the reader opened and when the
// providers' 430 reaches the overlay on a reader opened earlier.
func TestPaddingOffStillServesPatch(t *testing.T) {
	const deadID = "<dead-normal-read@test>"
	t.Setenv("DECYPHARR_FORCE_MISSING_SEGMENTS", deadID+",<dead-verification-read@test>")
	patch := bytes.Repeat([]byte{0xA5}, 1024)

	t.Run("patched before the reader opened", func(t *testing.T) {
		sr, _ := paddingOffReader(t, "nzb-padoff-before", 120, deadID, patch)
		p := make([]byte, 1024)
		n, err := sr.ReadAtContext(ContextForPlayback(context.Background()), p, 120*1024)
		if err != nil || n != len(p) {
			t.Fatalf("read of the patched segment: n=%d err=%v, want the patch", n, err)
		}
		if !bytes.Equal(p, patch) {
			t.Fatalf("read returned %x..., want the patch bytes %x...", p[:4], patch[:4])
		}
	})

	t.Run("patched after the reader opened", func(t *testing.T) {
		sr, store := paddingOffReader(t, "nzb-padoff-after", 120, deadID, nil)
		if err := store.WritePatch("nzb-padoff-after", "movie.mkv", 120, patch); err != nil {
			t.Fatalf("WritePatch: %v", err)
		}
		p := make([]byte, 1024)
		n, err := sr.ReadAtContext(ContextForPlayback(context.Background()), p, 120*1024)
		if err != nil || n != len(p) {
			t.Fatalf("read of the patched segment: n=%d err=%v, want the patch", n, err)
		}
		if !bytes.Equal(p, patch) {
			t.Fatalf("read returned %x..., want the patch bytes %x...", p[:4], patch[:4])
		}
	})
}

// With playback padding off, a dead segment with no patch fails the read as
// it did before the overlay existed: no zero-fill, no dead record.
func TestPaddingOffDoesNotPadUnpatchedSegment(t *testing.T) {
	const deadID = "<dead-normal-read@test>"
	t.Setenv("DECYPHARR_FORCE_MISSING_SEGMENTS", deadID+",<dead-verification-read@test>")
	const nzbID = "nzb-padoff-dead"
	sr, store := paddingOffReader(t, nzbID, 120, deadID, nil)

	err := sr.fetcher.Fetch(ContextForPlayback(context.Background()), 120)
	if err == nil {
		t.Fatal("playback read of an unpatched dead segment succeeded, want article-not-found")
	}
	if !nntp.IsArticleNotFoundError(err) {
		t.Fatalf("playback read error %v, want article-not-found", err)
	}
	if sr.fetcher.isPadded(120) {
		t.Error("segment 120 marked padded with padding off")
	}
	if data, ok := sr.cache.Get(120); ok {
		t.Errorf("segment 120 cached (%d bytes) with padding off, want nothing", len(data))
	}
	if v := store.Verdict(nzbID, "movie.mkv"); v != overlay.VerdictClean {
		t.Errorf("overlay verdict = %v, want clean (no dead record with padding off)", v)
	}
	if pending, err := store.PendingRepair(nzbID); err != nil || len(pending) != 0 {
		t.Errorf("PendingRepair = %v (err %v), want none", pending, err)
	}
}
