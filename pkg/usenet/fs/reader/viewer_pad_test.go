package reader

import (
	"context"
	"fmt"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// A client's read past the pad caps is padded (the viewer keeps watching
// while repair runs); a background read of the same file still fails.
func TestPlaybackReadPadsPastCaps(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	// forceMissingMessageIDs is read once per test binary - reuse the IDs
	// TestDoFetchPaddingIsSuppressedForVerificationReads forces.
	const deadA, deadB = "<dead-normal-read@test>", "<dead-verification-read@test>"
	t.Setenv("DECYPHARR_FORCE_MISSING_SEGMENTS", deadA+","+deadB)

	segments := make([]SegmentMeta, 200)
	for i := range segments {
		segments[i] = SegmentMeta{MessageID: fmt.Sprintf("<viewer-pad-%d@test>", i), Number: i + 1, Bytes: 1024}
	}
	segments[100].MessageID = deadA
	segments[150].MessageID = deadB

	store, err := overlay.NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	store.SetPolicy(overlay.Policy{MaxRunSegments: 4, MaxTotalSegments: 0, MaxByteRatio: 1}) // any damage is past the caps
	cfg := DefaultConfig()
	cfg.DiskPath = t.TempDir()
	cfg.Overlay = store.Handle("nzb-v")
	cfg.OverlayFile = "movie.mkv"
	stats := &ReaderStats{}
	cache, err := NewSegmentCache(context.Background(), segments, cfg, stats, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	sf := NewSegmentFetcher(context.Background(), nil, cache, cfg, stats, zerolog.Nop())
	defer sf.Close()

	if err := sf.Fetch(ContextForPlayback(context.Background()), 100); err != nil {
		t.Fatalf("playback read past the caps: %v, want padded", err)
	}
	if got := cfg.Overlay.Verdict("movie.mkv"); got != overlay.VerdictFailed {
		t.Fatalf("verdict = %v, want failed (so repair re-grabs it)", got)
	}
	if err := sf.Fetch(context.Background(), 150); err == nil {
		t.Fatal("background read of a failed file was padded")
	}
	// ...and the viewer reaching that same segment is not stopped by the
	// Failed slot the background read left behind.
	if err := sf.Fetch(ContextForPlayback(context.Background()), 150); err != nil {
		t.Fatalf("playback read of a segment a background read failed: %v, want padded", err)
	}
}

// A pre-cache burst never pads, so it leaves in-cap damage Failed in the
// shared reader; the viewer starting that episode must get it padded, not
// the cached error (which tripped the mount's circuit breaker).
func TestPlaybackReadRecoversSegmentBurstLeftFailed(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	const deadA, deadB = "<dead-normal-read@test>", "<dead-verification-read@test>"
	t.Setenv("DECYPHARR_FORCE_MISSING_SEGMENTS", deadA+","+deadB)

	segments := make([]SegmentMeta, 200)
	for i := range segments {
		segments[i] = SegmentMeta{MessageID: fmt.Sprintf("<burst-failed-%d@test>", i), Number: i + 1, Bytes: 1024}
	}
	segments[120].MessageID = deadA
	store, err := overlay.NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	store.SetPolicy(overlay.Policy{MaxRunSegments: 10, MaxTotalSegments: 10, MaxByteRatio: 1})
	cfg := DefaultConfig()
	cfg.DiskPath = t.TempDir()
	cfg.Overlay = store.Handle("nzb-b")
	cfg.OverlayFile = "movie.mkv"
	stats := &ReaderStats{}
	cache, err := NewSegmentCache(context.Background(), segments, cfg, stats, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	sf := NewSegmentFetcher(context.Background(), nil, cache, cfg, stats, zerolog.Nop())
	defer sf.Close()

	if err := sf.Fetch(ContextForBurstDownload(context.Background()), 120); err == nil {
		t.Fatal("setup: a burst read padded")
	}
	if got := cache.GetState(120); got != StateFailed {
		t.Fatalf("setup: state after burst = %v, want Failed", got)
	}
	if err := sf.Fetch(ContextForPlayback(context.Background()), 120); err != nil {
		t.Fatalf("playback read after the burst: %v, want padded", err)
	}
	if got := cache.GetState(120); got != StateOnDisk {
		t.Fatalf("state after playback read = %v, want OnDisk", got)
	}
}
