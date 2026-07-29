package reader

import (
	"context"
	"fmt"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// TestDoFetchPaddingIsSuppressedForVerificationReads proves commit A's
// central safety property: the SAME confirmed-dead segment is padded for a
// normal read but hard-errors for a read whose context is marked via
// ContextWithoutPadding (what the WebDAV handler does for every
// internal-bearer-token read, i.e. every ffprobe import/sweep check - see
// webdav.Handler.isInternalBearer). Padding must never let a broken grab
// look healthy to the very check that's supposed to catch it.
func TestDoFetchPaddingIsSuppressedForVerificationReads(t *testing.T) {
	// NewSegmentCache reaches usenetBufferPool() -> config.Get(), which
	// os.Exit(1)s if it can't read/create config.json at the default path.
	config.SetConfigPath(t.TempDir())

	const (
		msgIDNormal = "<dead-normal-read@test>"
		msgIDNoPad  = "<dead-verification-read@test>"
	)
	// forceMissingMessageIDs is a sync.OnceValue over os.Getenv - every
	// message ID this test (or any other test in this binary) wants forced
	// missing must be set before the first Fetch call in the package.
	t.Setenv("DECYPHARR_FORCE_MISSING_SEGMENTS", msgIDNormal+","+msgIDNoPad)

	// 200 segments so the header region ([0, max(1, 200/100)) = [0,2))
	// stays clear of the two forced-missing segments below - this test is
	// about the no-pad context bypassing the overlay, not the positional
	// header-region rule (see policy_test.go for that).
	segments := make([]SegmentMeta, 200)
	for i := range segments {
		segments[i] = SegmentMeta{MessageID: fmt.Sprintf("<filler-%d@test>", i), Number: i + 1, Bytes: 1024}
	}
	segments[100].MessageID = msgIDNormal
	segments[101].MessageID = msgIDNoPad

	store, err := overlay.NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("overlay.NewStore: %v", err)
	}
	// Generous caps: the default policy's byte-ratio cap (2% of total file
	// size) would fail this tiny two-segment fixture outright, which isn't
	// what this test is about - it's about the no-pad context bypassing the
	// overlay entirely, not about the padding-cap math (see overlay/policy_test.go
	// for that).
	store.SetPolicy(overlay.Policy{MaxRunSegments: 10, MaxTotalSegments: 10, MaxByteRatio: 1.0})
	handle := store.Handle("nzb-1")

	cfg := DefaultConfig()
	cfg.DiskPath = t.TempDir()
	cfg.Overlay = handle
	cfg.OverlayFile = "movie.mkv" // must be a video container ext for Decide to pad

	ctx := context.Background()
	stats := &ReaderStats{}

	cache, err := NewSegmentCache(ctx, segments, cfg, stats, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewSegmentCache: %v", err)
	}
	defer cache.Close()

	// sf.client is never dereferenced on the forced-missing path (doFetch
	// checks isForcedMissing before ever calling sf.client.ExecuteWithFailover),
	// so a nil *nntp.Client is safe here.
	sf := NewSegmentFetcher(ctx, nil, cache, cfg, stats, zerolog.Nop())
	defer sf.Close()

	t.Run("normal read pads the dead segment", func(t *testing.T) {
		if err := sf.Fetch(context.Background(), 100); err != nil {
			t.Fatalf("Fetch(normal) = %v, want nil (padded)", err)
		}
		if got := cache.GetState(100); got != StateOnDisk {
			t.Fatalf("segment 100 state = %v, want StateOnDisk (padded)", got)
		}
		data, ok := cache.Get(100)
		if !ok {
			t.Fatalf("segment 100: expected padded bytes on disk")
		}
		for i, b := range data {
			if b != 0 {
				t.Fatalf("segment 100 byte %d = %#x, want zero-fill (padding)", i, b)
			}
		}
	})

	t.Run("verification read (no-pad context) hard-errors on the same dead-segment pattern", func(t *testing.T) {
		noPadCtx := ContextWithoutPadding(context.Background())
		err := sf.Fetch(noPadCtx, 101)
		if err == nil {
			t.Fatalf("Fetch(verification) = nil, want the real article-not-found error")
		}
		if !nntp.IsArticleNotFoundError(err) {
			t.Fatalf("Fetch(verification) error = %v, want article-not-found", err)
		}
		if got := cache.GetState(101); got != StateFailed {
			t.Fatalf("segment 101 state = %v, want StateFailed (no padding/patch applied)", got)
		}
	})
}
