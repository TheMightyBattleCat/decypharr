package reader

import (
	"context"
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

	segments := []SegmentMeta{
		{MessageID: msgIDNormal, Number: 1, Bytes: 1024},
		{MessageID: msgIDNoPad, Number: 2, Bytes: 1024},
	}

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
		if err := sf.Fetch(context.Background(), 0); err != nil {
			t.Fatalf("Fetch(normal) = %v, want nil (padded)", err)
		}
		if got := cache.GetState(0); got != StateOnDisk {
			t.Fatalf("segment 0 state = %v, want StateOnDisk (padded)", got)
		}
		data, ok := cache.Get(0)
		if !ok {
			t.Fatalf("segment 0: expected padded bytes on disk")
		}
		for i, b := range data {
			if b != 0 {
				t.Fatalf("segment 0 byte %d = %#x, want zero-fill (padding)", i, b)
			}
		}
	})

	t.Run("verification read (no-pad context) hard-errors on the same dead-segment pattern", func(t *testing.T) {
		noPadCtx := ContextWithoutPadding(context.Background())
		err := sf.Fetch(noPadCtx, 1)
		if err == nil {
			t.Fatalf("Fetch(verification) = nil, want the real article-not-found error")
		}
		if !nntp.IsArticleNotFoundError(err) {
			t.Fatalf("Fetch(verification) error = %v, want article-not-found", err)
		}
		if got := cache.GetState(1); got != StateFailed {
			t.Fatalf("segment 1 state = %v, want StateFailed (no padding/patch applied)", got)
		}
	})
}
