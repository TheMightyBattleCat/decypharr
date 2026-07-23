package usenet

import (
	"context"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/usenet/fs/reader"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

func newTestUsenetWithOverlay(t *testing.T) *Usenet {
	t.Helper()
	store, err := overlay.NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("overlay.NewStore: %v", err)
	}
	return &Usenet{overlay: store}
}

// TestShouldPoisonFailedFile proves commits A and B's central safety
// property: failedFiles (the permanent-failure cache consulted by
// preStreamChecks) must only ever be poisoned by a real playback failure
// against a file the overlay has already given up on (Verdict == Failed),
// never by a verification-only read and never while the file is merely
// Clean (no overlay record yet - padding hasn't had a chance to run) or
// Degraded (padding already covers it, within caps).
func TestShouldPoisonFailedFile(t *testing.T) {
	const nzbID, file = "nzb-1", "movie.mkv"

	t.Run("no overlay record yet (clean) does not poison", func(t *testing.T) {
		u := newTestUsenetWithOverlay(t)
		if got := u.shouldPoisonFailedFile(context.Background(), nzbID, file); got {
			t.Fatalf("shouldPoisonFailedFile() = true, want false (verdict clean)")
		}
	})

	t.Run("degraded (padded, within caps) does not poison", func(t *testing.T) {
		u := newTestUsenetWithOverlay(t)
		u.overlay.SetPolicy(overlay.Policy{MaxRunSegments: 10, MaxTotalSegments: 10, MaxByteRatio: 1.0})
		if _, verdict := u.overlay.Decide(nzbID, file, 0, "<msg1@test>", 1024, 1<<20); verdict != overlay.VerdictDegraded {
			t.Fatalf("setup: Decide verdict = %v, want degraded", verdict)
		}
		if got := u.shouldPoisonFailedFile(context.Background(), nzbID, file); got {
			t.Fatalf("shouldPoisonFailedFile() = true, want false (verdict degraded) - would permanently block the padding path from ever running again")
		}
	})

	t.Run("failed (beyond caps) poisons", func(t *testing.T) {
		u := newTestUsenetWithOverlay(t)
		u.overlay.SetPolicy(overlay.Policy{MaxRunSegments: 1, MaxTotalSegments: 1, MaxByteRatio: 1.0})
		// First segment exhausts the (tiny) caps; second is beyond them.
		_, _ = u.overlay.Decide(nzbID, file, 0, "<msg1@test>", 1024, 1<<20)
		if _, verdict := u.overlay.Decide(nzbID, file, 2, "<msg2@test>", 1024, 1<<20); verdict != overlay.VerdictFailed {
			t.Fatalf("setup: Decide verdict = %v, want failed", verdict)
		}
		if got := u.shouldPoisonFailedFile(context.Background(), nzbID, file); !got {
			t.Fatalf("shouldPoisonFailedFile() = false, want true (verdict failed)")
		}
	})

	t.Run("verification read never poisons, even when verdict is failed", func(t *testing.T) {
		u := newTestUsenetWithOverlay(t)
		u.overlay.SetPolicy(overlay.Policy{MaxRunSegments: 1, MaxTotalSegments: 1, MaxByteRatio: 1.0})
		_, _ = u.overlay.Decide(nzbID, file, 0, "<msg1@test>", 1024, 1<<20)
		_, _ = u.overlay.Decide(nzbID, file, 2, "<msg2@test>", 1024, 1<<20)

		verificationCtx := reader.ContextWithoutPadding(context.Background())
		if got := u.shouldPoisonFailedFile(verificationCtx, nzbID, file); got {
			t.Fatalf("shouldPoisonFailedFile() = true, want false (verification read must never poison)")
		}
	})
}
