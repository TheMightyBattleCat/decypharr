package manager

import (
	"context"
	"testing"
	"time"
)

func TestVerifyBudgetFor_Sizing(t *testing.T) {
	const gb = int64(1024 * 1024 * 1024)
	tests := []struct {
		name  string
		bytes int64
		want  int64 // 0 => expect nil (unbounded)
	}{
		{"unknown size is unbounded", 0, 0},
		{"negative size is unbounded", -1, 0},
		{"small file gets the floor", 300 * 1024 * 1024, verifyBudgetFloor},
		{"1GB still under the floor", gb, verifyBudgetFloor},
		{"4GB scales to an eighth", 4 * gb, 4 * gb / 8},
		{"40GB REMUX scales too", 40 * gb, 40 * gb / 8},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := verifyBudgetFor(tc.bytes)
			if tc.want == 0 {
				if got != nil {
					t.Fatalf("verifyBudgetFor(%d) = %d, want nil", tc.bytes, got.Limit())
				}
				return
			}
			if got == nil {
				t.Fatalf("verifyBudgetFor(%d) = nil, want %d", tc.bytes, tc.want)
			}
			if got.Limit() != tc.want {
				t.Fatalf("verifyBudgetFor(%d) limit = %d, want %d", tc.bytes, got.Limit(), tc.want)
			}
		})
	}
}

// The budget must never be able to produce a broken verdict: truncating the
// body ourselves makes ffprobe complain about a container we cut short, and
// acting on that would blocklist a grab on the strength of our own cap.
func TestCheckConfirmed_ExhaustedBudgetIsInconclusiveNotBroken(t *testing.T) {
	f := newTestFFProbeChecker(t, 3600, func(ctx context.Context, entryFolder, fileName string, probed time.Duration) bool {
		return true
	})

	budget := NewVerifyBudget(100)
	budget.Add(1000) // spend it before the probe runs

	ok, reason, conclusive := f.checkConfirmed(context.Background(), "Some.Show.S01E01", "episode.mkv", expectedRuntime{
		Seconds:               3600,
		EpisodeCountConfirmed: true,
		Bytes:                 4 * 1024 * 1024 * 1024,
	}, false, nil, budget)

	if !ok {
		t.Fatalf("a spent budget must never yield a broken verdict, got ok=false reason=%q", reason)
	}
	if conclusive {
		t.Fatal("a spent budget must yield conclusive=false so nothing is stamped decode-verified")
	}
	if reason != "" {
		t.Errorf("expected empty reason, got %q", reason)
	}
}

func TestVerifyBudgetRegistry_RoundTripAndScoping(t *testing.T) {
	const hash, file = "abc123", "episode.mkv"

	if got := VerifyBudgetForVerificationRead(hash, file); got != nil {
		t.Fatalf("registry should start empty, got %v", got)
	}

	b := NewVerifyBudget(1024)
	registerVerifyBudget(hash, file, b)
	if got := VerifyBudgetForVerificationRead(hash, file); got != b {
		t.Fatalf("lookup returned %v, want %v", got, b)
	}
	// Keyed by both parts: another file under the same hash is unaffected.
	if got := VerifyBudgetForVerificationRead(hash, "other.mkv"); got != nil {
		t.Fatalf("lookup for a different file returned %v, want nil", got)
	}

	// A stale unregister from a probe that already lost the slot must not
	// evict the live one (CompareAndDelete semantics).
	unregisterVerifyBudget(hash, file, NewVerifyBudget(2048))
	if got := VerifyBudgetForVerificationRead(hash, file); got != b {
		t.Fatalf("a foreign unregister evicted the live budget: got %v", got)
	}

	unregisterVerifyBudget(hash, file, b)
	if got := VerifyBudgetForVerificationRead(hash, file); got != nil {
		t.Fatalf("registry should be empty after unregister, got %v", got)
	}
}

func TestRegisterVerifyBudget_IgnoresNilAndEmptyHash(t *testing.T) {
	registerVerifyBudget("", "episode.mkv", NewVerifyBudget(1024))
	if got := VerifyBudgetForVerificationRead("", "episode.mkv"); got != nil {
		t.Fatalf("an empty infoHash must not be registered, got %v", got)
	}
	registerVerifyBudget("hash", "episode.mkv", nil)
	if got := VerifyBudgetForVerificationRead("hash", "episode.mkv"); got != nil {
		t.Fatalf("a nil budget must not be registered, got %v", got)
	}
}
