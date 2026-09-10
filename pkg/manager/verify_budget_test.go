package manager

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
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

	ok, reason, conclusive, _ := f.checkConfirmed(context.Background(), "Some.Show.S01E01", "episode.mkv", expectedRuntime{
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

// newFailingFFProbeBinary writes a stand-in ffprobe that fails the way a
// truncated container does: something on stderr and a non-zero exit. This is
// what the metadata probe sees when the budget was spent mid-body, and it is
// the case that escapes the decode-path guard as ffprobe_unreadable.
func newFailingFFProbeBinary(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ffprobe")
	script := "#!/bin/sh\necho '[matroska,webm @ 0x0] File ended prematurely' >&2\nexit 1\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("writing failing fake ffprobe: %v", err)
	}
	return path
}

// A spent budget must stay inconclusive even when the *metadata* probe is the
// thing that fails - that failure is our own truncation, not the file's.
func TestCheckConfirmed_ExhaustedBudget_UnreadableStillInconclusive(t *testing.T) {
	f := &ffprobeChecker{
		binPath: newFailingFFProbeBinary(t),
		timeout: 5 * time.Second,
		baseURL: "http://127.0.0.1:1/",
		logger:  zerolog.Nop(),
	}

	budget := NewVerifyBudget(100)
	budget.Add(1000) // spent before the probe runs

	ok, reason, conclusive, _ := f.checkConfirmed(context.Background(), "Some.Show.S01E01", "episode.mkv", expectedRuntime{
		Seconds: 3600,
		Bytes:   4 * 1024 * 1024 * 1024,
	}, false, nil, budget)

	if !ok {
		t.Fatalf("a spent budget must never produce a broken verdict, got ok=false reason=%q", reason)
	}
	if conclusive {
		t.Fatal("a spent budget must yield conclusive=false")
	}
}

// Sanity anchor for the test above: with no budget in play the very same
// failing binary MUST still be reported broken, so the guard is suppressing
// our truncation rather than suppressing real failures.
func TestCheckConfirmed_NoBudget_UnreadableStillBroken(t *testing.T) {
	f := &ffprobeChecker{
		binPath: newFailingFFProbeBinary(t),
		timeout: 5 * time.Second,
		baseURL: "http://127.0.0.1:1/",
		logger:  zerolog.Nop(),
	}

	ok, reason, _, _ := f.checkConfirmed(context.Background(), "Some.Show.S01E01", "episode.mkv", expectedRuntime{
		Seconds: 3600,
		Bytes:   4 * 1024 * 1024 * 1024,
	}, false, nil, nil)

	if ok {
		t.Fatal("an unreadable file with no budget must still be broken")
	}
	if !strings.HasPrefix(reason, ffprobeReasonUnreadable) {
		t.Fatalf("expected an %s reason, got %q", ffprobeReasonUnreadable, reason)
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

// The WebDAV handler resolves a request's budget once, as the request starts,
// so a probe run under a phase must be handed that phase - and the file budget
// again once the phase ends.
func TestVerifyBudgetForVerificationRead_FollowsTheOpenPhase(t *testing.T) {
	const hash, file = "phase-hash", "episode.mkv"
	parent := NewVerifyBudget(1 << 30)
	registerVerifyBudget(hash, file, parent)
	defer unregisterVerifyBudget(hash, file, parent)

	phase := parent.BeginPhase(1 << 20)
	if got := VerifyBudgetForVerificationRead(hash, file); got != phase {
		t.Fatalf("lookup during a phase returned %p, want the phase %p", got, phase)
	}
	parent.EndPhase(phase)
	if got := VerifyBudgetForVerificationRead(hash, file); got != parent {
		t.Fatalf("lookup after the phase returned %p, want the file budget %p", got, parent)
	}
}

// budget_cut reads as "the byte cap cut this read". An Exhaust is a stop, not
// a cut, and must not masquerade as one - a line with served_mb far below
// budget_mb would then read as a byte cut. It gets its own key.
func TestBudgetStats_TellsAnExhaustFromACut(t *testing.T) {
	for _, tc := range []struct {
		name          string
		spend         func(*VerifyBudget)
		wantCut       bool
		wantExhausted bool
	}{
		{"byte cut", func(b *VerifyBudget) { b.Add(b.Limit() + 1) }, true, false},
		{"exhausted", func(b *VerifyBudget) { b.Exhaust() }, false, true},
		{"unspent", func(*VerifyBudget) {}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out strings.Builder
			log := zerolog.New(&out)
			b := NewVerifyBudget(1 << 20)
			tc.spend(b)
			budgetStats(log.Info(), b).Msg("verdict")

			if got := strings.Contains(out.String(), `"budget_cut":true`); got != tc.wantCut {
				t.Fatalf("budget_cut=true present: %v, want %v in %s", got, tc.wantCut, out.String())
			}
			if got := strings.Contains(out.String(), `"budget_exhausted":true`); got != tc.wantExhausted {
				t.Fatalf("budget_exhausted=true present: %v, want %v in %s", got, tc.wantExhausted, out.String())
			}
		})
	}
}
