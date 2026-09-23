package manager

import (
	"context"
	"testing"
	"time"
)

// A PAR2-terminal mark on a season pack's nzbID must not block the re-grab
// of a sibling episode (terminal is exactly when the policy picks a
// re-grab), and must be back in place once that re-grab ends.
func TestTryAcquireRegrabTakesOverTerminalMark(t *testing.T) {
	reg := newRepairHandlerRegistry(0)
	reg.MarkTerminal("pack")

	if reg.TryAcquire("pack", handlerRegrab) {
		t.Fatal("setup: plain TryAcquire should still refuse a terminal entry")
	}
	release, ok := reg.TryAcquireRegrab("pack")
	if !ok {
		t.Fatal("TryAcquireRegrab refused a terminal-only entry")
	}
	if kind, terminal, _ := reg.State("pack"); kind != handlerRegrab || terminal {
		t.Fatalf("during re-grab: kind=%s terminal=%v, want regrab/false", kind, terminal)
	}
	if _, ok := reg.TryAcquireRegrab("pack"); ok {
		t.Fatal("a second re-grab won while the first holds the entry")
	}
	release()
	if !reg.IsTerminal("pack") {
		t.Fatal("terminal mark not restored after the re-grab")
	}
}

func TestTryAcquireRegrabYieldsToLiveClaim(t *testing.T) {
	reg := newRepairHandlerRegistry(0)
	if !reg.TryAcquire("e", handlerPar2Running) {
		t.Fatal("setup")
	}
	if _, ok := reg.TryAcquireRegrab("e"); ok {
		t.Fatal("re-grab took an entry a PAR2 pass is running on")
	}

	reg2 := newRepairHandlerRegistry(0)
	release, ok := reg2.TryAcquireRegrab("free")
	if !ok {
		t.Fatal("TryAcquireRegrab refused a free entry")
	}
	release()
	if _, _, exists := reg2.State("free"); exists {
		t.Fatal("claim left behind after releasing a re-grab of a free entry")
	}
}

func TestRegrabOutcomeCooldownIsRetryable(t *testing.T) {
	r := &Repair{}
	out, _ := r.regrabOutcome(false, reasonWithinCooldown+", 1m2s remaining", nil)
	if !out.retry || out.acted {
		t.Fatalf("cooldown outcome = %+v, want retry", out)
	}
	out, _ = r.regrabOutcome(false, "every candidate so far shares the same missing articles", nil)
	if out.retry {
		t.Fatal("a regrab-guard trip must not be retried")
	}
}

func TestAwaitPrecacheRepairRetriesBusyEntry(t *testing.T) {
	asks := 0
	handle := func() autoRepairOutcome {
		asks++
		if asks < 3 {
			return autoRepairOutcome{retry: true, reason: "busy"}
		}
		return autoRepairOutcome{acted: true}
	}
	polls := 0
	pending := func() int {
		polls++
		if asks >= 3 && polls > 5 {
			return 0
		}
		return 4
	}
	repaired, remaining := awaitPrecacheRepair(context.Background(), time.Second, time.Millisecond, 0, pending, handle)
	if !repaired || remaining != 0 {
		t.Fatalf("repaired=%v remaining=%d, want repaired", repaired, remaining)
	}
	if asks != 3 {
		t.Fatalf("handle called %d times, want 3 (two retryable no-ops, then acted)", asks)
	}
}

func TestAwaitPrecacheRepairDoesNotReaskFinalNoop(t *testing.T) {
	asks := 0
	handle := func() autoRepairOutcome { asks++; return autoRepairOutcome{reason: "guard tripped"} }
	repaired, remaining := awaitPrecacheRepair(context.Background(), 30*time.Millisecond, time.Millisecond, 0,
		func() int { return 2 }, handle)
	if repaired || remaining != 2 {
		t.Fatalf("repaired=%v remaining=%d, want still damaged with 2 pending", repaired, remaining)
	}
	if asks != 1 {
		t.Fatalf("handle called %d times, want 1", asks)
	}
}
