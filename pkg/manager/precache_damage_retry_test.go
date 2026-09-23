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
	pending := func() (int, bool) {
		polls++
		if asks >= 3 && polls > 5 {
			return 0, false
		}
		return 4, false
	}
	res := awaitPrecacheRepair(context.Background(), time.Second, time.Millisecond, 0, pending, handle)
	if !res.repaired || res.gone || res.remaining != 0 {
		t.Fatalf("result = %+v, want repaired", res)
	}
	if asks != 3 {
		t.Fatalf("handle called %d times, want 3 (two retryable no-ops, then acted)", asks)
	}
}

func TestAwaitPrecacheRepairDoesNotReaskFinalNoop(t *testing.T) {
	asks := 0
	handle := func() autoRepairOutcome { asks++; return autoRepairOutcome{reason: "guard tripped"} }
	res := awaitPrecacheRepair(context.Background(), 30*time.Millisecond, time.Millisecond, 0,
		func() (int, bool) { return 2, false }, handle)
	if res.repaired || res.gone || res.remaining != 2 {
		t.Fatalf("result = %+v, want still damaged with 2 pending", res)
	}
	if asks != 1 {
		t.Fatalf("handle called %d times, want 1", asks)
	}
}

// A re-grab that deletes the entry also deletes its overlay record, which
// used to read as "0 pending" - a repair the row then claimed.
func TestAwaitPrecacheRepairReportsDeletedEntryAsGone(t *testing.T) {
	res := awaitPrecacheRepair(context.Background(), time.Second, time.Millisecond, 0,
		func() (int, bool) { return 0, true },
		func() autoRepairOutcome { return autoRepairOutcome{acted: true, regrab: true} })
	if !res.gone || res.repaired {
		t.Fatalf("result = %+v, want gone and not repaired", res)
	}
}
