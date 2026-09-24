package manager

import (
	"testing"
	"time"
)

// An automatic (URGENT-lane) pass claims its entry, so the sweep cannot
// re-grab and delete it under the pass.
func TestUrgentPassClaimsEntry(t *testing.T) {
	p, repair := newTestPar2Repair(t)
	const id = "nzb-urgent"
	p.EnqueueUrgent(id, 0)
	if !p.IsQueued(id) {
		t.Fatal("precondition: urgent job not queued")
	}
	if kind, _, exists := repair.handlers.State(id); !exists || kind != handlerPar2Queued {
		t.Fatalf("claim = %v (exists=%v), want par2_queued", kind, exists)
	}
	if repair.handlers.TryAcquire(id, handlerRegrab) {
		t.Fatal("sweep re-grab claim acquired under a queued URGENT pass")
	}
	// runJob's start and end.
	repair.handlers.EnsurePar2Running(id)
	if kind, _, _ := repair.handlers.State(id); kind != handlerPar2Running {
		t.Fatalf("claim = %v, want par2_running", kind)
	}
	repair.handlers.ReleasePar2(id)
	if _, _, exists := repair.handlers.State(id); exists {
		t.Fatal("pass did not release its own claim")
	}
}

// A live re-grab owns the entry: no pass is queued against the release it is
// replacing, and a pass that ends does not free or overwrite its claim.
func TestPar2PassLeavesRegrabClaim(t *testing.T) {
	p, repair := newTestPar2Repair(t)
	const id = "nzb-regrab"
	if !repair.handlers.TryAcquire(id, handlerRegrab) {
		t.Fatal("setup: regrab claim")
	}
	p.EnqueueUrgent(id, 0)
	if p.IsQueued(id) {
		t.Fatal("URGENT pass queued under a live re-grab")
	}

	repair.handlers.EnsurePar2Running(id)
	repair.handlers.ReleasePar2(id)
	if kind, _, exists := repair.handlers.State(id); !exists || kind != handlerRegrab {
		t.Fatalf("after a pass released: claim = %v (exists=%v), want the re-grab's", kind, exists)
	}
	repair.handlers.MarkTerminalPar2(id)
	if kind, terminal, _ := repair.handlers.State(id); terminal || kind != handlerRegrab {
		t.Fatalf("after a terminal pass: kind=%v terminal=%v, want the re-grab's claim untouched", kind, terminal)
	}

	// With no foreign claim, a terminal pass still marks terminal.
	repair.handlers.Release(id)
	repair.handlers.MarkTerminalPar2(id)
	if !repair.handlers.IsTerminal(id) {
		t.Fatal("terminal pass did not mark terminal")
	}
	// ClaimPar2Queued takes over a terminal mark the storage gate cleared.
	if !repair.handlers.ClaimPar2Queued(id) {
		t.Fatal("ClaimPar2Queued refused a terminal mark")
	}
}

// A running pass (up to par2JobTimeoutMax) keeps its claim past the normal
// 30-minute stale bound; a queued claim does not.
func TestRunningPar2ClaimOutlivesDefaultTTL(t *testing.T) {
	reg := newRepairHandlerRegistry(defaultRepairHandlerTTL)
	now := time.Now()
	reg.nowFn = func() time.Time { return now }
	reg.EnsurePar2Running("run")
	reg.ClaimPar2Queued("queued")

	now = now.Add(2 * time.Hour)
	if reg.TryAcquire("run", handlerRegrab) {
		t.Fatal("running pass lost its claim after 2 h")
	}
	if !reg.TryAcquire("queued", handlerRegrab) {
		t.Fatal("stale queued claim was not reaped")
	}
	now = now.Add(par2RunningClaimTTL)
	if !reg.TryAcquire("run", handlerRegrab) {
		t.Fatal("running claim never goes stale")
	}
}
