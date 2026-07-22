package manager

import (
	"testing"
	"time"
)

func TestRepairHandlerRegistryTryAcquireBlocksDoubleHandling(t *testing.T) {
	reg := newRepairHandlerRegistry(0)

	if !reg.TryAcquire("nzb1", handlerPar2Queued) {
		t.Fatalf("first TryAcquire should succeed on a free entry")
	}
	if reg.TryAcquire("nzb1", handlerRegrab) {
		t.Fatalf("second TryAcquire must fail while nzb1 is already claimed (par2_queued) - this is exactly the double-handling the registry exists to prevent")
	}
	if reg.TryAcquire("nzb1", handlerPar2Queued) {
		t.Fatalf("re-acquiring the same kind must also fail while claimed")
	}

	// A different entry is unaffected.
	if !reg.TryAcquire("nzb2", handlerRegrab) {
		t.Fatalf("TryAcquire on an unrelated entry must succeed")
	}
}

func TestRepairHandlerRegistryReleaseFreesTheSlot(t *testing.T) {
	reg := newRepairHandlerRegistry(0)

	if !reg.TryAcquire("nzb1", handlerRegrab) {
		t.Fatalf("TryAcquire should succeed")
	}
	reg.Release("nzb1")
	if !reg.TryAcquire("nzb1", handlerPar2Queued) {
		t.Fatalf("TryAcquire after Release should succeed - the slot must be free again")
	}
}

func TestRepairHandlerRegistryTransitionUpdatesKindInPlace(t *testing.T) {
	reg := newRepairHandlerRegistry(0)
	reg.TryAcquire("nzb1", handlerPar2Queued)

	reg.Transition("nzb1", handlerPar2Running)
	kind, terminal, exists := reg.State("nzb1")
	if !exists || terminal {
		t.Fatalf("State = kind=%v terminal=%v exists=%v, want exists=true terminal=false", kind, terminal, exists)
	}
	if kind != handlerPar2Running {
		t.Fatalf("kind = %v, want par2_running after Transition", kind)
	}

	// Still claimed - a concurrent caller must not be able to acquire it.
	if reg.TryAcquire("nzb1", handlerRegrab) {
		t.Fatalf("TryAcquire must still fail after Transition - the entry is still claimed, just under a new kind")
	}
}

func TestRepairHandlerRegistryMarkTerminalBlocksAutoAcquireUntilCleared(t *testing.T) {
	reg := newRepairHandlerRegistry(0)
	reg.MarkTerminal("nzb1")

	if !reg.IsTerminal("nzb1") {
		t.Fatalf("IsTerminal should be true right after MarkTerminal")
	}
	if reg.TryAcquire("nzb1", handlerRegrab) {
		t.Fatalf("TryAcquire must fail while terminal - an automatic caller must never silently override a terminal-unrepairable mark")
	}
	// Release must NOT clear a terminal mark - only ClearTerminal does.
	reg.Release("nzb1")
	if !reg.IsTerminal("nzb1") {
		t.Fatalf("Release must not clear a terminal mark")
	}

	// The manual override path: ClearTerminal un-sticks it.
	reg.ClearTerminal("nzb1")
	if reg.IsTerminal("nzb1") {
		t.Fatalf("IsTerminal should be false after ClearTerminal")
	}
	if !reg.TryAcquire("nzb1", handlerRegrab) {
		t.Fatalf("TryAcquire should succeed once ClearTerminal has freed the entry")
	}
}

func TestRepairHandlerRegistrySetOverridesAndClearsTerminal(t *testing.T) {
	reg := newRepairHandlerRegistry(0)
	reg.MarkTerminal("nzb1")

	// Set is the manual-override primitive: it must proceed even though the
	// entry is terminal, and clear the terminal mark in the process.
	reg.Set("nzb1", handlerRegrab)

	if reg.IsTerminal("nzb1") {
		t.Fatalf("Set must clear a terminal mark")
	}
	kind, _, exists := reg.State("nzb1")
	if !exists || kind != handlerRegrab {
		t.Fatalf("State after Set = kind=%v exists=%v, want regrab/true", kind, exists)
	}
}

func TestRepairHandlerRegistryTTLReleasesStaleClaims(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	reg := newRepairHandlerRegistry(10 * time.Minute)
	reg.nowFn = func() time.Time { return now }

	if !reg.TryAcquire("nzb1", handlerPar2Running) {
		t.Fatalf("TryAcquire should succeed")
	}
	if reg.TryAcquire("nzb1", handlerRegrab) {
		t.Fatalf("TryAcquire should still fail before the TTL elapses")
	}

	// Advance time past the TTL without ever calling Release - simulating a
	// goroutine that panicked or was killed before its deferred release ran.
	now = now.Add(11 * time.Minute)

	if !reg.TryAcquire("nzb1", handlerRegrab) {
		t.Fatalf("TryAcquire should succeed once the stale claim has passed its TTL")
	}
}

func TestRepairHandlerRegistryTTLNeverExpiresTerminal(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	reg := newRepairHandlerRegistry(10 * time.Minute)
	reg.nowFn = func() time.Time { return now }

	reg.MarkTerminal("nzb1")
	now = now.Add(24 * time.Hour)

	if !reg.IsTerminal("nzb1") {
		t.Fatalf("a terminal mark must survive past the TTL - it is sticky until ClearTerminal, not time-based")
	}
	if reg.TryAcquire("nzb1", handlerRegrab) {
		t.Fatalf("TryAcquire must still fail - terminal claims never expire on their own")
	}
}

func TestRepairHandlerRegistryReapStaleSweepsExpiredNonTerminalClaims(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	reg := newRepairHandlerRegistry(10 * time.Minute)
	reg.nowFn = func() time.Time { return now }

	reg.TryAcquire("stale", handlerPar2Queued)
	reg.MarkTerminal("terminal")

	now = now.Add(11 * time.Minute)
	reg.TryAcquire("fresh", handlerRegrab)

	reaped := reg.ReapStale()
	if reaped != 1 {
		t.Fatalf("ReapStale reaped %d entries, want 1 (only the stale non-terminal one)", reaped)
	}
	if _, _, exists := reg.State("stale"); exists {
		t.Fatalf("stale entry should have been reaped")
	}
	if !reg.IsTerminal("terminal") {
		t.Fatalf("terminal entry must survive ReapStale")
	}
	if _, _, exists := reg.State("fresh"); !exists {
		t.Fatalf("fresh (not yet stale) entry must survive ReapStale")
	}
}

func TestRepairHandlerRegistryEmptyNzbIDAlwaysFails(t *testing.T) {
	reg := newRepairHandlerRegistry(0)
	if reg.TryAcquire("", handlerRegrab) {
		t.Fatalf("TryAcquire with an empty nzbID must fail - there is nothing to key the claim on")
	}
}
