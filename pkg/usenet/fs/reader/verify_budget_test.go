package reader

import (
	"context"
	"testing"
	"time"
)

func TestVerifyBudget_NilIsUnbounded(t *testing.T) {
	var b *VerifyBudget
	if !b.Add(1 << 40) {
		t.Fatal("nil budget must never report exhaustion")
	}
	if b.Exceeded() {
		t.Fatal("nil budget must never be Exceeded")
	}
	if b.Used() != 0 || b.Limit() != 0 {
		t.Fatalf("nil budget accessors must be zero, got used=%d limit=%d", b.Used(), b.Limit())
	}
}

func TestNewVerifyBudget_NonPositiveIsNil(t *testing.T) {
	for _, limit := range []int64{0, -1} {
		if got := NewVerifyBudget(limit); got != nil {
			t.Fatalf("NewVerifyBudget(%d) = %v, want nil (unbounded)", limit, got)
		}
	}
}

func TestVerifyBudget_AddLatchesOnceExceeded(t *testing.T) {
	b := NewVerifyBudget(100)
	if !b.Add(60) {
		t.Fatal("60 of 100 should still be within budget")
	}
	if b.Exceeded() {
		t.Fatal("budget must not be exceeded at 60/100")
	}
	if b.Add(50) {
		t.Fatal("110 of 100 should report exhaustion")
	}
	if !b.Exceeded() {
		t.Fatal("budget must latch as exceeded")
	}
	// The latch is one-way: a later zero-byte read must not clear it, or a
	// subsequent range request would get a fresh allowance.
	if b.Add(0) {
		t.Fatal("exhausted budget must stay exhausted")
	}
	if b.Used() != 110 {
		t.Fatalf("Used() = %d, want 110", b.Used())
	}
	if b.Limit() != 100 {
		t.Fatalf("Limit() = %d, want 100", b.Limit())
	}
}

func TestVerifyBudget_ObserveAccounting(t *testing.T) {
	b := NewVerifyBudget(1 << 30)

	// Nothing observed yet: every accounting accessor is zero, and no divide
	// by a zero Elapsed.
	if b.Reads() != 0 || b.Wait() != 0 || b.Elapsed() != 0 || b.MiBPerSec() != 0 {
		t.Fatalf("fresh budget accounting must be zero, got reads=%d wait=%v elapsed=%v mib=%f",
			b.Reads(), b.Wait(), b.Elapsed(), b.MiBPerSec())
	}

	b.Observe(4<<20, 10*time.Millisecond)
	time.Sleep(2 * time.Millisecond) // let wall time advance past firstAt
	b.Observe(4<<20, 20*time.Millisecond)
	b.Observe(0, 5*time.Millisecond) // zero-byte read: not counted
	b.Add(8 << 20)                   // Observe does not move the cap; Add does

	if b.Reads() != 2 {
		t.Fatalf("Reads() = %d, want 2 (zero-byte read excluded)", b.Reads())
	}
	if b.Wait() != 30*time.Millisecond {
		t.Fatalf("Wait() = %v, want 30ms", b.Wait())
	}
	if b.Elapsed() <= 0 {
		t.Fatal("Elapsed() must be positive once a byte has been observed")
	}
	if b.MiBPerSec() <= 0 {
		t.Fatal("MiBPerSec() must be positive once bytes have been observed")
	}
}

func TestVerifyBudget_RequestAccounting(t *testing.T) {
	b := NewVerifyBudget(1 << 30)
	b.ObserveRequest(1 << 20)
	b.ObserveRequest(0) // refused before a byte: a request, nothing written
	b.ObserveRequest(3 << 20)
	if b.Requests() != 3 || b.Written() != 4<<20 {
		t.Fatalf("requests=%d written=%d, want 3 and 4 MiB", b.Requests(), b.Written())
	}
	if b.Used() != 0 {
		t.Fatal("ObserveRequest must not charge the budget")
	}
}

func TestVerifyBudget_NilObserveIsSafe(t *testing.T) {
	var b *VerifyBudget
	b.ObserveRequest(1 << 20)
	if b.Requests() != 0 || b.Written() != 0 {
		t.Fatal("nil budget request accounting must be zero")
	}
	b.Observe(1<<20, time.Second) // must not panic
	if b.Reads() != 0 || b.Wait() != 0 || b.Elapsed() != 0 || b.MiBPerSec() != 0 {
		t.Fatal("nil budget accounting accessors must all be zero")
	}
}

func TestVerifyBudget_ExhaustLatchesWithoutACut(t *testing.T) {
	b := NewVerifyBudget(1 << 30)
	b.Add(10)
	b.Exhaust()
	if !b.Exceeded() {
		t.Fatal("Exhaust must latch Exceeded")
	}
	if b.Cut() {
		t.Fatal("Exhaust charged no bytes past the limit; Cut must stay false")
	}
	if b.Add(1) {
		t.Fatal("an exhausted budget must refuse further reads")
	}
	if b.Used() != 11 {
		t.Fatalf("Used() = %d, want 11 (Add still records what was delivered)", b.Used())
	}
}

func TestVerifyBudget_CutOnlyByPassingTheLimit(t *testing.T) {
	b := NewVerifyBudget(100)
	b.Add(100)
	if b.Cut() || b.Exceeded() {
		t.Fatal("reaching the limit exactly is not a cut")
	}
	b.Add(1)
	if !b.Cut() || !b.Exceeded() {
		t.Fatal("passing the limit must be both a cut and exceeded")
	}
}

// A phase is what range requests meter against while it is open, and it is
// independent of its parent in both directions.
func TestVerifyBudget_PhaseRoutingAndIndependence(t *testing.T) {
	parent := NewVerifyBudget(64)
	if parent.ForRequest() != parent {
		t.Fatal("with no phase open, requests must meter against the budget itself")
	}

	p := parent.BeginPhase(1000)
	if p == nil || p == parent {
		t.Fatalf("BeginPhase returned %v, want a distinct phase budget", p)
	}
	if parent.ForRequest() != p {
		t.Fatal("an open phase must be what requests meter against")
	}
	if p.Limit() != 1000 {
		t.Fatalf("phase Limit() = %d, want 1000", p.Limit())
	}

	// Larger than the parent's limit and not capped by it...
	if !p.Add(500) {
		t.Fatal("a phase must not be capped by its parent's limit")
	}
	// ...and spending it charges the parent nothing.
	p.Add(501)
	if !p.Cut() {
		t.Fatal("the phase must latch its own cut")
	}
	if parent.Used() != 0 || parent.Exceeded() {
		t.Fatalf("phase bytes leaked into the parent: used=%d exceeded=%v", parent.Used(), parent.Exceeded())
	}

	parent.EndPhase(p)
	if parent.ForRequest() != parent {
		t.Fatal("after EndPhase requests must meter against the budget itself again")
	}
}

// EndPhase is a compare-and-swap: ending a phase that was already replaced
// must not close its replacement.
func TestVerifyBudget_StaleEndPhaseKeepsTheNewerPhase(t *testing.T) {
	parent := NewVerifyBudget(1 << 30)
	first := parent.BeginPhase(10)
	second := parent.BeginPhase(20)
	parent.EndPhase(first)
	if parent.ForRequest() != second {
		t.Fatal("a stale EndPhase closed the newer phase")
	}
	parent.EndPhase(second)
	if parent.ForRequest() != parent {
		t.Fatal("EndPhase of the open phase must close it")
	}
}

// Phases do not draw on their parent, so a spent verification must not be
// able to read again just by opening one.
func TestVerifyBudget_PhaseOfASpentBudgetStartsSpent(t *testing.T) {
	for name, spend := range map[string]func(*VerifyBudget){
		"cut":       func(b *VerifyBudget) { b.Add(101) },
		"exhausted": func(b *VerifyBudget) { b.Exhaust() },
	} {
		t.Run(name, func(t *testing.T) {
			parent := NewVerifyBudget(100)
			spend(parent)
			p := parent.BeginPhase(1 << 30)
			defer parent.EndPhase(p)
			if !p.Exceeded() {
				t.Fatal("a phase opened on a spent verification must refuse reads")
			}
			if p.Cut() {
				t.Fatal("the phase itself read nothing; it must be exhausted, not cut")
			}
		})
	}
}

func TestVerifyBudget_NilAndEmptyPhasesAreSafe(t *testing.T) {
	var b *VerifyBudget
	if b.BeginPhase(10) != nil {
		t.Fatal("a nil budget must open no phase")
	}
	b.EndPhase(nil)
	b.Exhaust()
	if b.ForRequest() != nil || b.Cut() || b.Exceeded() {
		t.Fatal("nil budget phase and latch accessors must all be zero")
	}

	parent := NewVerifyBudget(100)
	if p := parent.BeginPhase(0); p != nil {
		t.Fatalf("a non-positive phase limit must open no phase, got %v", p)
	}
	if parent.ForRequest() != parent {
		t.Fatal("a refused BeginPhase must leave requests on the budget itself")
	}
	parent.EndPhase(nil) // must neither panic nor close anything
}

func TestVerifyBudget_ContextRoundTrip(t *testing.T) {
	ctx := context.Background()
	if got := VerifyBudgetFromContext(ctx); got != nil {
		t.Fatalf("bare context should carry no budget, got %v", got)
	}
	// A nil budget must not wrap the context at all.
	if ContextWithVerifyBudget(ctx, nil) != ctx {
		t.Fatal("ContextWithVerifyBudget(ctx, nil) must return ctx unchanged")
	}
	b := NewVerifyBudget(1024)
	got := VerifyBudgetFromContext(ContextWithVerifyBudget(ctx, b))
	if got != b {
		t.Fatalf("round-trip returned %v, want %v", got, b)
	}
}
