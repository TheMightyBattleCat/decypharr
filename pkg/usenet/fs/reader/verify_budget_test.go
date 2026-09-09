package reader

import (
	"context"
	"testing"
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
