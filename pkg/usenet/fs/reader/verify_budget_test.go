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

func TestVerifyBudget_NilObserveIsSafe(t *testing.T) {
	var b *VerifyBudget
	b.Observe(1<<20, time.Second) // must not panic
	if b.Reads() != 0 || b.Wait() != 0 || b.Elapsed() != 0 || b.MiBPerSec() != 0 {
		t.Fatal("nil budget accounting accessors must all be zero")
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
