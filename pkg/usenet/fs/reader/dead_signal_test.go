package reader

import (
	"context"
	"sync"
	"testing"
)

func TestDeadSegmentSignal(t *testing.T) {
	var nilSig *DeadSegmentSignal
	if nilSig.Detected() {
		t.Fatal("nil signal reports detected")
	}
	nilSig.Trip() // must not panic

	sig := NewDeadSegmentSignal()
	if sig.Detected() {
		t.Fatal("fresh signal reports detected")
	}
	sig.Trip()
	if !sig.Detected() {
		t.Fatal("signal not detected after Trip")
	}

	// Concurrent trips are safe and observable.
	sig2 := NewDeadSegmentSignal()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); sig2.Trip() }()
	}
	wg.Wait()
	if !sig2.Detected() {
		t.Fatal("signal not detected after concurrent Trips")
	}
}

func TestDeadSignalContextRoundTrip(t *testing.T) {
	ctx := context.Background()
	if DeadSignalFromContext(ctx) != nil {
		t.Fatal("bare context carried a signal")
	}
	if ContextWithDeadSignal(ctx, nil) != ctx {
		t.Fatal("ContextWithDeadSignal(nil) altered the context")
	}

	sig := NewDeadSegmentSignal()
	ctx = ContextWithDeadSignal(ctx, sig)
	got := DeadSignalFromContext(ctx)
	if got != sig {
		t.Fatalf("round-tripped signal mismatch: got %p want %p", got, sig)
	}
	got.Trip()
	if !sig.Detected() {
		t.Fatal("tripping the context signal did not affect the original pointer")
	}
}
