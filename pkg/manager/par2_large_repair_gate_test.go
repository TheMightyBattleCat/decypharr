package manager

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

const gateSlice = 10 << 20 // a 10 MiB REMUX slice

func newGatedPar2Repair() *Par2Repair {
	return &Par2Repair{logger: zerolog.Nop(), largeRepair: make(chan struct{}, 1)}
}

// A second large repair waits, shown as queued, and starts on its own as
// soon as the first one releases the gate.
func TestLargeRepairsRunOneAtATime(t *testing.T) {
	p := newGatedPar2Repair()
	ctx := context.Background()
	large := int(largeRepairBytes/gateSlice) + 1

	var firstHeld bool
	if err := p.waitForLargeRepair(ctx, nil, "first", large, gateSlice, &firstHeld); err != nil || !firstHeld {
		t.Fatalf("first large repair: held=%v err=%v, want it to take the gate at once", firstHeld, err)
	}

	progress := newPar2JobProgressState("second", "second")
	progress.SetPhase(Par2PhaseFetchingRecovery)
	var secondHeld bool
	done := make(chan error, 1)
	go func() { done <- p.waitForLargeRepair(ctx, progress, "second", large, gateSlice, &secondHeld) }()

	deadline := time.Now().Add(2 * time.Second)
	for progress.Phase() != Par2PhaseQueued {
		if time.Now().After(deadline) {
			t.Fatalf("second large repair phase = %q, want %q while it waits", progress.Phase(), Par2PhaseQueued)
		}
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case err := <-done:
		t.Fatalf("second large repair did not wait: %v", err)
	default:
	}

	p.releaseLargeRepair()
	select {
	case err := <-done:
		if err != nil || !secondHeld {
			t.Fatalf("second large repair after release: held=%v err=%v", secondHeld, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second large repair did not start after the first released the gate")
	}
	if got := progress.Phase(); got != Par2PhaseFetchingRecovery {
		t.Errorf("phase after the wait = %q, want the pre-wait phase %q restored", got, Par2PhaseFetchingRecovery)
	}
}

// Repairs at or under largeRepairBytes, and a job already holding the gate,
// never wait.
func TestSmallRepairPassesLargeRepairGate(t *testing.T) {
	p := newGatedPar2Repair()
	var held bool
	if err := p.waitForLargeRepair(context.Background(), nil, "large", int(largeRepairBytes/gateSlice)+1, gateSlice, &held); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var smallHeld bool
	if err := p.waitForLargeRepair(ctx, nil, "small", int(largeRepairBytes/gateSlice), gateSlice, &smallHeld); err != nil || smallHeld {
		t.Errorf("small repair: held=%v err=%v, want it through without the gate", smallHeld, err)
	}
	// The holder growing its damaged set does not wait on itself.
	if err := p.waitForLargeRepair(ctx, nil, "large", 128, gateSlice, &held); err != nil {
		t.Errorf("gate holder re-checking: %v, want no wait", err)
	}

	var ungated bool
	if err := (&Par2Repair{}).waitForLargeRepair(ctx, nil, "x", 128, gateSlice, &ungated); err != nil || ungated {
		t.Errorf("no gate: held=%v err=%v, want a no-op", ungated, err)
	}
}

// A wait cut short by the job's timeout or preemption returns promptly and
// is transient, so the repair is retried rather than re-grabbed.
func TestLargeRepairWaitCancelledIsTransient(t *testing.T) {
	p := newGatedPar2Repair()
	var held bool
	large := int(largeRepairBytes/gateSlice) + 1
	if err := p.waitForLargeRepair(context.Background(), nil, "first", large, gateSlice, &held); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var waiterHeld bool
	err := p.waitForLargeRepair(ctx, nil, "second", large, gateSlice, &waiterHeld)
	if err == nil || waiterHeld {
		t.Fatalf("cancelled wait: held=%v err=%v, want an error without the gate", waiterHeld, err)
	}
	if class := classifyPar2Failure(err); class.terminal || class.suspect {
		t.Errorf("classifyPar2Failure(%q) = %+v, want transient", err, class)
	}
}
