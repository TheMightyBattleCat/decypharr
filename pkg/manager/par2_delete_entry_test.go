package manager

import (
	"testing"
	"time"
)

// Deleting an entry cancels its running PAR2 pass, and the pass then records
// nothing: no state row, no attempt row, no "repair failed" line or
// notification.
func TestPar2DeleteEntryCancelsRunningPass(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	h := newP2eHarness(t, servers[0], fx, "p2e-delete")
	f1 := fx.posted[0]
	h.recordDead(p2eSeg{f1, 1})
	h.p.manager.par2Repair = h.p
	// Every article of the release hangs once the warm-up is through, so the
	// pass blocks on its first fetch until something cancels it.
	h.afterWarmup = func() {
		for id := range fx.all {
			servers[0].set(id, p2eStallAll)
		}
	}

	var deletedAt time.Time
	var deleteErr error
	var endedIn time.Duration
	h.run(func(done <-chan struct{}) {
		for !h.p.IsRunning(h.nzbID) {
			select {
			case <-done:
				return
			case <-time.After(2 * time.Millisecond):
			}
		}
		time.Sleep(300 * time.Millisecond) // well into the blocked fetch
		deletedAt = time.Now()
		deleteErr = h.p.manager.deleteEntry(h.nzbID, false)
		for h.p.IsRunning(h.nzbID) && time.Since(deletedAt) < 5*time.Second {
			time.Sleep(2 * time.Millisecond)
		}
		endedIn = time.Since(deletedAt)
	})
	if deletedAt.IsZero() {
		t.Fatalf("the pass ended before the delete")
	}
	if deleteErr != nil {
		t.Fatalf("deleteEntry: %v", deleteErr)
	}
	if endedIn > time.Second {
		t.Fatalf("pass ended %v after the delete, want within 1s", endedIn)
	}
	if pr, ok := h.p.Progress(h.nzbID); !ok || pr.Phase != Par2PhaseFailed || pr.LastError != "entry deleted" {
		t.Fatalf("progress after delete = %+v (ok=%v), want failed / entry deleted", pr, ok)
	}
	if st, err := h.p.manager.storage.GetPar2RepairState(h.nzbID); err == nil && st != nil {
		t.Fatalf("deleted entry left a PAR2 state row: %+v", st)
	}
	if a, err := h.p.manager.storage.LatestPar2RepairAttemptForNzb(h.nzbID); err == nil && a != nil {
		t.Fatalf("deleted entry's pass recorded an attempt: %+v", a)
	}
	for _, ln := range h.logs.lines() {
		if m, _ := ln["message"].(string); m == "par2 repair unavailable" {
			t.Fatalf("deleted entry's pass logged a failure (and notified): %v", ln)
		}
	}
	if h.p.IsRunning(h.nzbID) {
		t.Fatalf("pass still registered after the delete")
	}
	if _, _, exists := h.repair.handlers.State(h.nzbID); exists {
		t.Fatalf("handler claim not released after the delete")
	}
}

// A queued URGENT request for a deleted entry is dropped with its claim.
func TestPar2CancelEntryDropsQueuedUrgent(t *testing.T) {
	p, repair := newTestPar2Repair(t)
	if !repair.handlers.ClaimPar2Queued("q1") {
		t.Fatalf("claim refused")
	}
	p.urgentSeq++
	item := &urgentJob{nzbID: "q1", seq: p.urgentSeq}
	p.urgentSet["q1"] = item
	p.urgentHeap.Push(item)
	p.CancelEntry("q1")
	if p.IsQueued("q1") || len(p.urgentHeap) != 0 {
		t.Fatalf("urgent request still queued after CancelEntry")
	}
	if _, _, exists := repair.handlers.State("q1"); exists {
		t.Fatalf("queued request's claim not released")
	}
}
