package manager

import (
	"context"
	"testing"
	"time"
)

// Every lane's job is visible to IsRunning, and ending one job never hides
// another. The old single "active" pointer showed only the last job to start
// and was cleared by whichever finished first.
func TestPar2ClaimRunVisibleAcrossLanes(t *testing.T) {
	p, _ := newTestPar2Repair(t)
	ctx := context.Background()

	a, ok := p.claimRun(ctx, "a", laneUrgent, func() {})
	if !ok {
		t.Fatal("claim a")
	}
	b, ok := p.claimRun(ctx, "b", laneBatch, func() {})
	if !ok {
		t.Fatal("claim b")
	}
	if !p.IsRunning("a") || !p.IsRunning("b") {
		t.Fatalf("IsRunning a=%v b=%v, want both", p.IsRunning("a"), p.IsRunning("b"))
	}
	p.releaseRun("b", b)
	if !p.IsRunning("a") {
		t.Fatal("finishing b hid the urgent job a")
	}
	p.releaseRun("a", a)
	if p.IsRunning("a") {
		t.Fatal("a still running after release")
	}
}

// A second pass for an nzbID that is already running is refused, so RunNow
// can no longer start one beside an URGENT pass.
func TestPar2ClaimRunRefusesDuplicate(t *testing.T) {
	p, _ := newTestPar2Repair(t)
	ctx := context.Background()
	job, ok := p.claimRun(ctx, "x", laneUrgent, func() {})
	if !ok {
		t.Fatal("first claim")
	}
	defer p.releaseRun("x", job)
	if _, ok := p.claimRun(ctx, "x", laneBatch, func() {}); ok {
		t.Fatal("duplicate claim for a running nzbID succeeded")
	}
}

// An URGENT job waits for the batch pass it preempted to unwind (it may still
// be writing patches) rather than being dropped or running beside it.
func TestPar2ClaimRunWaitsOutPreemptedJob(t *testing.T) {
	p, _ := newTestPar2Repair(t)
	ctx := context.Background()
	batch, ok := p.claimRun(ctx, "x", laneBatch, func() {})
	if !ok {
		t.Fatal("batch claim")
	}
	batch.preempted.Store(true)

	got := make(chan bool, 1)
	go func() {
		job, ok := p.claimRun(ctx, "x", laneUrgent, func() {})
		if ok {
			p.releaseRun("x", job)
		}
		got <- ok
	}()
	select {
	case <-got:
		t.Fatal("urgent claim succeeded while the preempted batch job was still registered")
	case <-time.After(50 * time.Millisecond):
	}
	p.releaseRun("x", batch)
	select {
	case ok := <-got:
		if !ok {
			t.Fatal("urgent claim refused after the preempted job released")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("urgent claim never proceeded")
	}
}

// IsQueued covers the URGENT heap, and RunNow refuses an nzbID waiting there.
func TestPar2IsQueuedAndRunNowSeeUrgentHeap(t *testing.T) {
	p, _ := newTestPar2Repair(t)
	p.urgentMu.Lock()
	p.urgentSet["u"] = &urgentJob{nzbID: "u"}
	p.urgentMu.Unlock()
	if !p.IsQueued("u") {
		t.Fatal("IsQueued false for an nzbID in the urgent heap")
	}
	if err := p.RunNow("u"); err == nil {
		t.Fatal("RunNow accepted an nzbID already waiting in the urgent lane")
	}
}
