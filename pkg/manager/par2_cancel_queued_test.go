package manager

import (
	"context"
	"testing"
	"time"
)

// An entry deleted while its batch request still sits in the channel is
// dropped: it no longer reads as queued, its claim is released, and loop
// skips the request when it reaches it. A request queued again afterwards
// still runs.
func TestPar2CancelEntryDropsBatchQueued(t *testing.T) {
	p, repair := newTestPar2Repair(t)
	p.Enqueue("b1")
	if !p.IsQueued("b1") {
		t.Fatalf("Enqueue did not queue b1")
	}
	p.CancelEntry("b1")
	if p.IsQueued("b1") {
		t.Fatalf("batch request still queued after CancelEntry")
	}
	if _, _, exists := repair.handlers.State("b1"); exists {
		t.Fatalf("batch request's claim not released")
	}

	// Queued again before loop reached the dropped request: two in the
	// channel, the first skipped, the second run.
	p.Enqueue("b1")
	if len(p.queue) != 2 {
		t.Fatalf("channel holds %d requests, want 2", len(p.queue))
	}
	if p.takeQueued(<-p.queue) {
		t.Fatalf("loop would run the request dropped by CancelEntry")
	}
	if !p.IsQueued("b1") {
		t.Fatalf("skipping the dropped request cleared the new one")
	}
	if !p.takeQueued(<-p.queue) {
		t.Fatalf("loop would skip the request queued after the drop")
	}
	if p.IsQueued("b1") {
		t.Fatalf("taken request still reads as queued")
	}
}

// An entry deleted while its batch request waits for the repair window is
// dropped too: loop's ticker no longer runs it.
func TestPar2CancelEntryDropsBatchDeferred(t *testing.T) {
	p, repair := newTestPar2Repair(t)
	p.Enqueue("d1")
	p.Enqueue("d2")
	// loop took both off the channel while the window was closed.
	pending := []string{<-p.queue, <-p.queue}
	for _, id := range pending {
		if !p.takeQueued(id) {
			t.Fatalf("takeQueued(%s) = false", id)
		}
		p.deferred[id] = struct{}{}
	}

	p.CancelEntry("d1")
	if p.IsQueued("d1") {
		t.Fatalf("deferred request still queued after CancelEntry")
	}
	if _, _, exists := repair.handlers.State("d1"); exists {
		t.Fatalf("deferred request's claim not released")
	}
	if !p.IsQueued("d2") {
		t.Fatalf("CancelEntry dropped another entry's request")
	}

	todo := p.takeDeferred(pending)
	if len(todo) != 1 || todo[0] != "d2" {
		t.Fatalf("ticker would run %v, want [d2]", todo)
	}
	if p.IsQueued("d2") {
		t.Fatalf("taken deferred request still reads as queued")
	}
}

// A RunNow pass that has its slot still holds its nzbID in queued, but it
// is not a batch request: a CancelEntry for it must leave no drop mark, or
// loop would skip the next batch request for the same nzbID and strand its
// claim.
func TestPar2CancelEntryLeavesRunningRunNowAlone(t *testing.T) {
	id := "p2e-slot-run"
	h := newManualSlotsHarness(t, id)
	p := h.p
	if err := p.RunNow(id); err != nil {
		t.Fatalf("RunNow: %v", err)
	}
	waitManual(t, "the pass to run", func() bool { return manualRunning(p, []string{id}) == 1 })

	p.CancelEntry(id)
	p.mu.Lock()
	_, marked := p.dropped[id]
	p.mu.Unlock()
	if marked {
		t.Fatalf("CancelEntry marked a running RunNow pass as a dropped batch request")
	}
	waitManual(t, "the cancelled pass to end", func() bool { return manualRunning(p, []string{id}) == 0 })
}

// A pass whose entry is gone at start records nothing - no state row, no
// attempt row, no failure line - and releases its claim without a terminal
// mark, as a pass for a deleted entry does.
func TestPar2GhostEntryPassRecordsNothing(t *testing.T) {
	// A configured Usenet client (the pass stops before anything else
	// without one), and no stored entry for the nzbID the pass runs for.
	servers, fx := newP2eEnv(t, 1)
	h := newP2eHarness(t, servers[0], fx, "p2e-ghost")
	p, repair, logs := h.p, h.repair, h.logs

	if !repair.handlers.ClaimPar2Queued("ghost") {
		t.Fatalf("claim refused")
	}
	p.runJob("ghost", laneBatch)

	if st, err := p.manager.storage.GetPar2RepairState("ghost"); err == nil && st != nil {
		t.Fatalf("ghost pass left a PAR2 state row: %+v", st)
	}
	if a, err := p.manager.storage.LatestPar2RepairAttemptForNzb("ghost"); err == nil && a != nil {
		t.Fatalf("ghost pass recorded an attempt: %+v", a)
	}
	if _, _, exists := repair.handlers.State("ghost"); exists {
		t.Fatalf("ghost pass left a claim (or a terminal mark)")
	}
	if p.IsRunning("ghost") || p.IsQueued("ghost") {
		t.Fatalf("ghost pass still registered or queued a follow-up")
	}
	if pr, ok := p.Progress("ghost"); !ok || pr.Phase != Par2PhaseFailed || pr.LastError != "entry no longer exists" {
		t.Fatalf("progress = %+v (ok=%v), want failed / entry no longer exists", pr, ok)
	}
	for _, ln := range logs.lines() {
		if m, _ := ln["message"].(string); m == "par2 repair unavailable" {
			t.Fatalf("ghost pass logged a failure: %v", ln)
		}
	}
}

// The batch worker itself skips a request dropped by CancelEntry: no pass
// starts for the deleted entry (no progress record, which any pass that
// reaches runPass creates), and it leaves no state or attempt row. A request
// queued behind it still runs.
func TestPar2BatchLoopSkipsCancelledRequest(t *testing.T) {
	p, _ := newTestPar2Repair(t)
	if !p.readyToRun() {
		t.Fatalf("batch lane not ready in a default config")
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	// Both requests are in the channel, and the first one cancelled, before
	// the worker starts: a running worker could take "gone" off the channel
	// before CancelEntry reaches it.
	p.Enqueue("gone")
	p.CancelEntry("gone")
	p.Enqueue("sentinel")
	p.wg.Add(1)
	go p.loop()
	t.Cleanup(func() { p.cancel(); p.wg.Wait() })
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := p.Progress("sentinel"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the request queued behind the deleted entry never ran")
		}
		time.Sleep(2 * time.Millisecond)
	}

	if pr, ok := p.Progress("gone"); ok {
		t.Fatalf("a pass started for the deleted entry: %+v", pr)
	}
	if st, err := p.manager.storage.GetPar2RepairState("gone"); err == nil && st != nil {
		t.Fatalf("deleted entry left a PAR2 state row: %+v", st)
	}
	if a, err := p.manager.storage.LatestPar2RepairAttemptForNzb("gone"); err == nil && a != nil {
		t.Fatalf("deleted entry recorded an attempt: %+v", a)
	}
	if p.IsQueued("gone") {
		t.Fatalf("deleted entry still reads as queued")
	}
}

// A request that finds the channel full is not queued, so a drop mark
// CancelEntry left for its nzbID must go with it: otherwise the next request
// for the entry, which does sit in the channel, is skipped in its place and
// the entry reads as queued until a restart.
func TestPar2QueueFullClearsStaleDropMark(t *testing.T) {
	p, _ := newTestPar2Repair(t)
	// CancelEntry ran between enqueue's unlock and its channel send: the
	// request is marked dropped but the send then finds the channel full.
	p.mu.Lock()
	if p.dropped == nil {
		p.dropped = make(map[string]struct{})
	}
	p.dropped["v1"] = struct{}{}
	p.mu.Unlock()
	for i := 0; i < cap(p.queue); i++ {
		p.queue <- "filler"
	}
	p.enqueue("v1")
	if p.IsQueued("v1") {
		t.Fatalf("request that found the channel full still reads as queued")
	}
	for len(p.queue) > 0 {
		<-p.queue
	}

	p.enqueue("v1")
	if len(p.queue) != 1 {
		t.Fatalf("channel holds %d requests, want 1", len(p.queue))
	}
	if !p.takeQueued(<-p.queue) {
		t.Fatalf("loop would skip the entry's only queued request (stale drop mark)")
	}
	if p.IsQueued("v1") {
		t.Fatalf("entry still reads as queued after its request was taken")
	}
}
