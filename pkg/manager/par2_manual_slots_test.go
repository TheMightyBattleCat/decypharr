package manager

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// newManualSlotsHarness builds one harness holding the same damaged release
// under each of ids, with every article stalled once the warm-up is through,
// so each pass started blocks on its first fetch until it is cancelled. The
// worker's context is set (not Start, which would run the lanes) so Stop
// ends every pass and waiter.
func newManualSlotsHarness(t *testing.T, ids ...string) *p2eHarness {
	t.Helper()
	servers, fx := newP2eEnv(t, 1)
	h := newP2eHarness(t, servers[0], fx, ids[0])
	h.p.manager.par2Repair = h.p
	h.p.ctx, h.p.cancel = context.WithCancel(context.Background())
	t.Cleanup(h.p.Stop)
	for i, id := range ids {
		if i > 0 {
			if err := h.u.AddNZBForTest(fx.nzb(id)); err != nil {
				t.Fatalf("AddNZBForTest(%s): %v", id, err)
			}
			if err := h.p.manager.storage.AddOrUpdate(&storage.Entry{
				InfoHash: id, Name: p2eRelease + "." + id, Protocol: config.ProtocolNZB, Category: "p2e",
			}); err != nil {
				t.Fatalf("store entry %s: %v", id, err)
			}
		}
		h.nzbID = id
		h.recordDead(p2eSeg{fx.posted[0], 1})
	}
	h.nzbID = ids[0]
	h.warmup()
	for id := range fx.all {
		servers[0].set(id, p2eStallAll)
	}
	return h
}

func manualRunning(p *Par2Repair, ids []string) (n int) {
	for _, id := range ids {
		if p.IsRunning(id) {
			n++
		}
	}
	return n
}

// waitManual polls until cond holds, failing the test after 5 s.
func waitManual(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// wantManualSlots is the number of manual passes allowed at once.
const wantManualSlots = 2

// Manual "repair now" passes share two slots: a third request
// for another entry waits, reported as queued and not running, and starts
// once a running pass ends.
func TestPar2RunNowSharesManualSlots(t *testing.T) {
	ids := []string{"p2e-slot-a", "p2e-slot-b", "p2e-slot-c"}
	h := newManualSlotsHarness(t, ids...)
	p := h.p

	for _, id := range ids[:2] {
		if err := p.RunNow(id); err != nil {
			t.Fatalf("RunNow(%s): %v", id, err)
		}
	}
	waitManual(t, "the first two passes to run", func() bool { return manualRunning(p, ids[:2]) == 2 })
	if err := p.RunNow(ids[2]); err != nil {
		t.Fatalf("RunNow(%s): %v", ids[2], err)
	}
	time.Sleep(300 * time.Millisecond) // long enough for a free pass to have started

	if n := manualRunning(p, ids); n != wantManualSlots {
		t.Fatalf("%d manual passes running at once, want %d", n, wantManualSlots)
	}
	third := ids[2]
	if p.IsRunning(third) || !p.IsQueued(third) {
		t.Fatalf("third request: running=%v queued=%v, want waiting and reported as queued", p.IsRunning(third), p.IsQueued(third))
	}
	if _, ok := p.Progress(third); ok {
		t.Fatalf("third request started a pass while waiting for a slot")
	}
	if kind, _, exists := h.repair.handlers.State(third); !exists || kind != handlerPar2Running {
		t.Fatalf("waiting request's claim = (%q, exists=%v), want it held", kind, exists)
	}

	// End one running pass: the waiting request takes its slot.
	p.runningMu.Lock()
	p.running[ids[0]].cancel()
	p.runningMu.Unlock()
	waitManual(t, "the waiting request to start once a slot freed", func() bool { return p.IsRunning(third) })
	if n := manualRunning(p, ids); n > wantManualSlots {
		t.Fatalf("%d manual passes running after the hand-over, want at most %d", n, wantManualSlots)
	}
}

// Deleting an entry whose "repair now" request is still waiting for a slot
// drops the request: it never runs a pass and its claim is released.
func TestPar2CancelEntryDropsWaitingRunNow(t *testing.T) {
	ids := []string{"p2e-slot-a", "p2e-slot-b", "p2e-slot-c"}
	h := newManualSlotsHarness(t, ids...)
	p := h.p

	for _, id := range ids[:2] {
		if err := p.RunNow(id); err != nil {
			t.Fatalf("RunNow(%s): %v", id, err)
		}
	}
	waitManual(t, "the first two passes to run", func() bool { return manualRunning(p, ids[:2]) == 2 })
	third := ids[2]
	if err := p.RunNow(third); err != nil {
		t.Fatalf("RunNow(%s): %v", third, err)
	}
	time.Sleep(100 * time.Millisecond)

	p.CancelEntry(third)
	waitManual(t, "the waiting request to be dropped", func() bool { return !p.IsQueued(third) })

	// Free both slots: a request that survived the delete would start now.
	p.runningMu.Lock()
	for _, id := range ids[:2] {
		p.running[id].cancel()
	}
	p.runningMu.Unlock()
	waitManual(t, "the running passes to end", func() bool { return manualRunning(p, ids[:2]) == 0 })
	time.Sleep(200 * time.Millisecond)

	if p.IsRunning(third) || p.IsQueued(third) {
		t.Fatalf("deleted entry's request still active: running=%v queued=%v", p.IsRunning(third), p.IsQueued(third))
	}
	if pr, ok := p.Progress(third); ok {
		t.Fatalf("deleted entry's waiting request ran a pass: %+v", pr)
	}
	if _, _, exists := h.repair.handlers.State(third); exists {
		t.Fatalf("deleted entry's waiting request kept its claim")
	}
}

// A "repair now" request that waits for a slot longer than the running
// claim's TTL still holds its claim: a re-grab can't take the entry from
// under it while it waits, and the pass starts holding the claim.
func TestPar2WaitingRunNowClaimOutlivesTTL(t *testing.T) {
	ids := []string{"p2e-ttl-a", "p2e-ttl-b", "p2e-ttl-c"}
	h := newManualSlotsHarness(t, ids...)
	p := h.p
	var offset atomic.Int64
	h.repair.handlers.nowFn = func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }

	for _, id := range ids[:2] {
		if err := p.RunNow(id); err != nil {
			t.Fatalf("RunNow(%s): %v", id, err)
		}
	}
	waitManual(t, "the first two passes to run", func() bool { return manualRunning(p, ids[:2]) == 2 })
	third := ids[2]
	if err := p.RunNow(third); err != nil {
		t.Fatalf("RunNow(%s): %v", third, err)
	}

	// The wait outlasts the claim's TTL.
	offset.Store(int64(par2RunningClaimTTL + time.Hour))
	if release, ok := h.repair.handlers.TryAcquireRegrab(third); ok {
		release()
		t.Fatalf("a re-grab took the entry while its repair-now request waited past the claim TTL")
	}
	if kind, _, exists := h.repair.handlers.State(third); !exists || kind != handlerPar2Running {
		t.Fatalf("waiting request's claim after the TTL = (%q, exists=%v), want par2_running held", kind, exists)
	}

	p.runningMu.Lock()
	p.running[ids[0]].cancel()
	p.runningMu.Unlock()
	waitManual(t, "the waiting request to start once a slot freed", func() bool { return p.IsRunning(third) })
	if kind, _, exists := h.repair.handlers.State(third); !exists || kind != handlerPar2Running {
		t.Fatalf("started pass's claim = (%q, exists=%v), want par2_running", kind, exists)
	}
	if _, ok := h.repair.handlers.TryAcquireRegrab(third); ok {
		t.Fatalf("a re-grab took the entry from the started pass")
	}
}

// RunNowStatus reports a request that has to wait for a slot as waiting,
// and one that starts at once as not waiting.
func TestPar2RunNowStatusReportsWaiting(t *testing.T) {
	ids := []string{"p2e-st-a", "p2e-st-b", "p2e-st-c"}
	h := newManualSlotsHarness(t, ids...)
	p := h.p

	for _, id := range ids[:2] {
		waiting, err := p.RunNowStatus(id)
		if err != nil {
			t.Fatalf("RunNowStatus(%s): %v", id, err)
		}
		if waiting {
			t.Fatalf("RunNowStatus(%s) = waiting with a slot free", id)
		}
	}
	waiting, err := p.RunNowStatus(ids[2])
	if err != nil {
		t.Fatalf("RunNowStatus(%s): %v", ids[2], err)
	}
	if !waiting {
		t.Fatalf("RunNowStatus(%s) = not waiting with both slots taken", ids[2])
	}
}
