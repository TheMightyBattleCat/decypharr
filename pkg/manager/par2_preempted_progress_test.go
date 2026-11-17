package manager

import (
	"testing"
	"time"
)

// A pass preempted by an urgent request leaves its progress record queued,
// not stuck in the running phase it was cut out of. If the urgent pass never
// runs (PAR2 switched off before it starts), nothing else replaces the record.
func TestPar2PreemptedPassLeavesProgressQueued(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	h := newP2eHarness(t, servers[0], fx, "p2e-preempted")
	f1 := fx.posted[0]
	h.recordDead(p2eSeg{f1, 1})
	for _, s := range h.dead {
		h.srv.set(s.file.ids[s.idx], p2eNotFound)
	}
	h.srv.slowNS.Store(int64(150 * time.Millisecond))

	hooked := false
	h.run(func(done <-chan struct{}) {
		if !h.waitPhase(done, Par2PhaseStreamingIntact) {
			return
		}
		// What EnqueueUrgent does to a running batch pass.
		h.p.runningMu.Lock()
		if j, ok := h.p.running[h.nzbID]; ok {
			j.preempted.Store(true)
			j.cancel()
			hooked = true
		}
		h.p.runningMu.Unlock()
	})
	if !hooked {
		t.Fatal("preemption hook never fired (phase streaming_intact not seen)")
	}

	pr, ok := h.p.Progress(h.nzbID)
	if !ok {
		t.Fatal("no progress record")
	}
	if pr.Phase != Par2PhaseQueued {
		t.Fatalf("phase = %q after preemption, want %q", pr.Phase, Par2PhaseQueued)
	}
	if st, _ := h.p.manager.storage.GetPar2RepairState(h.nzbID); st != nil && (st.AttemptCount != 0 || !st.NextRetryAt.IsZero()) {
		t.Fatalf("preemption recorded a backoff: %+v", st)
	}
}
