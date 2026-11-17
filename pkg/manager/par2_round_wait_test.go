package manager

import (
	"strings"
	"testing"
	"time"
)

// A round that stops early (an article found missing mid-stream while slow
// fetches are still out) ends its fetch workers before the pass uses the
// round's result. Before the fix the workers ran on after runRepair moved on,
// still adding to the pass's byte counter while runJob read it (a data race
// under -race), and were still alive when runJob returned.
func TestPar2EarlyStoppedRoundEndsItsWorkers(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	srv := servers[0]
	h := newP2eHarness(t, srv, fx, "p2e-round-wait")
	f1, f2 := fx.posted[0], fx.posted[1]
	h.recordDead(p2eSeg{f1, 1}, p2eSeg{f1, 3})
	for _, s := range h.dead {
		srv.set(s.file.ids[s.idx], p2eNotFound)
	}
	srv.slowNS.Store(int64(150 * time.Millisecond))

	var lingering []string
	o := h.run(func(done <-chan struct{}) {
		before := p2eGoroutines() // only this pass's workers count below
		if !h.waitPhase(done, Par2PhaseStreamingIntact) {
			return
		}
		// Goes missing after the pass's STAT sweep: the stream finds it in
		// the middle of the release, with the slices after it still being
		// fetched.
		srv.set(f2.ids[2], p2eNotFound)
		<-done
		// runJob has just returned: no worker of any round may be left.
		for id, stack := range p2eGoroutines() {
			if _, old := before[id]; !old && strings.Contains(stack, "concurrentSliceSource") {
				lingering = append(lingering, p2eTopFrame(stack))
			}
		}
	})
	t.Logf("outcome: %s err=%q tally=%s attempt=%s", o.LogMsg, o.Err, o.Tally, o.Attempt)

	if !strings.Contains(o.Err, "pass stopped") {
		t.Fatalf("the round did not stop early (err %q); the test needs an early-stopped round", o.Err)
	}
	if len(lingering) > 0 {
		t.Fatalf("%d slice-source goroutine(s) still running after runJob returned: %v", len(lingering), lingering)
	}
}
