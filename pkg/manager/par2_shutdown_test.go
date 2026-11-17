package manager

import (
	"context"
	"testing"
)

// A restart during a pass is not a failed pass: no attempt row, no backoff,
// no "repair failed" notification, and the progress phase is not "failed".
func TestPar2ShutdownDuringPassIsNotRecordedAsFailure(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	h := newP2eHarness(t, servers[0], fx, "p2e-shutdown")
	f1 := fx.posted[0]
	h.recordDead(p2eSeg{f1, 1})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the service is stopping: Stop cancels p.ctx
	h.p.ctx = ctx

	h.run(nil)

	if st, _ := h.p.manager.storage.GetPar2RepairState(h.nzbID); st != nil && (st.AttemptCount != 0 || !st.NextRetryAt.IsZero()) {
		t.Fatalf("shutdown recorded a backoff: %+v", st)
	}
	for _, ln := range h.logs.lines() {
		if m, _ := ln["message"].(string); m == "par2 repair unavailable" {
			t.Fatalf("shutdown logged a failure: %v", ln)
		}
	}
	if pr, ok := h.p.Progress(h.nzbID); ok && pr.Phase == Par2PhaseFailed {
		t.Fatalf("phase = failed after shutdown")
	}
}
