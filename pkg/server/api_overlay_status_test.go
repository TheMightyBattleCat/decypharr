package server

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// A repaired file must read "completed" from its patches alone. Live
// 2026-09-22: 49 of 54 repaired files showed "none" because their attempt
// records had been cleared and a successful repair deletes its repair state.
func TestOverlayRepairStatusRepairedWithoutHistory(t *testing.T) {
	status, reason := overlayRepairStatusFor(false, false, false, false, "no damage pending repair", 16, nil, nil)
	if status != OverlayRepairCompleted {
		t.Fatalf("status = %q, want %q", status, OverlayRepairCompleted)
	}
	if reason == "" {
		t.Fatal("want a reason saying no attempt record was retained")
	}

	done := &storage.Par2RepairAttempt{Outcome: storage.Par2RepairOutcomeCompleted}
	if status, reason := overlayRepairStatusFor(false, false, false, false, "", 3, nil, done); status != OverlayRepairCompleted || reason != "" {
		t.Fatalf("with a completed attempt: status = %q reason = %q", status, reason)
	}
}

func TestOverlayRepairStatusPrecedence(t *testing.T) {
	failed := &storage.Par2RepairAttempt{Outcome: storage.Par2RepairOutcomeFailed, FailReason: "boom"}
	terminal := &storage.Par2RepairState{Terminal: true, TerminalReason: "gone"}
	cases := []struct {
		name                         string
		running, queued, pending, ok bool
		patched                      int
		state                        *storage.Par2RepairState
		last                         *storage.Par2RepairAttempt
		want                         OverlayRepairStatus
	}{
		{"running wins", true, false, true, true, 5, terminal, failed, OverlayRepairRunning},
		{"queued", false, true, true, true, 0, nil, nil, OverlayRepairQueued},
		{"terminal", false, false, true, true, 0, terminal, failed, OverlayRepairUnrepairable},
		{"pending unavailable", false, false, true, false, 0, nil, nil, OverlayRepairUnavailable},
		{"pending after failure", false, false, true, true, 0, nil, failed, OverlayRepairFailed},
		{"partly patched, still pending", false, false, true, true, 4, nil, nil, OverlayRepairNone},
		{"clean, never damaged", false, false, false, false, 0, nil, nil, OverlayRepairNone},
	}
	for _, c := range cases {
		got, _ := overlayRepairStatusFor(c.running, c.queued, c.pending, c.ok, "", c.patched, c.state, c.last)
		if got != c.want {
			t.Errorf("%s: status = %q, want %q", c.name, got, c.want)
		}
	}
}
