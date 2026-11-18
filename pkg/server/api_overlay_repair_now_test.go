package server

import "testing"

// repair-now answers "waiting" for a request held back by the manual-slot
// limit and keeps "queued" for one that starts at once.
func TestOverlayRepairNowStatus(t *testing.T) {
	if got := overlayRepairNowStatus(true); got != "waiting" {
		t.Fatalf("waiting request: status = %q, want %q", got, "waiting")
	}
	if got := overlayRepairNowStatus(false); got != "queued" {
		t.Fatalf("started request: status = %q, want %q", got, "queued")
	}
}
