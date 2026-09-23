package manager

import (
	"testing"
)

// Pending damage is enough: the name-only coverage estimate is not
// consulted, so a coverage-short failed file goes to a PAR2 pass (which heals
// intact dead segments first) instead of straight to delete + blocklist +
// re-search.
func TestPar2CoverageGate(t *testing.T) {
	short := func() (bool, string) { return false, "only 2 recovery slices retained, need at least 9" }
	called := false
	counted := func() (bool, string) { called = true; return short() }

	if ok, _ := par2CoverageGate(9, counted); !ok || called {
		t.Fatalf("with damage: ok=%v, estimate consulted=%v; want usable without the estimate", ok, called)
	}
	// Nothing pending: the gate does not invent a reason to run.
	called = false
	if ok, reason := par2CoverageGate(0, counted); ok || !called || reason == "" {
		t.Fatalf("without damage: ok=%v estimate consulted=%v reason=%q, want the estimate's answer", ok, called, reason)
	}
}
