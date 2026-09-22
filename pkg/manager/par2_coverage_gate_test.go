package manager

import (
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

// With "try PAR2 before re-grabbing" on, pending damage is enough: the
// name-only coverage estimate is not consulted, so a coverage-short failed
// file goes to a PAR2 pass (which heals intact dead segments first) instead
// of straight to delete + blocklist + re-search.
func TestPar2CoverageGate(t *testing.T) {
	short := func() (bool, string) { return false, "only 2 recovery slices retained, need at least 9" }
	called := false
	counted := func() (bool, string) { called = true; return short() }

	if ok, _ := par2CoverageGate(true, 9, counted); !ok || called {
		t.Fatalf("tryFirst with damage: ok=%v, estimate consulted=%v; want usable without the estimate", ok, called)
	}
	if ok, reason := par2CoverageGate(false, 9, short); ok || reason == "" {
		t.Fatalf("estimate gate: ok=%v reason=%q, want not usable with the estimate's reason", ok, reason)
	}
	// Nothing pending: the toggle does not invent a reason to run.
	called = false
	if ok, _ := par2CoverageGate(true, 0, counted); ok || !called {
		t.Fatalf("tryFirst without damage: ok=%v estimate consulted=%v, want the estimate's answer", ok, called)
	}
}

// Unset means on, and an explicit false is kept.
func TestPar2TryBeforeRegrabDefault(t *testing.T) {
	if !(config.RepairConfig{}).Par2TryBeforeRegrabEnabled() {
		t.Fatal("unset Par2TryBeforeRegrab should default to enabled")
	}
	off := false
	if (config.RepairConfig{Par2TryBeforeRegrab: &off}).Par2TryBeforeRegrabEnabled() {
		t.Fatal("explicit false was ignored")
	}
}
