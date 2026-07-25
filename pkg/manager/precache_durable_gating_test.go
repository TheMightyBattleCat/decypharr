package manager

import (
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

// TestReserveBudgetDeniesWhenCapDisabled proves PrecacheMaxBytes=0 (the "0 =
// disabled" cap from the Block 1 UI work) blocks a next-episode burst's
// budget reservation - the gate precacheEpisodeFile checks BEFORE
// ReadAhead/persistCleanRanges ever run, so no durable write can happen
// when the cap is disabled, without needing a real DFS mount/usenet client
// to prove it.
func TestReserveBudgetDeniesWhenCapDisabled(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)

	cfg := config.Get()
	zero := int64(0)
	cfg.Precache.PrecacheMaxBytes = &zero

	p := &Precache{}
	if p.reserveBudget(1024) {
		t.Fatalf("reserveBudget(1024) = true with PrecacheMaxBytes=0 (disabled), want false")
	}
}

// TestReserveBudgetAllowsWithinCap is the inverse sanity check: a positive
// cap with room left allows the reservation.
func TestReserveBudgetAllowsWithinCap(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)

	cfg := config.Get()
	limit := int64(10 * 1024 * 1024 * 1024)
	cfg.Precache.PrecacheMaxBytes = &limit

	p := &Precache{}
	if !p.reserveBudget(1024) {
		t.Fatalf("reserveBudget(1024) = false within a 10 GiB cap, want true")
	}
}

// TestReserveBudgetDeniesOnceCapExhausted proves reserveBudget - the gate
// that runs before any durable write - correctly refuses once prior
// reservations have consumed the configured cap, regardless of the
// per-call size.
func TestReserveBudgetDeniesOnceCapExhausted(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)

	cfg := config.Get()
	limit := int64(1000)
	cfg.Precache.PrecacheMaxBytes = &limit

	p := &Precache{}
	if !p.reserveBudget(900) {
		t.Fatalf("reserveBudget(900) = false within a 1000-byte cap, want true")
	}
	if p.reserveBudget(200) {
		t.Fatalf("reserveBudget(200) = true after 900/1000 already reserved, want false (only 100 bytes left)")
	}
}
