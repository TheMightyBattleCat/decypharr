package nntp

import (
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// A provider hovering around statSlowFactor x the fastest used to flip on
// every evaluation (18% of the production install's log). Joining needs <= 4x; once in, it
// leaves only past 6x.
func TestStatEligibleHysteresis(t *testing.T) {
	providers := []config.UsenetProvider{
		{Host: "fast", Priority: 1, MaxConnections: 10},
		{Host: "edge", Priority: 2, MaxConnections: 10},
	}
	c := newStatTestClient(t, providers, 80)
	setLatency(c, "fast", 20*time.Millisecond)
	edge := c.pools["edge"]

	setLatency(c, "edge", 100*time.Millisecond) // 5x: too slow to join
	if ok, _, _ := c.statEligible(edge); ok {
		t.Fatal("an ineligible provider at 5x joined")
	}
	setLatency(c, "edge", 70*time.Millisecond) // 3.5x: joins
	ok, _, _ := c.statEligible(edge)
	if !ok {
		t.Fatal("provider at 3.5x not eligible")
	}
	edge.stat.eligible.Store(ok)

	setLatency(c, "edge", 100*time.Millisecond) // 5x: stays in
	if ok, _, _ := c.statEligible(edge); !ok {
		t.Fatal("an eligible provider at 5x dropped out (flap)")
	}
	setLatency(c, "edge", 140*time.Millisecond) // 7x: leaves
	if ok, _, _ := c.statEligible(edge); ok {
		t.Fatal("an eligible provider at 7x stayed in")
	}
}
