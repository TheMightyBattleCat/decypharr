package nntp

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
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
	setLatency(c, "fast", 30*time.Millisecond)
	edge := c.pools["edge"]

	setLatency(c, "edge", 150*time.Millisecond) // 5x: too slow to join
	if ok, _, _ := c.statEligible(edge); ok {
		t.Fatal("an ineligible provider at 5x joined")
	}
	setLatency(c, "edge", 105*time.Millisecond) // 3.5x: joins
	ok, _, _ := c.statEligible(edge)
	if !ok {
		t.Fatal("provider at 3.5x not eligible")
	}
	edge.stat.eligible.Store(ok)

	setLatency(c, "edge", 150*time.Millisecond) // 5x: stays in
	if ok, _, _ := c.statEligible(edge); !ok {
		t.Fatal("an eligible provider at 5x dropped out (flap)")
	}
	setLatency(c, "edge", 210*time.Millisecond) // 7x: leaves
	if ok, _, _ := c.statEligible(edge); ok {
		t.Fatal("an eligible provider at 7x stayed in")
	}
}

// With one provider answering in a few milliseconds, the floor sets the
// cutoffs: a second fast provider that measures 60-95 ms under load stays in
// (it used to be dropped past 60 ms), and a genuinely slow one stays out.
func TestStatEligibleFloorKeepsBusyFastProvider(t *testing.T) {
	providers := []config.UsenetProvider{
		{Host: "quick", Priority: 1, MaxConnections: 10},
		{Host: "busy", Priority: 2, MaxConnections: 10},
		{Host: "slow", Priority: 3, MaxConnections: 10},
	}
	// No repair pool: its workers write the eligibility flag this test sets.
	c := &Client{pools: map[string]*ProviderPool{}, providers: providers, logger: zerolog.Nop()}
	for _, p := range providers {
		c.pools[p.Host] = &ProviderPool{slots: make(chan struct{}, p.MaxConnections), max: p.MaxConnections, config: p}
	}
	c.statHomes = c.statHomePools()
	setLatency(c, "quick", 5*time.Millisecond)
	busy := c.pools["busy"]
	busy.stat.eligible.Store(true)
	for _, d := range []time.Duration{60 * time.Millisecond, 95 * time.Millisecond, 6 * statLatencyFloor} {
		setLatency(c, "busy", d)
		if ok, _, _ := c.statEligible(busy); !ok {
			t.Errorf("an eligible provider at %v dropped out next to a 5 ms one", d)
		}
	}
	setLatency(c, "busy", 6*statLatencyFloor+time.Millisecond)
	if ok, _, _ := c.statEligible(busy); ok {
		t.Error("an eligible provider past 6x the floor stayed in")
	}
	setLatency(c, "slow", 300*time.Millisecond)
	if ok, _, _ := c.statEligible(c.pools["slow"]); ok {
		t.Error("a 300 ms provider joined next to a 5 ms one")
	}
}

// The re-test wait starts short, doubles while the provider keeps measuring
// slow, stops at the cap, and starts short again once it has been eligible.
func TestStatRetestBackoff(t *testing.T) {
	var s statLatency
	now := time.Now()
	s.record(500*time.Millisecond, now)

	if s.tryExplore(now.Add(statExploreAfter - time.Second)) {
		t.Fatal("re-test allowed before the first wait was up")
	}
	if !s.tryExplore(now.Add(statExploreAfter)) {
		t.Fatal("re-test refused once the first wait was up")
	}
	if s.tryExplore(now.Add(statExploreAfter)) {
		t.Fatal("a second explorer was allowed while the first held the role")
	}

	want := statExploreAfter
	for range 10 {
		s.exploreDone(false)
		want = min(2*want, statExploreMax)
		if got := s.retestWait(); got != want {
			t.Fatalf("wait after a slow re-test = %v, want %v", got, want)
		}
		s.record(500*time.Millisecond, now)
		if s.tryExplore(now.Add(want - time.Second)) {
			t.Fatalf("re-test allowed %v into a %v wait", want-time.Second, want)
		}
		if !s.tryExplore(now.Add(want)) {
			t.Fatalf("re-test refused after the %v wait", want)
		}
	}
	if want != statExploreMax {
		t.Fatalf("wait settled at %v, want the cap %v", want, statExploreMax)
	}

	s.exploreDone(true)
	if got := s.retestWait(); got != statExploreAfter {
		t.Fatalf("wait after an eligible re-test = %v, want %v", got, statExploreAfter)
	}
	if s.exploring.Load() {
		t.Fatal("explorer role still held after exploreDone")
	}
}
