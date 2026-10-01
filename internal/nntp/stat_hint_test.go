package nntp

import (
	"context"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// Whatever the hint says, the order holds every provider exactly once and
// keeps primaries ahead of backups: the hint may only move a provider back
// within its own tier.
func TestStatHintOrderKeepsEveryProviderAndTiers(t *testing.T) {
	providers := []config.UsenetProvider{
		{Host: "A", Priority: 1, MaxConnections: 10},
		{Host: "B", Priority: 2, MaxConnections: 10, Backup: true},
		{Host: "C", Priority: 3, MaxConnections: 10},
		{Host: "D", Priority: 4, MaxConnections: 10},
		{Host: "E", Priority: 5, MaxConnections: 10},
		{Host: "F", Priority: 6, MaxConnections: 10, Backup: true},
	}
	c := newStatTestClient(t, providers, 80)
	setLatency(c, "A", 125*time.Millisecond)
	setLatency(c, "C", 12*time.Millisecond)
	setLatency(c, "E", 30*time.Millisecond)

	ordered := func(hint *statHint, home config.UsenetProvider) string {
		order := c.statOrder(home)
		c.applyStatHint(hint, order)
		return hosts(order)
	}
	home := providers[4] // E

	hint := c.newStatHint()
	if got, want := ordered(hint, home), "E C A D B F"; got != want {
		t.Errorf("empty hint: order %q, want it unchanged (%q)", got, want)
	}

	// Too few answers to judge by.
	hint.add("C", statHintMinAnswers-1, 0)
	if got, want := ordered(hint, home), "E C A D B F"; got != want {
		t.Errorf("below the threshold: order %q, want %q", got, want)
	}

	hint.add("C", 1, 0)
	hint.add("B", statHintMinAnswers, 0)
	if got, want := ordered(hint, home), "E A D C F B"; got != want {
		t.Errorf("C and B absent: order %q, want %q", got, want)
	}
	if got := c.pools["C"].stat.hintDemotions.Load(); got != 1 {
		t.Errorf("C demotions = %d after several orderings in one call, want 1", got)
	}

	// The home is moved back like any other.
	hint.add("E", statHintMinAnswers, 0)
	if got, want := ordered(hint, home), "A D E C F B"; got != want {
		t.Errorf("home absent too: order %q, want %q", got, want)
	}

	// One hit puts a provider back.
	hint.add("C", 1, 1)
	if got, want := ordered(hint, home), "C A D E F B"; got != want {
		t.Errorf("C found one: order %q, want %q", got, want)
	}

	// Every provider absent: still all there, once each.
	for _, p := range providers {
		hint.byHost[p.Host].hits.Store(0)
		hint.add(p.Host, statHintMinAnswers, 0)
	}
	for _, h := range providers {
		order := c.statOrder(h)
		c.applyStatHint(hint, order)
		seen := map[string]bool{}
		backups := false
		for _, p := range order {
			seen[p.Host] = true
			if backups && !p.Backup {
				t.Errorf("home %s: a primary after a backup in %q", h.Host, hosts(order))
			}
			backups = backups || p.Backup
		}
		if len(order) != len(providers) || len(seen) != len(providers) {
			t.Errorf("home %s: order %q does not hold every provider once", h.Host, hosts(order))
		}
	}
}

// A file that only the second provider holds: once the first has answered
// enough of the call's articles without a hit, later chunks stop asking it
// for articles the second provider has, even from a worker homed on it. An
// article nothing else has is still asked there, so a missing verdict still
// means every provider said so.
func TestBatchStatHintStopsAskingProviderWithoutTheFile(t *testing.T) {
	present := ids("file", 3*statHintMinAnswers)
	missingOnA := append([]string{"nowhere@test", "back@test"}, present...)
	a := startFakeNNTP(t, 0, missingOnA...)
	b := startFakeNNTP(t, 0, "nowhere@test", "back@test")
	pa, pb := twoLocalProviders(t, a, b)
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)
	hint := c.newStatHint()
	ctx := context.Background()

	allFound := func(res []StatResult) bool {
		for _, r := range res {
			if !r.Available {
				return false
			}
		}
		return true
	}

	// First chunk, homed on A: A is asked and has none of it.
	chunk := present[:statHintMinAnswers]
	res, err := c.batchStatAcrossProviders(ctx, chunk, pa, hint)
	if err != nil || !allFound(res) {
		t.Fatalf("first chunk: %+v, %v", res, err)
	}
	if got := a.stats.Load(); got != int64(len(chunk)) {
		t.Fatalf("A served %d STATs for the first chunk, want %d", got, len(chunk))
	}

	// Second chunk, same home: B answers all of it, A is not asked.
	before := a.stats.Load()
	chunk = present[statHintMinAnswers : 2*statHintMinAnswers]
	res, err = c.batchStatAcrossProviders(ctx, chunk, pa, hint)
	if err != nil || !allFound(res) {
		t.Fatalf("second chunk: %+v, %v", res, err)
	}
	if got := a.stats.Load() - before; got != 0 {
		t.Fatalf("A served %d STATs for articles B has, want 0", got)
	}
	if got := c.pools[pa.Host].stat.hintDemotions.Load(); got != 1 {
		t.Fatalf("A demotions = %d, want 1", got)
	}

	// An article missing on B too is still asked on A, and only that one.
	before = a.stats.Load()
	chunk = append([]string{"nowhere@test"}, present[2*statHintMinAnswers:2*statHintMinAnswers+5]...)
	res, err = c.batchStatAcrossProviders(ctx, chunk, pa, hint)
	if err != nil {
		t.Fatalf("third chunk: %v", err)
	}
	if res[0].Available || !IsArticleNotFoundError(res[0].Error) {
		t.Fatalf("article missing everywhere = %+v, want article-not-found", res[0])
	}
	if !allFound(res[1:]) {
		t.Fatalf("third chunk: present articles reported unavailable: %+v", res[1:])
	}
	if got := a.stats.Load() - before; got != 1 {
		t.Fatalf("A served %d STATs, want exactly the one article B lacks", got)
	}

	// A finds an article B lacks: it goes back to its usual place.
	delete(a.missing, "back@test")
	res, err = c.batchStatAcrossProviders(ctx, []string{"back@test"}, pa, hint)
	if err != nil || !allFound(res) {
		t.Fatalf("fourth chunk: %+v, %v", res, err)
	}
	if hint.absent(pa.Host) {
		t.Fatal("A still asked last after finding an article")
	}

	// A call without a hint asks home first, as before.
	before = a.stats.Load()
	if _, err := c.batchStatAcrossProviders(ctx, present[:4], pa, nil); err != nil {
		t.Fatal(err)
	}
	if got := a.stats.Load() - before; got != 4 {
		t.Fatalf("no hint: A served %d STATs, want 4", got)
	}
}

// A window that fails says nothing about what the provider holds: it must not
// count towards asking the provider last.
func TestStatHintIgnoresFailedWindows(t *testing.T) {
	a := startFakeNNTP(t, 0)
	a.dropAfter = 1
	b := startFakeNNTP(t, 0)
	pa, pb := twoLocalProviders(t, a, b)
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)
	hint := c.newStatHint()

	for i := range 4 {
		chunk := ids("f", 4*statHintMinAnswers)[i*statHintMinAnswers : (i+1)*statHintMinAnswers]
		if _, err := c.batchStatAcrossProviders(context.Background(), chunk, pa, hint); err != nil {
			t.Fatal(err)
		}
	}
	if hint.absent(pa.Host) {
		t.Fatalf("A marked absent on failed windows alone (answered %d, hits %d)",
			hint.byHost[pa.Host].answered.Load(), hint.byHost[pa.Host].hits.Load())
	}
}

// End to end through the pool, with the hint in play: a file only the second
// provider holds is found in full, and a dead article in it is still reported
// missing.
func TestBatchStatWithHintKeepsVerdicts(t *testing.T) {
	present := ids("file", 600)
	a := startFakeNNTP(t, 0, append([]string{"dead@test"}, present...)...)
	a.missDelay = 2 * time.Millisecond
	b := startFakeNNTP(t, 0, "dead@test")
	pa, pb := twoLocalProviders(t, a, b)
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)

	res, err := c.BatchStatComplete(context.Background(), append(append([]string{}, present...), "dead@test"))
	if err != nil {
		t.Fatal(err)
	}
	if res.FoundCount != len(present) || res.ErrorCount != 0 {
		t.Fatalf("found %d/%d with %d errors, want every present article found", res.FoundCount, len(present), res.ErrorCount)
	}
	last := res.Results[len(res.Results)-1]
	if last.Available || !IsArticleNotFoundError(last.Error) {
		t.Fatalf("dead article = %+v, want article-not-found", last)
	}
}
