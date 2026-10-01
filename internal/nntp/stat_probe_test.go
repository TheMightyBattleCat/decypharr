package nntp

import (
	"context"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// statRoundTrips is how many pipelined exchanges one chunk of n STATs costs
// on a provider: one per statPipelineDepth IDs, the last one part filled.
func statRoundTrips(n int) int {
	return (n + statPipelineDepth - 1) / statPipelineDepth
}

// Whatever the call's size, a chunk is a whole number of pipeline windows:
// one to three of them. A part-filled window is a round trip spent on a few
// IDs (a chunk of 36 took three round trips where 32 takes two).
func TestStatBatchSizeIsWholeWindows(t *testing.T) {
	const (
		workers = 330
		ceil    = 3 * statPipelineDepth
		floor   = statPipelineDepth
	)
	for _, tc := range []struct {
		name string
		ids  int
		want int
	}{
		{"one article", 1, 16},
		{"0.7 GB episode", 1000, 16},
		{"2 GB episode", 3000, 16},
		{"8 GB film", 11500, 16},
		{"25 GB REMUX", 35000, 32},
		{"40 GB REMUX", 57000, 48},
		{"70 GB UHD REMUX", 100000, 48},
	} {
		got := pickStatBatchSize(tc.ids, workers, ceil, floor)
		if got != tc.want {
			t.Errorf("%s (%d IDs): chunk %d, want %d", tc.name, tc.ids, got, tc.want)
		}
		if got%statPipelineDepth != 0 || statRoundTrips(got)*statPipelineDepth != got {
			t.Errorf("%s: chunk %d is not a whole number of %d-ID windows", tc.name, got, statPipelineDepth)
		}
	}

	// The old bounds (50 and 10) and any size between come out whole too,
	// never over the ceiling and never under one window.
	for ids := 1; ids <= 60000; ids += 97 {
		got := pickStatBatchSize(ids, workers, 50, 10)
		if got%statPipelineDepth != 0 || got < statPipelineDepth || got > 50 {
			t.Fatalf("%d IDs, bounds 10-50: chunk %d", ids, got)
		}
	}
	if got := pickStatBatchSize(100, 0, 50, 10); got != 48 {
		t.Errorf("no workers: chunk %d, want the ceiling in whole windows (48)", got)
	}
	// A ceiling under one window still gives one.
	if got := pickStatBatchSize(100, 4, 8, 4); got != statPipelineDepth {
		t.Errorf("ceiling 8: chunk %d, want %d", got, statPipelineDepth)
	}
}

// The probe's rule, on the hint alone. Once another probed provider has a
// hit, a provider is asked last if it answered without one, or if it still
// has not answered when the wait for the probe is over. Its own hit puts it
// back; a failed probe says nothing about it.
func TestStatHintProbeRule(t *testing.T) {
	newHint := func(probed ...string) *statHint {
		h := &statHint{byHost: map[string]*statHintCounts{"A": {}, "B": {}, "C": {}}}
		for _, host := range probed {
			h.probeStart(host)
		}
		return h
	}

	// Four answers through the old rule alone change nothing.
	h := newHint()
	h.add("A", statProbeIDs, 0)
	if h.absent("A") {
		t.Fatal("four misses outside the probe mark a provider absent")
	}

	// B hits first. A has not answered: it may hold the file and be a
	// moment behind, so it keeps its place until the wait is over.
	h = newHint("A", "B")
	h.probeResult("B", statProbeIDs, statProbeIDs)
	if h.absent("A") {
		t.Error("A asked last at B's hit, before it had the chance to answer")
	}
	h.probeSettle()
	if !h.absent("A") {
		t.Error("A not asked last though it had not answered when the wait ended")
	}
	if h.absent("B") || h.absent("C") {
		t.Error("the provider with the hit, or one the probe never asked, is asked last")
	}

	// The wait ends with no hit; a later hit then demotes a provider still
	// to answer at once.
	h = newHint("A", "B")
	h.probeSettle()
	if h.absent("A") || h.absent("B") {
		t.Error("a provider asked last though no probed provider had a hit")
	}
	h.probeResult("B", statProbeIDs, statProbeIDs)
	if !h.absent("A") {
		t.Error("A not asked last on a hit that came after the wait")
	}

	h = newHint("A", "B")
	h.probeResult("B", statProbeIDs, statProbeIDs)
	h.probeSettle()
	// A's misses arrive: still last.
	h.probeResult("A", statProbeIDs, 0)
	if !h.absent("A") {
		t.Error("A no longer asked last after answering only misses")
	}
	// A finds one article in a later chunk: back in its place.
	h.add("A", 1, 1)
	if h.absent("A") {
		t.Error("A still asked last after a hit")
	}

	// The misses arrive before the other provider's hit.
	h = newHint("A", "B")
	h.probeResult("A", statProbeIDs, 0)
	if h.absent("A") {
		t.Error("A asked last on four misses before any provider had a hit")
	}
	h.probeResult("B", statProbeIDs, 1)
	if !h.absent("A") {
		t.Error("A not asked last once B had a hit")
	}

	// Both have the file: neither moves, and one that answers only after
	// the wait is last just until its hit arrives.
	h = newHint("A", "B")
	h.probeResult("B", statProbeIDs, statProbeIDs)
	h.probeResult("A", statProbeIDs, statProbeIDs)
	h.probeSettle()
	if h.absent("A") || h.absent("B") {
		t.Error("a provider with the file is asked last")
	}
	h = newHint("A", "B")
	h.probeResult("B", statProbeIDs, statProbeIDs)
	h.probeSettle()
	h.probeResult("A", statProbeIDs, statProbeIDs)
	if h.absent("A") {
		t.Error("A still asked last after its late hit")
	}

	// A failed probe is not a miss, before or after the other's hit.
	h = newHint("A", "B")
	h.probeResult("B", statProbeIDs, statProbeIDs)
	h.probeResult("A", 0, 0)
	if h.absent("A") {
		t.Error("A asked last after its probe failed")
	}
	h = newHint("A", "B")
	h.probeResult("A", 0, 0)
	h.probeResult("B", statProbeIDs, statProbeIDs)
	if h.absent("A") {
		t.Error("A asked last though its probe had failed before B's hit")
	}

	// No provider has a hit: nobody moves.
	h = newHint("A", "B")
	h.probeResult("A", statProbeIDs, 0)
	h.probeResult("B", statProbeIDs, 0)
	if h.absent("A") || h.absent("B") {
		t.Error("a provider asked last though no probed provider had a hit")
	}
}

// probeProviders returns two fast, eligible providers: absent holds none of
// the file and answers each miss slowly, holder has all of it.
func probeProviders(t *testing.T, missDelay, hitDelay time.Duration, file []string) (c *Client, absent, holder *fakeNNTP, pa, pb config.UsenetProvider) {
	t.Helper()
	absent = startFakeNNTP(t, 0, file...)
	absent.missDelay = missDelay
	holder = startFakeNNTP(t, hitDelay)
	pa, pb = twoLocalProviders(t, absent, holder)
	c = newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)
	setLatency(c, pa.Host, 10*time.Millisecond)
	setLatency(c, pb.Host, 10*time.Millisecond)
	return c, absent, holder, pa, pb
}

// A file only the second provider holds, with both providers fast enough to
// take chunks. Without the probe every worker homed on the first provider
// starts a chunk there and waits out its misses. With it the first provider
// is asked about the probe's few articles and nothing else.
func TestBatchStatProbeSkipsProviderWithoutTheFile(t *testing.T) {
	file := ids("file", 640)
	c, absent, _, pa, pb := probeProviders(t, 2*time.Second, 5*time.Millisecond, file)

	began := time.Now()
	res, err := c.BatchStat(context.Background(), file)
	if err != nil || res.FoundCount != len(file) {
		t.Fatalf("BatchStat: found %d/%d, err %v", res.FoundCount, len(file), err)
	}
	took := time.Since(began)
	if got := absent.stats.Load(); got > statProbeIDs {
		t.Errorf("the provider without the file served %d STATs, want at most the probe's %d", got, statProbeIDs)
	}
	// One 2 s miss would show here: the call never waited on one.
	if took >= 2*time.Second {
		t.Errorf("the call took %v: it waited on the absent provider's misses", took)
	}
	if took >= statProbeWait {
		if got := c.pools[pa.Host].stat.probeDemotions.Load(); got != 1 {
			t.Errorf("probe demotions for the absent provider = %d, want 1", got)
		}
	}
	if got := c.pools[pb.Host].stat.probeDemotions.Load(); got != 0 {
		t.Errorf("probe demotions for the provider with the file = %d, want 0", got)
	}
	if got := c.pools[pa.Host].stat.hintDemotions.Load(); got != 0 {
		t.Errorf("the 32-answer hint also counted a demotion (%d): one line per cause", got)
	}
	if got := c.statProbing.Load(); got != 0 {
		t.Errorf("probes still counted as out after the call: %d", got)
	}
}

// The probe only reorders: an article nothing but the demoted provider holds
// is still found there, and one no provider holds is still a definitive miss.
func TestBatchStatProbeKeepsVerdicts(t *testing.T) {
	file := ids("file", 200)
	c, absent, holder, _, _ := probeProviders(t, 10*time.Millisecond, 0, file)
	// file-77 is only on the provider that lacks the rest; file-150 is nowhere.
	delete(absent.missing, "file-77@test")
	holder.missing["file-77@test"] = true
	holder.missing["file-150@test"] = true

	res, err := c.BatchStatComplete(context.Background(), file)
	if err != nil {
		t.Fatal(err)
	}
	if res.FoundCount != len(file)-1 || res.ErrorCount != 0 {
		t.Fatalf("found %d, errors %d; want %d found and no errors", res.FoundCount, res.ErrorCount, len(file)-1)
	}
	for i, r := range res.Results {
		switch r.MessageID {
		case "file-150@test":
			if r.Available || !IsArticleNotFoundError(r.Error) {
				t.Errorf("article on no provider = %+v, want article-not-found", r)
			}
		default:
			if !r.Available {
				t.Errorf("result %d (%s) unavailable: %v", i, r.MessageID, r.Error)
			}
		}
	}
}

// Two providers that both hold the file: neither ends the call counted as
// asked last, whichever answered the probe first.
func TestBatchStatProbeLeavesHoldersAlone(t *testing.T) {
	file := ids("file", 640)
	a := startFakeNNTP(t, 2*time.Millisecond)
	b := startFakeNNTP(t, 2*time.Millisecond)
	pa, pb := twoLocalProviders(t, a, b)
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)
	setLatency(c, pa.Host, 10*time.Millisecond)
	setLatency(c, pb.Host, 10*time.Millisecond)

	res, err := c.BatchStat(context.Background(), file)
	if err != nil || res.FoundCount != len(file) {
		t.Fatalf("BatchStat: found %d/%d, err %v", res.FoundCount, len(file), err)
	}
	for _, p := range []config.UsenetProvider{pa, pb} {
		if got := c.pools[p.Host].stat.probeDemotions.Load(); got != 0 {
			t.Errorf("%s: probe demotions = %d, want 0", p.Host, got)
		}
	}
}

// Two providers hold the file and one answers a few milliseconds after the
// other. The slower one must still take its share of a file small enough for
// every chunk to start at once: demoting it at the other's first hit sent
// all of them to the faster provider, on nearly every file.
func TestBatchStatProbeKeepsSlowerHolderInFirstWave(t *testing.T) {
	quick := startFakeNNTP(t, 0)
	slower := startFakeNNTP(t, 3*time.Millisecond)
	pa, pb := twoLocalProviders(t, quick, slower)
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)
	setLatency(c, pa.Host, 10*time.Millisecond)
	setLatency(c, pb.Host, 10*time.Millisecond)

	// The workers started before the latencies were set and found their
	// homes unmeasured; let every one of them look again and start waiting
	// for a chunk.
	time.Sleep(statIneligibleRecheck + 200*time.Millisecond)

	// One chunk per worker, so no chunk waits for a second look at the hint.
	file := ids("file", c.repairPool.Capacity()*statPipelineDepth)
	if got := pickStatBatchSize(len(file), c.repairPool.Capacity(), 3*statPipelineDepth, statPipelineDepth); got != statPipelineDepth {
		t.Fatalf("chunk size %d, want %d (one chunk per worker)", got, statPipelineDepth)
	}
	res, err := c.BatchStat(context.Background(), file)
	if err != nil || res.FoundCount != len(file) {
		t.Fatalf("BatchStat: found %d/%d, err %v", res.FoundCount, len(file), err)
	}
	if got := slower.stats.Load(); got < statProbeIDs+statPipelineDepth {
		t.Errorf("the slower holder served %d STATs: the probe's %d and no chunk", got, statProbeIDs)
	}
}

// The probe is skipped where it cannot help: a call no larger than the probe,
// fewer than two fast providers, or too many probes out already.
func TestStatProbeSkipped(t *testing.T) {
	file := ids("file", 64)
	c, absent, _, pa, _ := probeProviders(t, 0, 0, file)
	ctx := context.Background()

	c.statProbe(ctx, file[:statProbeIDs], c.newStatHint())()
	if got := absent.stats.Load(); got != 0 {
		t.Errorf("a %d-ID call was probed (%d STATs)", statProbeIDs, got)
	}

	c.statProbing.Store(statProbeMaxCalls)
	c.statProbe(ctx, file, c.newStatHint())()
	if got := absent.stats.Load(); got != 0 {
		t.Errorf("probed over the cap (%d STATs)", got)
	}
	if got := c.statProbing.Load(); got != statProbeMaxCalls {
		t.Errorf("probe count = %d after a skipped probe, want %d", got, statProbeMaxCalls)
	}
	c.statProbing.Store(0)

	// One provider too slow to take chunks: nothing to choose between.
	setLatency(c, pa.Host, time.Second)
	c.statProbe(ctx, file, c.newStatHint())()
	if got := absent.stats.Load(); got != 0 {
		t.Errorf("probed with one fast provider (%d STATs)", got)
	}
}
