package nntp

import (
	"sync/atomic"
	"time"
)

// Fetch timing: where the time of an article download goes, to tell a
// connection that waits on the server (send -> first byte) from one that
// waits for the reader to ask for more (idle between uses). Cumulative since
// start, reported under "fetch_timing" in Client.Stats. Recording costs two
// clock reads and a few atomic adds per article. Each phase also reports
// its raw bucket counts and summed duration, so two snapshots taken around a
// test run can be subtracted to isolate it.

// timingBoundsMS are the histogram bucket upper bounds, in milliseconds.
var timingBoundsMS = [...]int64{1, 2, 5, 10, 20, 50, 100, 200, 300, 500, 750, 1000, 2000, 5000, 10000}

// phaseHist is a lock-free histogram of one phase's durations.
type phaseHist struct {
	counts [len(timingBoundsMS) + 1]atomic.Int64
	sumNS  atomic.Int64
}

func (h *phaseHist) observe(d time.Duration) {
	if d < 0 {
		d = 0
	}
	ms := d.Milliseconds()
	i := 0
	for i < len(timingBoundsMS) && ms >= timingBoundsMS[i] {
		i++
	}
	h.counts[i].Add(1)
	h.sumNS.Add(int64(d))
}

// snapshot reports the count, mean and bucketed quantiles. A quantile is the
// upper bound of the bucket it falls in; -1 means above the largest bound.
func (h *phaseHist) snapshot() map[string]any {
	var counts [len(timingBoundsMS) + 1]int64
	var total int64
	for i := range counts {
		counts[i] = h.counts[i].Load()
		total += counts[i]
	}
	out := map[string]any{
		"count":        total,
		"sum_ns":       h.sumNS.Load(),
		"buckets":      counts,
		"bucket_le_ms": timingBoundsMS, // bucket i counts durations under bound i; the last is everything above
	}
	if total == 0 {
		return out
	}
	out["mean_ms"] = float64(h.sumNS.Load()) / float64(total) / 1e6
	quantile := func(q float64) int64 {
		rank := int64(q * float64(total))
		var seen int64
		for i, c := range counts {
			seen += c
			if seen > rank {
				if i < len(timingBoundsMS) {
					return timingBoundsMS[i]
				}
				return -1
			}
		}
		return -1
	}
	out["p50_ms"] = quantile(0.50)
	out["p90_ms"] = quantile(0.90)
	out["p99_ms"] = quantile(0.99)
	return out
}

// providerTiming is one provider's per-article phases.
type providerTiming struct {
	latency  phaseHist // BODY sent -> first response byte
	transfer phaseHist // first response byte -> article decoded
	idle     phaseHist // connection returned to the pool -> checked out again
	hold     phaseHist // checked out -> returned to the pool
}

func (t *providerTiming) snapshot() map[string]any {
	return map[string]any{
		"latency":  t.latency.snapshot(),
		"transfer": t.transfer.snapshot(),
		"idle":     t.idle.snapshot(),
		"hold":     t.hold.snapshot(),
	}
}

// clientTiming holds the phases measured before a provider is chosen.
type clientTiming struct {
	since       time.Time
	checkout    phaseHist // ExecuteWithFailover waiting for a first connection
	segmentWait phaseHist // a segment fetch waiting for its reader's slot
}

// ObserveSegmentWait records how long a segment fetch waited for a slot in
// its reader's own connection limit before asking the pool for a connection.
// A nil client (reader tests) records nothing.
func (c *Client) ObserveSegmentWait(d time.Duration) {
	if c == nil {
		return
	}
	c.timing.segmentWait.observe(d)
}

func (c *Client) timingSnapshot() map[string]any {
	providers := make(map[string]any, len(c.providers))
	for _, p := range c.providers {
		if pp, ok := c.pools[p.Host]; ok {
			providers[p.Host] = pp.timing.snapshot()
		}
	}
	return map[string]any{
		"since":        c.timing.since,
		"checkout":     c.timing.checkout.snapshot(),
		"segment_wait": c.timing.segmentWait.snapshot(),
		"providers":    providers,
	}
}
