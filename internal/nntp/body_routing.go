package nntp

import (
	"sort"
	"sync/atomic"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// Throughput routing for article bodies.
//
// getAnyAvailableConnection takes the first provider in priority order with a
// free slot. A stream uses a handful of connections and a provider pool holds
// far more, so every playback, import and precache fetch lands on the
// priority-1 provider however slowly it serves. Measured on a production install
// 2026-09-17, the same 24 articles on 4 connections each: eunews.frugalusenet
// 1.25 MiB/s (846 ms per article), newshosting 28 and eweka 48 MiB/s (53-75
// ms). With frugal at priority 1 a 34 Mbit/s Remux buffered for two hours on
// ~2.5 MiB/s; a minute after newshosting took priority 1 the same read-ahead
// ran at ~44 MiB/s.
//
// Every body download of at least bodySampleMinBytes records its throughput
// against its provider. When a primary is measurably slow - under
// bodyFastEnough AND more than bodySlowFactor below the fastest measured
// primary - the connection scan tries it after the other primaries instead of
// at its priority position. While every primary is fast the scan order is
// exactly priority order. Configured backups are never measured against and
// never move: they are still only reached when no primary can provide the
// article. A primary its quota has demoted to the fill tier is still a
// primary here.
//
// A deferred primary gets no traffic, so its sample would never refresh. Once
// that sample is bodyExploreAfter old, one fetch is let through at its
// priority position to measure it again; the rest keep going to the faster
// primaries until that sample lands.
const (
	// bodySampleMinBytes: smaller bodies (header probes, a file's short final
	// article) are mostly round-trip time and say little about throughput.
	bodySampleMinBytes = 256 << 10
	// bodySlowFactor: a primary is slow only when the fastest measured primary
	// is more than this many times faster.
	bodySlowFactor = 4
	// bodyFastEnough (bytes/s per article): a primary at or above this is never
	// slow, however much faster another is - a ~700 KB article in ~175 ms.
	bodyFastEnough = 4 << 20
	// bodyRecoverPercent: a slow primary gets its position back only once it
	// clears the slow cut by this much, so a rate hovering at the cut doesn't
	// flip the order on every sample.
	bodyRecoverPercent = 125
	// bodyMinSamples: samples a primary needs before it can be judged, or be
	// the fastest others are judged against.
	bodyMinSamples = 5
	// bodyExploreAfter: how old a sample may get before it stops counting, and
	// how often a deferred primary is let one fetch through to re-measure it.
	bodyExploreAfter = 5 * time.Minute
	// bodyEWMADivisor: each sample moves the average 1/bodyEWMADivisor of the
	// way towards itself.
	bodyEWMADivisor = 5
)

// bodyThroughput is one provider's measured body download rate.
type bodyThroughput struct {
	bytesPerSec atomic.Int64 // moving average; 0 = never measured
	samples     atomic.Int64 // samples recorded, ever
	sampledAt   atomic.Int64 // unix nanoseconds of the latest sample
	exploredAt  atomic.Int64 // unix nanoseconds of the latest explorer let through
	slow        atomic.Bool  // latest verdict: deferred behind the other primaries
}

// record folds one body download into the average. The first sample, and any
// taken bodyExploreAfter or longer after the previous one, replace it.
func (b *bodyThroughput) record(n int64, d time.Duration, now time.Time) {
	if n < bodySampleMinBytes {
		return
	}
	d = max(d, time.Microsecond)
	sample := max(int64(float64(n)/d.Seconds()), 1)
	last := b.sampledAt.Load()
	stale := last == 0 || now.Sub(time.Unix(0, last)) >= bodyExploreAfter
	for {
		old := b.bytesPerSec.Load()
		next := sample
		if old > 0 && !stale {
			next = max(old+(sample-old)/bodyEWMADivisor, 1)
		}
		if b.bytesPerSec.CompareAndSwap(old, next) {
			break
		}
	}
	b.samples.Add(1)
	b.sampledAt.Store(now.UnixNano())
}

// usable reports the provider's rate when it has enough samples and the latest
// is younger than bodyExploreAfter, else 0.
func (b *bodyThroughput) usable(now time.Time) int64 {
	last := b.sampledAt.Load()
	if b.samples.Load() < bodyMinSamples || last == 0 || now.Sub(time.Unix(0, last)) >= bodyExploreAfter {
		return 0
	}
	return b.bytesPerSec.Load()
}

// tryExplore lets one fetch through to a deferred primary whose sample has gone
// stale, at most once per bodyExploreAfter.
func (b *bodyThroughput) tryExplore(now time.Time) bool {
	last := b.exploredAt.Load()
	if last != 0 && now.Sub(time.Unix(0, last)) < bodyExploreAfter {
		return false
	}
	return b.exploredAt.CompareAndSwap(last, now.UnixNano())
}

// recordBody records a body download against provider host.
func (c *Client) recordBody(host string, n int64, d time.Duration) {
	if pp, ok := c.pools[host]; ok {
		pp.body.record(n, d, time.Now())
	}
}

// fastestBody is the highest usable rate among primaries not over their hard
// quota, or 0 when none is measured.
func (c *Client) fastestBody(now time.Time) int64 {
	var best int64
	for _, p := range c.providers {
		pp, ok := c.pools[p.Host]
		if !ok || p.Backup || c.statHomeBlocked(pp) {
			continue
		}
		best = max(best, pp.body.usable(now))
	}
	return best
}

// bodyDeferred decides whether primary pp goes behind the other primaries in
// this scan, logging a change of verdict.
func (c *Client) bodyDeferred(pp *ProviderPool, fastest int64, now time.Time) bool {
	b := &pp.body
	rate := b.usable(now)
	if rate == 0 {
		// No usable sample. A primary last seen slow stays behind the others,
		// except for one explorer per bodyExploreAfter to measure it again.
		return b.slow.Load() && !b.tryExplore(now)
	}
	wasSlow := b.slow.Load()
	cut := min(int64(bodyFastEnough), fastest/bodySlowFactor)
	slow := rate < cut
	if wasSlow && !slow {
		slow = rate*100 < cut*bodyRecoverPercent
	}
	if b.slow.CompareAndSwap(wasSlow, slow) && wasSlow != slow {
		msg := "Body routing: provider is fast enough again, back at its priority position"
		if slow {
			msg = "Body routing: provider is too slow, trying the other primaries first"
		}
		c.logger.Debug().Str("provider", pp.config.Host).
			Float64("mib_s", float64(rate)/(1<<20)).
			Float64("fastest_mib_s", float64(fastest)/(1<<20)).
			Msg(msg)
	}
	return slow
}

// bodyScanOrder is the order getAnyAvailableConnection's non-blocking scan
// tries providers in: priority order, except that deferred primaries move to
// just after the last remaining primary, fastest of them first. Backups keep
// their places. Returns c.providers itself when nothing is deferred, which is
// the common case and allocates nothing.
func (c *Client) bodyScanOrder(now time.Time) []config.UsenetProvider {
	fastest := c.fastestBody(now)
	var deferred []config.UsenetProvider
	for _, p := range c.providers {
		pp, ok := c.pools[p.Host]
		if !ok || p.Backup {
			continue
		}
		if c.bodyDeferred(pp, fastest, now) {
			deferred = append(deferred, p)
		}
	}
	if len(deferred) == 0 {
		return c.providers
	}

	order := make([]config.UsenetProvider, 0, len(c.providers))
	lastPrimary := -1
	for _, p := range c.providers {
		if !p.Backup && isDeferred(deferred, p.Host) {
			continue
		}
		if !p.Backup {
			lastPrimary = len(order)
		}
		order = append(order, p)
	}
	if lastPrimary < 0 {
		// Every primary is deferred (all last seen slow, samples stale): there
		// is nothing faster to prefer.
		return c.providers
	}
	rate := func(p config.UsenetProvider) int64 { return c.pools[p.Host].body.bytesPerSec.Load() }
	sort.SliceStable(deferred, func(i, j int) bool { return rate(deferred[i]) > rate(deferred[j]) })
	tail := append(deferred, order[lastPrimary+1:]...)
	return append(order[:lastPrimary+1], tail...)
}

func isDeferred(deferred []config.UsenetProvider, host string) bool {
	for _, p := range deferred {
		if p.Host == host {
			return true
		}
	}
	return false
}
