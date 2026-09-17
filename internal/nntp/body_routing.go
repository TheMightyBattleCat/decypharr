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
// Every body download of at least bodySampleMinBytes records its throughput -
// one article on one connection - against its provider. Per-article time holds
// up under load: the same 60 articles at 1, 4 and 15 connections took a median
// 71/71/80 ms on newshosting, 51/51/85 ms on eweka and 1019/1005/1283 ms on
// frugal, so a provider's rate doesn't collapse just because it carries the
// traffic. The time runs to the end of the caller's write, so a stalled disk
// cache is charged to whichever provider was writing into it.
//
// When a primary is measurably slow - under bodyFastEnough AND more than
// bodySlowFactor below the fastest measured primary - the connection scan
// tries it after the other primaries instead of at its priority position. While every primary is fast the scan order is
// exactly priority order. Configured backups are never measured against and
// never move: they are still only reached when no primary can provide the
// article. A primary its quota has demoted to the fill tier is still a
// primary here.
//
// A deferred primary gets no traffic, so its sample would never refresh. Once
// that sample is bodyExploreAfter old, one fetch is let through at its
// priority position to measure it again; the rest keep going to the faster
// primaries until that sample lands.
//
// Connection acquisition runs hundreds of times a second, so the verdicts and
// the order they give are taken at most once per bodyVerdictEvery and shared.
// Only the explorer check runs on every acquisition.
//
// Settings > Providers > Usenet > Prefer Faster Servers (usenet
// prefer_faster_servers) turns the reordering off: the scan is then plain
// priority order. Rates are still recorded, so turning it back on acts on
// current measurements. It is read with each set of verdicts, so a change
// applies within bodyVerdictEvery.
const (
	// bodySampleMinBytes: smaller bodies (header probes, a file's short final
	// article) are mostly round-trip time and say little about throughput.
	bodySampleMinBytes = 256 << 10
	// bodySlowFactor: a primary is slow only when the fastest measured primary
	// is more than this many times faster.
	bodySlowFactor = 4
	// bodyFastEnough (bytes/s per article, i.e. per connection): a primary at or
	// above this is never slow, however much faster another is - a ~700 KB
	// article in ~175 ms. Healthy providers run 10-13 MiB/s per connection, so
	// bodySlowFactor usually sets the cut; this guards a merely good provider
	// against an exceptionally fast one.
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
	// bodyVerdictEvery: how long one set of verdicts, and the scan order they
	// give, serves connection acquisitions before being taken again.
	bodyVerdictEvery = time.Second
)

// bodyScan is one set of verdicts and the scan order it gives.
type bodyScan struct {
	at       int64                   // unix nanoseconds the verdicts were taken
	deferred uint64                  // bit i set: c.providers[i] is deferred
	order    []config.UsenetProvider // c.providers itself when deferred is 0
}

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

// bodyVerdict decides whether primary pp goes behind the other primaries,
// logging a change of verdict. A primary without a usable sample keeps its
// last verdict.
func (c *Client) bodyVerdict(pp *ProviderPool, fastest int64, now time.Time) bool {
	b := &pp.body
	wasSlow := b.slow.Load()
	rate := b.usable(now)
	if rate == 0 {
		return wasSlow
	}
	cut := min(int64(bodyFastEnough), fastest/bodySlowFactor)
	slow := rate < cut
	if wasSlow && !slow {
		slow = rate*100 < cut*bodyRecoverPercent
	}
	if slow != wasSlow && b.slow.CompareAndSwap(wasSlow, slow) {
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

// bodyRoutingOn reports whether Prefer Faster Servers is on.
func (c *Client) bodyRoutingOn() bool {
	return c.preferFaster == nil || c.preferFaster()
}

// takeBodyVerdicts judges every primary and builds the scan order - plain
// priority order while Prefer Faster Servers is off.
func (c *Client) takeBodyVerdicts(now time.Time) *bodyScan {
	if !c.bodyRoutingOn() {
		return &bodyScan{at: now.UnixNano(), order: c.providers}
	}
	fastest := c.fastestBody(now)
	var deferred uint64
	for i, p := range c.providers {
		pp, ok := c.pools[p.Host]
		if !ok || p.Backup || i >= 64 {
			continue
		}
		if c.bodyVerdict(pp, fastest, now) {
			deferred |= 1 << i
		}
	}
	return &bodyScan{at: now.UnixNano(), deferred: deferred, order: c.orderDeferring(deferred)}
}

// bodyScanOrder is the order getAnyAvailableConnection's non-blocking scan
// tries providers in: priority order, except that deferred primaries move to
// just after the last remaining primary, fastest of them first. Backups keep
// their places. Returns c.providers itself when nothing is deferred.
func (c *Client) bodyScanOrder(now time.Time) []config.UsenetProvider {
	s := c.bodyScan.Load()
	if s == nil || now.UnixNano()-s.at >= int64(bodyVerdictEvery) || now.UnixNano() < s.at {
		s = c.takeBodyVerdicts(now)
		c.bodyScan.Store(s)
	}
	if s.deferred == 0 {
		return s.order
	}
	for i, p := range c.providers {
		if i >= 64 || s.deferred&(1<<i) == 0 {
			continue
		}
		b := &c.pools[p.Host].body
		if b.usable(now) == 0 && b.tryExplore(now) {
			// This one acquisition measures the stale primary again.
			return c.orderDeferring(s.deferred &^ (1 << i))
		}
	}
	return s.order
}

// orderDeferring is c.providers with the primaries in deferred moved to just
// after the last primary not in it, fastest first. c.providers itself when
// nothing is deferred, or when every primary is: then nothing is faster to
// prefer.
func (c *Client) orderDeferring(deferred uint64) []config.UsenetProvider {
	if deferred == 0 {
		return c.providers
	}
	order := make([]config.UsenetProvider, 0, len(c.providers))
	var moved []config.UsenetProvider
	lastPrimary := -1
	for i, p := range c.providers {
		if i < 64 && deferred&(1<<i) != 0 {
			moved = append(moved, p)
			continue
		}
		if !p.Backup {
			lastPrimary = len(order)
		}
		order = append(order, p)
	}
	if lastPrimary < 0 {
		return c.providers
	}
	rate := func(p config.UsenetProvider) int64 { return c.pools[p.Host].body.bytesPerSec.Load() }
	sort.SliceStable(moved, func(i, j int) bool { return rate(moved[i]) > rate(moved[j]) })
	tail := append(moved, order[lastPrimary+1:]...)
	return append(order[:lastPrimary+1], tail...)
}
