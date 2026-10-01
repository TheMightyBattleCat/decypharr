package nntp

import (
	"errors"
	"io"
	"net"
	"os"
	"sort"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// STAT routing for BatchStat.
//
// Providers answer STAT at very different speeds. Measured on a production install
// (2026-09-11): frugal's backends take ~125-320 ms per STAT on any connection,
// while eweka and newshosting answer in about one round trip (~10-20 ms with
// 40-64 connections open). BatchStat used to try providers in priority order,
// so every chunk ran on the priority-1 provider, whatever its speed: a sweep
// did ~810 STAT/s on 100 frugal sockets while 330 faster ones sat idle.
//
// Each repair-pool worker now has a home provider and takes chunks only while
// that provider answers STAT within statSlowFactor of the fastest measured
// home. A provider that is slower, or not yet measured, keeps a single
// explorer worker that takes one chunk when its last sample is older than
// statExploreAfter, so a provider that speeds up (or recovers from an outage)
// is picked up again. Unresolved IDs still fall through every other provider
// (see statOrder), so an article counts as missing only when every provider
// says so - exactly as before.
const (
	// statSlowFactor: a home stays eligible while its STAT latency is at most
	// this multiple of the fastest measured home.
	statSlowFactor = 4
	// statLeaveFactor: an eligible home only drops out past this multiple.
	// A single threshold flipped a provider hovering near 4x on every
	// worker's evaluation - four flips in one second on the production install, 18% of the
	// log - as the fastest home's latency jumped between samples.
	statLeaveFactor = 6
	// statLatencyFloor is the least the fastest home's latency counts as when
	// computing the cutoff, so one lucky sample can't shrink the eligible set.
	// About one network round trip to a remote provider.
	statLatencyFloor = 10 * time.Millisecond
	// statExploreAfter: how old a provider's last sample must be before an
	// ineligible home sends one chunk to re-measure it. A sample taken after
	// a gap this long replaces the average instead of blending into it.
	statExploreAfter = 5 * time.Minute
	// statErrorPenalty is recorded as the per-STAT latency when a provider
	// fails to give a connection or drops one mid-chunk.
	statErrorPenalty = time.Second
	// statIneligibleRecheck: how often an idle, ineligible worker looks again.
	statIneligibleRecheck = time.Second
	// statEWMADivisor: each sample moves the average 1/statEWMADivisor of the
	// way towards itself.
	statEWMADivisor = 5
)

// statLatency is one provider's measured STAT latency.
type statLatency struct {
	nsPerStat atomic.Int64 // moving average, nanoseconds per STAT; 0 = never measured
	sampledAt atomic.Int64 // unix nanoseconds of the latest sample
	exploring atomic.Bool  // an ineligible home has a worker out re-measuring it
	eligible  atomic.Bool  // last eligibility a worker saw, for transition logs
	desyncs   atomic.Int64 // pipelined STAT windows discarded as out of step

	// Failures that recorded statErrorPenalty, by cause (see statFailureCause).
	// Desyncs are counted above.
	errCheckout atomic.Int64
	errTimeout  atomic.Int64
	errEOF      atomic.Int64
	errClosed   atomic.Int64
	errOther    atomic.Int64
	// cancelled counts windows cut short by the caller's context: the
	// connection is closed to unblock the read, but no penalty is recorded.
	cancelled atomic.Int64
}

// STAT failure causes, as logged and as the suffix of the stat_err_* stats.
const (
	statCauseCheckout = "checkout" // no connection could be had from the pool
	statCauseTimeout  = "timeout"  // a read or write deadline passed
	statCauseEOF      = "eof"      // the provider closed or reset the connection
	statCauseClosed   = "closed"   // the connection was closed on our side mid-window
	statCauseDesync   = "desync"
	statCauseOther    = "other"
)

// statFailureCause names why a STAT window failed.
func statFailureCause(err error) string {
	var netErr net.Error
	switch {
	case errors.Is(err, errStatDesync):
		return statCauseDesync
	case errors.Is(err, os.ErrDeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout():
		return statCauseTimeout
	case errors.Is(err, net.ErrClosed):
		return statCauseClosed
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF),
		errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE), errors.Is(err, syscall.ECONNABORTED):
		return statCauseEOF
	default:
		return statCauseOther
	}
}

// penalties is the number of failures that recorded statErrorPenalty.
func (s *statLatency) penalties() int64 {
	return s.errCheckout.Load() + s.errTimeout.Load() + s.errEOF.Load() + s.errClosed.Load() +
		s.errOther.Load() + s.desyncs.Load()
}

// notePenalty records statErrorPenalty against provider for a failed checkout
// or window, counts it by cause and logs the error behind it.
func (c *Client) notePenalty(provider config.UsenetProvider, cause string, err error, window, done, chunk int, took time.Duration) {
	stage := "window"
	if cause == statCauseCheckout {
		stage = "checkout"
	}
	if pp, ok := c.pools[provider.Host]; ok {
		switch cause {
		case statCauseCheckout:
			pp.stat.errCheckout.Add(1)
		case statCauseTimeout:
			pp.stat.errTimeout.Add(1)
		case statCauseEOF:
			pp.stat.errEOF.Add(1)
		case statCauseClosed:
			pp.stat.errClosed.Add(1)
		case statCauseDesync:
			pp.stat.desyncs.Add(1)
		default:
			pp.stat.errOther.Add(1)
		}
	}
	c.logger.Debug().Err(err).Str("provider", provider.Host).Str("stage", stage).Str("cause", cause).
		Int("window", window).Int("done", done).Int("chunk", chunk).Dur("took", took).
		Msg("STAT penalty: a chunk failed on this provider, error penalty recorded")
	c.recordStatSample(provider, statErrorPenalty)
}

// record folds one per-STAT latency sample into the average. The first sample,
// and any taken statExploreAfter or longer after the previous one, replace it.
func (s *statLatency) record(perStat time.Duration, now time.Time) {
	sample := max(int64(perStat), 1)
	last := s.sampledAt.Load()
	stale := last == 0 || now.Sub(time.Unix(0, last)) >= statExploreAfter
	for {
		old := s.nsPerStat.Load()
		next := sample
		if old > 0 && !stale {
			next = max(old+(sample-old)/statEWMADivisor, 1)
		}
		if s.nsPerStat.CompareAndSwap(old, next) {
			break
		}
	}
	s.sampledAt.Store(now.UnixNano())
}

// tryExplore claims the explorer role for an ineligible home. It fails while
// another worker holds it, and while the provider has a sample younger than
// statExploreAfter (fall-through STATs keep a slow provider measured).
func (s *statLatency) tryExplore(now time.Time) bool {
	if last := s.sampledAt.Load(); last != 0 && now.Sub(time.Unix(0, last)) < statExploreAfter {
		return false
	}
	return s.exploring.CompareAndSwap(false, true)
}

// recordStatSample records a per-STAT latency sample against provider.
func (c *Client) recordStatSample(provider config.UsenetProvider, perStat time.Duration) {
	if pp, ok := c.pools[provider.Host]; ok {
		pp.stat.record(perStat, time.Now())
	}
}

// recordStatHits records the average time of a chunk's found articles. A chunk
// that found nothing leaves the average alone but still counts as a fresh look
// at the provider, so a provider answering 430 for everything doesn't hand its
// explorer one chunk after another.
func (c *Client) recordStatHits(provider config.UsenetProvider, hitTime time.Duration, hits int) {
	pp, ok := c.pools[provider.Host]
	if !ok {
		return
	}
	if hits == 0 {
		pp.stat.sampledAt.Store(time.Now().UnixNano())
		return
	}
	pp.stat.record(hitTime/time.Duration(hits), time.Now())
}

// statHomeBlocked reports whether home is over its hard bandwidth quota. A
// blocked provider serves no downloads, so it takes no STAT chunks either
// (fall-through still asks it, so verdicts don't change). A provider in its
// reserve band keeps taking them: a STAT reply is tens of bytes, ~0.5 GB/h
// metered on eweka at ~4k STAT/s against a ~550 GB reserve (a production install).
func (c *Client) statHomeBlocked(pp *ProviderPool) bool {
	return c.bw != nil && c.bw.Tier(pp.config.Host) == QuotaBlocked
}

// statHomePools returns the pools repair-pool workers are homed on: every
// primary provider with connections, or every provider with connections when
// all are backups. Priority order.
func (c *Client) statHomePools() []*ProviderPool {
	var primaries, all []*ProviderPool
	for _, p := range c.providers {
		pp, ok := c.pools[p.Host]
		if !ok || pp.max <= 0 {
			continue
		}
		all = append(all, pp)
		if !p.Backup {
			primaries = append(primaries, pp)
		}
	}
	if len(primaries) > 0 {
		return primaries
	}
	return all
}

// statEligible reports whether home currently answers STAT fast enough to take
// chunks, and the latencies behind the decision. A home over its hard quota is
// never eligible and doesn't count towards the fastest.
func (c *Client) statEligible(home *ProviderPool) (ok bool, lat, fastest time.Duration) {
	l := home.stat.nsPerStat.Load()
	var best int64
	for _, pp := range c.statHomes {
		if c.statHomeBlocked(pp) {
			continue
		}
		if v := pp.stat.nsPerStat.Load(); v > 0 && (best == 0 || v < best) {
			best = v
		}
	}
	if l == 0 || c.statHomeBlocked(home) {
		return false, time.Duration(l), time.Duration(best)
	}
	ref := max(best, int64(statLatencyFloor))
	factor := int64(statSlowFactor)
	if home.stat.eligible.Load() {
		factor = statLeaveFactor // hysteresis: see statLeaveFactor
	}
	return l <= factor*ref, time.Duration(l), time.Duration(best)
}

// statOrder is the order BatchStat asks providers about a chunk: home first,
// then the other primaries fastest first (unmeasured ones after the measured,
// in priority order), then backups in priority order. It always holds every
// configured provider exactly once, so a definitive not-found still means
// every provider was asked.
func (c *Client) statOrder(home config.UsenetProvider) []config.UsenetProvider {
	order := make([]config.UsenetProvider, 0, len(c.providers))
	var primaries, backups []config.UsenetProvider
	for _, p := range c.providers {
		switch {
		case p.Host == home.Host:
			order = append(order, p)
		case p.Backup:
			backups = append(backups, p)
		default:
			primaries = append(primaries, p)
		}
	}
	if len(order) == 0 {
		// home isn't configured (can't happen for a pool worker): keep it
		// first anyway so the caller's choice is honoured.
		order = append(order, home)
	}
	lat := func(p config.UsenetProvider) int64 {
		if pp, ok := c.pools[p.Host]; ok {
			return pp.stat.nsPerStat.Load()
		}
		return 0
	}
	sort.SliceStable(primaries, func(i, j int) bool {
		li, lj := lat(primaries[i]), lat(primaries[j])
		switch {
		case li == 0:
			return false
		case lj == 0:
			return true
		default:
			return li < lj
		}
	})
	order = append(order, primaries...)
	return append(order, backups...)
}
