package nntp

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sort"
	"sync"
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
// explorer worker that takes one chunk when its last sample is older than its
// re-test wait, so a provider that speeds up (or recovers from an outage) is
// picked up again. The wait starts at statExploreAfter and doubles, up to
// statExploreMax, each time the re-test still finds the provider too slow. Unresolved IDs still fall through every other provider
// (see statOrder), so an article counts as missing only when every provider
// says so - exactly as before. Within one BatchStat call a provider that turns
// out to have none of the call's articles is asked last (see statHint).
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
	// At 10 ms (one round trip) a provider answering in 5 ms set the leave
	// cutoff at 60 ms, and on a production install a second fast provider
	// working flat out measured 60-95 ms and was dropped again and again,
	// while the genuinely slow providers sat at 300 ms and above. 25 ms puts
	// the cutoffs at 100 ms to join and 150 ms to leave.
	statLatencyFloor = 25 * time.Millisecond
	// statExploreAfter: how old a provider's last sample must be before an
	// ineligible home first sends one chunk to re-measure it. A sample taken
	// after a gap this long replaces the average instead of blending into it.
	// This was 5 minutes with no back-off: on a production install a fast
	// provider whose latency spiked for a few seconds then sat out the full
	// 5 minutes, and spent most of a sweep out of the rotation.
	statExploreAfter = 30 * time.Second
	// statExploreMax caps the re-test wait. A provider that stays slow backs
	// off to one re-test chunk this often, as before.
	statExploreMax = 5 * time.Minute
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
	// exploreWait is the current re-test wait in nanoseconds; 0 means
	// statExploreAfter. See exploreDone.
	exploreWait atomic.Int64
	desyncs     atomic.Int64 // pipelined STAT windows discarded as out of step

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
	// hintDemotions counts BatchStat calls that moved this provider to the
	// back because it had none of the call's articles (see statHint).
	hintDemotions atomic.Int64
	// probeDemotions counts BatchStat calls that asked this provider last
	// from the start, because another provider answered the call's probe with
	// a hit and this one did not (see statProbe).
	probeDemotions atomic.Int64
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

// retestWait is how old the provider's last sample must be before its next
// re-test chunk.
func (s *statLatency) retestWait() time.Duration {
	if w := time.Duration(s.exploreWait.Load()); w > 0 {
		return w
	}
	return statExploreAfter
}

// tryExplore claims the explorer role for an ineligible home. It fails while
// another worker holds it, and while the provider has a sample younger than
// its re-test wait (fall-through STATs keep a slow provider measured).
func (s *statLatency) tryExplore(now time.Time) bool {
	if last := s.sampledAt.Load(); last != 0 && now.Sub(time.Unix(0, last)) < s.retestWait() {
		return false
	}
	return s.exploring.CompareAndSwap(false, true)
}

// exploreDone ends an explorer's turn. A provider the re-test still finds too
// slow waits twice as long for the next one, up to statExploreMax; one that
// is eligible again starts from statExploreAfter the next time it drops out.
func (s *statLatency) exploreDone(eligible bool) {
	if eligible {
		s.exploreWait.Store(0)
	} else {
		s.exploreWait.Store(int64(min(2*s.retestWait(), statExploreMax)))
	}
	s.exploring.Store(false)
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

// statHintMinAnswers is how many of one BatchStat call's articles a provider
// must have answered, without finding any, before the rest of the call asks
// it last.
const statHintMinAnswers = 32

// statHint is what one BatchStat call has learned about where its articles
// are. A call checks one file's (or one PAR2 set's) articles, which were
// posted together: a provider that has none of the first few dozen almost
// never has the rest. Providers answer "no such article" 40-100x slower than
// a hit, one after another, so a chunk that starts on such a provider waits
// seconds per article for nothing before it reaches the provider that has
// the file.
//
// The hint only reorders. A provider with no hits is asked after the others
// in its tier (primaries stay ahead of backups), and still gets every ID
// nothing else had, so "missing" still means every provider said so. One hit
// puts it back in its usual place.
//
// A provider is asked last on either of two grounds: it has answered
// statHintMinAnswers of the call's articles and found none, or the call's
// probe (statProbe) got a hit from another provider and none from this one.
type statHint struct {
	byHost map[string]*statHintCounts
	// probeMu serialises probe results, which read and set the flags of
	// several providers at once.
	probeMu sync.Mutex
	// probeSettled: the wait for the probe is over (probeSettle).
	probeSettled bool
}

type statHintCounts struct {
	answered atomic.Int64 // definitive replies: found or not found
	hits     atomic.Int64
	noted    atomic.Bool // the demotion was logged and counted for this call

	probing       atomic.Bool // the call's probe asked this provider and has not failed on it
	probeAnswered atomic.Bool // the probe got definitive replies from it
	demoted       atomic.Bool // another provider answered the probe with a hit first
}

// newStatHint returns an empty hint covering every configured provider.
func (c *Client) newStatHint() *statHint {
	h := &statHint{byHost: make(map[string]*statHintCounts, len(c.providers))}
	for _, p := range c.providers {
		h.byHost[p.Host] = &statHintCounts{}
	}
	return h
}

// add records a provider's definitive replies for part of the call.
func (h *statHint) add(host string, answered, hits int) {
	if h == nil || answered == 0 {
		return
	}
	if n, ok := h.byHost[host]; ok {
		n.hits.Add(int64(hits))
		n.answered.Add(int64(answered))
	}
}

// absent reports whether host is to be asked last: it has found none of the
// call's articles, and has either answered enough of them or lost the probe.
func (h *statHint) absent(host string) bool {
	if h == nil {
		return false
	}
	n, ok := h.byHost[host]
	return ok && n.hits.Load() == 0 && (n.demoted.Load() || n.answered.Load() >= statHintMinAnswers)
}

// probeStart marks host as asked by the call's probe.
func (h *statHint) probeStart(host string) {
	if n, ok := h.byHost[host]; ok {
		n.probing.Store(true)
	}
}

// probeResult records what the probe learned from host. Once any probed
// provider has a hit, a probed provider that answered without one is asked
// last. One still to answer is left alone until the probe has settled
// (probeSettle): it may hold the file and be a few milliseconds behind, and
// demoting it at the first hit would send the file's whole first wave of
// chunks to the provider that answered first. A probe that failed
// (answered == 0) says nothing about what host holds, so host is taken out
// of the probe and keeps its usual place.
func (h *statHint) probeResult(host string, answered, hits int) {
	n, ok := h.byHost[host]
	if !ok {
		return
	}
	h.probeMu.Lock()
	defer h.probeMu.Unlock()
	if answered == 0 {
		n.probing.Store(false)
		n.demoted.Store(false)
		return
	}
	n.probeAnswered.Store(true)
	h.add(host, answered, hits)
	h.probeDemoteLocked()
}

// probeSettle ends the wait for the probe's replies: the call's chunks are
// about to start. From here on a probed provider that still has not answered
// is asked last as soon as another has a hit. A hit takes about one round
// trip and a miss 40-100x that, so by now it is answering misses. If it does
// come back with a hit, the hit puts it back (see absent).
func (h *statHint) probeSettle() {
	h.probeMu.Lock()
	defer h.probeMu.Unlock()
	h.probeSettled = true
	h.probeDemoteLocked()
}

// probeDemoteLocked applies the probe's rule; see probeResult and probeSettle.
func (h *statHint) probeDemoteLocked() {
	found := false
	for _, o := range h.byHost {
		if o.probing.Load() && o.hits.Load() > 0 {
			found = true
			break
		}
	}
	if !found {
		return
	}
	for _, o := range h.byHost {
		if o.probing.Load() && o.hits.Load() == 0 && (h.probeSettled || o.probeAnswered.Load()) {
			o.demoted.Store(true)
		}
	}
}

// applyStatHint reorders the providers a chunk has still to ask, in place:
// within the primaries, and within the backups, those the hint marks absent
// go last. It never drops or adds a provider, and leaves the order alone when
// none is absent.
func (c *Client) applyStatHint(hint *statHint, rest []config.UsenetProvider) {
	if hint == nil || len(rest) < 2 {
		return
	}
	rank := func(p config.UsenetProvider) int {
		r := 0
		if p.Backup {
			r = 2
		}
		if hint.absent(p.Host) {
			r++
		}
		return r
	}
	demoted := false
	for _, p := range rest {
		if !hint.absent(p.Host) {
			continue
		}
		demoted = true
		// A provider asked last on the probe's word alone is logged and
		// counted when the call ends (noteProbeDemotions): by then a late
		// hit from it has had the chance to put it back.
		if n := hint.byHost[p.Host]; n.answered.Load() >= statHintMinAnswers && n.noted.CompareAndSwap(false, true) {
			if pp, ok := c.pools[p.Host]; ok {
				pp.stat.hintDemotions.Add(1)
			}
			c.logger.Debug().Str("provider", p.Host).Int64("answered", n.answered.Load()).
				Msg("STAT hint: provider has none of this check's articles so far, asking it last for the rest")
		}
	}
	if demoted {
		sort.SliceStable(rest, func(i, j int) bool { return rank(rest[i]) < rank(rest[j]) })
	}
}

const (
	// statProbeIDs is how many of a BatchStat call's articles the probe asks
	// each fast provider about before the call's chunks start.
	statProbeIDs = 4
	// statProbeWait is the longest a call waits for the probe's first hit.
	// A hit takes about one round trip, so a probe with none by now is
	// waiting on misses everywhere: the chunks start as they did before.
	statProbeWait = 250 * time.Millisecond
	// statProbeGraceFactor: after the first hit the call waits for the other
	// probes up to this many times as long as that hit took (at least
	// statLatencyFloor, at most statProbeWait) before it starts the chunks
	// and asks a provider still to answer last.
	statProbeGraceFactor = 3
	// statProbeMaxCalls caps the calls with a probe out at once. A probe
	// takes its connections outside the repair pool's share of each provider,
	// and one waiting on a provider's misses holds its connection until the
	// call ends; a call over the cap goes without.
	statProbeMaxCalls = 4
)

// statProbe asks every fast provider (a STAT home that is currently eligible)
// about statProbeIDs of the call's articles, all at once. Once one answers
// with a hit it waits a short grace for the rest (statProbeGraceFactor) and
// returns. A provider that answered without a hit, or has still not answered,
// is then asked last from the call's first chunk on (statHint.probeResult,
// probeSettle), without waiting for its slow misses.
//
// Without it the hint cannot help a file's first chunks: they all start
// before any has an answer, so each worker homed on a provider without the
// file pays a whole chunk of misses first. On a production install the hint
// fired after a median of 800 such misses, 4 to 16 s into the file.
//
// It costs a file one extra round trip on the slowest provider that has it,
// or the grace when one does not. Like the hint it only reorders. A call too
// small to gain, with fewer than two fast providers, or over
// statProbeMaxCalls skips it.
//
// The probes run on ctx and are not stopped when the chunks start, so a
// provider that has the file but answers after the grace puts itself back
// for the chunks still to start. The caller
// cancels ctx when the call is over and then calls the returned function,
// which waits for the probes to end and logs what they decided.
func (c *Client) statProbe(ctx context.Context, messageIDs []string, hint *statHint) (done func()) {
	done = func() {}
	if hint == nil || len(messageIDs) <= statProbeIDs {
		return done
	}
	var homes []*ProviderPool
	skipHosts := statExcludedHosts(ctx)
	for _, pp := range c.statHomes {
		// A provider the caller left out is not asked here either.
		if _, skip := skipHosts[pp.config.Host]; skip {
			continue
		}
		if ok, _, _ := c.statEligible(pp); ok {
			homes = append(homes, pp)
		}
	}
	if len(homes) < 2 {
		return done
	}
	if c.statProbing.Add(1) > statProbeMaxCalls {
		c.statProbing.Add(-1)
		return done
	}

	// Spread over the file, so one dead article at its start is not the
	// whole sample.
	probeIDs := make([]string, statProbeIDs)
	for i := range probeIDs {
		probeIDs[i] = messageIDs[i*len(messageIDs)/statProbeIDs]
	}
	for _, pp := range homes {
		hint.probeStart(pp.config.Host)
	}

	began := time.Now()
	hit := make(chan struct{}, len(homes))
	var wg sync.WaitGroup
	for _, pp := range homes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results, _ := c.batchStatOnProvider(ctx, pp.config, probeIDs)
			answered, hits := 0, 0
			for _, r := range results {
				switch {
				case r.Available:
					answered++
					hits++
				case IsArticleNotFoundError(r.Error):
					answered++
				}
			}
			if answered == 0 && ctx.Err() != nil {
				// Cut short by the end of the call, not a failure: the
				// provider was still answering when the call finished.
				return
			}
			hint.probeResult(pp.config.Host, answered, hits)
			if hits > 0 {
				hit <- struct{}{}
			}
		}()
	}
	all := make(chan struct{})
	go func() {
		wg.Wait()
		close(all)
	}()

	wait := time.NewTimer(statProbeWait)
	defer wait.Stop()
	select {
	case <-hit:
		// Give the others a moment to answer too: one that holds the file
		// is at most a round trip behind, one that does not is seconds away.
		grace := time.NewTimer(min(max(statProbeGraceFactor*time.Since(began), statLatencyFloor), statProbeWait))
		select {
		case <-all:
		case <-grace.C:
		case <-ctx.Done():
		}
		grace.Stop()
	case <-all:
	case <-wait.C:
	case <-ctx.Done():
	}
	hint.probeSettle()

	return func() {
		<-all
		c.statProbing.Add(-1)
		c.noteProbeDemotions(hint, time.Since(began))
	}
}

// noteProbeDemotions logs and counts, once the call is over, the providers
// its probe had asked last. A provider counts when its probe answered and
// found nothing, or when it still had not answered after statProbeWait (it
// was working through misses). One whose probe was simply cut short by a
// call that finished sooner than that is left out: nothing is known about it.
func (c *Client) noteProbeDemotions(hint *statHint, took time.Duration) {
	for host, n := range hint.byHost {
		if !n.demoted.Load() || n.hits.Load() != 0 {
			continue
		}
		answered := n.probeAnswered.Load()
		if !answered && took < statProbeWait {
			continue
		}
		if !n.noted.CompareAndSwap(false, true) {
			continue
		}
		if pp, ok := c.pools[host]; ok {
			pp.stat.probeDemotions.Add(1)
		}
		c.logger.Debug().Str("provider", host).Bool("probe_answered", answered).Dur("check_took", took).
			Msg("STAT probe: another provider had this check's articles and this one did not, it was asked last from the start")
	}
}
