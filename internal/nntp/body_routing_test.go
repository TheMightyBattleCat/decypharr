package nntp

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
)

// newBodyTestClient builds a Client over providers without NewClient's
// bandwidth tracker, reaper or repair pool.
func newBodyTestClient(t *testing.T, providers []config.UsenetProvider) *Client {
	t.Helper()
	c := &Client{pools: map[string]*ProviderPool{}, providers: providers, logger: zerolog.Nop()}
	for _, p := range providers {
		c.pools[p.Host] = &ProviderPool{slots: make(chan struct{}, p.MaxConnections), max: p.MaxConnections, config: p}
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// setBody gives host a fresh, fully sampled body rate of mibs MiB/s.
func setBody(c *Client, host string, mibs float64) {
	b := &c.pools[host].body
	b.bytesPerSec.Store(int64(mibs * (1 << 20)))
	b.samples.Store(bodyMinSamples)
	b.sampledAt.Store(time.Now().UnixNano())
}

func bodyHosts(ps []config.UsenetProvider) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.Host
	}
	return out
}

func wantOrder(t *testing.T, got []config.UsenetProvider, want ...string) {
	t.Helper()
	g := bodyHosts(got)
	if len(g) != len(want) {
		t.Fatalf("order = %v, want %v", g, want)
	}
	for i := range want {
		if g[i] != want[i] {
			t.Fatalf("order = %v, want %v", g, want)
		}
	}
}

func TestBodyThroughputRecord(t *testing.T) {
	var b bodyThroughput
	now := time.Now()

	b.record(64<<10, 10*time.Millisecond, now)
	if b.samples.Load() != 0 || b.bytesPerSec.Load() != 0 {
		t.Fatal("a body under bodySampleMinBytes was recorded")
	}

	b.record(1<<20, time.Second, now)
	if got := b.bytesPerSec.Load(); got != 1<<20 {
		t.Fatalf("first sample: got %d B/s, want %d", got, 1<<20)
	}
	b.record(1<<20, 250*time.Millisecond, now.Add(time.Second))
	if got, want := b.bytesPerSec.Load(), int64(1<<20+(4<<20-1<<20)/bodyEWMADivisor); got != want {
		t.Fatalf("blended sample: got %d B/s, want %d", got, want)
	}

	// After a gap of bodyExploreAfter the sample replaces the average: a
	// provider that recovered must not need many samples to look fast again.
	b.record(1<<20, 50*time.Millisecond, now.Add(time.Second+bodyExploreAfter))
	if got := b.bytesPerSec.Load(); got != 20<<20 {
		t.Fatalf("stale sample: got %d B/s, want %d", got, 20<<20)
	}
	if b.samples.Load() != 3 {
		t.Fatalf("samples = %d, want 3", b.samples.Load())
	}
}

// While no primary is slow the scan is plain priority order, with no copy.
func TestBodyScanOrderUnchangedWhenNothingSlow(t *testing.T) {
	providers := []config.UsenetProvider{
		{Host: "p1", Priority: 1, MaxConnections: 4},
		{Host: "p2", Priority: 2, MaxConnections: 4},
		{Host: "p3", Priority: 3, MaxConnections: 4},
		{Host: "b1", Priority: 4, MaxConnections: 4, Backup: true},
	}
	c := newBodyTestClient(t, providers)
	same := func() {
		t.Helper()
		got := c.bodyScanOrder(time.Now())
		if len(got) != len(c.providers) || &got[0] != &c.providers[0] {
			t.Fatalf("order = %v, want c.providers itself", bodyHosts(got))
		}
	}

	same() // nothing measured

	// 10x apart, but p1 is above bodyFastEnough: fast enough is fast enough.
	setBody(c, "p1", 5)
	setBody(c, "p2", 50)
	same()

	// Far below the others, but too few samples to judge.
	setBody(c, "p1", 1)
	c.pools["p1"].body.samples.Store(bodyMinSamples - 1)
	same()
}

func TestBodyScanOrderDefersSlowPrimary(t *testing.T) {
	providers := []config.UsenetProvider{
		{Host: "frugal", Priority: 1, MaxConnections: 4},
		{Host: "newshosting", Priority: 2, MaxConnections: 4},
		{Host: "eweka", Priority: 3, MaxConnections: 4},
		{Host: "backup", Priority: 4, MaxConnections: 4, Backup: true},
	}
	c := newBodyTestClient(t, providers)
	setBody(c, "frugal", 1.25)
	setBody(c, "newshosting", 28)
	// eweka unmeasured: keeps its priority position.

	wantOrder(t, c.bodyScanOrder(time.Now()), "newshosting", "eweka", "frugal", "backup")
	if !c.pools["frugal"].body.slow.Load() {
		t.Error("frugal's verdict not recorded as slow")
	}
}

// Two slow primaries both go behind the fast one, the faster of them first:
// an article missing on the fast one should come from the less slow.
func TestBodyScanOrderSlowPrimariesFastestFirst(t *testing.T) {
	providers := []config.UsenetProvider{
		{Host: "newshosting", Priority: 1, MaxConnections: 4},
		{Host: "eunews", Priority: 2, MaxConnections: 4},
		{Host: "bonus", Priority: 3, MaxConnections: 4},
		{Host: "backup", Priority: 4, MaxConnections: 4, Backup: true},
	}
	c := newBodyTestClient(t, providers)
	setBody(c, "newshosting", 40)
	setBody(c, "eunews", 1.2)
	setBody(c, "bonus", 3)
	wantOrder(t, c.bodyScanOrder(time.Now()), "newshosting", "bonus", "eunews", "backup")
}

// Backups never move, are never judged, and never set the rate primaries are
// judged against.
func TestBodyScanOrderBackupsKeepTheirPlaces(t *testing.T) {
	providers := []config.UsenetProvider{
		{Host: "slow", Priority: 1, MaxConnections: 4},
		{Host: "b1", Priority: 2, MaxConnections: 4, Backup: true},
		{Host: "fast", Priority: 3, MaxConnections: 4},
		{Host: "b2", Priority: 4, MaxConnections: 4, Backup: true},
	}
	c := newBodyTestClient(t, providers)
	setBody(c, "slow", 1)
	setBody(c, "fast", 30)
	setBody(c, "b1", 0.1) // a crawling backup is still not deferred
	wantOrder(t, c.bodyScanOrder(time.Now()), "b1", "fast", "slow", "b2")
	if c.pools["b1"].body.slow.Load() {
		t.Error("a backup was judged slow")
	}

	// Only a backup is fast: the primaries are judged against each other.
	setBody(c, "fast", 1.5)
	setBody(c, "b2", 100)
	got := c.bodyScanOrder(time.Now())
	if &got[0] != &c.providers[0] {
		t.Fatalf("order = %v: a backup's rate made a primary slow", bodyHosts(got))
	}
}

// A primary over its hard quota serves nothing, so it isn't the fastest the
// others are cut against. A reserve-band primary still is, and is still judged.
func TestBodyScanOrderHardQuota(t *testing.T) {
	providers := []config.UsenetProvider{
		{Host: "capped", Priority: 1, MaxConnections: 4},
		{Host: "slower", Priority: 2, MaxConnections: 4},
		{Host: "slowest", Priority: 3, MaxConnections: 4},
	}
	c := newBodyTestClient(t, providers)
	setBody(c, "capped", 40)
	setBody(c, "slower", 3)
	setBody(c, "slowest", 1)

	c.bw = quotaTracker("capped", 95, 100, 10) // reserve band
	wantOrder(t, c.bodyScanOrder(time.Now()), "capped", "slower", "slowest")
	if !c.pools["slower"].body.slow.Load() {
		t.Error("slower not judged against a reserve-band primary")
	}

	c.bw = quotaTracker("capped", 100, 100, 10) // hard quota
	got := c.bodyScanOrder(time.Now())
	wantOrder(t, got, "capped", "slower", "slowest")
	if c.pools["slowest"].body.slow.Load() {
		t.Error("slowest (1 MiB/s) judged slow against slower (3): only a hard-quota provider was 4x faster")
	}
}

// A slow primary gets its position back only once it clears the cut by
// bodyRecoverPercent.
func TestBodyScanOrderRecoveryMargin(t *testing.T) {
	providers := []config.UsenetProvider{
		{Host: "p1", Priority: 1, MaxConnections: 4},
		{Host: "p2", Priority: 2, MaxConnections: 4},
	}
	c := newBodyTestClient(t, providers)
	setBody(c, "p2", 40) // cut = min(4, 40/4) = 4 MiB/s

	setBody(c, "p1", 2)
	wantOrder(t, c.bodyScanOrder(time.Now()), "p2", "p1")
	setBody(c, "p1", 4.5) // over the cut, under cut x 1.25
	wantOrder(t, c.bodyScanOrder(time.Now()), "p2", "p1")
	setBody(c, "p1", 5.5)
	wantOrder(t, c.bodyScanOrder(time.Now()), "p1", "p2")
	setBody(c, "p1", 4.5) // back under 5, but not under the cut itself
	wantOrder(t, c.bodyScanOrder(time.Now()), "p1", "p2")
}

// A deferred primary with a stale sample stays behind the others, except for
// one explorer per bodyExploreAfter. One never judged slow isn't held back.
func TestBodyScanOrderStaleSampleExplorer(t *testing.T) {
	providers := []config.UsenetProvider{
		{Host: "p1", Priority: 1, MaxConnections: 4},
		{Host: "p2", Priority: 2, MaxConnections: 4},
	}
	c := newBodyTestClient(t, providers)
	now := time.Now()
	setBody(c, "p2", 40)
	setBody(c, "p1", 1)
	wantOrder(t, c.bodyScanOrder(now), "p2", "p1")

	stale := now.Add(-bodyExploreAfter).UnixNano()
	c.pools["p1"].body.sampledAt.Store(stale)
	wantOrder(t, c.bodyScanOrder(now), "p1", "p2") // the explorer
	wantOrder(t, c.bodyScanOrder(now), "p2", "p1")
	wantOrder(t, c.bodyScanOrder(now.Add(time.Minute)), "p2", "p1")
	later := now.Add(bodyExploreAfter)
	c.pools["p2"].body.sampledAt.Store(later.UnixNano())
	wantOrder(t, c.bodyScanOrder(later), "p1", "p2") // the next explorer

	// Stale but never slow: nothing to hold back.
	c.pools["p1"].body.slow.Store(false)
	wantOrder(t, c.bodyScanOrder(later.Add(time.Second)), "p1", "p2")
}

// Every primary last seen slow, every sample stale: no primary is faster to
// prefer, so the scan is priority order.
func TestBodyScanOrderEveryPrimaryDeferred(t *testing.T) {
	providers := []config.UsenetProvider{
		{Host: "p1", Priority: 1, MaxConnections: 4},
		{Host: "p2", Priority: 2, MaxConnections: 4},
		{Host: "b1", Priority: 3, MaxConnections: 4, Backup: true},
	}
	c := newBodyTestClient(t, providers)
	now := time.Now()
	for _, h := range []string{"p1", "p2"} {
		b := &c.pools[h].body
		b.slow.Store(true)
		b.exploredAt.Store(now.UnixNano())
	}
	got := c.bodyScanOrder(now)
	if &got[0] != &c.providers[0] {
		t.Fatalf("order = %v, want c.providers itself", bodyHosts(got))
	}
}

// End to end over fake NNTP servers: a body download records its provider's
// rate, a 430 records nothing, and the next fetch goes to the faster provider
// even though the slow one has the article and the higher priority.
func TestBodyRoutingOverConnections(t *testing.T) {
	data := bytes.Repeat([]byte("decypharr body routing "), (bodySampleMinBytes+1<<16)/23)
	wire := yencWire(data, 0)
	slow := startFakeNNTP(t, 0)
	slow.bodies = map[string]string{"seg@test": wire}
	fast := startFakeNNTP(t, 0)
	fast.bodies = map[string]string{"seg@test": wire}
	ps, pf := twoLocalProviders(t, slow, fast)
	providers := []config.UsenetProvider{ps, pf}
	c := newBodyTestClient(t, providers)

	for _, id := range []string{"seg@test", "missing@test"} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var out bytes.Buffer
		err := c.ExecuteWithFailover(ctx, func(conn *Connection) error {
			out.Reset()
			_, _, err := conn.StreamBodyMeta(id, &out)
			return err
		})
		cancel()
		if id == "seg@test" && err != nil {
			t.Fatalf("fetch: %v", err)
		}
	}
	if got := c.pools[ps.Host].body.samples.Load(); got != 1 {
		t.Fatalf("priority-1 provider samples = %d after one body and one 430, want 1", got)
	}
	if c.pools[pf.Host].body.samples.Load() != 0 {
		t.Fatal("the 430 fell through to the second provider and was recorded there")
	}
	if slow.bodyReqs.Load() != 2 || fast.bodyReqs.Load() != 1 {
		t.Fatalf("BODY requests slow=%d fast=%d, want 2 and 1", slow.bodyReqs.Load(), fast.bodyReqs.Load())
	}

	setBody(c, ps.Host, 1)
	setBody(c, pf.Host, 40)
	var out bytes.Buffer
	if err := fetchBody(c, "seg@test", &out); err != nil {
		t.Fatalf("fetch after routing: %v", err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Fatalf("got %d bytes, want %d", out.Len(), len(data))
	}
	if slow.bodyReqs.Load() != 2 || fast.bodyReqs.Load() != 2 {
		t.Fatalf("BODY requests slow=%d fast=%d, want the fetch on the fast provider (2 and 2)", slow.bodyReqs.Load(), fast.bodyReqs.Load())
	}
}
