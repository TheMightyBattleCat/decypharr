package nntp

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
)

// TestMain starts the cached clock main starts in production: connection
// deadlines come from utils.Now(), which is frozen at process start
// otherwise, so every dial after the first HandshakeTimeout of the test
// binary's life would time out at once.
func TestMain(m *testing.M) {
	utils.StartGlobalCachedTime()
	os.Exit(m.Run())
}

// fakeNNTP is a plain-TCP NNTP server that answers STAT after a fixed delay.
type fakeNNTP struct {
	ln        net.Listener
	delay     time.Duration   // before a 223
	missDelay time.Duration   // before a 430 (real providers answer misses far slower)
	missing   map[string]bool // message IDs (without brackets) answered 430
	dropAfter int64           // close each connection after this many STATs (0: never)
	stats     atomic.Int64
	wg        sync.WaitGroup
}

func startFakeNNTP(t *testing.T, delay time.Duration, missing ...string) *fakeNNTP {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fakeNNTP{ln: ln, delay: delay, missing: map[string]bool{}}
	for _, id := range missing {
		s.missing[id] = true
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.wg.Add(1)
			go s.serve(conn)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
	})
	return s
}

func (s *fakeNNTP) port() int { return s.ln.Addr().(*net.TCPAddr).Port }

func (s *fakeNNTP) serve(conn net.Conn) {
	defer s.wg.Done()
	defer func() { _ = conn.Close() }()
	w := bufio.NewWriter(conn)
	r := bufio.NewReader(conn)
	_, _ = w.WriteString("200 fake ready\r\n")
	_ = w.Flush()
	var served int64
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case strings.HasPrefix(line, "STAT "):
			if s.dropAfter > 0 && served >= s.dropAfter {
				return
			}
			served++
			s.stats.Add(1)
			id := strings.Trim(strings.TrimPrefix(line, "STAT "), "<>")
			if s.missing[id] {
				time.Sleep(s.missDelay)
				_, _ = w.WriteString("430 no such article\r\n")
			} else {
				time.Sleep(s.delay)
				_, _ = w.WriteString("223 0 <" + id + ">\r\n")
			}
		case line == "QUIT":
			_, _ = w.WriteString("205 bye\r\n")
			_ = w.Flush()
			return
		default:
			_, _ = w.WriteString("500 unknown\r\n")
		}
		_ = w.Flush()
	}
}

// newStatTestClient builds a Client over providers without NewClient's
// bandwidth tracker and reaper.
func newStatTestClient(t *testing.T, providers []config.UsenetProvider, percent int) *Client {
	t.Helper()
	c := &Client{
		pools:     map[string]*ProviderPool{},
		providers: providers,
		logger:    zerolog.Nop(),
	}
	for _, p := range providers {
		c.pools[p.Host] = &ProviderPool{
			slots:  make(chan struct{}, p.MaxConnections),
			max:    p.MaxConnections,
			config: p,
		}
	}
	c.statHomes = c.statHomePools()
	c.repairPool = c.newRepairPool(percent)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func setLatency(c *Client, host string, d time.Duration) {
	pp := c.pools[host]
	pp.stat.nsPerStat.Store(int64(d))
	pp.stat.sampledAt.Store(time.Now().UnixNano())
}

func ids(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%d@test", prefix, i)
	}
	return out
}

func TestStatLatencyRecord(t *testing.T) {
	var s statLatency
	now := time.Now()

	s.record(100*time.Millisecond, now)
	if got := time.Duration(s.nsPerStat.Load()); got != 100*time.Millisecond {
		t.Fatalf("first sample: got %v, want 100ms", got)
	}

	s.record(50*time.Millisecond, now.Add(time.Second))
	if got := time.Duration(s.nsPerStat.Load()); got != 90*time.Millisecond {
		t.Fatalf("blended sample: got %v, want 90ms (1/%d of the way to 50ms)", got, statEWMADivisor)
	}

	// A sample after a gap of statExploreAfter replaces the stale average:
	// a provider that recovered must not take many samples to look fast again.
	s.record(10*time.Millisecond, now.Add(time.Second+statExploreAfter))
	if got := time.Duration(s.nsPerStat.Load()); got != 10*time.Millisecond {
		t.Fatalf("stale sample: got %v, want 10ms", got)
	}
}

func TestStatEligible(t *testing.T) {
	providers := []config.UsenetProvider{
		{Host: "slow", Priority: 1, MaxConnections: 10},
		{Host: "fast", Priority: 2, MaxConnections: 10},
		{Host: "mid", Priority: 3, MaxConnections: 10},
		{Host: "new", Priority: 4, MaxConnections: 10},
		{Host: "backup", Priority: 5, MaxConnections: 10, Backup: true},
	}
	c := newStatTestClient(t, providers, 80)
	setLatency(c, "slow", 125*time.Millisecond)
	setLatency(c, "fast", 12*time.Millisecond)
	setLatency(c, "mid", 30*time.Millisecond)
	setLatency(c, "backup", time.Microsecond) // not a home: must not set the cutoff

	for host, want := range map[string]bool{"slow": false, "fast": true, "mid": true, "new": false} {
		if ok, _, _ := c.statEligible(c.pools[host]); ok != want {
			t.Errorf("%s eligible = %v, want %v", host, ok, want)
		}
	}

	// One lucky sample on the fastest home can't shrink the eligible set below
	// statSlowFactor × statLatencyFloor.
	setLatency(c, "fast", time.Millisecond)
	if ok, _, _ := c.statEligible(c.pools["mid"]); !ok {
		t.Errorf("mid (30ms) excluded after a 1ms sample on fast; the floor should keep it")
	}

	// Every home slow at once (e.g. a network blip): all stay eligible rather
	// than none.
	setLatency(c, "fast", 900*time.Millisecond)
	setLatency(c, "mid", time.Second)
	setLatency(c, "slow", 950*time.Millisecond)
	for _, host := range []string{"slow", "fast", "mid"} {
		if ok, _, _ := c.statEligible(c.pools[host]); !ok {
			t.Errorf("%s excluded while every home is equally slow", host)
		}
	}
}

func TestStatTryExplore(t *testing.T) {
	var s statLatency
	now := time.Now()
	if !s.tryExplore(now) {
		t.Fatal("unmeasured provider: first explorer should be allowed")
	}
	if s.tryExplore(now) {
		t.Fatal("second explorer allowed while the first holds the role")
	}
	s.exploring.Store(false)

	s.record(125*time.Millisecond, now)
	if s.tryExplore(now.Add(time.Minute)) {
		t.Fatal("explorer allowed with a fresh sample")
	}
	if !s.tryExplore(now.Add(statExploreAfter)) {
		t.Fatal("explorer refused once the sample is statExploreAfter old")
	}
}

// statOrder must always hold every provider exactly once, whatever the home:
// that is what keeps "missing on every provider" meaning every provider.
func TestStatOrderHoldsEveryProvider(t *testing.T) {
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
	setLatency(c, "F", time.Millisecond)
	// D unmeasured.

	got := hosts(c.statOrder(providers[4])) // home E
	if want := "E C A D B F"; got != want {
		t.Errorf("order from E = %q, want %q", got, want)
	}
	for _, home := range providers {
		order := c.statOrder(home)
		if order[0].Host != home.Host {
			t.Errorf("home %s not first: %q", home.Host, hosts(order))
		}
		seen := map[string]int{}
		for _, p := range order {
			seen[p.Host]++
		}
		if len(order) != len(providers) || len(seen) != len(providers) {
			t.Errorf("home %s: order %q does not hold every provider once", home.Host, hosts(order))
		}
	}
}

func hosts(ps []config.UsenetProvider) string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.Host
	}
	return strings.Join(out, " ")
}

func TestBatchStatOnProviderRecordsLatency(t *testing.T) {
	srv := startFakeNNTP(t, 20*time.Millisecond, "gone@test")
	p := config.UsenetProvider{Host: "127.0.0.1", Port: srv.port(), Priority: 1, MaxConnections: 2}
	c := newStatTestClient(t, []config.UsenetProvider{p}, 100)

	res, err := c.batchStatOnProvider(context.Background(), p, []string{"a@test", "gone@test", "b@test"})
	if err != nil {
		t.Fatalf("batchStatOnProvider: %v", err)
	}
	if !res[0].Available || res[1].Available || !res[2].Available {
		t.Fatalf("availability = %v %v %v, want true false true", res[0].Available, res[1].Available, res[2].Available)
	}
	if !IsArticleNotFoundError(res[1].Error) {
		t.Fatalf("missing article error = %v, want article-not-found", res[1].Error)
	}
	got := time.Duration(c.pools[p.Host].stat.nsPerStat.Load())
	if got < 20*time.Millisecond || got > 500*time.Millisecond {
		t.Fatalf("recorded latency %v, want about 20ms per STAT", got)
	}
}

func TestBatchStatOnProviderErrorPenalty(t *testing.T) {
	// Connection refused.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	dead := config.UsenetProvider{Host: "127.0.0.1", Port: port, Priority: 1, MaxConnections: 2}
	c := newStatTestClient(t, []config.UsenetProvider{dead}, 100)
	if _, err := c.batchStatOnProvider(context.Background(), dead, []string{"a@test"}); err == nil {
		t.Fatal("expected a connection error")
	}
	if got := time.Duration(c.pools[dead.Host].stat.nsPerStat.Load()); got != statErrorPenalty {
		t.Fatalf("refused connection recorded %v, want %v", got, statErrorPenalty)
	}

	// Connection dropped mid-chunk.
	srv := startFakeNNTP(t, 0)
	srv.dropAfter = 2
	drop := config.UsenetProvider{Host: "127.0.0.1", Port: srv.port(), Priority: 1, MaxConnections: 2}
	c2 := newStatTestClient(t, []config.UsenetProvider{drop}, 100)
	if _, err := c2.batchStatOnProvider(context.Background(), drop, []string{"a@test", "b@test", "c@test", "d@test"}); err == nil {
		t.Fatal("expected a mid-chunk connection error")
	}
	if got := time.Duration(c2.pools[drop.Host].stat.nsPerStat.Load()); got != statErrorPenalty {
		t.Fatalf("dropped connection recorded %v, want %v", got, statErrorPenalty)
	}
}

// twoLocalProviders returns two providers on distinct pool keys that both
// reach local fake servers ("127.0.0.1" and "localhost"), skipping the test
// where localhost doesn't resolve to the IPv4 loopback.
func twoLocalProviders(t *testing.T, a, b *fakeNNTP) (config.UsenetProvider, config.UsenetProvider) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("localhost", strconv.Itoa(b.port())), time.Second)
	if err != nil {
		t.Skipf("localhost does not reach 127.0.0.1: %v", err)
	}
	_ = conn.Close()
	// Let the probe connection's server goroutine see EOF before counting.
	time.Sleep(20 * time.Millisecond)
	pa := config.UsenetProvider{Host: "127.0.0.1", Port: a.port(), Priority: 1, MaxConnections: 4}
	pb := config.UsenetProvider{Host: "localhost", Port: b.port(), Priority: 2, MaxConnections: 4}
	return pa, pb
}

// Whatever the home, an ID missing on it but present elsewhere is available,
// and one missing everywhere is a definitive not-found.
func TestBatchStatAcrossProvidersAsksEveryProvider(t *testing.T) {
	a := startFakeNNTP(t, 0, "only-b@test", "nowhere@test")
	b := startFakeNNTP(t, 0, "only-a@test", "nowhere@test")
	pa, pb := twoLocalProviders(t, a, b)
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)

	msgIDs := []string{"only-a@test", "only-b@test", "nowhere@test", "both@test"}
	for _, home := range []config.UsenetProvider{pa, pb} {
		res, err := c.batchStatAcrossProviders(context.Background(), msgIDs, home)
		if err != nil {
			t.Fatalf("home %s: %v", home.Host, err)
		}
		if !res[0].Available || !res[1].Available || !res[3].Available {
			t.Errorf("home %s: an article present on one provider reported unavailable: %+v", home.Host, res)
		}
		if res[2].Available || !IsArticleNotFoundError(res[2].Error) {
			t.Errorf("home %s: article missing everywhere = %+v, want article-not-found", home.Host, res[2])
		}
	}
}

// With one slow and one fast provider, the pool sends at most the slow
// provider's single explorer chunk there, then nothing while the sample is
// fresh; the fast provider takes the rest.
func TestRepairPoolRoutesAwayFromSlowProvider(t *testing.T) {
	slow := startFakeNNTP(t, 80*time.Millisecond)
	fast := startFakeNNTP(t, 0)
	ps, pf := twoLocalProviders(t, slow, fast)
	c := newStatTestClient(t, []config.UsenetProvider{ps, pf}, 100)

	msgIDs := ids("first", 400)
	batch := pickStatBatchSize(len(msgIDs), c.repairPool.Capacity(), 50, 10)
	res, err := c.BatchStat(context.Background(), msgIDs)
	if err != nil || res.FoundCount != len(msgIDs) {
		t.Fatalf("first BatchStat: found %d/%d, err %v", res.FoundCount, len(msgIDs), err)
	}
	if got := slow.stats.Load(); got > int64(batch) {
		t.Fatalf("slow provider served %d STATs on the first call, want at most one chunk (%d)", got, batch)
	}
	if ok, _, _ := c.statEligible(c.pools[ps.Host]); ok {
		t.Fatalf("slow provider still eligible after being measured")
	}

	before := slow.stats.Load()
	res, err = c.BatchStat(context.Background(), ids("second", 400))
	if err != nil || res.FoundCount != 400 {
		t.Fatalf("second BatchStat: found %d/400, err %v", res.FoundCount, err)
	}
	if got := slow.stats.Load() - before; got != 0 {
		t.Fatalf("slow provider served %d STATs on the second call, want 0", got)
	}
}

// Misses take real providers 40-100x longer to answer than hits; only hits may
// feed the latency, or every fall-through chunk makes a fast provider look slow.
func TestBatchStatOnProviderTimesOnlyHits(t *testing.T) {
	srv := startFakeNNTP(t, 0, "gone1@test", "gone2@test", "gone3@test")
	srv.missDelay = 150 * time.Millisecond
	p := config.UsenetProvider{Host: "127.0.0.1", Port: srv.port(), Priority: 1, MaxConnections: 2}
	c := newStatTestClient(t, []config.UsenetProvider{p}, 100)
	st := &c.pools[p.Host].stat

	if _, err := c.batchStatOnProvider(context.Background(), p, []string{"a@test", "gone1@test", "gone2@test", "b@test"}); err != nil {
		t.Fatalf("batchStatOnProvider: %v", err)
	}
	if got := time.Duration(st.nsPerStat.Load()); got <= 0 || got > 40*time.Millisecond {
		t.Fatalf("recorded latency %v with two slow misses in the chunk, want the hits' time only", got)
	}

	// A chunk of nothing but misses leaves the average alone and still counts
	// as a fresh look, so no explorer is sent straight back. No repair pool
	// here: its own explorer would hold the role and hide the answer.
	fresh := &Client{pools: map[string]*ProviderPool{}, providers: []config.UsenetProvider{p}, logger: zerolog.Nop()}
	fresh.pools[p.Host] = &ProviderPool{slots: make(chan struct{}, p.MaxConnections), max: p.MaxConnections, config: p}
	t.Cleanup(func() { _ = fresh.Close() })
	fst := &fresh.pools[p.Host].stat
	if _, err := fresh.batchStatOnProvider(context.Background(), p, []string{"gone3@test"}); err != nil {
		t.Fatalf("batchStatOnProvider: %v", err)
	}
	if fst.nsPerStat.Load() != 0 {
		t.Fatalf("all-miss chunk recorded latency %v, want none", time.Duration(fst.nsPerStat.Load()))
	}
	if fst.tryExplore(time.Now()) {
		t.Fatal("explorer allowed straight after an all-miss chunk")
	}
}

// quotaTracker returns a bandwidth tracker (no file, no saver) with one
// provider at used/limit bytes in the current daily window.
func quotaTracker(host string, used, limit, reserve int64) *BandwidthTracker {
	q := providerQuota{limitBytes: limit, reserveBytes: reserve, period: "day"}
	bp := &bwProvider{quota: q}
	bp.used.Store(used)
	bp.periodStart.Store(currentWindowStart(time.Now(), q).UnixNano())
	return &BandwidthTracker{byHost: map[string]*bwProvider{host: bp}, stop: make(chan struct{}), logger: zerolog.Nop()}
}

func TestStatEligibleHardQuota(t *testing.T) {
	providers := []config.UsenetProvider{
		{Host: "capped", Priority: 1, MaxConnections: 10},
		{Host: "mid", Priority: 2, MaxConnections: 10},
		{Host: "slower", Priority: 3, MaxConnections: 10},
	}
	// No repair pool: this test swaps c.bw, which running workers read.
	c := &Client{pools: map[string]*ProviderPool{}, providers: providers, logger: zerolog.Nop()}
	for _, p := range providers {
		c.pools[p.Host] = &ProviderPool{slots: make(chan struct{}, p.MaxConnections), max: p.MaxConnections, config: p}
	}
	c.statHomes = c.statHomePools()
	setLatency(c, "capped", 5*time.Millisecond)
	setLatency(c, "mid", 30*time.Millisecond)
	setLatency(c, "slower", 100*time.Millisecond)

	// Reserve band (used past limit-reserve, below limit): still a STAT home.
	c.bw = quotaTracker("capped", 95, 100, 10)
	if ok, _, _ := c.statEligible(c.pools["capped"]); !ok {
		t.Error("provider in its reserve band excluded; only a hard-quota block should exclude it")
	}
	if ok, _, _ := c.statEligible(c.pools["slower"]); ok {
		t.Error("slower (100ms) eligible while capped (5ms) sets the cutoff at 40ms")
	}

	// Hard quota: not eligible, and no longer the fastest the others are cut
	// against (100ms is within 4x of mid's 30ms).
	c.bw = quotaTracker("capped", 100, 100, 10)
	if ok, _, _ := c.statEligible(c.pools["capped"]); ok {
		t.Error("provider over its hard quota still eligible")
	}
	if ok, _, _ := c.statEligible(c.pools["slower"]); !ok {
		t.Error("a hard-quota provider still sets the cutoff for the others")
	}
}

// A home over its hard quota sends no explorer: the pool gives it no chunks.
func TestRepairPoolSkipsHardQuotaHome(t *testing.T) {
	capped := startFakeNNTP(t, 0)
	open := startFakeNNTP(t, 0)
	pc, po := twoLocalProviders(t, capped, open)
	c := &Client{pools: map[string]*ProviderPool{}, providers: []config.UsenetProvider{pc, po}, logger: zerolog.Nop()}
	for _, p := range c.providers {
		c.pools[p.Host] = &ProviderPool{slots: make(chan struct{}, p.MaxConnections), max: p.MaxConnections, config: p}
	}
	c.bw = quotaTracker(pc.Host, 100, 100, 10)
	c.statHomes = c.statHomePools()
	c.repairPool = c.newRepairPool(100)
	t.Cleanup(func() { _ = c.Close() })

	res, err := c.BatchStat(context.Background(), ids("quota", 400))
	if err != nil || res.FoundCount != 400 {
		t.Fatalf("BatchStat: found %d/400, err %v", res.FoundCount, err)
	}
	if got := capped.stats.Load(); got != 0 {
		t.Fatalf("hard-quota provider served %d STATs, want 0", got)
	}
}
