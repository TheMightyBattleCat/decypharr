package manager

// The sweep's warm PAR2 pass (attemptWarmSweepRepair) runs only when every
// intact byte the solve needs is already in the local DFS cache, so the pass
// costs the PAR2 data and nothing else.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// p2eWarmCache is a DFS MountManager whose cache holds the posted files'
// real bytes. A dead article's range holds zeros, as the reader's padding
// left it. Ranges overlapping cold[file] are not cached.
type p2eWarmCache struct {
	mu    sync.Mutex
	files map[string][]byte
	dead  map[string][][2]int64
	cold  map[string][2]int64
	peeks int
}

func (c *p2eWarmCache) Start(context.Context) error                    { return nil }
func (c *p2eWarmCache) Stop() error                                    { return nil }
func (c *p2eWarmCache) Stats() map[string]any                          { return nil }
func (c *p2eWarmCache) IsReady() bool                                  { return true }
func (c *p2eWarmCache) Type() string                                   { return "test" }
func (c *p2eWarmCache) Refresh([]string) error                         { return nil }
func (c *p2eWarmCache) ForgetCachedRange(string, string, int64, int64) {}
func (c *p2eWarmCache) PeekCachedRange(_, file string, p []byte, off int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.peeks++
	data, ok := c.files[file]
	end := off + int64(len(p))
	if !ok || off < 0 || end > int64(len(data)) {
		return false
	}
	if r, ok := c.cold[file]; ok && off < r[1] && end > r[0] {
		return false
	}
	copy(p, data[off:end])
	for _, r := range c.dead[file] {
		for i := max(off, r[0]); i < min(end, r[1]); i++ {
			p[i-off] = 0
		}
	}
	return true
}

func newP2eWarmCache(fx *p2eFixture, dead []p2eSeg) *p2eWarmCache {
	c := &p2eWarmCache{files: map[string][]byte{}, dead: map[string][][2]int64{}, cold: map[string][2]int64{}}
	for _, f := range fx.posted {
		c.files[f.name] = f.data
	}
	for _, s := range dead {
		off := int64(s.idx * p2eArticleSize)
		c.dead[s.file.name] = append(c.dead[s.file.name], [2]int64{off, off + int64(len(s.file.payloads[s.idx]))})
	}
	return c
}

// warmHarness is a harness whose NZB marks the posted files stored (the
// cache source maps only stored files), with recorded damage gone from the
// server and the given cache mounted.
func warmHarness(t *testing.T, srv *p2eServer, fx *p2eFixture, nzbID string, cold func(c *p2eWarmCache)) (*p2eHarness, *p2eWarmCache) {
	t.Helper()
	nzb := fx.nzb(nzbID)
	for i := range nzb.Files {
		nzb.Files[i].IsStored = true
	}
	h := newP2eHarnessNZB(t, srv, fx, nzb)
	f1, f2 := fx.posted[0], fx.posted[1]
	h.recordDead(p2eSeg{f1, 1}, p2eSeg{f2, 3})
	for _, s := range h.dead {
		h.srv.set(s.file.ids[s.idx], p2eNotFound)
	}
	c := newP2eWarmCache(fx, h.dead)
	if cold != nil {
		cold(c)
	}
	h.p.manager.mountManager = c
	h.warmup()
	h.srv.resetCounters()
	return h, c
}

// Every intact slice is cached: the pass runs, patches the damage with the
// posted bytes, and fetches no posted article from the news server.
func TestP2EPar2WarmSweepRepairsFromTheCache(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	h, c := warmHarness(t, servers[0], fx, "p2e-warm", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	passed := h.p.attemptWarmSweepRepair(ctx, h.nzbID)
	h.p.wg.Wait()
	var o p2eOutcome
	h.collectOverlay(&o)
	if !passed {
		h.report(o)
		t.Fatalf("attemptWarmSweepRepair = false, want true (every intact slice is cached)")
	}
	if o.Patched != 2 || o.StillDead != 0 || o.Mismatch != 0 {
		t.Errorf("overlay %s %v, want 2 patches equal to the posted bytes", o.Overlay, o.OverlayDiff)
	}
	if c.peeks == 0 {
		t.Errorf("the cache was never read")
	}
	// Allowed: the recorded-dead articles (asked once more before parity),
	// and the first article of each posted file whose length another file
	// shares: the two 600 KiB files are told apart by the MD5 of their first
	// 16 KiB, which both the preflight and the pass fetch from the server.
	allowed := map[string]bool{}
	for _, s := range h.dead {
		allowed[s.file.ids[s.idx]] = true
	}
	for _, f := range fx.posted {
		for _, g := range fx.posted {
			if f != g && len(f.data) == len(g.data) {
				allowed[f.ids[0]] = true
			}
		}
	}
	for _, f := range fx.posted {
		for _, id := range f.ids {
			if n := h.srv.bodyCount(id); n > 0 && !allowed[id] {
				t.Errorf("intact article %s fetched %d time(s) from the server, want none (it is cached)", id, n)
			}
		}
	}
	if a, _ := h.p.manager.storage.LatestPar2RepairAttemptForNzb(h.nzbID); a == nil || a.Outcome != storage.Par2RepairOutcomeCompleted {
		t.Errorf("attempt %+v, want completed", a)
	}
}

// One intact article of the third file is not cached: the pass would have
// to fetch it, so the preflight declines and no pass runs; the sweep's own
// re-grab handles the file.
func TestP2EPar2WarmSweepDeclinesWhenNotFullyCached(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	f3 := fx.posted[2]
	h, _ := warmHarness(t, servers[0], fx, "p2e-warm-cold", func(c *p2eWarmCache) {
		c.cold[f3.name] = [2]int64{0, 1}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	passed := h.p.attemptWarmSweepRepair(ctx, h.nzbID)
	h.p.wg.Wait()
	if passed {
		t.Fatalf("attemptWarmSweepRepair = true with %s's first article not cached, want false", short(f3.name))
	}
	if h.p.IsRunning(h.nzbID) || h.p.IsQueued(h.nzbID) {
		t.Errorf("a pass is running or queued after the preflight declined")
	}
	if a, _ := h.p.manager.storage.LatestPar2RepairAttemptForNzb(h.nzbID); a != nil {
		t.Errorf("attempt %+v recorded, want no pass run", a)
	}
	var o p2eOutcome
	h.collectOverlay(&o)
	if o.Patched != 0 {
		t.Errorf("%d segment(s) patched, want none: %v", o.Patched, o.OverlayDiff)
	}
	if !h.logged("par2 warm sweep pass declined; the sweep re-grabs") {
		t.Errorf("no decline log line")
	}
	for _, ln := range h.logs.lines() {
		if ln["message"] == "par2 warm sweep pass declined; the sweep re-grabs" && ln["stage"] != "intact data not fully cached" {
			t.Errorf("declined at stage %v, want \"intact data not fully cached\"", ln["stage"])
		}
	}
}
