package nntp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
)

// batchStatWithin runs BatchStat on a goroutine and fails the test if it has
// not returned within limit, so a stalled call fails fast instead of holding
// the test for the pool's re-test wait (or for ever).
func batchStatWithin(t *testing.T, c *Client, ctx context.Context, msgIDs []string, limit time.Duration) (*BatchStatResult, time.Duration) {
	t.Helper()
	type out struct {
		res *BatchStatResult
		err error
	}
	ch := make(chan out, 1)
	start := time.Now()
	go func() {
		res, err := c.BatchStat(ctx, msgIDs)
		ch <- out{res, err}
	}()
	select {
	case o := <-ch:
		if o.err != nil {
			t.Fatalf("BatchStat: %v", o.err)
		}
		return o.res, time.Since(start)
	case <-time.After(limit):
		t.Fatalf("BatchStat still running after %v", limit)
		return nil, 0
	}
}

// After a start, a first STAT call whose every article is missing leaves the
// provider with no latency sample. That must not hold the pool: the next call
// for a live article is answered at once, well inside its 2 s deadline (it
// used to wait 60 s for the next explorer turn, past its deadline, and come
// back with the deadline error).
func TestBatchStatAfterAllMissFirstCallIsAnsweredAtOnce(t *testing.T) {
	dead := ids("dead", 5)
	srv := startFakeNNTP(t, 0, dead...)
	c, p := pipelineTestClient(t, srv)

	ctxA, cancelA := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelA()
	res, _ := batchStatWithin(t, c, ctxA, dead, 5*time.Second)
	if res.FoundCount != 0 || res.ErrorCount != 0 {
		t.Fatalf("all-miss call: found %d, errors %d, want 0 and 0 (all definitively missing)", res.FoundCount, res.ErrorCount)
	}
	if got := c.pools[p.Host].stat.nsPerStat.Load(); got != 0 {
		t.Fatalf("all-miss call recorded latency %v; the scenario needs an unmeasured provider", time.Duration(got))
	}

	for _, name := range []string{"first", "second"} {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		res, took := batchStatWithin(t, c, ctx, []string{"live-" + name + "@test"}, 3*time.Second)
		cancel()
		if res.FoundCount != 1 || !res.Results[0].Available {
			t.Fatalf("%s live call after an all-miss start: found %d, err %v; want the article found", name, res.FoundCount, res.Results[0].Error)
		}
		if took > time.Second {
			t.Fatalf("%s live call took %v, want it answered at once", name, took)
		}
	}
}

// A STAT call returns when its context ends, even with its chunks still
// queued (no worker free to take them) or still running on a slow provider.
// The unanswered articles carry the context's error and count as errors, not
// as missing, and a chunk that finishes after the return does not change the
// result already handed back.
func TestBatchStatReturnsWhenItsContextEnds(t *testing.T) {
	t.Run("queued", func(t *testing.T) {
		// The only home is over its hard quota: no worker takes the chunk.
		srv := startFakeNNTP(t, 0)
		p := config.UsenetProvider{Host: "127.0.0.1", Port: srv.port(), Priority: 1, MaxConnections: 2}
		c := &Client{pools: map[string]*ProviderPool{}, providers: []config.UsenetProvider{p}, logger: zerolog.Nop()}
		c.pools[p.Host] = &ProviderPool{slots: make(chan struct{}, p.MaxConnections), max: p.MaxConnections, config: p}
		c.bw = quotaTracker(p.Host, 100, 100, 10)
		c.statHomes = c.statHomePools()
		c.repairPool = c.newRepairPool(100)
		t.Cleanup(func() { _ = c.Close() })

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		msgIDs := ids("queued", 3)
		res, took := batchStatWithin(t, c, ctx, msgIDs, 3*time.Second)
		if took > time.Second {
			t.Fatalf("returned after %v, want close to the 300ms deadline", took)
		}
		assertAllDeadline(t, res, len(msgIDs))
		if got := srv.stats.Load(); got != 0 {
			t.Fatalf("hard-quota provider served %d STATs", got)
		}
	})

	t.Run("running", func(t *testing.T) {
		srv := startFakeNNTP(t, 2*time.Second)
		c, _ := pipelineTestClient(t, srv)

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		msgIDs := ids("slow", 3)
		res, took := batchStatWithin(t, c, ctx, msgIDs, 3*time.Second)
		if took > time.Second {
			t.Fatalf("returned after %v, want close to the 300ms deadline", took)
		}
		assertAllDeadline(t, res, len(msgIDs))
		// The running chunk ends once the call is over (its context is
		// cancelled on return); its callback must not write over the result.
		time.Sleep(300 * time.Millisecond)
		assertAllDeadline(t, res, len(msgIDs))
	})
}

func assertAllDeadline(t *testing.T, res *BatchStatResult, n int) {
	t.Helper()
	if res.TotalCount != n || res.FoundCount != 0 || res.ErrorCount != n {
		t.Fatalf("total %d found %d errors %d, want %d, 0, %d (deadline is an error, not a miss)",
			res.TotalCount, res.FoundCount, res.ErrorCount, n, n)
	}
	for i, r := range res.Results {
		if r.Available || !errors.Is(r.Error, context.DeadlineExceeded) {
			t.Fatalf("result %d: available %v, err %v; want the caller's deadline error", i, r.Available, r.Error)
		}
	}
}

// An explorer that draws a task whose caller has already gone measures
// nothing, so its turn must neither count as a re-test nor double the wait.
func TestRepairPoolExpiredTaskLeavesExplorerTurn(t *testing.T) {
	providers := []config.UsenetProvider{
		{Host: "fast", Priority: 1, MaxConnections: 4},
		{Host: "slow", Priority: 2, MaxConnections: 4},
	}
	c := &Client{pools: map[string]*ProviderPool{}, providers: providers, logger: zerolog.Nop()}
	for _, p := range providers {
		c.pools[p.Host] = &ProviderPool{slots: make(chan struct{}, p.MaxConnections), max: p.MaxConnections, config: p}
	}
	c.statHomes = c.statHomePools()
	setLatency(c, "fast", 10*time.Millisecond)
	slow := &c.pools["slow"].stat
	slow.nsPerStat.Store(int64(200 * time.Millisecond))
	sampledAt := time.Now().Add(-statExploreAfter - time.Second).UnixNano()
	slow.sampledAt.Store(sampledAt)

	// One worker homed on slow, with an already-expired task waiting for it.
	pool := &RepairPool{tasks: make(chan repairTask, 1), workers: 1, quit: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	pool.tasks <- repairTask{ctx: ctx, msgIDs: []string{"x@test"}, done: func(_ []StatResult, err error) { done <- err }}
	pool.wg.Add(1)
	go pool.worker(c, c.pools["slow"])

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expired task answered with %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("explorer never took the expired task")
	}
	pool.Stop() // the worker's post-task bookkeeping has finished once Stop returns

	if got := slow.retestWait(); got != statExploreAfter {
		t.Fatalf("re-test wait = %v after an expired task, want it unchanged at %v", got, statExploreAfter)
	}
	if got := slow.sampledAt.Load(); got != sampledAt {
		t.Fatal("expired task changed the provider's sample time")
	}
}
