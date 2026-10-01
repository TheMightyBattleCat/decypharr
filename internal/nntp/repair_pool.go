package nntp

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// repairDefaultPercent is used when cfg.Repair.NNTPConnectionPercent is unset.
const repairDefaultPercent = 20

// repairPoolMinWorkers is the floor for the worker count. Even with no
// repair-mode budget configured (BatchStat called by non-repair paths) we
// keep a small pool around so the caller doesn't have to branch.
const repairPoolMinWorkers = 2

// RepairPool is a process-wide worker pool for BatchStat chunks. It
// replaces the previous RepairBank counting-semaphore design where each
// BatchStat call spawned its own pool sized to bank.Capacity — under M
// concurrent BatchStat calls that produced M × bank.Capacity goroutines
// all racing for the same bank.Capacity tokens, with most blocked.
//
// The new shape: one shared pool sized to bank.Capacity, BatchStat
// submits chunks onto the shared task channel, workers pull them off in
// FIFO order. Total goroutines is exactly len(workers), period, no
// matter how many BatchStat calls run concurrently.
type RepairPool struct {
	tasks   chan repairTask
	workers int
	wg      sync.WaitGroup
	quit    chan struct{}
	once    sync.Once
}

// repairTask describes one chunk of work the pool will execute. done is
// invoked exactly once — either with the per-chunk results (on success or
// connection error from batchStatAcrossProviders) or with a non-nil err
// when the caller's context expires before a worker picks the task up.
type repairTask struct {
	ctx    context.Context
	msgIDs []string
	hint   *statHint // shared by the chunks of one BatchStat call; may be nil
	done   func(results []StatResult, err error)
}

// errRepairPoolClosed is returned by Submit after Stop has been called.
var errRepairPoolClosed = errors.New("repair pool closed")

// newRepairPool starts the worker pool. Each STAT home (every primary
// provider, see statHomePools) gets `percent` of its own connections as
// workers, so the pool never holds more than that share of any one provider
// and leaves the rest for playback. Workers take chunks only while their home
// answers STAT fast enough (see stat_routing.go).
func (c *Client) newRepairPool(percent int) *RepairPool {
	if c == nil {
		return nil
	}
	if percent <= 0 {
		percent = repairDefaultPercent
	}
	if percent > 100 {
		percent = 100
	}
	type homeWorkers struct {
		home *ProviderPool
		n    int
	}
	var plan []homeWorkers
	capacity := 0
	for _, pp := range c.statHomes {
		n := max((pp.max*percent+99)/100, 1)
		plan = append(plan, homeWorkers{pp, n})
		capacity += n
	}
	if capacity == 0 {
		// No provider with connections: keep a couple of homeless workers so
		// callers don't have to branch. They ask providers in priority order.
		capacity = repairPoolMinWorkers
		plan = []homeWorkers{{nil, capacity}}
	}

	p := &RepairPool{
		// Buffered = capacity so a burst of chunk submissions from a single
		// BatchStat doesn't immediately block on the first send. Once the
		// buffer fills, the submitter naturally backpressures.
		tasks:   make(chan repairTask, capacity),
		workers: capacity,
		quit:    make(chan struct{}),
	}
	p.wg.Add(capacity)
	for _, h := range plan {
		for range h.n {
			go p.worker(c, h.home)
		}
	}
	return p
}

// pickStatBatchSize picks the per-chunk batch size for a BatchStat call.
//
// Default is the ceiling (statBatchSize). For small inputs vs. the
// available worker pool, we shrink the chunk size so a single call can
// keep the whole pool busy — otherwise a 100-ID call against a 30-worker
// pool would only fan out to 2 chunks (chunks=ceil(100/50)) and leave
// 28 workers idle. Floors at minSize so the per-chunk overhead doesn't
// dominate when input is tiny.
//
// overcommit > 1 means we aim for more chunks than workers so a worker
// finishing a fast chunk has a queued one waiting rather than idling
// while the rest of the pool finishes slower chunks.
func pickStatBatchSize(totalIDs, workers, ceilSize, minSize int) int {
	if workers <= 0 || totalIDs <= 0 {
		return ceilSize
	}
	const overcommit = 3
	want := (totalIDs + workers*overcommit - 1) / (workers * overcommit)
	if want >= ceilSize {
		return ceilSize
	}
	if want < minSize {
		return minSize
	}
	return want
}

// TotalConnections is the sum of MaxConnections across configured providers.
func (c *Client) TotalConnections() int {
	if c == nil {
		return 0
	}
	total := 0
	for _, p := range c.providers {
		if p.MaxConnections > 0 {
			total += p.MaxConnections
		}
	}
	return total
}

// Capacity returns the number of workers in the pool, including those whose
// home is currently too slow to take chunks. pickStatBatchSize sizes chunks
// from it; counting idle workers only makes chunks smaller (never below
// statBatchMinSize), which leaves the eligible workers more to take, not less.
func (p *RepairPool) Capacity() int {
	if p == nil {
		return 0
	}
	return p.workers
}

// Submit hands a chunk to the pool. hint is the call's shared statHint (nil
// for none). done is invoked on a worker goroutine
// exactly once when the chunk has been processed (or rejected). Returns
// an error and does NOT call done when the caller's ctx expires before a
// worker takes the task, or when the pool has been stopped.
func (p *RepairPool) Submit(ctx context.Context, msgIDs []string, hint *statHint, done func([]StatResult, error)) error {
	if p == nil {
		return errRepairPoolClosed
	}
	task := repairTask{ctx: ctx, msgIDs: msgIDs, hint: hint, done: done}
	// quit takes priority: once Stop closes it, refuse new work even if
	// the buffered tasks channel still has room.
	select {
	case <-p.quit:
		return errRepairPoolClosed
	default:
	}
	select {
	case p.tasks <- task:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.quit:
		return errRepairPoolClosed
	}
}

// Stop signals workers to drain and exit. Blocks until all in-flight
// chunks finish. Pending tasks still in the channel are abandoned —
// their done callbacks never fire, which is acceptable because Stop is
// only called during Client teardown when callers' contexts are already
// being cancelled upstream.
func (p *RepairPool) Stop() {
	if p == nil {
		return
	}
	p.once.Do(func() { close(p.quit) })
	p.wg.Wait()
}

// worker pulls chunks until the pool stops. A worker with a home takes chunks
// only while that home is STAT-eligible; otherwise it waits, except for the
// one explorer per home that takes a chunk to re-measure it (see tryExplore) -
// none for a home over its hard quota. The pool's worker count is the
// concurrency cap; no token accounting.
func (p *RepairPool) worker(c *Client, home *ProviderPool) {
	defer p.wg.Done()
	for {
		exploring := false
		if home != nil {
			ok, lat, fastest := c.statEligible(home)
			blocked := !ok && c.statHomeBlocked(home)
			if home.stat.eligible.Swap(ok) != ok {
				msg := "STAT routing: provider is fast enough, taking BatchStat chunks"
				switch {
				case blocked:
					msg = "STAT routing: provider is over its hard bandwidth quota, no longer taking BatchStat chunks"
				case !ok:
					msg = "STAT routing: provider is too slow, no longer taking BatchStat chunks"
				}
				c.logger.Debug().Str("provider", home.config.Host).Dur("stat_latency", lat).
					Dur("fastest", fastest).Msg(msg)
			}
			if !ok {
				if blocked || !home.stat.tryExplore(time.Now()) {
					select {
					case <-p.quit:
						return
					case <-time.After(statIneligibleRecheck):
					}
					continue
				}
				exploring = true
			}
		}
		select {
		case <-p.quit:
			if exploring {
				home.stat.exploring.Store(false)
			}
			return
		case t := <-p.tasks:
			p.run(c, home, t)
			if exploring {
				home.stat.exploring.Store(false)
			}
		}
	}
}

// run processes one chunk for a worker homed on home (nil: the priority-1
// provider).
func (p *RepairPool) run(c *Client, home *ProviderPool, t repairTask) {
	if t.done == nil {
		return
	}
	// Caller may have cancelled while we were waiting. Surface it as the
	// task's error without doing the work.
	if err := t.ctx.Err(); err != nil {
		t.done(nil, err)
		return
	}
	var homeCfg config.UsenetProvider
	if home != nil {
		homeCfg = home.config
	} else if len(c.providers) > 0 {
		homeCfg = c.providers[0]
	}
	results, err := c.batchStatAcrossProviders(t.ctx, t.msgIDs, homeCfg, t.hint)
	t.done(results, err)
}
