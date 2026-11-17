package manager

import (
	"context"
	"sync"
	"testing"
	"time"
)

// watchIdle polls every 15 s (a constant), so each case waits one tick.
const watchIdleTick = 15 * time.Second

func staleProgress(phase Par2JobPhase) *par2JobProgressState {
	pr := newPar2JobProgressState("nzb-idle", "Idle.Rel")
	pr.SetPhase(phase)
	pr.updatedAt.Store(time.Now().Add(-par2JobIdleTimeout - time.Minute).UnixNano())
	return pr
}

// A pass in a network phase with no progress for longer than
// par2JobIdleTimeout is cancelled at the next poll; the same stale progress
// in a CPU or queued phase is left alone, and the watchdog exits once the
// job's context ends.
func TestPar2WatchIdle(t *testing.T) {
	if testing.Short() {
		t.Skip("waits one 15 s watchdog tick; skipped under -short")
	}
	type result struct {
		cancelled bool
		returned  bool
	}
	run := func(phase Par2JobPhase) <-chan result {
		out := make(chan result, 1)
		p, _ := newTestPar2Repair(t)
		go func() {
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			cancelled := make(chan struct{})
			var once sync.Once
			cancel := func() { once.Do(func() { close(cancelled) }) }
			returned := make(chan struct{})
			go func() { p.watchIdle(ctx, cancel, staleProgress(phase)); close(returned) }()
			var r result
			select {
			case <-cancelled:
				r.cancelled = true
			case <-time.After(watchIdleTick + 3*time.Second):
			}
			// The job ends: the watchdog must return promptly either way.
			stop()
			select {
			case <-returned:
				r.returned = true
			case <-time.After(2 * time.Second):
			}
			out <- r
		}()
		return out
	}

	cases := []struct {
		phase      Par2JobPhase
		wantCancel bool
	}{
		{Par2PhaseStreamingIntact, true},
		{Par2PhaseFetchingRecovery, true},
		{Par2PhaseQueued, false},
		{Par2PhaseSolving, false},
	}
	results := make([]<-chan result, len(cases))
	for i, c := range cases {
		results[i] = run(c.phase) // all wait out the same tick in parallel
	}
	for i, c := range cases {
		r := <-results[i]
		if r.cancelled != c.wantCancel {
			t.Errorf("phase %s with progress stale for %s: cancelled=%v, want %v", c.phase, par2JobIdleTimeout+time.Minute, r.cancelled, c.wantCancel)
		}
		if !r.returned {
			t.Errorf("phase %s: watchdog still running 2 s after the job context ended", c.phase)
		}
	}
}
