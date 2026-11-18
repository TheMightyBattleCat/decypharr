package manager

import (
	"context"
	"sync"
	"testing"
	"time"
)

// setPar2JobTiming shortens the job deadline, the idle window and the
// watchdog's poll for one test, and restores them after it. Call it before
// any goroutine that reads them starts.
func setPar2JobTiming(t *testing.T, job, idle, poll time.Duration) {
	t.Helper()
	oldJob, oldIdle, oldPoll := par2JobTimeout, par2JobIdleTimeout, par2IdlePollInterval
	par2JobTimeout, par2JobIdleTimeout, par2IdlePollInterval = job, idle, poll
	t.Cleanup(func() {
		par2JobTimeout, par2JobIdleTimeout, par2IdlePollInterval = oldJob, oldIdle, oldPoll
	})
}

func staleProgress(phase Par2JobPhase, stale bool) *par2JobProgressState {
	pr := newPar2JobProgressState("nzb-idle", "Idle.Rel")
	pr.SetPhase(phase)
	if stale {
		pr.updatedAt.Store(time.Now().Add(-par2JobIdleTimeout - time.Minute).UnixNano())
	}
	return pr
}

// A pass in a network phase with no progress for longer than
// par2JobIdleTimeout is cancelled at the next poll; the same stale progress
// in a CPU or queued phase is left alone, and the watchdog exits once the
// job's context ends. Progress that moved within the window is left alone in
// a network phase too. Runs with a 2 s idle window and a 50 ms poll.
func TestPar2WatchIdle(t *testing.T) {
	setPar2JobTiming(t, par2JobTimeout, 2*time.Second, 50*time.Millisecond)
	// Ten polls: long enough for a due cancel to land, and shorter than the
	// 2 s window, so progress stamped as the case starts is never due.
	watchIdleTick := 10 * par2IdlePollInterval
	type result struct {
		cancelled bool
		returned  bool
	}
	run := func(phase Par2JobPhase, stale bool) <-chan result {
		out := make(chan result, 1)
		p, _ := newTestPar2Repair(t)
		go func() {
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			cancelled := make(chan struct{})
			var once sync.Once
			cancel := func() { once.Do(func() { close(cancelled) }) }
			returned := make(chan struct{})
			go func() { p.watchIdle(ctx, cancel, staleProgress(phase, stale)); close(returned) }()
			var r result
			select {
			case <-cancelled:
				r.cancelled = true
			case <-time.After(watchIdleTick):
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
		stale      bool
		wantCancel bool
	}{
		{Par2PhaseStreamingIntact, true, true},
		{Par2PhaseFetchingRecovery, true, true},
		{Par2PhaseQueued, true, false},
		{Par2PhaseSolving, true, false},
		{Par2PhaseStreamingIntact, false, false},
	}
	results := make([]<-chan result, len(cases))
	for i, c := range cases {
		results[i] = run(c.phase, c.stale) // all wait out the same tick in parallel
	}
	for i, c := range cases {
		r := <-results[i]
		if r.cancelled != c.wantCancel {
			t.Errorf("phase %s, progress stale=%v: cancelled=%v, want %v", c.phase, c.stale, r.cancelled, c.wantCancel)
		}
		if !r.returned {
			t.Errorf("phase %s: watchdog still running 2 s after the job context ended", c.phase)
		}
	}
}
