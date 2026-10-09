package manager

import (
	"context"
	"sync"
	"testing"
	"time"
)

func urgentWaiting(p *Par2Repair, nzbID string) bool {
	p.urgentMu.Lock()
	defer p.urgentMu.Unlock()
	_, ok := p.urgentSet[nzbID]
	return ok
}

// An URGENT request for an entry whose URGENT pass is running is merged into
// that pass, not queued: a queued one was popped by a second worker, refused
// by claimRun and dropped, once per missing article a read-ahead found.
func TestEnqueueUrgentMergesIntoRunningUrgentPass(t *testing.T) {
	p, _ := newTestPar2Repair(t)
	job, ok := p.claimRun(context.Background(), "x", laneUrgent, func() {})
	if !ok {
		t.Fatal("claim")
	}

	for _, prox := range []time.Duration{30 * time.Second, 5 * time.Second, time.Minute} {
		if got := p.enqueueUrgent("x", prox); got != urgentMerged {
			t.Fatalf("enqueueUrgent during a running urgent pass = %v, want urgentMerged", got)
		}
	}
	if urgentWaiting(p, "x") {
		t.Fatal("a merged request was also put on the urgent heap")
	}

	merged, proximity := p.releaseRun("x", job)
	if merged != 3 {
		t.Fatalf("releaseRun merged = %d, want 3", merged)
	}
	if proximity != 5*time.Second {
		t.Fatalf("releaseRun proximity = %s, want the smallest merged (5s)", proximity)
	}

	// With the pass gone the next request queues normally.
	if got := p.enqueueUrgent("x", 0); got != urgentQueued {
		t.Fatalf("enqueueUrgent after the pass ended = %v, want urgentQueued", got)
	}
	if !urgentWaiting(p, "x") {
		t.Fatal("request after the pass ended is not on the urgent heap")
	}
}

// A running BATCH pass is still preempted and the URGENT request queued, as
// before: merging is only for a pass already in the URGENT lane.
func TestEnqueueUrgentStillPreemptsBatchPass(t *testing.T) {
	p, _ := newTestPar2Repair(t)
	cancelled := false
	job, ok := p.claimRun(context.Background(), "x", laneBatch, func() { cancelled = true })
	if !ok {
		t.Fatal("claim")
	}
	if got := p.enqueueUrgent("x", 0); got != urgentQueued {
		t.Fatalf("enqueueUrgent during a batch pass = %v, want urgentQueued", got)
	}
	if !cancelled || !job.preempted.Load() {
		t.Fatalf("batch pass not preempted (cancelled=%v preempted=%v)", cancelled, job.preempted.Load())
	}
	if merged, _ := p.releaseRun("x", job); merged != 0 {
		t.Fatalf("batch pass collected %d merged requests, want 0", merged)
	}
}

// After a pass that received requests, exactly one follow-up is queued when
// damage is still recorded, and none when nothing is left or nothing asked.
func TestFollowUpMergedQueuesOnePassOnlyWhenDamageRemains(t *testing.T) {
	cases := []struct {
		name    string
		merged  int
		pending int
		want    bool
	}{
		{"requests and damage left", 4, 2, true},
		{"requests but nothing left", 4, 0, false},
		{"damage left but nothing asked", 0, 2, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := newTestPar2Repair(t)
			lookups := 0
			p.pendingDamage = func(string) (int, error) { lookups++; return tc.pending, nil }

			p.followUpMerged("x", "Some Release", tc.merged, 7*time.Second)

			if got := urgentWaiting(p, "x"); got != tc.want {
				t.Fatalf("follow-up queued = %v, want %v", got, tc.want)
			}
			if tc.want {
				p.urgentMu.Lock()
				n, prox := len(p.urgentHeap), p.urgentSet["x"].proximity
				p.urgentMu.Unlock()
				if n != 1 {
					t.Fatalf("urgent heap holds %d jobs, want exactly 1", n)
				}
				if prox != 7*time.Second {
					t.Fatalf("follow-up proximity = %s, want the merged requests' 7s", prox)
				}
			}
			if tc.merged == 0 && lookups != 0 {
				t.Fatal("a pass nothing interrupted looked up pending damage")
			}
		})
	}
}

// A backoff or terminal verdict still holds the follow-up back: it goes
// through the same gate as any automatic request.
func TestFollowUpMergedRespectsHandlerOwnedByRegrab(t *testing.T) {
	p, repair := newTestPar2Repair(t)
	p.pendingDamage = func(string) (int, error) { return 3, nil }
	release, ok := repair.handlers.TryAcquireRegrab("x")
	if !ok {
		t.Fatal("regrab claim")
	}
	defer release()

	p.followUpMerged("x", "Some Release", 2, 0)

	if urgentWaiting(p, "x") {
		t.Fatal("follow-up queued a pass on an entry a re-grab owns")
	}
}

// A request racing the end of a pass is never lost: it is either counted by
// releaseRun or finds no running job and lands on the urgent heap.
func TestMergedRequestRacingReleaseIsNotLost(t *testing.T) {
	for i := 0; i < 200; i++ {
		p, _ := newTestPar2Repair(t)
		job, ok := p.claimRun(context.Background(), "x", laneUrgent, func() {})
		if !ok {
			t.Fatal("claim")
		}
		const requests = 8
		results := make([]urgentEnqueueResult, requests)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for r := 0; r < requests; r++ {
			wg.Add(1)
			go func(r int) {
				defer wg.Done()
				<-start
				results[r] = p.enqueueUrgent("x", 0)
			}(r)
		}
		var merged int
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			merged, _ = p.releaseRun("x", job)
		}()
		close(start)
		wg.Wait()

		saidMerged, saidQueued := 0, 0
		for _, res := range results {
			switch res {
			case urgentMerged:
				saidMerged++
			case urgentQueued:
				saidQueued++
			default:
				t.Fatalf("request refused: %v", res)
			}
		}
		if saidMerged != merged {
			t.Fatalf("run %d: %d requests were told they merged, releaseRun collected %d", i, saidMerged, merged)
		}
		if saidQueued > 0 && !urgentWaiting(p, "x") {
			t.Fatalf("run %d: %d requests were told they queued, but nothing is on the urgent heap", i, saidQueued)
		}
	}
}

// An URGENT job that reaches runJob while another pass holds the entry is
// handed back, not dropped: it merges into the running URGENT pass.
func TestRunJobHandsBackUrgentDuplicate(t *testing.T) {
	p, _ := newTestPar2Repair(t)
	job, ok := p.claimRun(context.Background(), "x", laneUrgent, func() {})
	if !ok {
		t.Fatal("claim")
	}

	p.runJob("x", laneUrgent)

	if !p.IsRunning("x") {
		t.Fatal("the duplicate deregistered the pass that owns the entry")
	}
	if urgentWaiting(p, "x") {
		t.Fatal("the duplicate was queued beside a running urgent pass")
	}
	if merged, _ := p.releaseRun("x", job); merged != 1 {
		t.Fatalf("running pass collected %d merged requests, want 1", merged)
	}
}

// A BATCH duplicate is still just skipped: nothing is merged or queued.
func TestRunJobSkipsBatchDuplicate(t *testing.T) {
	p, _ := newTestPar2Repair(t)
	job, ok := p.claimRun(context.Background(), "x", laneUrgent, func() {})
	if !ok {
		t.Fatal("claim")
	}

	p.runJob("x", laneBatch)

	if urgentWaiting(p, "x") {
		t.Fatal("a batch duplicate queued an urgent job")
	}
	if merged, _ := p.releaseRun("x", job); merged != 0 {
		t.Fatalf("running pass collected %d merged requests from a batch duplicate, want 0", merged)
	}
}
