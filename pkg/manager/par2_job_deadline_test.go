package manager

// A whole urgent pass ended by its own deadlines: the job timeout
// (par2JobTimeoutFor) and the idle watchdog (watchIdle).
//
// The server is made slow, not stalled: a BODY the server never answers
// holds its worker until nntp's own read deadline (60 s) whatever the job's
// context says, so with a stall both cases would end at that deadline and
// the test could not tell them apart.

import (
	"context"
	"strings"
	"testing"
	"time"
)

const p2eWatchdogMsg = "par2 repair cancelled: no progress in a network phase"

func p2eLogged(h *p2eHarness, msg string) bool {
	for _, ln := range h.logs.lines() {
		if m, _ := ln["message"].(string); m == msg {
			return true
		}
	}
	return false
}

// newDeadlinePassHarness is a harness whose pass has two recorded dead
// articles to repair, enough damage that it reaches streaming_intact.
func newDeadlinePassHarness(t *testing.T, nzbID string) *p2eHarness {
	t.Helper()
	servers, fx := newP2eEnv(t, 1)
	f1 := fx.posted[0]
	h := newP2eHarness(t, servers[0], fx, nzbID)
	h.recordDead(p2eSeg{f1, 1}, p2eSeg{f1, 3})
	for _, s := range h.dead {
		h.srv.set(s.file.ids[s.idx], p2eNotFound)
	}
	return h
}

// The job deadline ends a pass that is still making progress, just slowly:
// transient (retried later), nothing patched, ended by the deadline and not
// by the idle watchdog, and soon after the deadline.
func TestP2EPar2JobTimeoutEndsSlowPass(t *testing.T) {
	const reply = 500 * time.Millisecond
	setPar2JobTiming(t, time.Second, time.Hour, 50*time.Millisecond)
	h := newDeadlinePassHarness(t, "p2e-job-timeout")
	hdr, err := h.u.GetNZBHeader(h.nzbID)
	if err != nil || hdr == nil {
		t.Fatalf("GetNZBHeader: %v", err)
	}
	deadline := par2JobTimeoutFor(hdr.TotalSize)
	if deadline > 5*time.Second {
		t.Fatalf("job deadline %s for %d bytes: the fixture grew, shorten the test's base", deadline, hdr.TotalSize)
	}
	// Every reply from now on takes reply: the pass needs far longer than
	// the deadline, and a fetch in flight when it passes ends within one
	// reply.
	h.afterWarmup = func() { h.srv.slowNS.Store(int64(reply)) }

	o := h.run(nil)
	h.report(o)
	p2eFailed("transient").check(t, h, o)
	if o.Wall < deadline || o.Wall > deadline+2*reply+3*time.Second {
		t.Errorf("pass took %s, want it ended soon after the %s job deadline", o.Wall, deadline)
	}
	if !strings.Contains(o.Err, context.DeadlineExceeded.Error()) {
		t.Errorf("error %q, want the job deadline", o.Err)
	}
	if p2eLogged(h, p2eWatchdogMsg) {
		t.Errorf("the idle watchdog fired; want the job deadline to end the pass")
	}
}

// The idle watchdog ends a pass that stops making progress while streaming
// intact data, long before the job deadline: transient, nothing patched,
// with the watchdog's line logged.
func TestP2EPar2IdleWatchdogEndsStuckPass(t *testing.T) {
	const reply = 4 * time.Second
	setPar2JobTiming(t, time.Hour, time.Second, 50*time.Millisecond)
	h := newDeadlinePassHarness(t, "p2e-idle-watchdog")

	var slowFrom time.Time
	o := h.run(func(done <-chan struct{}) {
		// From streaming_intact on, every reply takes reply: no article
		// lands for longer than the idle window.
		if h.waitPhase(done, Par2PhaseStreamingIntact) {
			slowFrom = time.Now()
			h.srv.slowNS.Store(int64(reply))
		} else {
			h.notes = append(h.notes, "streaming_intact never seen")
		}
	})
	end := time.Now()
	h.report(o)
	p2eFailed("transient").check(t, h, o)
	if slowFrom.IsZero() {
		t.Fatalf("the pass never reached streaming_intact")
	}
	if !p2eLogged(h, p2eWatchdogMsg) {
		t.Errorf("no %q line; want the watchdog to end the pass", p2eWatchdogMsg)
	}
	// One slow reply in flight when the watchdog cancels, then the pass ends.
	if took := end.Sub(slowFrom); took > 2*reply+2*time.Second {
		t.Errorf("pass ran %s after the server went slow, want it ended about one %s reply after the %s idle window", took, reply, par2JobIdleTimeout)
	}
	if !strings.Contains(o.Err, context.Canceled.Error()) {
		t.Errorf("error %q, want the job cancelled", o.Err)
	}
}
