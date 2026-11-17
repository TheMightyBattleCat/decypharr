package manager

import (
	"errors"
	"fmt"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// A release with one PAR2 set per file group (a season pack with one set per
// episode) and a dead article in a file of each set. One pass repairs the set
// it chose and leaves the other set's damage for the next pass. For the file
// it repaired that is a success: the attempt is recorded completed, no
// backoff is armed, the file's failed verdict is cleared, and a pass for the
// other set is queued at once instead of waiting for a new trigger after a
// backoff.
func TestPar2MultisetPartialRepairCountsAsSuccessAndQueuesTheRest(t *testing.T) {
	servers, fx := newP2eEnvSets(t, 1,
		p2eSet{base: p2eRelease + ".part1", files: []int{0}},
		p2eSet{base: p2eRelease + ".part23", files: []int{1, 2}},
	)
	h := newP2eHarness(t, servers[0], fx, "p2e-multiset-partial")
	f1, f2 := fx.posted[0], fx.posted[1]

	h.recordDead(p2eSeg{f1, 1}, p2eSeg{f2, 1})
	servers[0].set(f1.ids[1], p2eNotFound)
	servers[0].set(f2.ids[1], p2eNotFound)
	for _, f := range []*p2eFile{f1, f2} {
		if v := h.u.OverlayVerdict(h.nzbID, f.name); v != overlay.VerdictFailed {
			t.Fatalf("precondition: %s verdict %q after recording its dead article, want failed", f.name, v)
		}
	}

	o := h.run(nil)
	h.report(o)

	pend, err := h.u.OverlayPendingRepair(h.nzbID)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	var repaired, deferred *p2eFile
	switch {
	case len(pend[f1.name]) == 0 && len(pend[f2.name]) > 0:
		repaired, deferred = f1, f2
	case len(pend[f2.name]) == 0 && len(pend[f1.name]) > 0:
		repaired, deferred = f2, f1
	default:
		t.Fatalf("pending after the pass = %+v, want one set repaired and the other left for the next pass", pend)
	}

	if o.LogMsg != "par2 repair completed" {
		t.Errorf("pass ended %q (class %q, err %q), want completed", o.LogMsg, o.Class, o.Err)
	}
	if o.Phase != string(Par2PhaseCompleted) {
		t.Errorf("progress phase %q, want %q", o.Phase, Par2PhaseCompleted)
	}
	a, err := h.p.manager.storage.LatestPar2RepairAttemptForNzb(h.nzbID)
	if err != nil || a == nil {
		t.Fatalf("attempt record: %v", err)
	}
	if a.Outcome != storage.Par2RepairOutcomeCompleted || a.SegmentsPatched != 1 {
		t.Errorf("attempt outcome %s, %d segment(s) patched (reason %q), want completed with 1", a.Outcome, a.SegmentsPatched, a.FailReason)
	}
	if st, err := h.p.manager.storage.GetPar2RepairState(h.nzbID); err == nil && st != nil {
		t.Errorf("repair state %+v left after the pass, want none (no backoff, not terminal)", st)
	}
	if v := h.u.OverlayVerdict(h.nzbID, repaired.name); v == overlay.VerdictFailed {
		t.Errorf("%s (repaired) verdict still failed, want cleared", repaired.name)
	}
	if v := h.u.OverlayVerdict(h.nzbID, deferred.name); v != overlay.VerdictFailed {
		t.Errorf("%s (other set, not repaired) verdict %q, want still failed", deferred.name, v)
	}
	if !h.p.IsQueued(h.nzbID) {
		t.Errorf("no pass queued for the other set's damage, want one queued at once")
	}
	if kind, _, exists := h.repair.handlers.State(h.nzbID); !exists || kind != handlerPar2Queued {
		t.Errorf("handler claim %v (exists %v), want %v for the queued pass", kind, exists, handlerPar2Queued)
	}
	if t.Failed() {
		return
	}

	// The queued pass (run here directly: the harness starts no urgent
	// worker) repairs the other set.
	o = h.run(nil)
	h.report(o)
	if o.LogMsg != "par2 repair completed" {
		t.Errorf("follow-up pass ended %q (err %q), want completed", o.LogMsg, o.Err)
	}
	if pend, err := h.u.OverlayPendingRepair(h.nzbID); err != nil || len(pend) != 0 {
		t.Errorf("pending after the follow-up = %+v (err %v), want none", pend, err)
	}
	if v := h.u.OverlayVerdict(h.nzbID, deferred.name); v == overlay.VerdictFailed {
		t.Errorf("%s verdict still failed after the follow-up, want cleared", deferred.name)
	}
	if o.Mismatch != 0 || o.Patched != 2 {
		t.Errorf("overlay after the follow-up: %s, want 2 patches matching the original bytes", o.Overlay)
	}
}

// A deferral counts as a success only when the pass wrote something: a pass
// that left its own set's damage as it was would be queued again at once,
// choose the same set and loop, so it keeps the failure and its backoff.
func TestPar2DeferredSetsSuccess(t *testing.T) {
	deferred := fmt.Errorf("solve: %w", &par2SetsDeferredError{dead: 3})
	for _, tc := range []struct {
		name  string
		err   error
		tally *par2PassTally
		want  int
	}{
		{"set repaired, others deferred", deferred, &par2PassTally{patchedRecorded: 1}, 3},
		{"healed counts as written", deferred, &par2PassTally{healed: 1}, 3},
		{"nothing written", deferred, &par2PassTally{}, 0},
		{"nil tally", deferred, nil, 0},
		{"other error", errors.New("parse PAR2 index: bad"), &par2PassTally{patchedRecorded: 1}, 0},
		{"chosen set has none", errors.New("every dead segment is in another PAR2 set (transient: chosen set has none)"), &par2PassTally{}, 0},
		{"success", nil, &par2PassTally{patchedRecorded: 1}, 0},
	} {
		if got := par2DeferredSetsSuccess(tc.err, tc.tally); got != tc.want {
			t.Errorf("%s: par2DeferredSetsSuccess = %d, want %d", tc.name, got, tc.want)
		}
	}
	// The text is unchanged, so it still backs off when it is not a success.
	if class := classifyPar2Failure(deferred); class.terminal || class.suspect {
		t.Errorf("%v classified %+v, want transient", deferred, class)
	}
}
