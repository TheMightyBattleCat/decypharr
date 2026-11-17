package manager

// Whole urgent passes (runJob) against the fake NNTP server for PAR2 paths
// that had no end-to-end test: the heal before a recovery-capacity gate,
// clearing the "failed" verdict on success, and a recorded segment whose
// patch the overlay refuses.

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// segStatus is file's overlay record for segment idx ("" when none).
func (h *p2eHarness) segStatus(file *p2eFile, idx int) overlay.SegmentStatus {
	h.t.Helper()
	man, err := h.u.OverlayManifest(h.nzbID)
	if err != nil || man == nil {
		return ""
	}
	fe := man.Files[file.name]
	if fe == nil {
		return ""
	}
	for _, d := range fe.DeadSegments {
		if d.Index == idx {
			return d.Status
		}
	}
	return ""
}

// logged reports whether the pass logged msg.
func (h *p2eHarness) logged(msg string) bool {
	for _, ln := range h.logs.lines() {
		if ln["message"] == msg {
			return true
		}
	}
	return false
}

// expireRecovery answers 430 for every article of every recovery volume, so
// the pass reaches a recovery-capacity gate.
func (h *p2eHarness) expireRecovery() {
	for _, v := range h.fx.par2[1:] {
		for _, id := range v.ids {
			h.srv.set(id, p2eNotFound)
		}
	}
}

// The recovery volumes are gone, so the pass is about to fail at the
// recovery gate; healBeforeRecoveryGate first re-fetches the recorded-dead
// articles. Every one alive: the pass completes with each patched to the
// posted bytes. Some alive: those are patched and the pass backs off
// (transient), so the next attempt weighs only the rest. (None alive is
// TestP2EPar2EndToEnd/S3: terminal.)
func TestP2EPar2HealBeforeRecoveryGate(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	srv := servers[0]
	f1, f2 := fx.posted[0], fx.posted[1]

	t.Run("all_healed", func(t *testing.T) {
		h := newP2eHarness(t, srv, fx, "p2e-heal-gate-all")
		h.recordDead(p2eSeg{f1, 1}, p2eSeg{f2, 3})
		h.expireRecovery()
		o := h.run(nil)
		h.report(o)
		p2eCompleted(2).check(t, h, o)
		if !h.logged("par2 repair: every dead segment healed without recovery data") {
			t.Errorf("pass completed without the recovery gate's heal; the test no longer reaches healBeforeRecoveryGate")
		}
	})

	t.Run("part_healed", func(t *testing.T) {
		h := newP2eHarness(t, srv, fx, "p2e-heal-gate-part")
		h.recordDead(p2eSeg{f1, 1}, p2eSeg{f2, 3})
		h.expireRecovery()
		srv.set(f2.ids[3], p2eNotFound)
		o := h.run(nil)
		h.report(o)
		if o.LogMsg != "par2 repair unavailable" || o.Class != "transient" {
			t.Fatalf("pass ended %q class=%q err=%q, want unavailable and transient", o.LogMsg, o.Class, o.Err)
		}
		if !strings.Contains(o.Err, "healed 1 of 2 dead segments without parity") {
			t.Errorf("error %q, want the partial-heal message", o.Err)
		}
		if got := h.segStatus(f1, 1); got != overlay.StatusPatched {
			t.Errorf("part1#1 (alive) status %q, want patched", got)
		}
		if got := h.segStatus(f2, 3); got == overlay.StatusPatched || got == "" {
			t.Errorf("part2#3 (gone) status %q, want still dead", got)
		}
		if o.Mismatch != 0 {
			t.Errorf("patch differs from the posted bytes: %v", o.OverlayDiff)
		}
		if st, _ := h.p.manager.storage.GetPar2RepairState(h.nzbID); st == nil || st.Terminal {
			t.Errorf("state %+v, want a non-terminal backoff", st)
		}
	})
}

// Files playback marked "failed" (past the padding caps) whose every dead
// segment the pass patches are no longer failed: unfailRepaired clears the
// verdict, so the next escalation does not re-grab a repaired file.
func TestP2EPar2SuccessClearsFailedVerdict(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	h := newP2eHarness(t, servers[0], fx, "p2e-unfail")
	f1, f2 := fx.posted[0], fx.posted[1]
	h.recordDead(p2eSeg{f1, 1}, p2eSeg{f2, 3})
	for _, s := range h.dead {
		h.srv.set(s.file.ids[s.idx], p2eNotFound)
	}
	for _, f := range []*p2eFile{f1, f2} {
		if v := h.u.OverlayVerdict(h.nzbID, f.name); v != overlay.VerdictFailed {
			t.Fatalf("%s verdict before the pass %q, want failed (precondition)", short(f.name), v)
		}
	}

	o := h.run(nil)
	h.report(o)
	p2eCompleted(2).check(t, h, o)
	for _, f := range []*p2eFile{f1, f2} {
		if v := h.u.OverlayVerdict(h.nzbID, f.name); v != overlay.VerdictClean {
			t.Errorf("%s verdict after a pass that patched every dead segment %q, want clean", short(f.name), v)
		}
	}
	if !h.logged("par2 repair: every dead segment is patched; the file is no longer failed") {
		t.Errorf("no unfail log line")
	}
}

// One recorded segment's reader slot is a byte longer than the article, so
// the overlay refuses its patch. The pass still writes the other segment's
// patch, and ends terminal naming the slot/article disagreement: retrying
// cannot change the geometry.
func TestP2EPar2UnwritablePatchWritesTheRest(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	h := newP2eHarness(t, servers[0], fx, "p2e-unwritable")
	f1, f2 := fx.posted[0], fx.posted[1]
	h.recordDead(p2eSeg{f2, 1})
	// Recorded as the reader's padding path does, with the slot one byte
	// longer than the posted article.
	st, err := overlay.NewStore(filepath.Join(config.GetMainPath(), "usenet", "overlay"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	st.Decide(h.nzbID, f1.name, 3, f1.ids[3], int64(len(f1.payloads[3]))+1, int64(len(f1.data)), len(f1.ids))
	h.dead = append(h.dead, p2eSeg{f1, 3})
	for _, s := range h.dead {
		h.srv.set(s.file.ids[s.idx], p2eNotFound)
	}

	o := h.run(nil)
	h.report(o)
	if o.LogMsg != "par2 repair unavailable" || o.Class != "terminal" {
		t.Fatalf("pass ended %q class=%q err=%q, want unavailable and terminal", o.LogMsg, o.Class, o.Err)
	}
	if !strings.Contains(o.Err, "cannot be patched: reader slot and article disagree") {
		t.Errorf("error %q, want the slot/article disagreement", o.Err)
	}
	if got := h.segStatus(f2, 1); got != overlay.StatusPatched {
		t.Errorf("part2#1 status %q, want patched although part1#3's patch, written first, was refused", got)
	}
	if got := h.segStatus(f1, 3); got == overlay.StatusPatched || got == "" {
		t.Errorf("part1#3 status %q, want still dead (its patch was refused)", got)
	}
	if o.Mismatch != 0 {
		t.Errorf("patch differs from the posted bytes: %v", o.OverlayDiff)
	}
}
