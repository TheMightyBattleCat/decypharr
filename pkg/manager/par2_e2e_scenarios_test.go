package manager

// P2E scenarios: whole URGENT passes (runJob) against the fake NNTP server,
// one subtest per injected failure mode, each with its expected outcome.

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"sort"
	"testing"
	"time"
)

type p2eScenario struct {
	name   string
	slow   bool // runs into a 60 s timeout; skipped under -short
	dead   func() []p2eSeg
	setup  func(h *p2eHarness)
	during func(h *p2eHarness) func(done <-chan struct{})
	want   p2eWant
}

// p2eWant is a scenario's expected outcome. An empty class means the pass
// completes and patches exactly `patched` segments. Otherwise the pass ends
// "unavailable" with that class and writes no patch.
type p2eWant struct {
	class   string
	patched int
	// requeued: shutdown stopped the pass before it decided anything; it
	// writes no attempt and leaves the job queued for the next start.
	requeued bool
	// deleted: the entry was deleted mid-pass; its overlay must be gone.
	deleted bool
}

func p2eCompleted(patched int) p2eWant { return p2eWant{patched: patched} }
func p2eFailed(class string) p2eWant   { return p2eWant{class: class} }

// check asserts o against w. Every outcome: no patch differs from the posted
// bytes, and the pass leaves no goroutine behind (the fake server's own
// excluded).
func (w p2eWant) check(t *testing.T, h *p2eHarness, o p2eOutcome) {
	t.Helper()
	if o.Mismatch != 0 {
		t.Errorf("%d patch(es) differ from the posted bytes: %v", o.Mismatch, o.OverlayDiff)
	}
	if o.GDeltaNoSrv > 0 {
		t.Errorf("%d goroutine(s) left after the pass: %v", o.GDeltaNoSrv, o.GNew)
	}
	attempt, _ := h.p.manager.storage.LatestPar2RepairAttemptForNzb(h.nzbID)
	state, _ := h.p.manager.storage.GetPar2RepairState(h.nzbID)
	switch {
	case w.requeued:
		if o.LogMsg == "par2 repair completed" || o.LogMsg == "par2 repair unavailable" {
			t.Errorf("pass ended %q, want it stopped before a verdict", o.LogMsg)
		}
		if o.Phase != string(Par2PhaseQueued) {
			t.Errorf("phase %q, want %q", o.Phase, Par2PhaseQueued)
		}
		if attempt != nil || state != nil {
			t.Errorf("attempt %+v state %+v recorded, want none", attempt, state)
		}
		if o.Patched != 0 {
			t.Errorf("%d segment(s) patched, want none", o.Patched)
		}
	case w.class == "":
		if o.LogMsg != "par2 repair completed" || o.Err != "" {
			t.Fatalf("pass ended %q err=%q, want completed", o.LogMsg, o.Err)
		}
		if o.Patched != w.patched || o.StillDead != 0 {
			t.Errorf("overlay %s %v, want %d patched and none still dead", o.Overlay, o.OverlayDiff, w.patched)
		}
		pend, err := h.u.OverlayPendingRepair(h.nzbID)
		if err != nil || len(pend) != 0 {
			t.Errorf("pending after the pass = %v (err %v), want none", pend, err)
		}
		if attempt == nil || attempt.Outcome != "completed" {
			t.Errorf("attempt %+v, want outcome completed", attempt)
		}
	default:
		if o.LogMsg != "par2 repair unavailable" || o.Class != w.class {
			t.Fatalf("pass ended %q class=%q err=%q, want unavailable and %s", o.LogMsg, o.Class, o.Err, w.class)
		}
		if o.Patched != 0 {
			t.Errorf("%d segment(s) patched by a failed pass, want none: %v", o.Patched, o.OverlayDiff)
		}
		if attempt == nil || attempt.Outcome != "failed" || attempt.SegmentsPatched != 0 {
			t.Errorf("attempt %+v, want outcome failed with nothing patched", attempt)
		}
		if state == nil || state.Terminal != (w.class == "terminal") {
			t.Errorf("state %+v, want terminal=%v", state, w.class == "terminal")
		}
		if w.deleted {
			if h.u.OverlayEntryExists(h.nzbID) {
				t.Errorf("overlay still exists after the entry was deleted")
			}
		} else if o.StillDead == 0 {
			t.Errorf("overlay %s, want the recorded damage kept", o.Overlay)
		}
	}
}

func TestP2EPar2EndToEnd(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	srv := servers[0]

	f1, f2, f3 := fx.posted[0], fx.posted[1], fx.posted[2]
	idxFile := fx.par2[0]
	vols := fx.par2[1:]
	vol := func(sub string) *p2eFile { return fx.file(sub) }

	s0dead := []p2eSeg{{f1, 1}, {f1, 3}}
	s0 := func() []p2eSeg { return s0dead }
	deadNotFound := func(h *p2eHarness) {
		for _, s := range h.dead {
			h.srv.set(s.file.ids[s.idx], p2eNotFound)
		}
	}
	atStream := func(act func(h *p2eHarness)) func(h *p2eHarness) func(done <-chan struct{}) {
		return func(h *p2eHarness) func(done <-chan struct{}) {
			return func(done <-chan struct{}) {
				if h.waitPhase(done, Par2PhaseStreamingIntact) {
					act(h)
				} else {
					h.notes = append(h.notes, "mid-stream hook never fired (phase streaming_intact not seen)")
				}
			}
		}
	}
	slowServer := func(h *p2eHarness) {
		deadNotFound(h)
		h.srv.slowNS.Store(int64(150 * time.Millisecond))
	}

	scenarios := []p2eScenario{
		{name: "S0_baseline", want: p2eCompleted(2), dead: s0, setup: deadNotFound},
		{name: "S1_recorded_dead_alive", want: p2eCompleted(1), dead: s0},
		{name: "S2_refuse", want: p2eFailed("transient"), dead: s0, setup: func(h *p2eHarness) {
			deadNotFound(h)
			h.afterWarmup = func() {
				h.srv.refuse.Store(true)
				h.srv.dropConns()
			}
		}},
		{name: "S3_recovery_volumes_expired", want: p2eFailed("terminal"), dead: s0, setup: func(h *p2eHarness) {
			deadNotFound(h)
			for _, v := range vols {
				for _, id := range v.ids {
					h.srv.set(id, p2eNotFound)
				}
			}
		}},
		{name: "S4_index_notfound", want: p2eCompleted(2), dead: s0, setup: func(h *p2eHarness) {
			deadNotFound(h)
			for _, id := range idxFile.ids {
				h.srv.set(id, p2eNotFound)
			}
		}},
		{name: "S5a_intact_notfound", want: p2eFailed("terminal"), dead: s0, setup: func(h *p2eHarness) {
			deadNotFound(h)
			h.srv.set(f2.ids[2], p2eNotFound)
		}},
		{name: "S5b_intact_statlie", want: p2eFailed("terminal"), dead: s0, setup: func(h *p2eHarness) {
			deadNotFound(h)
			h.srv.set(f2.ids[2], p2eStatLie)
		}},
		{name: "S6_too_much_damage", want: p2eFailed("terminal"), dead: s0, setup: func(h *p2eHarness) {
			deadNotFound(h)
			h.srv.set(f2.ids[1], p2eNotFound)
			h.srv.set(f2.ids[4], p2eNotFound)
		}},
		{name: "S7_intact_stall", want: p2eFailed("transient"), slow: true, dead: s0, setup: func(h *p2eHarness) {
			deadNotFound(h)
			h.srv.set(f2.ids[3], p2eStall)
		}},
		{name: "S8_recovery_stall", want: p2eFailed("transient"), slow: true, dead: s0, setup: func(h *p2eHarness) {
			deadNotFound(h)
			h.srv.set(vol("vol00+").ids[1], p2eStall)
		}},
		{name: "S9a_intact_truncate_close", want: p2eFailed("transient"), dead: s0, setup: func(h *p2eHarness) {
			deadNotFound(h)
			h.srv.set(f2.ids[2], p2eTruncateClose)
		}},
		{name: "S9b_intact_truncate_short_valid", want: p2eFailed("suspect"), dead: s0, setup: func(h *p2eHarness) {
			deadNotFound(h)
			h.srv.set(f2.ids[2], p2eTruncateShort)
		}},
		{name: "S10a_intact_wrongbody_same_file", want: p2eFailed("terminal"), dead: s0, setup: func(h *p2eHarness) {
			deadNotFound(h)
			h.srv.setWrong(f2.ids[2], f2.ids[4])
		}},
		{name: "S10b_intact_wrongbody_twin_file_same_part", want: p2eFailed("suspect"), dead: s0, setup: func(h *p2eHarness) {
			deadNotFound(h)
			h.srv.setWrong(f2.ids[2], f1.ids[2])
		}},
		// One provider: no other copy to ask, and the bad packet makes the
		// shortfall transient (row 70). S10d's body holds no PAR2 packet, so
		// nothing is skipped and its shortfall stays terminal.
		{name: "S10c_recovery_wrongbody_other_volume", want: p2eFailed("transient"), dead: s0, setup: func(h *p2eHarness) {
			deadNotFound(h)
			h.srv.setWrong(vol("vol00+").ids[0], vol("vol06+").ids[0])
		}},
		{name: "S10d_recovery_wrongbody_posted_article", want: p2eFailed("terminal"), dead: s0, setup: func(h *p2eHarness) {
			deadNotFound(h)
			h.srv.setWrong(vol("vol00+").ids[0], f3.ids[0])
		}},
		{name: "S11_quota_midstream", want: p2eFailed("transient"), dead: s0, setup: deadNotFound, during: atStream(func(h *p2eHarness) {
			n := h.srv.cmdBody.Load()
			h.srv.quotaAfter.Store(n + 3)
			h.notes = append(h.notes, fmt.Sprintf("quota armed at BODY #%d (+3)", n))
		})},
		{name: "S12a_dead_wrongbody_twin_same_part", want: p2eCompleted(2), dead: s0, setup: func(h *p2eHarness) {
			h.srv.setWrong(f1.ids[1], f2.ids[1])
			h.srv.set(f1.ids[3], p2eNotFound)
		}},
		{name: "S12b_dead_wrongbody_same_file", want: p2eCompleted(2), dead: s0, setup: func(h *p2eHarness) {
			h.srv.setWrong(f1.ids[1], f1.ids[4])
			h.srv.set(f1.ids[3], p2eNotFound)
		}},
		{name: "S13a_job_cancel_midstream", want: p2eFailed("transient"), dead: s0, setup: slowServer, during: atStream(func(h *p2eHarness) {
			time.Sleep(100 * time.Millisecond)
			h.notes = append(h.notes, fmt.Sprintf("job cancel called=%v", h.jobCancel()))
		})},
		{name: "S13b_shutdown_cancel_midstream", want: p2eWant{requeued: true}, dead: s0, setup: func(h *p2eHarness) {
			slowServer(h)
			h.p.ctx, h.p.cancel = context.WithCancel(context.Background())
		}, during: atStream(func(h *p2eHarness) {
			time.Sleep(100 * time.Millisecond)
			h.p.cancel()
		})},
		{name: "S14_smallest_file_wholly_dead", want: p2eCompleted(2), dead: func() []p2eSeg { return []p2eSeg{{f3, 0}, {f3, 1}} }, setup: deadNotFound},
		{name: "S15_delete_midstream", want: p2eWant{class: "transient", deleted: true}, dead: s0, setup: slowServer, during: atStream(func(h *p2eHarness) {
			time.Sleep(100 * time.Millisecond)
			err := h.u.Delete(h.nzbID)
			h.notes = append(h.notes, fmt.Sprintf("u.Delete mid-stream err=%v overlay_exists_after=%v", err, h.u.OverlayEntryExists(h.nzbID)))
		})},
		{name: "S16_clear_file_damage_midstream", want: p2eCompleted(2), dead: s0, setup: slowServer, during: atStream(func(h *p2eHarness) {
			time.Sleep(100 * time.Millisecond)
			kept, err := h.u.OverlayClearFileDamage(h.nzbID, f1.name)
			h.notes = append(h.notes, fmt.Sprintf("OverlayClearFileDamage mid-stream: patchesPreserved=%v err=%v", kept, err))
		})},
		{name: "S17_new_dead_recorded_midstream", want: p2eCompleted(2), dead: func() []p2eSeg { return []p2eSeg{{f1, 1}} }, setup: slowServer, during: atStream(func(h *p2eHarness) {
			time.Sleep(100 * time.Millisecond)
			h.srv.set(f3.ids[1], p2eNotFound)
			err := h.u.RecordOverlayDead(h.nzbID, f3.name, 1, f3.ids[1], int64(len(f3.payloads[1])))
			h.notes = append(h.notes, fmt.Sprintf("RecordOverlayDead(part3#1) mid-stream err=%v", err))
		})},
		{name: "S18_runnow_and_urgent_midstream", want: p2eCompleted(2), dead: s0, setup: slowServer, during: atStream(func(h *p2eHarness) {
			time.Sleep(100 * time.Millisecond)
			err := h.p.RunNow(h.nzbID)
			res := h.p.enqueueUrgent(h.nzbID, 0)
			h.notes = append(h.notes, fmt.Sprintf("RunNow mid-stream err=%v; enqueueUrgent merged=%v", err, res == urgentMerged))
		})},
	}

	t.Run("S00_fixture_roundtrip", func(t *testing.T) {
		p2eRoundtrip(t, newP2eHarness(t, srv, fx, "p2e-sc-roundtrip"))
	})
	for i, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			if sc.slow && testing.Short() {
				t.Skip("waits out a 60 s stall; skipped under -short")
			}
			h := newP2eHarness(t, srv, fx, fmt.Sprintf("p2e-sc-%02d", i))
			h.recordDead(sc.dead()...)
			if sc.setup != nil {
				sc.setup(h)
			}
			var during func(done <-chan struct{})
			if sc.during != nil {
				during = sc.during(h)
			}
			o := h.run(during)
			h.report(o)
			sc.want.check(t, h, o)
		})
	}
}

// p2eRoundtrip fetches every fixture article through the real client and
// compares it with the source bytes: proves the encoder and the wire.
func p2eRoundtrip(t *testing.T, h *p2eHarness) {
	ids := make([]string, 0, len(h.fx.all))
	for id := range h.fx.all {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		data, err := h.u.FetchArticle(ctx, id)
		cancel()
		if err != nil || string(data) != string(h.fx.all[id].data) {
			t.Errorf("roundtrip %s: err=%v len=%d want %d", id, err, len(data), len(h.fx.all[id].data))
		}
	}
	res, err := h.u.StatSegments(context.Background(), ids)
	if err != nil {
		t.Fatalf("StatSegments: %v", err)
	}
	avail := 0
	for _, r := range res {
		if r.Available {
			avail++
		}
	}
	if avail != len(ids) {
		t.Fatalf("StatSegments: %d of %d available, want all", avail, len(ids))
	}
}

// Two providers that disagree (§11.4): the first-priority provider serves a
// missing, wrong or short body for one article the pass needs, the second
// serves the right one. A 430 and a body that fails the identity check
// (K39: "belongs to a different upload") fail over to the second provider
// and the pass completes. A recovery article whose body fails only its PAR2
// packet MD5 is asked of the second provider once (row 70), and the pass
// completes; when the second provider serves the same bad body the shortfall
// is transient, not terminal. A body that passes the identity check and
// fails only after decode (IFSC checksum, short decode) does not fail over
// today: the pass ends without asking the second provider for that article.
// Those cases assert today's outcome; a fail-over fix flips them.
func TestP2EPar2TwoProvidersDisagree(t *testing.T) {
	servers, fx := newP2eEnv(t, 2)
	primary, backup := servers[0], servers[1]
	f1, f2 := fx.posted[0], fx.posted[1]
	vol0, vol6 := fx.file("vol00+"), fx.file("vol06+")

	cases := []struct {
		name  string
		bad   string // the article the primary mis-serves
		serve func(id string)
		want  p2eWant
		// backupBody: whether the second provider must be asked for bad.
		backupBody bool
		// backupBodies, when non-zero, is exactly how many times.
		backupBodies int64
	}{
		{"intact_notfound_on_primary", f2.ids[2], func(id string) { primary.set(id, p2eNotFound) }, p2eCompleted(2), true, 0},
		{"intact_wrongbody_other_part_on_primary", f2.ids[2], func(id string) { primary.setWrong(id, f2.ids[4]) }, p2eCompleted(2), true, 0},
		// No fail-over after decode: the IFSC and short-decode gap (row 51 Notes).
		{"intact_wrongbody_twin_part_on_primary", f2.ids[2], func(id string) { primary.setWrong(id, f1.ids[2]) }, p2eFailed("suspect"), false, 0},
		{"intact_short_on_primary", f2.ids[2], func(id string) { primary.set(id, p2eTruncateShort) }, p2eFailed("suspect"), false, 0},
		// A mis-served recovery article is asked of the other provider once
		// (row 70).
		{"recovery_wrongbody_on_primary", vol0.ids[0], func(id string) { primary.setWrong(id, vol6.ids[0]) }, p2eCompleted(2), true, 1},
		// Both serve the same wrong body: asked once, then a transient
		// shortfall, not a terminal one.
		{"recovery_wrongbody_on_both", vol0.ids[0], func(id string) {
			primary.setWrong(id, vol6.ids[0])
			backup.setWrong(id, vol6.ids[0])
		}, p2eFailed("transient"), true, 1},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backup.reset()
			h := newP2eHarness(t, primary, fx, fmt.Sprintf("p2e-two-%02d", i))
			h.recordDead(p2eSeg{f1, 1}, p2eSeg{f1, 3})
			for _, srv := range servers {
				srv.set(f1.ids[1], p2eNotFound)
				srv.set(f1.ids[3], p2eNotFound)
			}
			tc.serve(tc.bad)
			o := h.run(nil)
			h.report(o)
			b := backup.counts()
			t.Logf("  backup: BODY=%d (ok=%d 430=%d) STAT=%d conns=%d; BODY for the mis-served article: primary=%d backup=%d",
				b.Body, b.BodyOK, b.Body430, b.Stat, b.Conns, primary.bodyCount(tc.bad), backup.bodyCount(tc.bad))
			if primary.bodyCount(tc.bad) == 0 {
				t.Errorf("the primary was never asked for the mis-served article: the case does not test fail-over")
			}
			if got := backup.bodyCount(tc.bad) > 0; got != tc.backupBody {
				t.Errorf("second provider asked for the mis-served article = %v, want %v", got, tc.backupBody)
			}
			if tc.backupBodies > 0 && backup.bodyCount(tc.bad) != tc.backupBodies {
				t.Errorf("second provider asked for the mis-served article %d times, want exactly %d", backup.bodyCount(tc.bad), tc.backupBodies)
			}
			tc.want.check(t, h, o)
		})
	}
}

// The committed default set must be what par2cmdline makes for the fixture,
// or every test using it repairs against a stale set.
func TestP2eTestdataMatchesPar2cmdline(t *testing.T) {
	par2Bin, err := exec.LookPath("par2")
	if err != nil {
		t.Skip("par2 (par2cmdline) not installed")
	}
	fresh, err := buildP2eFixture(t.TempDir(), par2Bin)
	if err != nil {
		t.Fatalf("fixture with par2cmdline: %v", err)
	}
	committed, err := buildP2eFixture(t.TempDir(), "")
	if err != nil {
		t.Fatalf("fixture from testdata: %v", err)
	}
	if len(fresh.par2) != len(committed.par2) {
		t.Fatalf("par2cmdline made %d PAR2 files, testdata holds %d", len(fresh.par2), len(committed.par2))
	}
	for i, f := range fresh.par2 {
		c := committed.par2[i]
		if f.name != c.name || !bytes.Equal(f.data, c.data) {
			t.Errorf("%s differs from testdata %s: regenerate %s", f.name, c.name, p2eTestdata)
		}
	}
}
