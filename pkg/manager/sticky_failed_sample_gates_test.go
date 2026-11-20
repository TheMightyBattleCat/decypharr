package manager

import (
	"context"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// The sweep's sticky-failed reconciliation samples a file playback marked
// failed whose STAT check is healthy again. Only a 430 is evidence that an
// article is dead: a sample that could not fetch (the sweep stopped, a BODY
// quota while STAT still answers) must leave the file alone, and a file the
// sample finds still dead is re-grabbed only when the run may heal.
func TestStickyFailedSampleActsOnlyOnDeadArticlesAndUnderTheHealGate(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cancelled   bool
		quota       bool
		dead        bool // every article 430 on BODY (STAT still 223)
		autoRepair  bool
		wantRegrab  bool
		wantBroken  bool
		wantVerdict overlay.Verdict
		wantSample  usenet.SampleVerdict
	}{
		{name: "control_recovered", autoRepair: true, wantVerdict: overlay.VerdictClean, wantSample: usenet.VerdictClean},
		{name: "sweep_stopped_mid_sample", cancelled: true, autoRepair: true, wantVerdict: overlay.VerdictFailed, wantSample: usenet.VerdictInconclusive},
		{name: "body_quota_stat_ok", quota: true, autoRepair: true, wantVerdict: overlay.VerdictFailed, wantSample: usenet.VerdictInconclusive},
		{name: "dead_check_only_run", dead: true, wantBroken: true, wantVerdict: overlay.VerdictFailed, wantSample: usenet.VerdictBroken},
		{name: "dead_auto_repair_run", dead: true, autoRepair: true, wantRegrab: true, wantBroken: true, wantVerdict: overlay.VerdictFailed, wantSample: usenet.VerdictBroken},
	} {
		t.Run(tc.name, func(t *testing.T) {
			servers, fx := newP2eEnv(t, 1)
			h := newP2eHarness(t, servers[0], fx, "sticky-gates-"+tc.name)
			h.repair.regrabGuard = newRegrabGuard()
			h.p.manager.entry = NewEntryCache(h.p.manager)
			f := fx.posted[0]

			// Playback recorded one dead segment and marked the file failed.
			h.recordDead(p2eSeg{f, 1})
			if v := h.u.OverlayVerdict(h.nzbID, f.name); v != overlay.VerdictFailed {
				t.Fatalf("verdict %q, want failed (precondition)", v)
			}
			// PAR2 not usable, so the sample decides.
			cfg := config.Get()
			oldPar2 := cfg.Repair.Par2Repair
			off := false
			cfg.Repair.Par2Repair = &off
			t.Cleanup(func() { cfg.Repair.Par2Repair = oldPar2 })

			if tc.dead {
				for _, id := range f.ids {
					servers[0].set(id, p2eStatLie)
				}
			}
			if tc.quota {
				servers[0].quotaAfter.Store(1)
			}

			base, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := h.u.CheckFile(base, h.nzbID, f.name); err != nil {
				t.Fatalf("CheckFile = %v, want nil (STAT healthy: the precondition for the sample)", err)
			}

			ctx := contextWithSampleBudget(base, newSweepSampleBudget(stickyFailedSampleBudget))
			ctx = contextWithStickyRegrabAutoRepair(ctx, tc.autoRepair)
			if tc.cancelled {
				c2, cancel2 := context.WithCancel(ctx)
				cancel2()
				ctx = c2
			}
			entry := &storage.Entry{InfoHash: h.nzbID, Name: p2eRelease, Protocol: config.ProtocolNZB}
			c := &candidate{name: p2eRelease, item: &storage.EntryItem{Name: p2eRelease, Files: map[string]*storage.File{
				f.name: {Name: f.name, InfoHash: h.nzbID, Size: int64(len(f.data))},
			}}}
			res := fileResult{name: f.name, infoHash: h.nzbID, healthy: true, protocol: config.ProtocolNZB}

			// The sample as the path sees it, under the same context and server state.
			sample, _ := h.u.RunDamageSample(usenet.ContextForVerificationRead(ctx), h.nzbID, f.name, usenet.SampleOpts{})
			got, handled := h.repair.reconcileStickyFailed(ctx, c, entry, f.name, res)

			h.repair.playbackRepairMu.Lock()
			_, regrabEntered := h.repair.lastPlaybackRepair[normalizeCooldownKey(p2eRelease)]
			h.repair.playbackRepairMu.Unlock()
			verdict := h.u.OverlayVerdict(h.nzbID, f.name)

			t.Logf("sample=%+v handled=%v reason=%q healthy=%v regrab_entered=%v verdict_after=%q server=%+v",
				sample, handled, got.reason, got.healthy, regrabEntered, verdict, servers[0].counts())

			if sample.Verdict != tc.wantSample {
				t.Errorf("sample verdict %q, want %q", sample.Verdict, tc.wantSample)
			}
			if !handled {
				t.Errorf("handled=false, want the sticky-failed path to decide the file")
			}
			if regrabEntered != tc.wantRegrab {
				t.Errorf("re-grab entered=%v, want %v", regrabEntered, tc.wantRegrab)
			}
			if broken := got.reason == "sticky_failed_sample_broken" && !got.healthy; broken != tc.wantBroken {
				t.Errorf("listed broken=%v (reason %q, healthy %v), want %v", broken, got.reason, got.healthy, tc.wantBroken)
			}
			if verdict != tc.wantVerdict {
				t.Errorf("overlay verdict after %q, want %q", verdict, tc.wantVerdict)
			}
		})
	}
}

// The same dead sticky-failed file reached through probeEntry: the run's
// auto-repair value (a sweep's autoRepair, a Recheck's fix) is what gates the
// re-grab from inside the probe.
func TestStickyFailedRegrabFollowsTheProbingRunsAutoRepair(t *testing.T) {
	for _, autoRepair := range []bool{false, true} {
		t.Run(map[bool]string{false: "check_only", true: "auto_repair"}[autoRepair], func(t *testing.T) {
			servers, fx := newP2eEnv(t, 1)
			h := newP2eHarness(t, servers[0], fx, "sticky-probe-gate")
			h.repair.regrabGuard = newRegrabGuard()
			h.p.manager.entry = NewEntryCache(h.p.manager)
			f := fx.posted[0]
			h.recordDead(p2eSeg{f, 1})
			cfg := config.Get()
			oldPar2 := cfg.Repair.Par2Repair
			off := false
			cfg.Repair.Par2Repair = &off
			t.Cleanup(func() { cfg.Repair.Par2Repair = oldPar2 })
			for _, id := range f.ids {
				servers[0].set(id, p2eStatLie)
			}

			c := &candidate{name: p2eRelease, item: &storage.EntryItem{Name: p2eRelease, Files: map[string]*storage.File{
				f.name: {Name: f.name, InfoHash: h.nzbID, Size: int64(len(f.data))},
			}}}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			ctx = contextWithSampleBudget(ctx, newSweepSampleBudget(stickyFailedSampleBudget))
			eh, _ := h.repair.probeEntry(ctx, "run", c, newHealCache(), RepairRunOptions{}, autoRepair, &supersessionContext{})

			h.repair.playbackRepairMu.Lock()
			_, regrabEntered := h.repair.lastPlaybackRepair[normalizeCooldownKey(p2eRelease)]
			h.repair.playbackRepairMu.Unlock()
			var reason string
			if eh != nil {
				for _, bf := range eh.BrokenFiles {
					if bf.FileName == f.name {
						reason = bf.Reason
					}
				}
			}
			t.Logf("autoRepair=%v health=%+v regrab_entered=%v", autoRepair, eh, regrabEntered)
			// Check-only: listed broken for the user. With auto-repair the
			// re-grab has acted and the file is not listed again.
			if wantListed := !autoRepair; (reason == "sticky_failed_sample_broken") != wantListed {
				t.Errorf("broken reason %q, want listed=%v", reason, wantListed)
			}
			if regrabEntered != autoRepair {
				t.Errorf("re-grab entered=%v, want %v (the run's auto-repair)", regrabEntered, autoRepair)
			}
		})
	}
}
