package manager

// The sweep heals a broken entry while holding the re-grab claim
// routeAutoRepair took for it, and the warm and cold PAR2 passes run inside
// that heal. The pass must take over its caller's own claim: refusing it
// meant no sweep pass ever ran and every repairable release was re-grabbed.

import (
	"context"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// claimAsSweep takes the entry's re-grab claim the way routeAutoRepair does
// before the sweep heals it.
func claimAsSweep(t *testing.T, h *p2eHarness) {
	t.Helper()
	if !h.repair.handlers.TryAcquire(h.nzbID, handlerRegrab) {
		t.Fatal("TryAcquire(handlerRegrab) refused on an unclaimed entry")
	}
}

// assertSweepPassRepaired checks that a pass ran to completion and patched
// both dead segments, so healBrokenEntry drops the files instead of
// re-grabbing them.
func assertSweepPassRepaired(t *testing.T, h *p2eHarness, fixed map[string]struct{}) {
	t.Helper()
	var o p2eOutcome
	h.collectOverlay(&o)
	if _, ok := fixed[h.nzbID]; !ok {
		h.report(o)
		t.Fatalf("sweep PAR2 fixed = %v, want %s (the release is repairable; without it the sweep re-grabs)", fixed, h.nzbID)
	}
	if a, _ := h.p.manager.storage.LatestPar2RepairAttemptForNzb(h.nzbID); a == nil || a.Outcome != storage.Par2RepairOutcomeCompleted {
		t.Errorf("attempt %+v, want a completed pass", a)
	}
	if o.Patched != 2 || o.StillDead != 0 || o.Mismatch != 0 {
		t.Errorf("overlay %s %v, want 2 patches equal to the posted bytes", o.Overlay, o.OverlayDiff)
	}
}

// Warm: every intact slice is cached and the sweep holds the re-grab claim.
func TestSweepWarmPar2RunsUnderTheSweepsRegrabClaim(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	h, _ := warmHarness(t, servers[0], fx, "p2e-warm-claimed", nil)
	claimAsSweep(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	fixed := h.repair.warmSweepRepair(ctx, []storage.BrokenFile{{InfoHash: h.nzbID, FileName: fx.posted[0].name, Reason: "usenet_segment_missing"}})
	h.p.wg.Wait()
	assertSweepPassRepaired(t, h, fixed)
}

// Cold (sweep PAR2 on): nothing cached, the pass reads from the server, and
// the sweep holds the re-grab claim.
func TestSweepColdPar2RunsUnderTheSweepsRegrabClaim(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	h := newP2eHarness(t, servers[0], fx, "p2e-cold-claimed")
	f1, f2 := fx.posted[0], fx.posted[1]
	h.recordDead(p2eSeg{f1, 1}, p2eSeg{f2, 3})
	for _, s := range h.dead {
		h.srv.set(s.file.ids[s.idx], p2eNotFound)
	}
	h.warmup()

	cfg := config.Get()
	on := true
	cfg.Repair.Par2Repair = &on
	cfg.Repair.Par2RepairMode = config.Par2RepairModeAutoAll
	cfg.Repair.Par2RepairOnSweep = true
	claimAsSweep(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	fixed := h.repair.coldSweepRepair(ctx, []storage.BrokenFile{
		{InfoHash: h.nzbID, FileName: f1.name, Reason: "usenet_segment_missing"},
		{InfoHash: h.nzbID, FileName: f2.name, Reason: "usenet_segment_missing"},
	})
	h.p.wg.Wait()
	assertSweepPassRepaired(t, h, fixed)
	if h.srv.bodyCount(f1.ids[0]) == 0 {
		t.Errorf("no BODY read of %s's first article: the pass did not read the release", short(f1.name))
	}
}
