package manager

import (
	"bytes"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// The only recorded dead article turns out to be alive on one provider, so
// the heal patches it and no recorded damage is left. The STAT sweep (run
// without that provider, which answers present for a recorded-dead article)
// has also found an article nothing recorded, and its body is gone on every
// provider. The pass must not end "completed" there: it repairs that article
// in the same pass, records it and patches it with the real bytes, and the
// tally counts it.
func TestPar2PassPatchesSweepMissesWhenRecordedDamageHeals(t *testing.T) {
	servers, fx := newP2eEnv(t, 2)
	h := newP2eHarness(t, servers[0], fx, "p2e-sweep-miss")
	f1, f2 := fx.posted[0], fx.posted[1]

	h.recordDead(p2eSeg{f1, 1})
	servers[1].set(f1.ids[1], p2eNotFound) // recorded dead: gone on one provider, alive on the other
	for _, srv := range servers {
		srv.set(f2.ids[2], p2eNotFound) // not recorded, gone everywhere
	}

	o := h.run(nil)
	h.report(o)

	if o.LogMsg != "par2 repair completed" || o.Err != "" {
		t.Fatalf("pass ended %q err=%q, want completed", o.LogMsg, o.Err)
	}
	man, err := h.u.OverlayManifest(h.nzbID)
	if err != nil || man == nil {
		t.Fatalf("overlay manifest: %v", err)
	}
	fe := man.Files[f2.name]
	if fe == nil {
		t.Fatalf("no overlay record for %s: the sweep's miss was dropped", f2.name)
	}
	found := false
	for _, d := range fe.DeadSegments {
		if d.Index != 2 {
			continue
		}
		found = true
		if d.Status != overlay.StatusPatched {
			t.Fatalf("%s#2 status = %v, want patched", f2.name, d.Status)
		}
	}
	if !found {
		t.Fatalf("%s#2 not recorded: %+v", f2.name, fe.DeadSegments)
	}
	data, ok := h.u.OverlayPatchBytes(h.nzbID, f2.name, 2)
	if !ok || !bytes.Equal(data, f2.payloads[2]) {
		t.Fatalf("%s#2 patch ok=%v len=%d, want the posted %d bytes", f2.name, ok, len(data), len(f2.payloads[2]))
	}
	if o.Mismatch != 0 || o.StillDead != 0 {
		t.Fatalf("overlay %s %v, want every record patched with the posted bytes", o.Overlay, o.OverlayDiff)
	}
	for _, want := range []string{"healed=1", "patched_discovered=1"} {
		if !strings.Contains(o.Tally, want) {
			t.Fatalf("tally %q, want %s", o.Tally, want)
		}
	}
}

// As above, but the sweep, run with a provider left out, finds more missing
// than the recovery set can rebuild, so its misses are not seeded into the
// solve. The pass must not report the release repaired: the misses stay
// recorded dead, and the pass backs off (transient) so the next one takes
// them as recorded damage.
func TestPar2PassRecordsSweepMissesOverBudgetWhenRecordedDamageHeals(t *testing.T) {
	servers, fx := newP2eEnv(t, 2)
	h := newP2eHarness(t, servers[0], fx, "p2e-sweep-over")
	f1, f2 := fx.posted[0], fx.posted[1]

	h.recordDead(p2eSeg{f1, 1})
	servers[1].set(f1.ids[1], p2eNotFound)
	// Every article of part 2 but its first (kept so its geometry is
	// measured): far more slices than the recovery set holds.
	gone := f2.ids[1:]
	for _, srv := range servers {
		for _, id := range gone {
			srv.set(id, p2eNotFound)
		}
	}

	o := h.run(nil)
	h.report(o)

	if o.LogMsg != "par2 repair unavailable" || o.Class != "transient" {
		t.Fatalf("pass ended %q class=%q err=%q, want unavailable and transient", o.LogMsg, o.Class, o.Err)
	}
	if !strings.Contains(o.Err, "were recorded but not repaired") {
		t.Fatalf("err = %q, want the recorded-but-not-repaired cause", o.Err)
	}
	pend, err := h.u.OverlayPendingRepair(h.nzbID)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if got := len(pend[f2.name]); got != len(gone) {
		t.Fatalf("%s has %d dead segment(s) pending, want %d: %+v", f2.name, got, len(gone), pend)
	}
	if len(pend[f1.name]) != 0 {
		t.Fatalf("%s still pending %+v, want it healed", f1.name, pend[f1.name])
	}
}
