package manager

import (
	"fmt"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// part1 and part2 of the fixture share a length, so MatchFiles breaks the
// tie by MD5-16k. With every article of part2 dead, part2 cannot be hashed
// and is paired with its FileDesc only by elimination: nothing read from
// it backs the pairing, so the solve's bytes for it would be served as
// verified data whatever the posted file really is. The pass rebuilds the
// slices but writes no patch for part2, leaves its segments recorded dead,
// and ends transient (not terminal, no re-grab).
func TestPar2EliminationPairingWithNothingReadIsNotPatched(t *testing.T) {
	servers, fx := newP2eEnvSets(t, 1, p2eSet{base: p2eRelease, files: []int{0, 1, 2}, redundancy: 60})
	h := newP2eHarness(t, servers[0], fx, "p2e-unconfirmed-withheld")
	f2 := fx.posted[1]
	var segs []p2eSeg
	for i := range f2.ids {
		segs = append(segs, p2eSeg{f2, i})
		servers[0].set(f2.ids[i], p2eNotFound)
	}
	h.recordDead(segs...)

	o := h.run(nil)
	h.report(o)

	if o.Patched != 0 {
		t.Errorf("overlay %s %v, want no patch: part2's pairing was never confirmed by a read", o.Overlay, o.OverlayDiff)
	}
	// The overlay's pad policy records only the start of a dead run; the
	// pass finds the rest itself. Both kinds are withheld.
	if o.StillDead == 0 {
		t.Errorf("overlay %s %v, want part2's recorded segment still dead", o.Overlay, o.OverlayDiff)
	}
	if want := fmt.Sprintf("%d dead segment(s) left unpatched", len(f2.ids)); !strings.Contains(o.Err, want) {
		t.Errorf("error %q, want %q (recorded and discovered alike)", o.Err, want)
	}
	if o.LogMsg != "par2 repair unavailable" || o.Class != "transient" {
		t.Errorf("pass ended %q class=%q err=%q, want unavailable and transient", o.LogMsg, o.Class, o.Err)
	}
	// Only the writing phase, after a successful solve, gives this error:
	// it proves the pass reached the patch gate rather than stopping early.
	if !strings.Contains(o.Err, f2.name) || !strings.Contains(o.Err, "elimination") {
		t.Errorf("error %q, want it to name %s and the elimination pairing", o.Err, f2.name)
	}
	if st, err := h.p.manager.storage.GetPar2RepairState(h.nzbID); err == nil && st != nil && st.Terminal {
		t.Errorf("repair state terminal (%q), want transient", st.TerminalReason)
	}
	if v := h.u.OverlayVerdict(h.nzbID, f2.name); v != overlay.VerdictFailed {
		t.Errorf("part2 verdict %q, want still failed", v)
	}
}

// The same tie with only part2's first article dead: part2 still cannot
// be hashed and is paired by elimination, but its other slices are read
// through the pairing and pass their IFSC. That confirms it, so the dead
// article is patched with the original bytes and the pass completes.
func TestPar2EliminationPairingConfirmedByAReadIsPatched(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	h := newP2eHarness(t, servers[0], fx, "p2e-unconfirmed-confirmed")
	f2 := fx.posted[1]
	h.recordDead(p2eSeg{f2, 0})
	servers[0].set(f2.ids[0], p2eNotFound)

	o := h.run(nil)
	h.report(o)

	if o.LogMsg != "par2 repair completed" {
		t.Errorf("pass ended %q (class %q, err %q), want completed", o.LogMsg, o.Class, o.Err)
	}
	if o.Patched != 1 || o.Mismatch != 0 || o.StillDead != 0 {
		t.Errorf("overlay %s %v, want part2#0 patched with the original bytes", o.Overlay, o.OverlayDiff)
	}
	if v := h.u.OverlayVerdict(h.nzbID, f2.name); v == overlay.VerdictFailed {
		t.Errorf("part2 verdict still failed after its patch, want cleared")
	}
}
