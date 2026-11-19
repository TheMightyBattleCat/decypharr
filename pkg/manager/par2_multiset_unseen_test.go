package manager

import (
	"strings"
	"testing"
)

// p2eSetGone 430s every article of the fixture's PAR2 file named name.
func p2eSetGone(t *testing.T, srv *p2eServer, fx *p2eFixture, name string) {
	t.Helper()
	for _, f := range fx.par2 {
		if f.name == name {
			for _, id := range f.ids {
				srv.set(id, p2eNotFound)
			}
			return
		}
	}
	t.Fatalf("fixture has no PAR2 file %q", name)
}

// p2eSetVolumes gives every article of every recovery volume whose name
// starts with prefix behaviour b.
func p2eSetVolumes(srv *p2eServer, fx *p2eFixture, prefix string, b p2eBehavior) {
	for _, f := range fx.par2 {
		if strings.HasPrefix(f.name, prefix) && strings.Contains(f.name, ".vol") {
			for _, id := range f.ids {
				srv.set(id, b)
			}
		}
	}
}

func (h *p2eHarness) loggedUnseenSetSearch() int {
	n := 0
	for _, ln := range h.logs.lines() {
		if m, _ := ln["message"].(string); strings.HasPrefix(m, "par2: a damaged file is in no recovery set fetched so far") {
			n++
		}
	}
	return n
}

// A two-set pack (part1 | part2+part3) whose damaged set's index file is
// gone, and whose volumes are all larger than the other set's, so the first
// batch of volumes holds none of them. The pass only ever saw part1's set
// and ended "not part of any matched posted file", terminal: a re-grab of
// the whole pack for damage its own PAR2 data repairs. It must fetch a
// volume of the other set and repair from it.
func TestPar2SetWithNoIndexFoundAmongLaterVolumes(t *testing.T) {
	servers, fx := newP2eEnvSets(t, 1,
		p2eSet{base: p2eRelease + ".part1", files: []int{0}},
		p2eSet{base: p2eRelease + ".part23", files: []int{1, 2}},
	)
	h := newP2eHarness(t, servers[0], fx, "p2e-unseen-two-sets")
	f2 := fx.posted[1]
	h.recordDead(p2eSeg{f2, 1})
	servers[0].set(f2.ids[1], p2eNotFound)
	p2eSetGone(t, servers[0], fx, p2eRelease+".part23.par2")

	o := h.run(nil)
	h.report(o)
	if o.LogMsg != "par2 repair completed" {
		t.Fatalf("pass ended %q (class %q, err %q), want completed", o.LogMsg, o.Class, o.Err)
	}
	if o.Patched != 1 || o.Mismatch != 0 {
		t.Errorf("overlay: %s, want 1 patch matching the original bytes", o.Overlay)
	}
	if pend, err := h.u.OverlayPendingRepair(h.nzbID); err != nil || len(pend) != 0 {
		t.Errorf("pending after the pass = %+v (err %v), want none", pend, err)
	}
	if n := h.loggedUnseenSetSearch(); n != 1 {
		t.Errorf("fetched %d volume(s) looking for the set, want 1", n)
	}
}

// Three sets, one per file, and the index of part3's set is gone. The two
// index files that fetched name neither damaged file (shape b) or only one
// of them (mixed): the pass ended "could not be tied" or "not part of any
// matched posted file", both terminal. It must find part3's set among the
// volumes and repair.
func TestPar2ThreeSetsOneIndexGone(t *testing.T) {
	sets := []p2eSet{
		{base: p2eRelease + ".part1", files: []int{0}},
		{base: p2eRelease + ".part2", files: []int{1}},
		{base: p2eRelease + ".part3", files: []int{2}, redundancy: 40},
	}
	t.Run("damage in the unseen set", func(t *testing.T) {
		servers, fx := newP2eEnvSets(t, 1, sets...)
		h := newP2eHarness(t, servers[0], fx, "p2e-unseen-three-sets")
		f3 := fx.posted[2]
		h.recordDead(p2eSeg{f3, 1})
		servers[0].set(f3.ids[1], p2eNotFound)
		p2eSetGone(t, servers[0], fx, p2eRelease+".part3.par2")

		o := h.run(nil)
		h.report(o)
		if o.LogMsg != "par2 repair completed" {
			t.Fatalf("pass ended %q (class %q, err %q), want completed", o.LogMsg, o.Class, o.Err)
		}
		if o.Patched != 1 || o.Mismatch != 0 {
			t.Errorf("overlay: %s, want 1 patch matching the original bytes", o.Overlay)
		}
		if pend, err := h.u.OverlayPendingRepair(h.nzbID); err != nil || len(pend) != 0 {
			t.Errorf("pending after the pass = %+v (err %v), want none", pend, err)
		}
	})
	t.Run("damage in a seen and the unseen set", func(t *testing.T) {
		servers, fx := newP2eEnvSets(t, 1, sets...)
		h := newP2eHarness(t, servers[0], fx, "p2e-unseen-three-sets-mixed")
		f1, f3 := fx.posted[0], fx.posted[2]
		h.recordDead(p2eSeg{f1, 1}, p2eSeg{f3, 1})
		servers[0].set(f1.ids[1], p2eNotFound)
		servers[0].set(f3.ids[1], p2eNotFound)
		p2eSetGone(t, servers[0], fx, p2eRelease+".part3.par2")

		o := h.run(nil)
		h.report(o)
		if o.LogMsg != "par2 repair completed" {
			t.Fatalf("pass ended %q (class %q, err %q), want completed (one set repaired, the other deferred)", o.LogMsg, o.Class, o.Err)
		}
		pend, err := h.u.OverlayPendingRepair(h.nzbID)
		if err != nil {
			t.Fatalf("pending: %v", err)
		}
		if (len(pend[f1.name]) == 0) == (len(pend[f3.name]) == 0) {
			t.Fatalf("pending after the pass = %+v, want one set repaired and the other left for the next pass", pend)
		}
		if !h.p.IsQueued(h.nzbID) {
			t.Fatalf("no pass queued for the other set's damage")
		}

		// The queued pass repairs the other set; with part3's index still
		// gone, it must find that set among the volumes again when it is
		// the one left.
		o = h.run(nil)
		h.report(o)
		if o.LogMsg != "par2 repair completed" {
			t.Fatalf("follow-up pass ended %q (class %q, err %q), want completed", o.LogMsg, o.Class, o.Err)
		}
		if pend, err := h.u.OverlayPendingRepair(h.nzbID); err != nil || len(pend) != 0 {
			t.Errorf("pending after the follow-up = %+v (err %v), want none", pend, err)
		}
		if o.Patched != 2 || o.Mismatch != 0 {
			t.Errorf("overlay after the follow-up: %s, want 2 patches matching the original bytes", o.Overlay)
		}
	})
}

// The set searched for may be in the volumes that failed without a 430 (cut
// connections here): the pass that could not find it is transient, not a
// terminal re-grab.
func TestPar2UnseenSetVolumesFailingWithout430IsTransient(t *testing.T) {
	servers, fx := newP2eEnvSets(t, 1,
		p2eSet{base: p2eRelease + ".part1", files: []int{0}},
		p2eSet{base: p2eRelease + ".part23", files: []int{1, 2}},
	)
	h := newP2eHarness(t, servers[0], fx, "p2e-unseen-timeout")
	f2 := fx.posted[1]
	h.recordDead(p2eSeg{f2, 1})
	servers[0].set(f2.ids[1], p2eNotFound)
	p2eSetGone(t, servers[0], fx, p2eRelease+".part23.par2")
	p2eSetVolumes(servers[0], fx, p2eRelease+".part23.", p2eTruncateClose)

	o := h.run(nil)
	h.report(o)
	if o.LogMsg != "par2 repair unavailable" || o.Class != "transient" {
		t.Fatalf("pass ended %q class %q (err %q), want unavailable and transient", o.LogMsg, o.Class, o.Err)
	}
	if n := h.loggedUnseenSetSearch(); n != 3 {
		t.Errorf("tried %d volume(s) of the unseen set, want all 3", n)
	}
}

// Controls. A one-set release whose damage is in a file the set does not
// protect has no volume of another set to fetch: it ends terminal as
// before, having fetched nothing extra. And when every volume of the set
// searched for is gone (430), the pass ends terminal too.
func TestPar2UnseenSetSearchControls(t *testing.T) {
	t.Run("one set, damage outside it", func(t *testing.T) {
		servers, fx := newP2eEnvSets(t, 1, p2eSet{base: p2eRelease + ".part12", files: []int{0, 1}})
		h := newP2eHarness(t, servers[0], fx, "p2e-unseen-control-one-set")
		f3 := fx.posted[2]
		h.recordDead(p2eSeg{f3, 1})
		servers[0].set(f3.ids[1], p2eNotFound)

		o := h.run(nil)
		h.report(o)
		if o.Class != "terminal" || !strings.Contains(o.Err, "is not part of any matched posted file") {
			t.Errorf("pass ended %q class %q (err %q), want terminal \"not part of any matched posted file\"", o.LogMsg, o.Class, o.Err)
		}
		if n := h.loggedUnseenSetSearch(); n != 0 {
			t.Errorf("fetched %d volume(s) looking for another set, want none", n)
		}
	})
	t.Run("unseen set's volumes all 430", func(t *testing.T) {
		servers, fx := newP2eEnvSets(t, 1,
			p2eSet{base: p2eRelease + ".part1", files: []int{0}},
			p2eSet{base: p2eRelease + ".part23", files: []int{1, 2}},
		)
		h := newP2eHarness(t, servers[0], fx, "p2e-unseen-control-gone")
		f2 := fx.posted[1]
		h.recordDead(p2eSeg{f2, 1})
		servers[0].set(f2.ids[1], p2eNotFound)
		p2eSetGone(t, servers[0], fx, p2eRelease+".part23.par2")
		p2eSetVolumes(servers[0], fx, p2eRelease+".part23.", p2eNotFound)

		o := h.run(nil)
		h.report(o)
		if o.Class != "terminal" {
			t.Errorf("pass ended %q class %q (err %q), want terminal", o.LogMsg, o.Class, o.Err)
		}
	})
}
