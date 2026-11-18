package manager

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// largeRepairWaits returns the damaged_slices of every "large repair
// waiting" log line.
func (h *p2eHarness) largeRepairWaits() []float64 {
	var out []float64
	for _, ln := range h.logs.lines() {
		if m, _ := ln["message"].(string); strings.HasPrefix(m, "par2: large repair waiting") {
			k, _ := ln["damaged_slices"].(float64)
			out = append(out, k)
		}
	}
	return out
}

// earlyGateAvailable returns the available field of every early
// arithmetic-check trip.
func (h *p2eHarness) earlyGateAvailable() []float64 {
	var out []float64
	for _, ln := range h.logs.lines() {
		if m, _ := ln["message"].(string); strings.HasPrefix(m, "par2: repair provably unavailable before recovery fetch") {
			a, _ := ln["available"].(float64)
			out = append(out, a)
		}
	}
	return out
}

// Three sets, one per file, and the index of both part2's and part3's set
// gone along with every one of their volumes (STAT says present, BODY
// 430s, so the STAT check keeps them). The search for the damaged file's
// set tried every volume of both unseen sets, one at a time, as each try
// was its own fetch. It must stop after three volumes gone in a row, as a
// recovery fetch does, and end terminal.
func TestPar2UnseenSetSearchStopsAfterThreeGoneVolumes(t *testing.T) {
	servers, fx := newP2eEnvSets(t, 1,
		p2eSet{base: p2eRelease + ".part1", files: []int{0}},
		p2eSet{base: p2eRelease + ".part2", files: []int{1}},
		p2eSet{base: p2eRelease + ".part3", files: []int{2}, redundancy: 40},
	)
	h := newP2eHarness(t, servers[0], fx, "p2e-unseen-gone-streak")
	f2 := fx.posted[1]
	h.recordDead(p2eSeg{f2, 1})
	servers[0].set(f2.ids[1], p2eNotFound)
	for _, base := range []string{".part2", ".part3"} {
		p2eSetGone(t, servers[0], fx, p2eRelease+base+".par2")
		p2eSetVolumes(servers[0], fx, p2eRelease+base+".", p2eStatLie)
	}

	o := h.run(nil)
	h.report(o)
	if o.Class != "terminal" {
		t.Errorf("pass ended %q class %q (err %q), want terminal", o.LogMsg, o.Class, o.Err)
	}
	if n := h.loggedUnseenSetSearch(); n != par2RecoveryMaxConsecutiveNotFound {
		t.Errorf("tried %d volume(s) looking for the set, want %d", n, par2RecoveryMaxConsecutiveNotFound)
	}
}

// The volume the set search fetches is held through the repair on top of
// the k slices the repair fetches. A repair that is large only with it
// must wait for the running large repair, not run beside it.
func TestPar2UnseenSetSearchSlicesCountTowardLargeRepairGate(t *testing.T) {
	servers, fx := newP2eEnvSets(t, 1,
		p2eSet{base: p2eRelease + ".part1", files: []int{0}},
		p2eSet{base: p2eRelease + ".part23", files: []int{1, 2}},
	)
	h := newP2eHarness(t, servers[0], fx, "p2e-unseen-gate")
	// part3, not part2: part2 ties part1 by length, so the early lower
	// bound would skip it and the early gate would never run.
	f3 := fx.posted[2]
	h.recordDead(p2eSeg{f3, 0})
	servers[0].set(f3.ids[0], p2eNotFound)
	p2eSetGone(t, servers[0], fx, p2eRelease+".part23.par2")

	// part3's first 100 KiB article is 7 slices, by the early bound and
	// by the exact count, and the volume the search fetches holds 3: an
	// 8-slice limit is crossed only by the sum.
	old := largeRepairBytes
	largeRepairBytes = 8 * p2eSliceSize
	t.Cleanup(func() { largeRepairBytes = old })
	h.p.largeRepair = make(chan struct{}, 1)
	h.afterWarmup = func() { h.p.largeRepair <- struct{}{} } // another large repair is running

	o := h.run(func(done <-chan struct{}) {
		deadline := time.After(10 * time.Second)
		for len(h.largeRepairWaits()) == 0 {
			select {
			case <-done:
				<-h.p.largeRepair
				return
			case <-deadline:
				<-h.p.largeRepair
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
		<-h.p.largeRepair // the running repair ends
	})
	h.report(o)
	if o.LogMsg != "par2 repair completed" {
		t.Fatalf("pass ended %q (class %q, err %q), want completed", o.LogMsg, o.Class, o.Err)
	}
	if w := h.largeRepairWaits(); len(w) != 1 || w[0] != 7+3 {
		t.Errorf("large-repair waits = %v, want one at [10] (early k 7 + the search volume's 3)", w)
	}
}

var p2eVolCount = regexp.MustCompile(`\.vol\d+\+(\d+)\.par2$`)

// After the search fetches a volume of the unseen set, the early gate
// counted that volume's slices twice: once by name with the set's volumes,
// once among the parsed recovery slices. Its available must be the set's
// real count. At 12% the set has 6 slices in volumes of 2, and the early
// lower bound comes to 7 (part2 ties part1 by length and is skipped), so
// the gate trips at 6 but not at 6 + 2.
func TestPar2UnseenSetSearchEarlyGateCountsVolumesOnce(t *testing.T) {
	servers, fx := newP2eEnvSets(t, 1,
		p2eSet{base: p2eRelease + ".part1", files: []int{0}},
		p2eSet{base: p2eRelease + ".part23", files: []int{1, 2}, redundancy: 12},
	)
	h := newP2eHarness(t, servers[0], fx, "p2e-unseen-early-available")
	var segs []p2eSeg
	for _, f := range fx.posted[1:] {
		for i, id := range f.ids {
			segs = append(segs, p2eSeg{f, i})
			servers[0].set(id, p2eNotFound)
		}
	}
	h.recordDead(segs...)
	p2eSetGone(t, servers[0], fx, p2eRelease+".part23.par2")

	want := 0
	for _, f := range fx.par2 {
		if m := p2eVolCount.FindStringSubmatch(f.name); m != nil && strings.HasPrefix(f.name, p2eRelease+".part23.") {
			n, _ := strconv.Atoi(m[1])
			want += n
		}
	}

	o := h.run(nil)
	h.report(o)
	if n := h.loggedUnseenSetSearch(); n != 1 {
		t.Fatalf("fetched %d volume(s) looking for the set, want 1", n)
	}
	got := h.earlyGateAvailable()
	if len(got) != 1 || int(got[0]) != want {
		t.Fatalf("early gate available = %v, want [%d] (the set's volumes, each counted once)", got, want)
	}
	if o.Class != "terminal" || !strings.Contains(o.Err, "only "+strconv.Itoa(want)+" recovery slices retained") {
		t.Errorf("pass ended %q class %q (err %q), want terminal with %d slices retained", o.LogMsg, o.Class, o.Err, want)
	}
}
