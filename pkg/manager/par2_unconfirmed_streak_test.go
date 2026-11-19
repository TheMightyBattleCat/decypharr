package manager

import (
	"errors"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// A posted file paired by elimination whose every article is dead can never
// be confirmed by a read, so each pass ends on the same unconfirmed-pairing
// transient. The first two passes back off as transient; the third gives up
// with a reason that names the file and says a re-grab is the fix.
func TestPar2UnconfirmedPairingTerminalOnThirdPass(t *testing.T) {
	servers, fx := newP2eEnvSets(t, 1, p2eSet{base: p2eRelease, files: []int{0, 1, 2}, redundancy: 60})
	h := newP2eHarness(t, servers[0], fx, "p2e-unconfirmed-streak")
	f2 := fx.posted[1]
	var segs []p2eSeg
	for i := range f2.ids {
		segs = append(segs, p2eSeg{f2, i})
		servers[0].set(f2.ids[i], p2eNotFound)
	}
	h.recordDead(segs...)

	wantReason := "posted file \"" + f2.name + "\" can never be confirmed against its PAR2 file: re-grab the release"
	for pass := 1; pass <= 3; pass++ {
		o := h.run(nil)
		h.report(o)
		if o.Patched != 0 {
			t.Fatalf("pass %d: overlay %s %v, want no patch", pass, o.Overlay, o.OverlayDiff)
		}
		if !strings.Contains(o.Err, "left unpatched") || !strings.Contains(o.Err, f2.name) {
			t.Fatalf("pass %d: error %q, want the unconfirmed-pairing error naming %s", pass, o.Err, f2.name)
		}
		st, err := h.p.manager.storage.GetPar2RepairState(h.nzbID)
		if err != nil || st == nil {
			t.Fatalf("pass %d: no repair state: %v", pass, err)
		}
		if pass < 3 {
			if o.Class != "transient" || st.Terminal {
				t.Fatalf("pass %d: class %q terminal=%v (%q), want transient", pass, o.Class, st.Terminal, st.TerminalReason)
			}
			continue
		}
		if o.Class != "terminal" || !st.Terminal {
			t.Fatalf("pass 3: class %q terminal=%v, want terminal", o.Class, st.Terminal)
		}
		if st.TerminalReason != wantReason {
			t.Fatalf("pass 3: reason %q, want %q", st.TerminalReason, wantReason)
		}
	}
}

// unconfirmedErr builds the pass's unconfirmed-pairing error for withheld
// refs in the given posted files (each its own FileID).
func unconfirmedErr(files ...string) error {
	nzb := &storage.NZB{}
	var matches []par2.Match
	var withheld []par2DeadRef
	for i, name := range files {
		var fid [16]byte
		fid[0] = byte(i + 1)
		nzb.Par2Source = append(nzb.Par2Source, storage.PostedFileRef{Name: name})
		matches = append(matches, par2.Match{PostedIndex: i, FileID: fid, Evidence: par2.EvidenceElimination})
		withheld = append(withheld, par2DeadRef{file: name, rng: postedRange{fileID: fid}})
	}
	return unconfirmedPairingErr(withheld, matches, nzb)
}

func TestUnconfirmedPairingStreak(t *testing.T) {
	p, _ := newTestPar2Repair(t)
	terminalOn := func(id string, errs ...error) []bool {
		var out []bool
		for _, err := range errs {
			out = append(out, p.recordPar2Outcome(id, err, 0).terminal)
		}
		return out
	}
	a, b := unconfirmedErr("a.part1.rar"), unconfirmedErr("b.part1.rar")
	if c := classifyPar2Failure(a); c.terminal || c.suspect {
		t.Fatalf("one unconfirmed pass classed %q, want transient", c.name())
	}

	if got := terminalOn("same", a, a, a); got[0] || got[1] || !got[2] {
		t.Fatalf("same file three times: terminal %v, want [false false true]", got)
	}
	st, err := p.manager.storage.GetPar2RepairState("same")
	if err != nil || st == nil {
		t.Fatalf("no state: %v", err)
	}
	want := "posted file \"a.part1.rar\" can never be confirmed against its PAR2 file: re-grab the release"
	if !st.Terminal || st.TerminalReason != want {
		t.Fatalf("state terminal=%v reason %q, want %q", st.Terminal, st.TerminalReason, want)
	}

	if got := terminalOn("switch", a, a, b, a, a); got[4] {
		t.Fatalf("a, a, b, a, a: terminal %v, want b to reset the count", got)
	}
	if got := terminalOn("success", a, a, nil, a, a); got[4] {
		t.Fatalf("a, a, success, a, a: terminal %v, want success to reset the count", got)
	}
	other := errors.New("fetch: connection reset")
	if got := terminalOn("other", a, a, other, a, a); got[4] {
		t.Fatalf("a, a, other failure, a, a: terminal %v, want another outcome to reset the count", got)
	}
}

// Which file the error names must not depend on the order the withheld refs
// came in, or a per-file count could switch files every pass.
func TestUnconfirmedPairingErrNamesLowestPostedFile(t *testing.T) {
	for _, files := range [][]string{{"b.part2.rar", "a.part1.rar"}, {"a.part1.rar", "b.part2.rar"}} {
		msg := unconfirmedErr(files...).Error()
		if !strings.Contains(msg, `"a.part1.rar"`) {
			t.Errorf("withheld %v: error %q, want it to name a.part1.rar", files, msg)
		}
	}
}
