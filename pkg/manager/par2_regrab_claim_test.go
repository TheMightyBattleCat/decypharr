package manager

import (
	"strings"
	"testing"
)

// "Repair now" is refused while an automatic re-grab holds the entry: the
// re-grab is about to delete it, and overwriting its claim let the re-grab's
// release free the pass's claim, so a second re-grab could delete the entry
// under the running pass.
func TestPar2RunNowRefusedWhileRegrabClaimed(t *testing.T) {
	p, repair := newTestPar2Repair(t)
	release, ok := repair.handlers.TryAcquireRegrab("nzb1")
	if !ok {
		t.Fatal("TryAcquireRegrab refused on an empty registry")
	}
	defer release()

	err := p.RunNow("nzb1")
	if err == nil {
		p.wg.Wait()
		t.Fatal("RunNow started a pass while a re-grab held the entry")
	}
	if !strings.Contains(err.Error(), "re-grab") {
		t.Fatalf("RunNow error = %q, want it to say a re-grab is handling the entry", err)
	}
	if kind, terminal, exists := repair.handlers.State("nzb1"); !exists || terminal || kind != handlerRegrab {
		t.Fatalf("claim after refused RunNow = (%q, terminal=%v, exists=%v), want the re-grab claim untouched", kind, terminal, exists)
	}
	p.mu.Lock()
	_, queued := p.queued["nzb1"]
	p.mu.Unlock()
	if queued {
		t.Fatal("refused RunNow left the entry in p.queued")
	}
}

// A re-grab's release frees only its own claim. A PAR2 claim taken over it
// meanwhile (a manual override) survives, so no second re-grab can claim
// the entry while that pass runs.
func TestRegrabReleaseKeepsLivePar2Claim(t *testing.T) {
	for _, tc := range []struct {
		name        string
		wasTerminal bool
	}{
		{"fresh claim", false},
		{"taken over a terminal mark", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, repair := newTestPar2Repair(t)
			reg := repair.handlers
			if tc.wasTerminal {
				reg.MarkTerminal("nzb1")
			}
			release, ok := reg.TryAcquireRegrab("nzb1")
			if !ok {
				t.Fatal("TryAcquireRegrab refused")
			}
			reg.Set("nzb1", handlerPar2Running)
			release()
			kind, terminal, exists := reg.State("nzb1")
			if !exists || terminal || kind != handlerPar2Running {
				t.Fatalf("after re-grab release: (%q, terminal=%v, exists=%v), want the live par2_running claim kept", kind, terminal, exists)
			}
			if _, ok := reg.TryAcquireRegrab("nzb1"); ok {
				t.Fatal("a second re-grab claimed the entry under the running pass")
			}
		})
	}
}

// ReleaseRegrab clears a re-grab claim and nothing else.
func TestReleaseRegrabOnlyReleasesRegrab(t *testing.T) {
	_, repair := newTestPar2Repair(t)
	reg := repair.handlers

	reg.Set("a", handlerRegrab)
	reg.ReleaseRegrab("a")
	if _, _, exists := reg.State("a"); exists {
		t.Fatal("ReleaseRegrab left a re-grab claim in place")
	}

	reg.Set("b", handlerPar2Running)
	reg.ReleaseRegrab("b")
	if kind, _, exists := reg.State("b"); !exists || kind != handlerPar2Running {
		t.Fatalf("ReleaseRegrab freed a PAR2 claim: (%q, exists=%v)", kind, exists)
	}

	reg.MarkTerminal("c")
	reg.ReleaseRegrab("c")
	if !reg.IsTerminal("c") {
		t.Fatal("ReleaseRegrab cleared a terminal mark")
	}
}
