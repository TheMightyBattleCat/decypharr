package manager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// P-11: a stop at the round cap with the enlarged damaged set still inside
// the recovery budget is transient. With providers timing out, each round can
// be cut short and find a few more dead slices; 5 damaged slices against 100
// recovery slices must back off and retry, not end terminal (which on the
// urgent lane deletes and blocklists the files).
func TestPar2RoundStopAtCapWithinBudgetIsTransient(t *testing.T) {
	const k, newly, budget = 4, 1, 100
	last := maxIntactRepairRounds - 1
	timeout := errors.New("fetch segment 7: i/o timeout")

	stop, why := par2RoundShouldStop(newly, last, false, k, budget)
	if !stop || why != par2RoundStopCap {
		t.Fatalf("par2RoundShouldStop at the last round = (%v, %v), want (true, round cap)", stop, why)
	}
	if err := par2RoundStopError(newly, 0, false, k, budget, 1, 0, 0, 0, timeout, nil); err != nil {
		t.Fatalf("round 0 with the damage inside the budget stopped: %v", err)
	}

	for name, repairErr := range map[string]error{
		"timeout": timeout,
		// The last read may also have ended on a 430: classifyPar2Failure
		// checks for one before the transient tag, so it must not leak through.
		"430": fmt.Errorf("fetch segment 7: %w", &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Message: "430 no such article"}),
	} {
		err := par2RoundStopError(newly, last, false, k, budget, 1, 0, 0, 0, repairErr, nil)
		if err == nil {
			t.Fatalf("%s: round cap did not stop", name)
		}
		if !strings.Contains(err.Error(), "(transient:") {
			t.Errorf("%s: %q carries no transient tag", name, err)
		}
		if c := classifyPar2Failure(err); c.terminal || c.suspect {
			t.Errorf("%s: %q classified %+v, want transient (k+newly=%d, budget=%d)", name, err, c, k+newly, budget)
		}
	}
}

// The other stops keep their cause-labelled verdict: over budget with
// confirmed-missing slices stays terminal even at the cap, and a round that
// found nothing new is not relabelled.
func TestPar2RoundStopOverBudgetOrNothingNewKeepsVerdict(t *testing.T) {
	last := maxIntactRepairRounds - 1
	timeout := errors.New("fetch segment 7: i/o timeout")

	over := par2RoundStopError(5, last, false, 98, 100, 5, 0, 0, 0, timeout, nil)
	if over == nil || strings.Contains(over.Error(), "(transient:") {
		t.Fatalf("over budget at the cap = %v, want the cause-labelled error", over)
	}
	if c := classifyPar2Failure(over); !c.terminal {
		t.Fatalf("over budget with confirmed-missing slices %q: %+v, want terminal", over, c)
	}
	if stop, why := par2RoundShouldStop(5, 0, true, 98, 100); !stop || why != par2RoundStopOverBudget {
		t.Fatalf("over budget at round 0 = (%v, %v), want (true, over budget)", stop, why)
	}

	none := par2RoundStopError(0, last, true, 4, 100, 1, 0, 0, 0, timeout, nil)
	if none == nil || strings.Contains(none.Error(), "(transient:") {
		t.Fatalf("nothing new at the cap = %v, want the cause-labelled error", none)
	}
}

// par2RoundCutShort picks out the reads RepairWith stopped on a transport
// error, and nothing else: a slice it streamed past (430, short, corrupt), a
// checksum abort and runRepair's own classed errors are verdicts, not cuts.
func TestPar2RoundCutShort(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"nil":               {nil, false},
		"i/o timeout":       {errors.New("par2: read intact slice 7: fetch segment 7: i/o timeout"), true},
		"nntp timeout":      {fmt.Errorf("par2: read intact slice 7: fetch segment 7: %w", &nntp.Error{Type: nntp.ErrorTypeTimeout, Message: "read timeout"}), true},
		"unavailable (430)": {fmt.Errorf("2 intact slice(s) unavailable, first: %w", fmt.Errorf("%w: %w", par2.ErrSliceUnavailable, &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Message: "430"})), false},
		"checksum abort":    {fmt.Errorf("%w: 3 intact slices failed", par2.ErrChecksumMismatch), false},
		"missing fetcher":   {fmt.Errorf("par2: read intact slice 7: %w", missingFetcherErr([16]byte{1}, nil)), false},
		"transient (panic)": {fmt.Errorf("par2: read intact slice 7: %w", par2Failf(par2Transient, "reading PAR2 slice 7 panicked: x (transient: internal error)")), false},
	} {
		if got := par2RoundCutShort(tc.err); got != tc.want {
			t.Errorf("%s: par2RoundCutShort(%v) = %v, want %v", name, tc.err, got, tc.want)
		}
	}
}

// A round cut short by a transport error does not count toward
// maxIntactRepairRounds. Two cut rounds and one full read that finds new
// damage leave the loop free to go round again, so the next (good) read
// repairs it; before, the third failed round of any kind hit the cap.
func TestPar2TransportCutRoundsDoNotCountTowardCap(t *testing.T) {
	const budget = 100
	timeout := errors.New("par2: read intact slice 7: fetch segment 7: i/o timeout")
	unavailable := fmt.Errorf("1 intact slice(s) unavailable, first: par2: read intact slice 9: %w: %w",
		par2.ErrSliceUnavailable, &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Message: "430 no such article"})

	for name, seq := range map[string][]error{
		"cut, cut, full": {timeout, timeout, unavailable},
		"full, cut, cut": {unavailable, timeout, timeout},
		"cut, full, cut": {timeout, unavailable, timeout},
	} {
		var rounds par2FailedRounds
		k := 4
		for i, repairErr := range seq {
			round, cut := rounds.end(repairErr)
			if err := par2RoundStopError(1, round, cut, k, budget, 1, 0, 0, 0, repairErr, nil); err != nil {
				t.Fatalf("%s: failed round %d (cut=%v) stopped the pass with %d of %d slices: %v", name, i+1, cut, k+1, budget, err)
			}
			k++
		}
	}
}

// Both kinds stay bounded: three full reads still hit the cap whatever cuts
// came between them, and the third cut round stops on its own, transient
// while the damage fits the budget and still terminal when it does not.
func TestPar2TransportCutRoundsAreBounded(t *testing.T) {
	const budget = 100
	timeout := errors.New("par2: read intact slice 7: fetch segment 7: i/o timeout")
	unavailable := fmt.Errorf("1 intact slice(s) unavailable, first: %w", par2.ErrSliceUnavailable)

	stopAt := func(seq []error, k, newly int) (int, error) {
		var rounds par2FailedRounds
		for i, repairErr := range seq {
			round, cut := rounds.end(repairErr)
			if err := par2RoundStopError(newly, round, cut, k, budget, 1, 0, 0, 0, repairErr, nil); err != nil {
				return i + 1, err
			}
			k += newly
		}
		return 0, nil
	}

	n, err := stopAt([]error{timeout, unavailable, timeout, unavailable, unavailable, timeout}, 4, 1)
	if n != 5 || err == nil {
		t.Fatalf("full reads interleaved with cuts stopped at failed round %d (%v), want 5 (the third full read)", n, err)
	}
	if c := classifyPar2Failure(err); c.terminal || c.suspect {
		t.Errorf("round cap inside the budget %q classified %+v, want transient", err, c)
	}

	n, err = stopAt([]error{timeout, timeout, timeout, timeout}, 4, 1)
	if n != maxTransportCutRounds || err == nil {
		t.Fatalf("cut rounds stopped at failed round %d (%v), want %d", n, err, maxTransportCutRounds)
	}
	if !strings.Contains(err.Error(), "cut short by a transport error") {
		t.Errorf("cut-round stop %q does not say why it stopped", err)
	}
	if c := classifyPar2Failure(err); c.terminal || c.suspect {
		t.Errorf("cut-round stop inside the budget %q classified %+v, want transient", err, c)
	}

	n, err = stopAt([]error{timeout}, 98, 5)
	if n != 1 || err == nil {
		t.Fatalf("a cut round over budget stopped at %d (%v), want 1", n, err)
	}
	if c := classifyPar2Failure(err); !c.terminal {
		t.Errorf("a cut round over budget with confirmed-missing slices %q classified %+v, want terminal", err, c)
	}
}

// Row 73u: a round cut short by one article's fetch timeout (fetchSegment's
// par2ArticleFetchTimeout) leaves context.DeadlineExceeded in repairErr. When
// that round goes over budget with confirmed-missing slices and the job is
// still live, the stop is terminal like any over-budget stop; a deadline or
// cancel of the job itself stays transient, and so do the in-budget cut cap
// and a round that found nothing new.
func TestPar2RoundStopPerArticleDeadlineOverBudget(t *testing.T) {
	articleDeadline := fmt.Errorf("par2: read intact slice 56: fetch segment 3: %w", context.DeadlineExceeded)
	const k, budget = 98, 100

	over := par2RoundStopError(5, 0, true, k, budget, 5, 0, 0, 0, articleDeadline, nil)
	if over == nil {
		t.Fatal("over budget did not stop")
	}
	for _, want := range []string{"confirmed missing across every provider", "context deadline exceeded"} {
		if !strings.Contains(over.Error(), want) {
			t.Errorf("over-budget stop %q lost %q", over, want)
		}
	}
	if c := classifyPar2Failure(over); !c.terminal {
		t.Errorf("over budget with confirmed-missing slices, job live: %q classified %+v, want terminal", over, c)
	}

	for name, jobErr := range map[string]error{"job deadline": context.DeadlineExceeded, "job cancelled": context.Canceled} {
		err := par2RoundStopError(5, 0, true, k, budget, 5, 0, 0, 0, articleDeadline, jobErr)
		if c := classifyPar2Failure(err); c.terminal || c.suspect {
			t.Errorf("%s: %q classified %+v, want transient", name, err, c)
		}
	}

	inBudget := par2RoundStopError(1, maxTransportCutRounds-1, true, 4, budget, 1, 0, 0, 0, articleDeadline, nil)
	if inBudget == nil {
		t.Fatal("in-budget cut cap did not stop")
	}
	if c := classifyPar2Failure(inBudget); c.terminal || c.suspect {
		t.Errorf("in-budget cut cap %q classified %+v, want transient", inBudget, c)
	}

	nothingNew := par2RoundStopError(0, 0, true, 4, budget, 0, 0, 0, 0, articleDeadline, nil)
	if c := classifyPar2Failure(nothingNew); nothingNew == nil || c.terminal || c.suspect {
		t.Errorf("nothing new %v classified %+v, want transient", nothingNew, c)
	}
}
