package manager

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/internal/nntp"
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

	stop, why := par2RoundShouldStop(newly, last, k, budget)
	if !stop || why != par2RoundStopCap {
		t.Fatalf("par2RoundShouldStop at the last round = (%v, %v), want (true, round cap)", stop, why)
	}
	if err := par2RoundStopError(newly, 0, k, budget, 1, 0, 0, 0, timeout); err != nil {
		t.Fatalf("round 0 with the damage inside the budget stopped: %v", err)
	}

	for name, repairErr := range map[string]error{
		"timeout": timeout,
		// The last read may also have ended on a 430: classifyPar2Failure
		// checks for one before the transient tag, so it must not leak through.
		"430": fmt.Errorf("fetch segment 7: %w", &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Message: "430 no such article"}),
	} {
		err := par2RoundStopError(newly, last, k, budget, 1, 0, 0, 0, repairErr)
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

	over := par2RoundStopError(5, last, 98, 100, 5, 0, 0, 0, timeout)
	if over == nil || strings.Contains(over.Error(), "(transient:") {
		t.Fatalf("over budget at the cap = %v, want the cause-labelled error", over)
	}
	if c := classifyPar2Failure(over); !c.terminal {
		t.Fatalf("over budget with confirmed-missing slices %q: %+v, want terminal", over, c)
	}
	if stop, why := par2RoundShouldStop(5, 0, 98, 100); !stop || why != par2RoundStopOverBudget {
		t.Fatalf("over budget at round 0 = (%v, %v), want (true, over budget)", stop, why)
	}

	none := par2RoundStopError(0, last, 4, 100, 1, 0, 0, 0, timeout)
	if none == nil || strings.Contains(none.Error(), "(transient:") {
		t.Fatalf("nothing new at the cap = %v, want the cause-labelled error", none)
	}
}
