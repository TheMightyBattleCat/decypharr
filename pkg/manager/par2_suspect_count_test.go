package manager

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// The suspect limit counts suspect failures only: two timeouts must not make
// the first suspect failure terminal, while par2SuspectAttemptLimit suspect
// failures still do.
func TestSuspectLimitCountsOnlySuspectFailures(t *testing.T) {
	p, _ := newTestPar2Repair(t)
	suspect := errors.New("repair: 3 intact slice(s) decoded shorter than their recorded size: 3 intact slice(s) unavailable, more than the 2 spare recovery slices can cover")
	if !classifyPar2Failure(suspect).suspect {
		t.Fatal("precondition: suspect error")
	}

	const id = "nzb-suspect"
	p.recordPar2Outcome(id, fmt.Errorf("repair: %w", context.DeadlineExceeded), 0)
	p.recordPar2Outcome(id, fmt.Errorf("fetch interrupted: %w", context.Canceled), 0)
	if class := p.recordPar2Outcome(id, suspect, 0); class.terminal {
		t.Fatalf("first suspect failure after two transient ones went terminal: %q", class.reason)
	}

	const id2 = "nzb-suspect-repeat"
	var last par2FailureClass
	for i := 0; i < par2SuspectAttemptLimit; i++ {
		last = p.recordPar2Outcome(id2, suspect, 0)
	}
	if !last.terminal {
		t.Fatalf("%d suspect failures in a row did not go terminal", par2SuspectAttemptLimit)
	}

	// Success clears the state; with none recorded it is a no-op.
	p.recordPar2Outcome("nzb-never-failed", nil, 0)
	p.recordPar2Outcome(id2, nil, 0)
	if st, err := p.manager.storage.GetPar2RepairState(id2); err == nil && st != nil {
		t.Fatal("state not cleared after success")
	}
}
