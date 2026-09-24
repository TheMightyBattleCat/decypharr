package manager

import (
	"errors"
	"testing"
)

// The "par2 repair unavailable" line carries the failure class, including a
// suspect failure promoted to terminal after repeats (never logged before).
func TestPar2FailureClassName(t *testing.T) {
	if got := classifyPar2Failure(errors.New("something transient")).name(); got != "transient" {
		t.Errorf("transient: %q", got)
	}
	if got := classifyPar2Failure(errors.New("failed to fetch any PAR2 file")).name(); got != "terminal" {
		t.Errorf("terminal: %q", got)
	}
	suspect := errors.New("repair: 3 intact slice(s) decoded shorter than their recorded size")
	if got := par2OutcomeClass(suspect, 1).name(); got != "suspect" {
		t.Errorf("suspect: %q", got)
	}
	if got := par2OutcomeClass(suspect, par2SuspectAttemptLimit).name(); got != "terminal_after_suspect" {
		t.Errorf("promoted: %q", got)
	}
}
