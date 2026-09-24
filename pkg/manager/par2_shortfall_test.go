package manager

import (
	"errors"
	"fmt"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// Transient causes must not reach a terminal verdict through runRepair's
// shortfall, match and backfill failures; their structural forms stay terminal.
func TestPar2TransientShortfallsStayTransient(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		terminal bool
	}{
		{"shortfall, recovery gone", par2RecoveryShortfall(2, 0, 0), true},
		{"shortfall after a timed-out article", par2RecoveryShortfall(2, 0, 1), false},
		{"no posted file matched", errors.New("no posted file matched the PAR2 recovery set"), true},
		{"no posted file matched, files skipped", errors.New("no posted file matched the PAR2 recovery set (transient: 2 posted file(s) failed to fetch/hash during matching)"), false},
		{"backfill impossible", fmt.Errorf("no PAR2 data and backfill failed: %w", fmt.Errorf("gone: %w", usenet.ErrBackfillImpossible)), true},
		{"backfill read failed", fmt.Errorf("PAR2 refs backfill did not complete: %w (transient: backfill can be retried)", errors.New("i/o error")), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := classifyPar2Failure(c.err).terminal; got != c.terminal {
				t.Fatalf("terminal = %v, want %v (%v)", got, c.terminal, c.err)
			}
		})
	}
}
