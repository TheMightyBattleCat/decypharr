package manager

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

func TestPar2BackoffDurationDoublesAndCaps(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 0},
		{1, 5 * time.Minute},
		{2, 10 * time.Minute},
		{3, 20 * time.Minute},
		{4, 40 * time.Minute},
		{100, par2BackoffCap},
	}
	for _, c := range cases {
		if got := par2BackoffDuration(c.attempt); got != c.want {
			t.Errorf("par2BackoffDuration(%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}

func TestClassifyPar2FailureContextIsTransient(t *testing.T) {
	for _, err := range []error{
		context.DeadlineExceeded,
		context.Canceled,
		fmt.Errorf("fetch segment: %w", context.DeadlineExceeded),
	} {
		if class := classifyPar2Failure(err); class.terminal {
			t.Errorf("classifyPar2Failure(%v).terminal = true, want false (transient)", err)
		}
	}
}

func TestClassifyPar2FailureChecksumMismatchIsTerminal(t *testing.T) {
	err := fmt.Errorf("repair: %w", par2.ErrChecksumMismatch)
	class := classifyPar2Failure(err)
	if !class.terminal {
		t.Fatalf("classifyPar2Failure(checksum mismatch).terminal = false, want true")
	}
	if class.reason == "" {
		t.Fatalf("classifyPar2Failure(checksum mismatch).reason is empty")
	}
}

func TestClassifyPar2FailureArticleNotFoundIsTerminal(t *testing.T) {
	nntpErr := &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430}
	err := fmt.Errorf("repair: par2: read intact slice 42: %w", nntpErr)
	class := classifyPar2Failure(err)
	if !class.terminal {
		t.Fatalf("classifyPar2Failure(article not found).terminal = false, want true")
	}
	if class.reason == "" {
		t.Fatalf("classifyPar2Failure(article not found).reason is empty")
	}
}

func TestClassifyPar2FailureNoDataIsTerminal(t *testing.T) {
	cases := []error{
		errors.New("no PAR2 data available"),
		errors.New("no PAR2 data and backfill failed: some reason"),
		errors.New("no PAR2 recovery volumes retained"),
		errors.New("failed to fetch any PAR2 file"),
		errors.New("no posted file matched the PAR2 recovery set"),
	}
	for _, err := range cases {
		if class := classifyPar2Failure(err); !class.terminal {
			t.Errorf("classifyPar2Failure(%v).terminal = false, want true", err)
		}
	}
}

func TestClassifyPar2FailureUnknownDefaultsTransient(t *testing.T) {
	err := errors.New("some unexpected network hiccup")
	if class := classifyPar2Failure(err); class.terminal {
		t.Errorf("classifyPar2Failure(unrecognized error).terminal = true, want false (default to transient)")
	}
}

func TestClassifyPar2FailureMoreDamageThanRecordedIsTerminal(t *testing.T) {
	err := fmt.Errorf("more damage than recorded; 12 slices unrecoverable (recovery cap 64, 8 slices retained)")
	if class := classifyPar2Failure(err); !class.terminal {
		t.Errorf("classifyPar2Failure(more damage than recorded).terminal = false, want true")
	}
}

// A "no posted-file fetcher" / "not part of any matched posted file"
// failure that runRepair tagged "(transient: ...)" - the file was in
// Par2Source but a fetch/hash error during matching left it uncovered
// this pass - must NOT be terminal, even though the untagged variants of
// both strings are in par2TerminalSubstrings.
func TestClassifyPar2FailureTransientTagOverridesTerminalSubstring(t *testing.T) {
	cases := []error{
		errors.New(`no posted-file fetcher for file 0a1b (transient: posted file "vol.007" failed to fetch/hash during matching)`),
		errors.New(`dead segment <x@y> (file "show.mkv") is not part of any matched posted file (transient: posted file "vol.007" failed to fetch/hash during matching)`),
	}
	for _, err := range cases {
		if class := classifyPar2Failure(err); class.terminal {
			t.Errorf("classifyPar2Failure(%v).terminal = true, want false (transient tag)", err)
		}
	}
}

// The untagged (structural) variant stays terminal.
func TestClassifyPar2FailureStructuralNoFetcherStaysTerminal(t *testing.T) {
	err := errors.New(`no posted-file fetcher for file 0a1b (no retained posted file for FileDesc "o5.7z.010" (len 51200000))`)
	if class := classifyPar2Failure(err); !class.terminal {
		t.Errorf("classifyPar2Failure(structural no-fetcher).terminal = false, want true")
	}
}
