package manager

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

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
