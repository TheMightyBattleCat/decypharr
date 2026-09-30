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

// A "parse PAR2 index" failure whose cause is a packet MD5 mismatch
// (par2.walkPackets on a mis-served/mis-decoded volume article) is
// transient - a bare re-fetch can clear it - even though "parse PAR2
// index" itself is in par2TerminalSubstrings.
func TestClassifyPar2FailurePacketMD5MismatchIsTransient(t *testing.T) {
	err := errors.New(`parse PAR2 index: par2: 243c38ce.vol000+001.par2: par2: packet MD5 mismatch at offset 0 (type "RecvSlic")`)
	if class := classifyPar2Failure(err); class.terminal {
		t.Errorf("classifyPar2Failure(packet MD5 mismatch).terminal = true, want false (transient)")
	}
}

// A genuinely structural parse failure with no MD5-mismatch substring
// stays terminal via "parse PAR2 index".
func TestClassifyPar2FailureStructuralParseStaysTerminal(t *testing.T) {
	for _, err := range []error{
		errors.New("parse PAR2 index: par2: no Main packet found"),
		errors.New("parse PAR2 index: par2: no PAR2 packets found in any source"),
	} {
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
	for _, err := range []error{
		errMoreDamageThanRecorded(12, 8, 1<<20),
		errMoreDamageThanRecorded(par2.MaxRepairSlices+1, par2.MaxRepairSlices+10, 1<<20),
		errMoreDamageThanRecorded(319, 25, 1<<20),
		errMoreDamageThanRecorded(120, 200, 10<<20),
	} {
		if class := classifyPar2Failure(err); !class.terminal {
			t.Errorf("classifyPar2Failure(%q).terminal = false, want true", err)
		}
	}
}

// The message names only the limit that was exceeded, so a posting short of
// recovery data doesn't read as a repair-cap problem.
func TestErrMoreDamageThanRecordedNamesTheBindingLimit(t *testing.T) {
	maxSlices := par2.MaxRepairSlices
	const mib = 1 << 20
	tenMiBLimit := par2.MaxRepairSlicesFor(10 * mib)
	memMiB := par2.MaxAccumulatorMemory >> 20
	cases := []struct {
		damaged   int
		available uint32
		sliceSize int64
		want      string
	}{
		{48, 31, mib, "more damage than recorded; 48 damaged slices, only 31 recovery slices retained"},
		{maxSlices + 1, uint32(maxSlices + 10), mib, fmt.Sprintf("more damage than recorded; %d damaged slices is over the %d-slice repair cap (%d recovery slices retained)", maxSlices+1, maxSlices, maxSlices+10)},
		{319, 25, mib, fmt.Sprintf("more damage than recorded; 319 damaged slices, only 25 recovery slices retained (also over the %d-slice repair cap)", maxSlices)},
		// A 10 MiB REMUX slice is bound by repair memory, not the slice cap.
		{tenMiBLimit + 1, 200, 10 * mib, fmt.Sprintf("more damage than recorded; %d damaged slices is over the %d-slice limit for 10.0 MiB slices (%d MiB repair memory) (200 recovery slices retained)", tenMiBLimit+1, tenMiBLimit, memMiB)},
	}
	for _, c := range cases {
		if got := errMoreDamageThanRecorded(c.damaged, c.available, c.sliceSize).Error(); got != c.want {
			t.Errorf("errMoreDamageThanRecorded(%d, %d, %d) = %q, want %q", c.damaged, c.available, c.sliceSize, got, c.want)
		}
	}
}

// Large slices are bound by repair memory before the slice cap.
func TestMaxRepairSlicesForBindsOnMemory(t *testing.T) {
	const mib = 1 << 20
	if got := par2.MaxRepairSlicesFor(mib); got != par2.MaxRepairSlices {
		t.Errorf("MaxRepairSlicesFor(1 MiB) = %d, want the slice cap %d", got, par2.MaxRepairSlices)
	}
	if got, want := par2.MaxRepairSlicesFor(10*mib), int(par2.MaxAccumulatorMemory/(10*mib)); got != want {
		t.Errorf("MaxRepairSlicesFor(10 MiB) = %d, want %d", got, want)
	}
	if got := par2.MaxRepairSlicesFor(0); got != par2.MaxRepairSlices {
		t.Errorf("MaxRepairSlicesFor(0) = %d, want the slice cap %d", got, par2.MaxRepairSlices)
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
