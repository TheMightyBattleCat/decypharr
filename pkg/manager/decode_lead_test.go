package manager

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// leadChecker fakes decode probes by phase: each outcome may note a decode
// cause on ctx the way runDecodeProbe does for errors that decoded through.
func leadChecker(outcomes map[string]struct {
	probeOutcome
	stderr string
}) (*ffprobeChecker, *[]probeCall) {
	calls := &[]probeCall{}
	f := &ffprobeChecker{timeout: 90 * time.Second, logger: zerolog.Nop()}
	f.runProbeFn = func(ctx context.Context, _, _, phase string, intervals []string, _ time.Duration, _ int64, budget *VerifyBudget) (bool, string, bool) {
		*calls = append(*calls, probeCall{phase: phase, intervals: intervals, budget: budget})
		o, found := outcomes[phase]
		if !found {
			return true, "", true
		}
		if o.stderr != "" {
			noteDecodeErrors(ctx, o.stderr, o.stderr)
		}
		return o.ok, o.reason, o.conclusive
	}
	return f, calls
}

const (
	codecStderr     = "[h264 @ 0x559b9886bd00] top block unavailable for requested intra mode -1\n[h264 @ 0x559b9886bd00] error while decoding MB 0 0, bytestream 23084"
	containerStderr = "[matroska,webm @ 0x55be85c5ff00] Element at 0x5e4055be ending at 0x3ea96b4e7 exceeds containing master element ending at 0x5e528cfe"
)

func TestDecodeWindows_CodecErrorsDecodedAgainFromEarlier(t *testing.T) {
	type outcome = struct {
		probeOutcome
		stderr string
	}
	for _, tc := range []struct {
		name           string
		outcomes       map[string]outcome
		wantPhases     []string
		wantOK         bool
		wantConclusive bool
		wantExhausted  bool
	}{
		{
			// Halvard, Bold Q, Red Fox 2, Evening Walk, Barnaby & Quill.
			name: "clean from earlier passes",
			outcomes: map[string]outcome{
				decodePhaseSpread: {decodedThrough, codecStderr},
			},
			wantPhases: []string{decodePhaseSpread, decodePhaseSpreadLead}, wantOK: true, wantConclusive: true,
		},
		{
			// La Belle Odette: the same errors from earlier.
			name: "errors again stay unverified",
			outcomes: map[string]outcome{
				decodePhaseSpread:     {decodedThrough, codecStderr},
				decodePhaseSpreadLead: {decodedThrough, codecStderr},
			},
			wantPhases: []string{decodePhaseSpread, decodePhaseSpreadLead}, wantOK: true, wantExhausted: true,
		},
		{
			name: "container errors are not re-decoded",
			outcomes: map[string]outcome{
				decodePhaseSpread: {decodedThrough, containerStderr},
			},
			wantPhases: []string{decodePhaseSpread}, wantOK: true, wantExhausted: true,
		},
		{
			name: "a decode error from earlier is a failed check",
			outcomes: map[string]outcome{
				decodePhaseSpread:     {decodedThrough, codecStderr},
				decodePhaseSpreadLead: {probeOutcome{ok: false, reason: ffprobeReasonDecodeError, conclusive: true}, ""},
			},
			wantPhases: []string{decodePhaseSpread, decodePhaseSpreadLead}, wantOK: false, wantConclusive: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := verifyBudgetFor(testSmall)
			f, calls := leadChecker(tc.outcomes)
			ctx := contextWithUnverifiedCause(context.Background(), &unverifiedCause{})
			ok, _, conclusive, _ := f.decodeWindows(ctx, "E", "f.mkv", testDuration, testSmall, budget)
			if ok != tc.wantOK || conclusive != tc.wantConclusive {
				t.Fatalf("got ok=%v conclusive=%v, want ok=%v conclusive=%v", ok, conclusive, tc.wantOK, tc.wantConclusive)
			}
			if got := phases(*calls); !slices.Equal(got, tc.wantPhases) {
				t.Fatalf("phases %v, want %v", got, tc.wantPhases)
			}
			if budget.Exceeded() != tc.wantExhausted {
				t.Fatalf("budget exceeded=%v, want %v", budget.Exceeded(), tc.wantExhausted)
			}
			if len(*calls) == 2 && (*calls)[1].budget != budget {
				t.Fatal("the lead-in pass must meter against the same file budget")
			}
		})
	}
}

func TestSpreadIntervalsLead(t *testing.T) {
	f := &ffprobeChecker{}
	const fps = 23.976
	plain := f.spreadIntervals(6000*time.Second, testSmall, fps)
	lead := f.spreadIntervalsLead(6000*time.Second, testSmall, fps, decodeSpreadLead)
	if len(plain) != len(lead) || len(plain) < 2 {
		t.Fatalf("plain %v, lead %v", plain, lead)
	}
	// Window 0 cannot start earlier; window 1 starts 5 s earlier and runs 5 s
	// longer, through the same frames.
	if lead[0] != plain[0] {
		t.Errorf("window 0: lead %q, want %q", lead[0], plain[0])
	}
	if want := decodeInterval(400*time.Second-decodeSpreadLead, ffprobeDecodeWindowSpan+decodeSpreadLead, fps); lead[1] != want || plain[1] != decodeInterval(400*time.Second, ffprobeDecodeWindowSpan, fps) {
		t.Errorf("window 1: lead %q (want %q), plain %q", lead[1], want, plain[1])
	}
}
