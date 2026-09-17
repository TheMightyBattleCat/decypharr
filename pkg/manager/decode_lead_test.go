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

// detectLeadOutcome is one faked detect-path probe: its result, what it
// prints (noted as decode errors), a cut, or a reason it notes itself.
type detectLeadOutcome struct {
	probeOutcome
	stderr string
	note   string
}

func detectLeadChecker(outcomes map[string]detectLeadOutcome) (*ffprobeChecker, *[]probeCall) {
	calls := &[]probeCall{}
	f := &ffprobeChecker{timeout: 90 * time.Second, detectBytes: defaultDecodeDetectBytes, headBytes: defaultDecodeHeadBytes, logger: zerolog.Nop()}
	f.runProbeFn = func(ctx context.Context, _, _, phase string, intervals []string, _ time.Duration, _ int64, budget *VerifyBudget) (bool, string, bool) {
		*calls = append(*calls, probeCall{phase: phase, intervals: intervals, budget: budget})
		o, found := outcomes[phase]
		if !found {
			return true, "", true
		}
		if o.cut {
			budget.Add(budget.Limit() + 1)
		}
		if o.stderr != "" {
			noteDecodeErrors(ctx, o.stderr, o.stderr)
		}
		if o.note != "" {
			noteUnverified(ctx, o.note)
		}
		return o.ok, o.reason, o.conclusive
	}
	return f, calls
}

// Codec errors in the detect phase (Emberly, Constitution Day Reckonings) are
// decoded again from earlier, like the spread's.
func TestDecodeWindows_DetectCodecErrorsDecodedAgainFromEarlier(t *testing.T) {
	codecThrough := detectLeadOutcome{probeOutcome: decodedThrough, stderr: codecStderr}
	for _, tc := range []struct {
		name           string
		outcomes       map[string]detectLeadOutcome
		wantPhases     []string
		wantOK         bool
		wantConclusive bool
		wantReason     string // recorded unverified reason, "" for a verdict
		wantCause      string
	}{
		{
			name:       "clean from earlier goes on to the spread",
			outcomes:   map[string]detectLeadOutcome{decodePhaseDetect: codecThrough},
			wantPhases: []string{decodePhaseDetect, decodePhaseDetectLead, decodePhaseSpread}, wantOK: true, wantConclusive: true,
		},
		{
			name:       "errors again stay unverified",
			outcomes:   map[string]detectLeadOutcome{decodePhaseDetect: codecThrough, decodePhaseDetectLead: codecThrough},
			wantPhases: []string{decodePhaseDetect, decodePhaseDetectLead}, wantOK: true,
			wantReason: unverifiedDecodeErrors, wantCause: decodeCauseCodec,
		},
		{
			name: "container errors are not re-decoded",
			outcomes: map[string]detectLeadOutcome{
				decodePhaseDetect: {probeOutcome: decodedThrough, stderr: containerStderr},
			},
			wantPhases: []string{decodePhaseDetect}, wantOK: true,
			wantReason: unverifiedDecodeErrors, wantCause: decodeCauseContainer,
		},
		{
			name: "a decode error from earlier is a failed check",
			outcomes: map[string]detectLeadOutcome{
				decodePhaseDetect:     codecThrough,
				decodePhaseDetectLead: {probeOutcome: probeOutcome{ok: false, reason: ffprobeReasonDecodeError, conclusive: true}},
			},
			wantPhases: []string{decodePhaseDetect, decodePhaseDetectLead}, wantOK: false, wantConclusive: true,
		},
		{
			name: "a cut lead-in keeps the first finding",
			outcomes: map[string]detectLeadOutcome{
				decodePhaseDetect:     codecThrough,
				decodePhaseDetectLead: {probeOutcome: unseekable},
			},
			wantPhases: []string{decodePhaseDetect, decodePhaseDetectLead}, wantOK: true,
			wantReason: unverifiedDecodeErrors, wantCause: decodeCauseCodec,
		},
		{
			name: "a timed-out lead-in keeps the first finding",
			outcomes: map[string]detectLeadOutcome{
				decodePhaseDetect:     codecThrough,
				decodePhaseDetectLead: {probeOutcome: probeOutcome{ok: true, conclusive: false}, note: unverifiedTimeout},
			},
			wantPhases: []string{decodePhaseDetect, decodePhaseDetectLead}, wantOK: true,
			wantReason: unverifiedDecodeErrors, wantCause: decodeCauseCodec,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := verifyBudgetFor(testBigFile)
			f, calls := detectLeadChecker(tc.outcomes)
			cause := &unverifiedCause{}
			ctx := contextWithUnverifiedCause(context.Background(), cause)
			ok, _, conclusive, _ := f.decodeWindows(ctx, "E", "f.mkv", testDuration, testBigFile, budget)
			if ok != tc.wantOK || conclusive != tc.wantConclusive {
				t.Fatalf("got ok=%v conclusive=%v, want ok=%v conclusive=%v", ok, conclusive, tc.wantOK, tc.wantConclusive)
			}
			if got := phases(*calls); !slices.Equal(got, tc.wantPhases) {
				t.Fatalf("phases %v, want %v", got, tc.wantPhases)
			}
			if tc.wantReason != "" && (cause.reason != tc.wantReason || cause.decodeCause != tc.wantCause) {
				t.Fatalf("recorded %q/%q, want %q/%q", cause.reason, cause.decodeCause, tc.wantReason, tc.wantCause)
			}
			if ok && conclusive && cause.reason != "" {
				t.Fatalf("a passed check still records %q", cause.reason)
			}
			if len(*calls) >= 2 {
				detect, lead := (*calls)[0].intervals, (*calls)[1].intervals
				if len(detect) != 2 || len(lead) != 2 || lead[0] != detect[0] || lead[1] == detect[1] {
					t.Fatalf("detect %v, lead %v: want window 0 unchanged and window 1 moved earlier", detect, lead)
				}
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
