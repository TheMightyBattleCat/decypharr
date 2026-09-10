package manager

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// probeCall records one decode probe the runProbeFn seam intercepted.
type probeCall struct {
	phase     string
	intervals []string
	timeout   time.Duration
	budget    *VerifyBudget // the budget the probe was handed
	routed    bool          // the parent's ForRequest() returned budget while the probe ran
	startUsed int64         // bytes already charged to budget when the probe started
}

// probeOutcome is what the fake returns for one phase. cut spends the budget
// the probe was handed, standing in for the metered read path cutting the
// body mid-probe.
type probeOutcome struct {
	ok         bool
	reason     string
	conclusive bool
	cut        bool
}

// unseekable is the detect outcome for a file with no usable index: the read
// path cuts the probe before it reaches its second window, and runDecodeProbe
// reports a cut probe as inconclusive.
var unseekable = probeOutcome{ok: true, conclusive: false, cut: true}

// newTreeChecker builds a checker whose decode probes are faked, returning the
// per-phase outcome from outcomes (default: clean, conclusive, uncut). parent
// is the file budget the test hands decodeWindows; each call records whether
// the probe's budget is the one the WebDAV handler would give a range request
// made while that probe runs.
func newTreeChecker(parent *VerifyBudget, outcomes map[string]probeOutcome) (*ffprobeChecker, *[]probeCall) {
	calls := &[]probeCall{}
	f := &ffprobeChecker{
		timeout:     90 * time.Second,
		detectBytes: defaultDecodeDetectBytes,
		headBytes:   defaultDecodeHeadBytes,
		logger:      zerolog.Nop(),
	}
	f.runProbeFn = func(_ context.Context, _, _, phase string, intervals []string, timeout time.Duration, _ int64, budget *VerifyBudget) (bool, string, bool) {
		*calls = append(*calls, probeCall{
			phase:     phase,
			intervals: intervals,
			timeout:   timeout,
			budget:    budget,
			routed:    parent.ForRequest() == budget,
			startUsed: budget.Used(),
		})
		o, found := outcomes[phase]
		if !found {
			return true, "", true
		}
		if o.cut {
			budget.Add(budget.Limit() + 1)
		}
		return o.ok, o.reason, o.conclusive
	}
	return f, calls
}

func phases(calls []probeCall) []string {
	out := make([]string, 0, len(calls))
	for _, c := range calls {
		out = append(out, c.phase)
	}
	return out
}

// headSeconds parses the span of a "0%+<seconds>" head interval, failing the
// test unless the interval starts at offset 0.
func headSeconds(t *testing.T, interval string) float64 {
	t.Helper()
	if !strings.HasPrefix(interval, "0%+") {
		t.Fatalf("head interval %q must start at offset 0", interval)
	}
	sec, err := strconv.ParseFloat(strings.TrimPrefix(interval, "0%+"), 64)
	if err != nil {
		t.Fatalf("unparseable head interval %q: %v", interval, err)
	}
	return sec
}

const (
	testDuration = 105 * time.Minute // ~ a REMUX feature
	testBigFile  = int64(26) << 30   // 26 GiB, well over the 3 GiB head size
	testSmall    = int64(2) << 30    // 2 GiB, under the head size

	// wantHeadCap is the head scan's cut at the default head size.
	wantHeadCap = int64(defaultDecodeHeadBytes) / decodeHeadCapDen * decodeHeadCapNum
)

// A file no larger than the head scan costs no more to sample in full, so it
// skips detection and keeps the spread on the file budget, exactly as before.
func TestDecodeWindows_SmallFileSkipsDetection(t *testing.T) {
	parent := verifyBudgetFor(testSmall)
	f, calls := newTreeChecker(parent, nil)
	ok, reason, conclusive, coverage := f.decodeWindows(context.Background(), "E", "f.mkv", testDuration, testSmall, parent)

	if !ok || !conclusive || reason != "" {
		t.Fatalf("got ok=%v reason=%q conclusive=%v, want a clean conclusive pass", ok, reason, conclusive)
	}
	if coverage != decodeCoverageFull {
		t.Fatalf("coverage = %q, want %q", coverage, decodeCoverageFull)
	}
	if got := phases(*calls); !slices.Equal(got, []string{decodePhaseSpread}) {
		t.Fatalf("phases = %v, want exactly [spread] (detection must be skipped)", got)
	}
	if (*calls)[0].budget != parent {
		t.Fatal("the spread must meter against the file budget")
	}
}

// Detection is a byte cut. With no budget (file size unknown) nothing would
// cut a forward scanner, which would reach the window and be misread as
// seekable, so such a file keeps the plain spread.
func TestDecodeWindows_NoBudgetSkipsDetection(t *testing.T) {
	f, calls := newTreeChecker(nil, nil)
	f.decodeWindows(context.Background(), "E", "f.mkv", testDuration, testBigFile, nil)

	if got := phases(*calls); !slices.Equal(got, []string{decodePhaseSpread}) {
		t.Fatalf("phases = %v, want exactly [spread]", got)
	}
}

// When the detect probe completes inside its cut the file has a usable index,
// and the check is exactly the pre-detection one: the full spread, on the
// file budget.
func TestDecodeWindows_SeekableTakesSpreadPath(t *testing.T) {
	parent := verifyBudgetFor(testBigFile)
	f, calls := newTreeChecker(parent, nil)
	ok, _, conclusive, coverage := f.decodeWindows(context.Background(), "E", "f.mkv", testDuration, testBigFile, parent)

	if !ok || !conclusive {
		t.Fatalf("got ok=%v conclusive=%v, want a clean conclusive pass", ok, conclusive)
	}
	if coverage != decodeCoverageFull {
		t.Fatalf("coverage = %q, want %q", coverage, decodeCoverageFull)
	}
	if got := phases(*calls); !slices.Equal(got, []string{decodePhaseDetect, decodePhaseSpread}) {
		t.Fatalf("phases = %v, want [detect spread]", got)
	}
	spread := (*calls)[1]
	if n := len(spread.intervals); n != ffprobeDecodeWindowCountLarge {
		t.Fatalf("spread used %d windows, want %d (unchanged large-file behaviour)", n, ffprobeDecodeWindowCountLarge)
	}
	if spread.budget != parent {
		t.Fatal("the spread must meter against the file budget, not a phase")
	}
	if parent.Exceeded() {
		t.Fatal("a seekable file's check must leave the file budget unspent")
	}
}

// The point of the change: a file that cannot reach the detect window inside
// the cut gets ONE bounded scan from offset 0, never the whole file.
func TestDecodeWindows_UnseekableTakesBoundedHead(t *testing.T) {
	parent := verifyBudgetFor(testBigFile)
	f, calls := newTreeChecker(parent, map[string]probeOutcome{decodePhaseDetect: unseekable})
	ok, _, conclusive, coverage := f.decodeWindows(context.Background(), "E", "f.mkv", testDuration, testBigFile, parent)

	if !ok || !conclusive {
		t.Fatalf("got ok=%v conclusive=%v, want a clean conclusive head pass", ok, conclusive)
	}
	if coverage != decodeCoveragePartial {
		t.Fatalf("coverage = %q, want %q", coverage, decodeCoveragePartial)
	}
	if got := phases(*calls); !slices.Equal(got, []string{decodePhaseDetect, decodePhaseHead}) {
		t.Fatalf("phases = %v, want [detect head] (never a full spread)", got)
	}

	head := (*calls)[1]
	if len(head.intervals) != 1 {
		t.Fatalf("head used %d intervals, want exactly 1 contiguous span", len(head.intervals))
	}
	headSec := headSeconds(t, head.intervals[0])
	wantSec := testDuration.Seconds() * float64(defaultDecodeHeadBytes) / float64(testBigFile)
	if diff := headSec - wantSec; diff > 1 || diff < -1 {
		t.Fatalf("head span = %.0fs, want ~%.0fs (duration * headBytes / fileBytes)", headSec, wantSec)
	}
	if headSec >= testDuration.Seconds() {
		t.Fatalf("head span %.0fs covers the whole %.0fs duration - not bounded", headSec, testDuration.Seconds())
	}
	if parent.Exceeded() {
		t.Fatal("a head scan that reached a verdict must not exhaust the verification")
	}
}

// The detect window is placed so the read an unseekable file needs to reach it
// is detectBytes regardless of file size, and the cut sits below that read -
// at or above it, a forward scanner would reach the window uncut and be
// misread as seekable.
func TestDecodeWindows_DetectCutSitsBelowAFixedByteTarget(t *testing.T) {
	for _, tc := range []struct {
		name      string
		fileBytes int64
		duration  time.Duration
	}{
		{"3.6GB web episode", 3_870_000_000, 58 * time.Minute},
		{"14.5GB MPEG-2", 14_569_367_615, 99 * time.Minute},
		{"26GB VC-1", 26_925_104_754, 105 * time.Minute},
		{"33GB VC-1", 33_379_099_203, 124 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := verifyBudgetFor(tc.fileBytes)
			f, calls := newTreeChecker(parent, map[string]probeOutcome{decodePhaseDetect: unseekable})
			f.decodeWindows(context.Background(), "E", "f.mkv", tc.duration, tc.fileBytes, parent)

			detect := (*calls)[0]
			if detect.phase != decodePhaseDetect || len(detect.intervals) != 2 {
				t.Fatalf("first probe = %s with %d intervals, want detect with 2 (offset 0 + one seek)", detect.phase, len(detect.intervals))
			}
			if !strings.HasPrefix(detect.intervals[0], "0%+") {
				t.Fatalf("first detect window %q must be at offset 0 (needs no seek)", detect.intervals[0])
			}
			offSec, err := strconv.ParseFloat(strings.Split(strings.TrimSuffix(detect.intervals[1], "+2"), "%")[0], 64)
			if err != nil {
				t.Fatalf("unparseable detect interval %q: %v", detect.intervals[1], err)
			}
			impliedBytes := offSec / tc.duration.Seconds() * float64(tc.fileBytes)
			if ratio := impliedBytes / float64(defaultDecodeDetectBytes); ratio < 0.98 || ratio > 1.02 {
				t.Fatalf("implied read %.2f GiB, want ~%.2f GiB (fixed target regardless of file size)",
					impliedBytes/(1<<30), float64(defaultDecodeDetectBytes)/(1<<30))
			}
			if got, want := detect.budget.Limit(), int64(defaultDecodeDetectBytes)/decodeDetectCapDivisor; got != want {
				t.Fatalf("detect cut = %d bytes, want %d", got, want)
			}
			if float64(detect.budget.Limit()) >= impliedBytes {
				t.Fatalf("detect cut %d bytes >= the %.0f-byte read to reach the window: a forward scanner would never be cut",
					detect.budget.Limit(), impliedBytes)
			}
		})
	}
}

// A real decode error in the detect windows, on an intact body, settles the
// verdict; no further bytes are spent.
func TestDecodeWindows_DetectDecodeErrorIsBrokenImmediately(t *testing.T) {
	parent := verifyBudgetFor(testBigFile)
	f, calls := newTreeChecker(parent, map[string]probeOutcome{
		decodePhaseDetect: {ok: false, reason: ffprobeReasonDecodeError + ": boom", conclusive: true},
	})
	ok, reason, conclusive, _ := f.decodeWindows(context.Background(), "E", "f.mkv", testDuration, testBigFile, parent)

	if ok || !conclusive {
		t.Fatalf("got ok=%v conclusive=%v, want a conclusive broken verdict", ok, conclusive)
	}
	if !strings.HasPrefix(reason, ffprobeReasonDecodeError) {
		t.Fatalf("reason = %q, want a decode error", reason)
	}
	if got := phases(*calls); !slices.Equal(got, []string{decodePhaseDetect}) {
		t.Fatalf("phases = %v, want just [detect] - no further probing after a verdict", got)
	}
}

// A detect probe that times out or is cancelled short of its cut is not
// evidence the file cannot seek, so it must NOT fall through to the head scan
// or exhaust the verification - it is simply inconclusive (fail-open), and a
// retry may try again.
func TestDecodeWindows_DetectTimeoutProvesNothing(t *testing.T) {
	parent := verifyBudgetFor(testBigFile)
	f, calls := newTreeChecker(parent, map[string]probeOutcome{
		decodePhaseDetect: {ok: true, conclusive: false},
	})
	ok, _, conclusive, coverage := f.decodeWindows(context.Background(), "E", "f.mkv", testDuration, testBigFile, parent)

	if !ok {
		t.Fatal("ok = false; a timed-out probe must fail open")
	}
	if conclusive {
		t.Fatal("conclusive = true; a timed-out probe proved nothing")
	}
	if coverage != "" {
		t.Fatalf("coverage = %q, want empty; nothing was verified, so it must claim no coverage", coverage)
	}
	if got := phases(*calls); !slices.Equal(got, []string{decodePhaseDetect}) {
		t.Fatalf("phases = %v, want just [detect] - must not fall through to head", got)
	}
	if parent.Exceeded() {
		t.Fatal("a detect timeout must not exhaust the verification")
	}
}

// Whatever the head scan concludes is partial coverage, and an outcome that is
// not a verdict ends the verification: exhausted rather than cut, so the retry
// guards stop and budget_cut does not claim a byte cut that never happened.
func TestDecodeWindows_HeadOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name           string
		head           probeOutcome
		wantOK         bool
		wantConclusive bool
		wantExhausted  bool
	}{
		{"clean", probeOutcome{ok: true, conclusive: true}, true, true, false},
		{"decode error", probeOutcome{ok: false, reason: ffprobeReasonDecodeError, conclusive: true}, false, true, false},
		{"cut", probeOutcome{ok: true, conclusive: false, cut: true}, true, false, true},
		{"timed out", probeOutcome{ok: true, conclusive: false}, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := verifyBudgetFor(testBigFile)
			f, _ := newTreeChecker(parent, map[string]probeOutcome{
				decodePhaseDetect: unseekable,
				decodePhaseHead:   tc.head,
			})
			ok, _, conclusive, coverage := f.decodeWindows(context.Background(), "E", "f.mkv", testDuration, testBigFile, parent)

			if ok != tc.wantOK || conclusive != tc.wantConclusive {
				t.Fatalf("got ok=%v conclusive=%v, want ok=%v conclusive=%v", ok, conclusive, tc.wantOK, tc.wantConclusive)
			}
			if coverage != decodeCoveragePartial {
				t.Fatalf("coverage = %q, want %q", coverage, decodeCoveragePartial)
			}
			if parent.Exceeded() != tc.wantExhausted {
				t.Fatalf("file budget Exceeded() = %v, want %v", parent.Exceeded(), tc.wantExhausted)
			}
			if parent.Cut() {
				t.Fatal("the file budget read nothing during detection; it must never report a byte cut")
			}
		})
	}
}

// Across the REMUX size range the head scan covers a bounded prefix, and the
// read path enforces it: the most detection can pull is its two cuts, well
// short of the file.
func TestDecodeWindows_NeverReadsWholeFileWhenUnseekable(t *testing.T) {
	for _, tc := range []struct {
		name      string
		fileBytes int64
		duration  time.Duration
	}{
		{"14.5GB", 14_569_367_615, 99 * time.Minute},
		{"26GB", 26_925_104_754, 105 * time.Minute},
		{"33GB", 33_379_099_203, 124 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := verifyBudgetFor(tc.fileBytes)
			f, calls := newTreeChecker(parent, map[string]probeOutcome{decodePhaseDetect: unseekable})
			f.decodeWindows(context.Background(), "E", "f.mkv", tc.duration, tc.fileBytes, parent)

			if got := phases(*calls); !slices.Equal(got, []string{decodePhaseDetect, decodePhaseHead}) {
				t.Fatalf("phases = %v, want [detect head]", got)
			}
			detect, head := (*calls)[0], (*calls)[1]
			scanned := headSeconds(t, head.intervals[0]) / tc.duration.Seconds() * float64(tc.fileBytes)
			if scanned > float64(defaultDecodeHeadBytes)*1.02 {
				t.Fatalf("head interval spans %.2f GiB, over the %.0f GiB head size",
					scanned/(1<<30), float64(defaultDecodeHeadBytes)/(1<<30))
			}
			if head.budget.Limit() != wantHeadCap {
				t.Fatalf("head cut = %d bytes, want %d", head.budget.Limit(), wantHeadCap)
			}
			if most := detect.budget.Limit() + head.budget.Limit(); most >= tc.fileBytes {
				t.Fatalf("detect + head may read %d bytes of a %d-byte file - not bounded", most, tc.fileBytes)
			}
		})
	}
}

// The invariant that makes byte cuts safe, pinned on the two broken returns
// detection adds: whatever ffprobe reports about a body we cut, a spent phase
// budget must never become ok=false. Each cut case has a companion with the
// identical probe result on an intact body, which MUST still be broken - so
// the guard is shown to suppress our truncation, not real failures.
func TestDecodeWindows_SpentPhaseBudgetIsNeverBroken(t *testing.T) {
	decodeErr := probeOutcome{ok: false, reason: ffprobeReasonDecodeError + ": File ended prematurely", conclusive: true}
	cutDecodeErr := decodeErr
	cutDecodeErr.cut = true

	for _, tc := range []struct {
		name       string
		outcomes   map[string]probeOutcome
		wantBroken bool
	}{
		{"detect: decode error on a cut body", map[string]probeOutcome{decodePhaseDetect: cutDecodeErr}, false},
		{"detect: same error on an intact body", map[string]probeOutcome{decodePhaseDetect: decodeErr}, true},
		{"head: decode error on a cut body", map[string]probeOutcome{decodePhaseDetect: unseekable, decodePhaseHead: cutDecodeErr}, false},
		{"head: same error on an intact body", map[string]probeOutcome{decodePhaseDetect: unseekable, decodePhaseHead: decodeErr}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := verifyBudgetFor(testBigFile)
			f, _ := newTreeChecker(parent, tc.outcomes)
			ok, reason, _, _ := f.decodeWindows(context.Background(), "E", "f.mkv", testDuration, testBigFile, parent)

			if tc.wantBroken && ok {
				t.Fatal("a decode error on an intact body must be broken, got ok=true")
			}
			if !tc.wantBroken && !ok {
				t.Fatalf("a cut body must never be broken, got ok=false reason=%q", reason)
			}
		})
	}
}

// Routing a cut detect probe to the head scan is only safe because the head
// scan re-reads the detect probe's prefix: it starts at offset 0, under a
// fresh budget of its own. If either stopped being true, a real error in that
// prefix could be skipped instead of found again - so a real error there must
// come back as the verdict.
func TestDecodeWindows_DetectCutRescansFromZeroUnderAFreshBudget(t *testing.T) {
	parent := verifyBudgetFor(testBigFile)
	f, calls := newTreeChecker(parent, map[string]probeOutcome{
		decodePhaseDetect: {ok: false, reason: ffprobeReasonDecodeError + ": boom", conclusive: true, cut: true},
		decodePhaseHead:   {ok: false, reason: ffprobeReasonDecodeError + ": boom", conclusive: true},
	})
	ok, reason, conclusive, coverage := f.decodeWindows(context.Background(), "E", "f.mkv", testDuration, testBigFile, parent)

	if ok || !conclusive || !strings.HasPrefix(reason, ffprobeReasonDecodeError) {
		t.Fatalf("got ok=%v conclusive=%v reason=%q, want the head scan to find the error again", ok, conclusive, reason)
	}
	if coverage != decodeCoveragePartial {
		t.Fatalf("coverage = %q, want %q", coverage, decodeCoveragePartial)
	}
	if got := phases(*calls); !slices.Equal(got, []string{decodePhaseDetect, decodePhaseHead}) {
		t.Fatalf("phases = %v, want [detect head]", got)
	}
	detect, head := (*calls)[0], (*calls)[1]
	headSeconds(t, head.intervals[0]) // fails the test unless the scan starts at offset 0
	if head.budget == detect.budget {
		t.Fatal("the head scan reused the detect probe's spent budget")
	}
	if head.startUsed != 0 {
		t.Fatalf("the head scan started with %d bytes already charged; it must get a fresh budget", head.startUsed)
	}
	if head.budget.Limit() != wantHeadCap {
		t.Fatalf("head cut = %d bytes, want %d", head.budget.Limit(), wantHeadCap)
	}
}

// The read path finds a probe's budget through the registry, once per range
// request (VerifyBudgetForVerificationRead -> ForRequest). Each probe must
// therefore run while its own budget is what that lookup returns, and no phase
// may outlive the check.
func TestDecodeWindows_EachProbeIsWhatTheReadPathMeters(t *testing.T) {
	for name, outcomes := range map[string]map[string]probeOutcome{
		"seekable":   nil,
		"unseekable": {decodePhaseDetect: unseekable},
	} {
		t.Run(name, func(t *testing.T) {
			parent := verifyBudgetFor(testBigFile)
			f, calls := newTreeChecker(parent, outcomes)
			f.decodeWindows(context.Background(), "E", "f.mkv", testDuration, testBigFile, parent)

			if len(*calls) != 2 {
				t.Fatalf("phases = %v, want two probes", phases(*calls))
			}
			for _, c := range *calls {
				if !c.routed {
					t.Fatalf("the %s probe ran while range requests would have metered against a different budget", c.phase)
				}
			}
			if parent.ForRequest() != parent {
				t.Fatal("a phase was left open after the check returned")
			}
		})
	}
}

// Phases are independent of the file budget in both directions: detection
// charges it nothing, and a file budget smaller than a phase's cut neither
// meters nor caps that phase.
func TestDecodeWindows_PhasesDoNotDrawOnTheFileBudget(t *testing.T) {
	parent := NewVerifyBudget(64 << 20) // far below either phase's cut
	f, calls := newTreeChecker(parent, map[string]probeOutcome{decodePhaseDetect: unseekable})
	ok, _, conclusive, _ := f.decodeWindows(context.Background(), "E", "f.mkv", testDuration, testBigFile, parent)

	if !ok || !conclusive {
		t.Fatalf("got ok=%v conclusive=%v, want the head scan's clean verdict", ok, conclusive)
	}
	if parent.Used() != 0 || parent.Exceeded() {
		t.Fatalf("detection charged the file budget: used=%d exceeded=%v", parent.Used(), parent.Exceeded())
	}
	for _, c := range *calls {
		if c.budget == parent || c.budget.Limit() <= parent.Limit() {
			t.Fatalf("the %s probe was metered against, or capped by, the %d-byte file budget", c.phase, parent.Limit())
		}
	}
}

// Phases do not draw on the file budget, so a verification already spent -
// cut by an earlier pass, or exhausted by an earlier head scan - must not
// start one and read again.
func TestDecodeWindows_SpentVerificationOpensNoPhase(t *testing.T) {
	for name, spend := range map[string]func(*VerifyBudget){
		"cut":       func(b *VerifyBudget) { b.Add(b.Limit() + 1) },
		"exhausted": func(b *VerifyBudget) { b.Exhaust() },
	} {
		t.Run(name, func(t *testing.T) {
			parent := verifyBudgetFor(testBigFile)
			spend(parent)
			f, calls := newTreeChecker(parent, nil)
			ok, _, conclusive, coverage := f.decodeWindows(context.Background(), "E", "f.mkv", testDuration, testBigFile, parent)

			if !ok || conclusive || coverage != "" {
				t.Fatalf("got ok=%v conclusive=%v coverage=%q, want inconclusive with no coverage", ok, conclusive, coverage)
			}
			if len(*calls) != 0 {
				t.Fatalf("a spent verification ran %v", phases(*calls))
			}
		})
	}
}

// The phases are bounded by their cuts, which come from decode_detect_bytes
// and decode_head_bytes alone: an operator-widened repair.ffprobe_timeout must
// not widen what either may read. (Before per-phase budgets the head bound was
// a timeout derived from an assumed scan rate, and a large ffprobe_timeout
// silently lifted it.)
func TestDecodeWindows_ConfiguredSizesAloneSetThePhaseCuts(t *testing.T) {
	parent := verifyBudgetFor(testBigFile)
	f, calls := newTreeChecker(parent, map[string]probeOutcome{decodePhaseDetect: unseekable})
	f.timeout = 30 * time.Minute
	f.detectBytes = 512 << 20
	f.headBytes = 1 << 30

	f.decodeWindows(context.Background(), "E", "f.mkv", testDuration, testBigFile, parent)

	if got := phases(*calls); !slices.Equal(got, []string{decodePhaseDetect, decodePhaseHead}) {
		t.Fatalf("phases = %v, want [detect head]", got)
	}
	if got, want := (*calls)[0].budget.Limit(), int64(256<<20); got != want {
		t.Fatalf("detect cut = %d bytes, want %d (half of decode_detect_bytes)", got, want)
	}
	if got, want := (*calls)[1].budget.Limit(), int64(1<<30)/decodeHeadCapDen*decodeHeadCapNum; got != want {
		t.Fatalf("head cut = %d bytes, want %d (1.5x decode_head_bytes)", got, want)
	}
}

// The stamp is per entry and must not claim more than was read: one healthy
// file scanned as a bounded head makes the whole entry partial, and one
// healthy file with no verdict blocks the stamp entirely.
func TestRollupDecode(t *testing.T) {
	full := fileResult{healthy: true, decodeConclusive: true, decodeCoverage: decodeCoverageFull}
	partial := fileResult{healthy: true, decodeConclusive: true, decodeCoverage: decodeCoveragePartial}
	noVerdict := fileResult{healthy: true}
	broken := fileResult{broken: true, decodeCoverage: decodeCoveragePartial}

	for _, tc := range []struct {
		name         string
		results      []fileResult
		skipDecode   bool
		wantRan      bool
		wantCoverage string
	}{
		{"all full", []fileResult{full, full}, false, true, decodeCoverageFull},
		{"one partial makes the entry partial", []fileResult{full, partial}, false, true, decodeCoveragePartial},
		{"one healthy file with no verdict blocks the stamp", []fileResult{partial, noVerdict}, false, false, ""},
		{"broken files do not count", []fileResult{full, broken}, false, true, decodeCoverageFull},
		{"skipped decode stamps nothing", []fileResult{full}, true, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ran, coverage := rollupDecode(tc.results, tc.skipDecode)
			if ran != tc.wantRan || coverage != tc.wantCoverage {
				t.Fatalf("rollupDecode = (%v, %q), want (%v, %q)", ran, coverage, tc.wantRan, tc.wantCoverage)
			}
		})
	}
}

// A head scan that reaches no verdict exhausts the verification, and the
// import gate must honour that: admit-and-flag-dirty after ONE pass instead of
// paying for detect + head again on each inconclusive retry. The companion -
// the same gate when the detect probe merely timed out, which exhausts nothing
// - must still use its retries, so the stop is shown to come from the
// exhaustion.
func TestVerifyImportFile_UnverdictedHeadScanIsNotRetried(t *testing.T) {
	saved := ffprobeImportRetryBackoffs
	ffprobeImportRetryBackoffs = []time.Duration{time.Millisecond, time.Millisecond}
	t.Cleanup(func() { ffprobeImportRetryBackoffs = saved })

	for _, tc := range []struct {
		name        string
		outcomes    map[string]probeOutcome
		wantDetects int
	}{
		{
			"head scan cut: exhausted after one pass",
			map[string]probeOutcome{decodePhaseDetect: unseekable, decodePhaseHead: {ok: true, conclusive: false, cut: true}},
			1,
		},
		{
			"detect timed out: nothing exhausted, retries used",
			map[string]probeOutcome{decodePhaseDetect: {ok: true, conclusive: false}},
			1 + ffprobeImportInconclusiveRetries,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, calls := newTreeChecker(nil, tc.outcomes)
			f.binPath = newFakeFFProbeBinary(t, `{"format":{"duration":"6300.000000"},"streams":[{"codec_type":"video"},{"codec_type":"audio"}]}`)
			f.baseURL = "http://127.0.0.1:1/"
			d := &Downloader{logger: zerolog.Nop()}

			ok, conclusive, reason := d.verifyImportFile(context.Background(), f, &storage.Entry{Name: "E"}, "E", "f.mkv", "import-hash",
				expectedRuntime{Seconds: 6300, EpisodeCountConfirmed: true, Bytes: testBigFile})

			if !ok || conclusive || reason != "" {
				t.Fatalf("got ok=%v conclusive=%v reason=%q, want admitted but inconclusive", ok, conclusive, reason)
			}
			detects := 0
			for _, c := range *calls {
				if c.phase == decodePhaseDetect {
					detects++
				}
			}
			if detects != tc.wantDetects {
				t.Fatalf("detect ran %d times, want %d", detects, tc.wantDetects)
			}
		})
	}
}
