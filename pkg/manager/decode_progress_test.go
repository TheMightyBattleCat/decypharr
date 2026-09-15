package manager

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// The csv shapes below were captured from real probes: ffprobe 4.2.7 on the
// box (a Blu-ray AVC REMUX whose frames carry side data, and a VC-1 REMUX
// whose first frame has no timestamp) and ffprobe 8.0.1.
func TestParseDecodeProgress(t *testing.T) {
	for _, tc := range []struct {
		name           string
		stdout         string
		wantFrames     int
		wantTimestamps int
		wantLast       float64
	}{
		{"plain", "0.000000\n0.042000\n0.083000\n", 3, 3, 0.083},
		{"4.2 side data prints empty lines", "0.000000\n\n\n0.042000\n\n\n0.083000\n\n\n", 3, 3, 0.083},
		{"8.0 side data prints a trailing comma", "0.000000,\n0.042000\n0.083000\n", 3, 3, 0.083},
		{"a frame without a timestamp still counts", "N/A\n0.042000\n0.083000\n", 3, 2, 0.083},
		{"nothing decoded", "", 0, 0, 0},
		{"4.2 with the old pts_time entry printed only empty lines", "\n\n\n\n", 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := parseDecodeProgress([]byte(tc.stdout), 1)
			if p.frames != tc.wantFrames || p.timestamps != tc.wantTimestamps || p.lastTS != tc.wantLast {
				t.Fatalf("got frames=%d timestamps=%d last=%v, want frames=%d timestamps=%d last=%v",
					p.frames, p.timestamps, p.lastTS, tc.wantFrames, tc.wantTimestamps, tc.wantLast)
			}
		})
	}
}

// frameLines prints the csv a decode of [from, to) seconds at 24000/1001 fps
// produces, one timestamp per line.
func frameLines(from, to float64) string {
	var b strings.Builder
	for ts := from; ts < to; ts += 1001.0 / 24000 {
		fmt.Fprintf(&b, "%.6f\n", ts)
	}
	return b.String()
}

func TestParseDecodeProgress_FirstInterval(t *testing.T) {
	// 8.x decodes every window of a spread; the first ends at the jump.
	p := parseDecodeProgress([]byte(frameLines(0, 2)+frameLines(591.2, 593.2)+frameLines(1200, 1202)), 3)
	if p.firstFrames != 48 || p.frames != 144 {
		t.Fatalf("first=%d frames=%d, want first=48 frames=144", p.firstFrames, p.frames)
	}
	if span := p.firstEndTS - p.firstStartTS; span < 1.9 || span > 2 {
		t.Fatalf("first interval spans %.3fs, want ~1.96s", span)
	}

	// A single interval keeps every frame, whatever its timestamps do: a
	// discontinuity inside a head scan is not the end of the scan.
	p = parseDecodeProgress([]byte(frameLines(0, 100)+frameLines(5000, 5100)), 1)
	if p.firstFrames != p.frames {
		t.Fatalf("single interval: first=%d of %d frames, want all", p.firstFrames, p.frames)
	}

	// A jump back also starts the next interval.
	p = parseDecodeProgress([]byte(frameLines(600, 602)+frameLines(10, 12)), 2)
	if p.firstFrames != 48 {
		t.Fatalf("first=%d, want 48 (the jump back ends the first interval)", p.firstFrames)
	}
}

func TestIntervalLength(t *testing.T) {
	for _, tc := range []struct {
		in         string
		wantFrames int
		wantSec    float64
		wantOK     bool
	}{
		{"0%+2", 0, 2, true},
		{"0%+578", 0, 578, true},
		{"752%+#48", 48, 0, true},
		{"0%+#0", 0, 0, false},
		{"0%+-3", 0, 0, false},
		{"0%", 0, 0, false},
		{"garbage", 0, 0, false},
	} {
		frames, sec, ok := intervalLength(tc.in)
		if frames != tc.wantFrames || sec != tc.wantSec || ok != tc.wantOK {
			t.Errorf("intervalLength(%q) = (%d, %v, %v), want (%d, %v, %v)", tc.in, frames, sec, ok, tc.wantFrames, tc.wantSec, tc.wantOK)
		}
	}
}

func spreadOf(n int) []string {
	out := make([]string, n)
	for i := range n {
		out[i] = fmt.Sprintf("%d%%+2", i*400)
	}
	return out
}

func TestReachedFirstIntervalEnd(t *testing.T) {
	for _, tc := range []struct {
		name        string
		intervals   []string
		stdout      string
		wantReached bool
		wantKnown   bool
	}{
		// Brom Rain and Thunder, 2026-09-10: 13858 frames to 578.0 s.
		{"head scan decoded to its end", []string{"0%+578"}, frameLines(0, 578.05), true, true},
		{"head scan stopped short", []string{"0%+578"}, frameLines(0, 300), false, true},
		// On 4.2 only the first window prints frames: judging the spread by the
		// sum of its windows would call every healthy spread stopped short.
		{"4.2 spread: first window only", spreadOf(15), frameLines(0, 2), true, true},
		{"8.x spread: every window", spreadOf(3), frameLines(0, 2) + frameLines(400, 402) + frameLines(800, 802), true, true},
		{"8.x spread: first window stopped short", spreadOf(3), frameLines(0, 0.5) + frameLines(400, 402) + frameLines(800, 802), false, true},
		{"frame-count window decoded", []string{"0%+#48"}, "N/A\n" + frameLines(0.042, 2), true, true},
		{"frame-count window stopped short", []string{"0%+#48"}, frameLines(0, 0.8), false, true},
		{"one timestamp cannot be measured", []string{"0%+2"}, "0.000000\n", false, false},
		{"no frames at all", []string{"0%+2"}, "", false, false},
		{"no intervals", nil, frameLines(0, 2), false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := parseDecodeProgress([]byte(tc.stdout), len(tc.intervals))
			reached, known := reachedFirstIntervalEnd(tc.intervals, p)
			if reached != tc.wantReached || known != tc.wantKnown {
				t.Fatalf("got reached=%v known=%v, want reached=%v known=%v", reached, known, tc.wantReached, tc.wantKnown)
			}
		})
	}
}

func TestStderrSummary(t *testing.T) {
	stderr := "\n[matroska,webm @ 0x1] File ended prematurely\n\n[h264 @ 0x2] error while decoding MB 1 2\n"
	got, n := stderrSummary(stderr)
	if n != 2 {
		t.Fatalf("counted %d lines, want 2 (empty lines are not lines)", n)
	}
	if want := "[matroska,webm @ 0x1] File ended prematurely | [h264 @ 0x2] error while decoding MB 1 2"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}

	long := strings.Repeat("x", 3*stderrSummaryLineLen)
	many := strings.Repeat(long+"\n", stderrSummaryLines+5)
	got, n = stderrSummary(many)
	if n != stderrSummaryLines+5 {
		t.Fatalf("counted %d lines, want %d", n, stderrSummaryLines+5)
	}
	parts := strings.Split(got, " | ")
	if len(parts) != stderrSummaryLines {
		t.Fatalf("kept %d lines, want %d", len(parts), stderrSummaryLines)
	}
	for _, p := range parts {
		if len(p) != stderrSummaryLineLen {
			t.Fatalf("kept a %d-byte line, want each cut to %d", len(p), stderrSummaryLineLen)
		}
	}
}

// newScriptedFFProbe writes a fake ffprobe that prints stdout and stderr and
// exits with code, recording its arguments one per line in the returned file.
func newScriptedFFProbe(t *testing.T, stdout, stderr string, code int) (bin, argsFile string) {
	t.Helper()
	dir := t.TempDir()
	outFile, errFile := filepath.Join(dir, "stdout"), filepath.Join(dir, "stderr")
	argsFile = filepath.Join(dir, "args")
	for path, body := range map[string]string{outFile: stdout, errFile: stderr} {
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("writing fake output: %v", err)
		}
	}
	bin = filepath.Join(dir, "ffprobe")
	script := fmt.Sprintf("#!/bin/sh\nfor a in \"$@\"; do printf '%%s\\n' \"$a\"; done > %q\ncat %q\ncat %q >&2\nexit %d\n",
		argsFile, outFile, errFile, code)
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("writing fake ffprobe: %v", err)
	}
	return bin, argsFile
}

// runDecodeProbe end to end against a scripted binary: errors that did not
// stop the decode are not a verdict, errors on a decode that stopped short
// still are, and a cut body is never broken whatever was printed.
func TestRunDecodeProbe_ErrorsThatDecodedThrough(t *testing.T) {
	const eof = "[matroska,webm @ 0x55e207973e40] File ended prematurely\n"
	for _, tc := range []struct {
		name           string
		intervals      []string
		stdout, stderr string
		code           int
		spent          bool
		wantOK         bool
		wantReason     string
		wantConclusive bool
	}{
		{"clean", []string{"0%+578"}, frameLines(0, 578.05), "", 0, false, true, "", true},
		{"errors, decoded to the end", []string{"0%+578"}, frameLines(0, 578.05), eof, 0, false, true, ffprobeReasonDecodedThrough, false},
		{"errors, 4.2 spread decoded its first window", spreadOf(15), frameLines(0, 2), "[h264 @ 0x1] mmco: unref short failure\n", 0, false, true, ffprobeReasonDecodedThrough, false},
		{"errors, decode stopped short", []string{"0%+578"}, frameLines(0, 120), eof, 0, false, false, ffprobeReasonDecodeError + ": " + strings.TrimSpace(eof), true},
		{"errors, nothing decoded", []string{"0%+2"}, "", eof, 1, false, false, ffprobeReasonDecodeError + ": " + strings.TrimSpace(eof), true},
		{"non-zero exit, no stderr", []string{"0%+2"}, "", "", 1, false, false, ffprobeReasonDecodeError, true},
		{"errors on a cut body", []string{"0%+578"}, frameLines(0, 120), eof, 0, true, true, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bin, argsFile := newScriptedFFProbe(t, tc.stdout, tc.stderr, tc.code)
			f := &ffprobeChecker{binPath: bin, timeout: 5 * time.Second, baseURL: "http://127.0.0.1:1/", logger: zerolog.Nop()}
			budget := NewVerifyBudget(1 << 30)
			if tc.spent {
				budget.Add(budget.Limit() + 1)
			}

			cause := &unverifiedCause{}
			ok, reason, conclusive := f.runDecodeProbe(contextWithUnverifiedCause(context.Background(), cause), "E", "f.mkv", decodePhaseHead, tc.intervals, 5*time.Second, testBigFile, budget)
			if ok != tc.wantOK || reason != tc.wantReason || conclusive != tc.wantConclusive {
				t.Fatalf("got ok=%v reason=%q conclusive=%v, want ok=%v reason=%q conclusive=%v",
					ok, reason, conclusive, tc.wantOK, tc.wantReason, tc.wantConclusive)
			}
			if reason == ffprobeReasonDecodedThrough {
				if want := decodeErrorCause(tc.stderr); cause.decodeCause != want || cause.detail != strings.TrimSpace(tc.stderr) {
					t.Fatalf("recorded cause=%q detail=%q, want cause=%q with the stderr line", cause.decodeCause, cause.detail, want)
				}
			}

			raw, err := os.ReadFile(argsFile)
			if err != nil {
				t.Fatalf("reading recorded args: %v", err)
			}
			args := strings.Split(strings.TrimSpace(string(raw)), "\n")
			i := slices.Index(args, "-show_entries")
			if i < 0 || i+1 >= len(args) || args[i+1] != "frame=best_effort_timestamp_time" {
				t.Fatalf("args %q must ask for frame=best_effort_timestamp_time", args)
			}
		})
	}
}

var decodedThrough = probeOutcome{ok: true, reason: ffprobeReasonDecodedThrough, conclusive: false}

// Errors that decoded through are not a verdict in any phase: the check ends
// unverified, with no reason, and the verification is exhausted so no retry
// reads the file again for the same errors.
func TestDecodeWindows_DecodedThroughEndsTheVerification(t *testing.T) {
	for _, tc := range []struct {
		name       string
		fileBytes  int64
		outcomes   map[string]probeOutcome
		wantPhases []string
	}{
		{"spread", testSmall, map[string]probeOutcome{decodePhaseSpread: decodedThrough}, []string{decodePhaseSpread}},
		{"detect", testBigFile, map[string]probeOutcome{decodePhaseDetect: decodedThrough}, []string{decodePhaseDetect}},
		{"head", testBigFile, map[string]probeOutcome{decodePhaseDetect: unseekable, decodePhaseHead: decodedThrough}, []string{decodePhaseDetect, decodePhaseHead}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := verifyBudgetFor(tc.fileBytes)
			f, calls := newTreeChecker(parent, tc.outcomes)
			ok, reason, conclusive, _ := f.decodeWindows(context.Background(), "E", "f.mkv", testDuration, tc.fileBytes, parent)

			if !ok || conclusive || reason != "" {
				t.Fatalf("got ok=%v reason=%q conclusive=%v, want unverified with no reason", ok, reason, conclusive)
			}
			if got := phases(*calls); !slices.Equal(got, tc.wantPhases) {
				t.Fatalf("phases = %v, want %v", got, tc.wantPhases)
			}
			if !parent.Exceeded() || parent.Cut() {
				t.Fatalf("verification exceeded=%v cut=%v, want exhausted without a byte cut", parent.Exceeded(), parent.Cut())
			}
		})
	}
}

// The import gate stops after one pass on errors that decoded through, rather
// than sleeping out its backoffs to print the same errors twice more.
func TestVerifyImportFile_DecodedThroughIsNotRetried(t *testing.T) {
	saved := ffprobeImportRetryBackoffs
	ffprobeImportRetryBackoffs = []time.Duration{time.Millisecond, time.Millisecond}
	t.Cleanup(func() { ffprobeImportRetryBackoffs = saved })

	f, calls := newTreeChecker(nil, map[string]probeOutcome{decodePhaseSpread: decodedThrough})
	f.binPath = newFakeFFProbeBinary(t, `{"format":{"duration":"6300.000000"},"streams":[{"codec_type":"video"},{"codec_type":"audio"}]}`)
	f.baseURL = "http://127.0.0.1:1/"
	d := &Downloader{logger: zerolog.Nop()}

	ok, conclusive, reason := d.verifyImportFile(context.Background(), f, &storage.Entry{Name: "E"}, "E", "f.mkv", "import-hash",
		expectedRuntime{Seconds: 6300, EpisodeCountConfirmed: true, Bytes: testSmall})

	if !ok || conclusive || reason != "" {
		t.Fatalf("got ok=%v conclusive=%v reason=%q, want admitted but unverified", ok, conclusive, reason)
	}
	if got := phases(*calls); !slices.Equal(got, []string{decodePhaseSpread}) {
		t.Fatalf("phases = %v, want one spread (no retry)", got)
	}
}
