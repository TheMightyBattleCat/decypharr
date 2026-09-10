package manager

import (
	"strings"
	"testing"
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
			p := parseDecodeProgress([]byte(tc.stdout))
			if p.frames != tc.wantFrames || p.timestamps != tc.wantTimestamps || p.lastTS != tc.wantLast {
				t.Fatalf("got frames=%d timestamps=%d last=%v, want frames=%d timestamps=%d last=%v",
					p.frames, p.timestamps, p.lastTS, tc.wantFrames, tc.wantTimestamps, tc.wantLast)
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
