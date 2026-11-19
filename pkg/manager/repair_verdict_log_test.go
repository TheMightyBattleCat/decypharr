package manager

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
)

// A sweep that skips the decode windows and whose metadata read failed
// verified nothing now; the log must say so, not "decode verified earlier".
func TestLogFileVerdictSkipDecodeReadFailed(t *testing.T) {
	cases := []struct {
		name       string
		res        fileResult
		wantNote   string
		wantMsg    string
		notWantSub string
	}{
		{"read failed", fileResult{protocol: config.ProtocolNZB, healthy: true, readFailed: true}, "read failed, not verified", "inconclusive", "decode verified earlier"},
		{"read fine", fileResult{protocol: config.ProtocolNZB, healthy: true}, "decode verified earlier", "healthy", "read failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			r := &Repair{logger: zerolog.New(&buf)}
			r.logFileVerdict("entry", "f.mkv", 0, nil, 0, tc.res, true)
			out := buf.String()
			if !strings.Contains(out, tc.wantNote) || !strings.Contains(out, `"message":"`+tc.wantMsg+`"`) {
				t.Fatalf("log = %s, want note %q and message %q", out, tc.wantNote, tc.wantMsg)
			}
			if strings.Contains(out, tc.notWantSub) {
				t.Fatalf("log = %s, must not contain %q", out, tc.notWantSub)
			}
		})
	}
}
