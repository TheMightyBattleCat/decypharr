package manager

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// countingFFProbe writes a fake ffprobe whose first run prints failStderr and
// exits 1, and whose later runs print a healthy 1-hour probe. It returns the
// checker and a func reporting how many times the binary ran.
func countingFFProbe(t *testing.T, failStderr string) (*ffprobeChecker, func() int) {
	t.Helper()
	dir := t.TempDir()
	counter := filepath.Join(dir, "count")
	script := "#!/bin/sh\n" +
		"n=$(cat " + counter + " 2>/dev/null || echo 0)\n" +
		"n=$((n+1))\n" +
		"echo $n > " + counter + "\n" +
		"if [ $n -eq 1 ]; then\n" +
		"cat >&2 <<'EOF'\n" + failStderr + "\nEOF\n" +
		"exit 1\n" +
		"fi\n" +
		"cat <<'EOF'\n" + `{"format":{"duration":"3600.000000"},"streams":[{"codec_type":"video"},{"codec_type":"audio"}]}` + "\nEOF\n"
	bin := filepath.Join(dir, "ffprobe")
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	f := &ffprobeChecker{
		binPath:      bin,
		timeout:      5 * time.Second,
		baseURL:      "http://127.0.0.1:1/",
		logger:       zerolog.Nop(),
		tailIntactFn: func(context.Context, string, string, time.Duration) bool { return true },
	}
	runs := func() int {
		b, _ := os.ReadFile(counter)
		n, _ := strconv.Atoi(strings.TrimSpace(string(b)))
		return n
	}
	return f, runs
}

// A transport error behind already-sent headers ends the body short; ffprobe
// then fails as "unreadable". That read failure is retried like any other
// broken verdict instead of blocklisting a good release on the spot.
func TestCheckConfirmedRetriesAFailedRead(t *testing.T) {
	f, runs := countingFFProbe(t, "[http @ 0x1] Stream ends prematurely at 1048576, should be 4294967296\nhttp://127.0.0.1/x.mkv: Input/output error")
	ok, reason, _, _ := f.checkConfirmed(context.Background(), "Some.Show.S01E01", "episode.mkv",
		expectedRuntime{Seconds: 3600, Bytes: 4 << 30}, true, nil, nil)
	if !ok {
		t.Fatalf("a failed read was not retried: ok=false reason=%q runs=%d", reason, runs())
	}
	if runs() < 2 {
		t.Fatalf("ffprobe ran %d time(s), want a retry", runs())
	}
}

// A container ffprobe cannot parse is still broken without a retry.
func TestCheckConfirmedUnparseableContainerNotRetried(t *testing.T) {
	f, runs := countingFFProbe(t, "episode.mkv: Invalid data found when processing input")
	ok, reason, _, _ := f.checkConfirmed(context.Background(), "Some.Show.S01E01", "episode.mkv",
		expectedRuntime{Seconds: 3600, Bytes: 4 << 30}, true, nil, nil)
	if ok || !strings.HasPrefix(reason, ffprobeReasonUnreadable) {
		t.Fatalf("ok=%v reason=%q, want an unreadable verdict", ok, reason)
	}
	if runs() != 1 {
		t.Fatalf("ffprobe ran %d times, want no retry for a parse failure", runs())
	}
}
