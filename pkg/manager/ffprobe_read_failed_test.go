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

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// countingFFProbe writes a fake ffprobe whose first run prints failStderr and
// exits 1, and whose later runs print a healthy 1-hour probe. It returns the
// checker and a func reporting how many times the binary ran.
func countingFFProbe(t *testing.T, failStderr string) (*ffprobeChecker, func() int) {
	t.Helper()
	return failingFFProbe(t, failStderr, 1)
}

// failingFFProbe is countingFFProbe with the first failRuns runs failing.
func failingFFProbe(t *testing.T, failStderr string, failRuns int) (*ffprobeChecker, func() int) {
	t.Helper()
	dir := t.TempDir()
	counter := filepath.Join(dir, "count")
	script := "#!/bin/sh\n" +
		"n=$(cat " + counter + " 2>/dev/null || echo 0)\n" +
		"n=$((n+1))\n" +
		"echo $n > " + counter + "\n" +
		"if [ $n -le " + strconv.Itoa(failRuns) + " ]; then\n" +
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

// ffprobe's stderr when the WebDAV body ends at byte 0 because no news server
// could be reached: headers sent, first article never fetched.
const outageStderr = "[http @ 0x1] Stream ends prematurely at 0, should be 4294967296\nhttp://127.0.0.1/x.mkv: Input/output error"

// A read that fails on the retry too, with no news server reporting an
// article missing, says nothing about the file: inconclusive, not broken.
// Broken would delete and blocklist every file a sweep probes during an
// outage.
func TestCheckConfirmedFailedReadTwiceIsInconclusive(t *testing.T) {
	f, runs := failingFFProbe(t, outageStderr, 1000)
	cause := &unverifiedCause{}
	ctx := contextWithUnverifiedCause(context.Background(), cause)
	ok, reason, conclusive, _ := f.checkConfirmed(ctx, "Some.Show.S01E01", "episode.mkv",
		expectedRuntime{Seconds: 3600, Bytes: 4 << 30}, true, NewDeadSegmentSignal(), nil)
	if !ok || conclusive || reason != "" {
		t.Fatalf("got ok=%v conclusive=%v reason=%q, want inconclusive (ok=true, conclusive=false, no reason)", ok, conclusive, reason)
	}
	if runs() != 2 {
		t.Fatalf("ffprobe ran %d time(s), want the first pass and one retry", runs())
	}
	if got := cause.get(); got != unverifiedInconclusive {
		t.Fatalf("unverified reason = %q, want %q", got, unverifiedInconclusive)
	}
}

// The same failed read with a confirmed 430 seen while it ran is broken:
// the dead-segment signal still overrides.
func TestCheckConfirmedFailedReadWithDeadSegmentIsBroken(t *testing.T) {
	f, runs := failingFFProbe(t, outageStderr, 1000)
	sig := NewDeadSegmentSignal()
	sig.Trip()
	ok, reason, conclusive, _ := f.checkConfirmed(context.Background(), "Some.Show.S01E01", "episode.mkv",
		expectedRuntime{Seconds: 3600, Bytes: 4 << 30}, true, sig, nil)
	if ok || reason != ffprobeReasonDeadSegment || !conclusive {
		t.Fatalf("got ok=%v conclusive=%v reason=%q, want broken %q", ok, conclusive, reason, ffprobeReasonDeadSegment)
	}
	if runs() != 1 {
		t.Fatalf("ffprobe ran %d times, want no retry once a 430 is seen", runs())
	}
}

// The import gate rejects and re-grabs only on ok=false. A file whose every
// read failed during an outage is admitted unverified instead (the gate then
// flags the entry for the next sweep), so nothing is deleted or blocklisted.
func TestVerifyImportFile_OutageReadFailureIsAdmitted(t *testing.T) {
	saved := ffprobeImportRetryBackoffs
	ffprobeImportRetryBackoffs = []time.Duration{time.Millisecond, time.Millisecond}
	t.Cleanup(func() { ffprobeImportRetryBackoffs = saved })

	f, runs := failingFFProbe(t, outageStderr, 1000)
	d := &Downloader{logger: zerolog.Nop()}
	ok, conclusive, reason := d.verifyImportFile(context.Background(), f, &storage.Entry{Name: "E"}, "E", "f.mkv", "outage-hash",
		expectedRuntime{Seconds: 3600, Bytes: 4 << 30})
	if !ok || conclusive || reason != "" {
		t.Fatalf("got ok=%v conclusive=%v reason=%q, want admitted but unverified", ok, conclusive, reason)
	}
	// Each of the 1+ffprobeImportInconclusiveRetries passes is a first read
	// and a retry.
	if want := 2 * (1 + ffprobeImportInconclusiveRetries); runs() != want {
		t.Fatalf("ffprobe ran %d times, want %d", runs(), want)
	}
}

// decodeFailingFFProbe writes a fake ffprobe whose metadata probe (the run
// with -show_format) prints a healthy 1-hour probe, and whose decode probes
// print two frames, then decodeStderr, and exit 1: a decode window whose body
// stopped short. It returns the checker and a func reporting how many decode
// probes ran.
func decodeFailingFFProbe(t *testing.T, decodeStderr string) (*ffprobeChecker, func() int) {
	t.Helper()
	dir := t.TempDir()
	counter := filepath.Join(dir, "decodes")
	script := "#!/bin/sh\n" +
		"for a in \"$@\"; do\n" +
		"if [ \"$a\" = \"-show_format\" ]; then\n" +
		"cat <<'EOF'\n" + `{"format":{"duration":"3600.000000"},"streams":[{"codec_type":"video"},{"codec_type":"audio"}]}` + "\nEOF\n" +
		"exit 0\n" +
		"fi\n" +
		"done\n" +
		"n=$(cat " + counter + " 2>/dev/null || echo 0)\n" +
		"echo $((n+1)) > " + counter + "\n" +
		"printf '0.000000\\n0.041708\\n'\n" +
		"cat >&2 <<'EOF'\n" + decodeStderr + "\nEOF\n" +
		"exit 1\n"
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

// ffprobe's stderr when a decode window's body stops short mid-file because
// no news server can be reached: the metadata probe read fine, then the
// fetch for a window's articles failed behind already-sent headers.
const decodeOutageStderr = "[http @ 0x1] Stream ends prematurely at 1073741824, should be 4294967296\n[matroska,webm @ 0x2] Read error at pos. 1073741824 (0x40000000)\nhttp://127.0.0.1/x.mkv: Input/output error"

// A decode window whose read fails on both passes, with no news server
// reporting an article missing, says nothing about the file: inconclusive,
// like the same failure in the metadata probe. Broken would delete and
// blocklist every file a sweep decodes during an outage.
func TestCheckConfirmedDecodeWindowFailedReadTwiceIsInconclusive(t *testing.T) {
	f, decodes := decodeFailingFFProbe(t, decodeOutageStderr)
	cause := &unverifiedCause{}
	ctx := contextWithUnverifiedCause(context.Background(), cause)
	ok, reason, conclusive, _ := f.checkConfirmed(ctx, "Some.Show.S01E01", "episode.mkv",
		expectedRuntime{Seconds: 3600, Bytes: 4 << 30}, false, NewDeadSegmentSignal(), nil)
	if !ok || conclusive || reason != "" {
		t.Fatalf("got ok=%v conclusive=%v reason=%q, want inconclusive (ok=true, conclusive=false, no reason)", ok, conclusive, reason)
	}
	if decodes() != 2 {
		t.Fatalf("decode probe ran %d time(s), want the first pass and one retry", decodes())
	}
	if got := cause.get(); got != unverifiedInconclusive {
		t.Fatalf("unverified reason = %q, want %q", got, unverifiedInconclusive)
	}
}

// The same failed decode read with a confirmed 430 seen while it ran is
// broken: the dead-segment signal still overrides.
func TestCheckConfirmedDecodeWindowFailedReadWithDeadSegmentIsBroken(t *testing.T) {
	f, decodes := decodeFailingFFProbe(t, decodeOutageStderr)
	sig := NewDeadSegmentSignal()
	sig.Trip()
	ok, reason, conclusive, _ := f.checkConfirmed(context.Background(), "Some.Show.S01E01", "episode.mkv",
		expectedRuntime{Seconds: 3600, Bytes: 4 << 30}, false, sig, nil)
	if ok || reason != ffprobeReasonDeadSegment || !conclusive {
		t.Fatalf("got ok=%v conclusive=%v reason=%q, want broken %q", ok, conclusive, reason, ffprobeReasonDeadSegment)
	}
	if decodes() != 1 {
		t.Fatalf("decode probe ran %d times, want no retry once a 430 is seen", decodes())
	}
}

// A decode window stopped by a codec error, with no failed read, is still
// broken after the retry: only a read that failed is let off.
func TestCheckConfirmedDecodeWindowCodecErrorStillBroken(t *testing.T) {
	f, decodes := decodeFailingFFProbe(t, "[h264 @ 0x1] Invalid NAL unit size (1234 > 567).\n[h264 @ 0x1] Error splitting the input into NAL units.")
	ok, reason, conclusive, _ := f.checkConfirmed(context.Background(), "Some.Show.S01E01", "episode.mkv",
		expectedRuntime{Seconds: 3600, Bytes: 4 << 30}, false, NewDeadSegmentSignal(), nil)
	if ok || !conclusive || !strings.HasPrefix(reason, ffprobeReasonDecodeError) || strings.Contains(reason, ffprobeReadFailedTag) {
		t.Fatalf("got ok=%v conclusive=%v reason=%q, want a broken %q verdict", ok, conclusive, reason, ffprobeReasonDecodeError)
	}
	if decodes() != 2 {
		t.Fatalf("decode probe ran %d time(s), want the first pass and one retry", decodes())
	}
}
