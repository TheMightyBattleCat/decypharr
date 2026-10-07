package manager

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSpreadIntervalsShift(t *testing.T) {
	f := &ffprobeChecker{}
	const dur = 7970 * time.Second
	big := int64(ffprobeDecodeWindowLargeFileSize + 1)
	plain := f.spreadIntervals(dur, big, 0, 0)
	if len(plain) != ffprobeDecodeWindowCountLarge || plain[0] != "0%+2" || plain[1] != "1328%+2" {
		t.Fatalf("unshifted windows moved: %v", plain)
	}
	// Half a gap later: the first window stays at 0, the rest move.
	later := f.spreadIntervals(dur, big, 0, 0.4)
	if later[0] != "0%+2" || later[1] != "1860%+2" || later[5] != "7173%+2" {
		t.Fatalf("shifted windows: %v", later)
	}
	earlier := f.spreadIntervals(dur, big, 0, -0.5)
	if earlier[0] != "0%+2" || earlier[1] != "664%+2" {
		t.Fatalf("shifted windows: %v", earlier)
	}
	// A window never starts past the end of a short file.
	short := f.spreadIntervals(20*time.Second, 1, 0, 0.49)
	for _, iv := range short {
		start, _, _ := strings.Cut(iv, "%")
		if sec, err := strconv.Atoi(start); err != nil || sec > 18 {
			t.Fatalf("window starts past the file's last span: %v", short)
		}
	}
}

func TestDecodeScopeSpreadShift(t *testing.T) {
	var none *decodeScope
	if none.spreadShift() != 0 {
		t.Fatal("no scope should leave the windows in place")
	}
	s := &decodeScope{}
	first := s.spreadShift()
	if first < -0.5 || first >= 0.5 {
		t.Fatalf("shift %v out of range", first)
	}
	// The retry of a verification decodes the same windows.
	if s.spreadShift() != first {
		t.Fatal("the shift changed inside one verification")
	}
}

func TestOffPosition(t *testing.T) {
	intervals := []string{"0%+#48", "1328%+#48", "2657%+#48"}
	frames := func(ts ...string) []byte { return []byte(strings.Join(ts, "\n") + "\n") }

	// Seeks land seconds before each window; side-data lines are empty.
	clean := frames("0.000000", "0.041000", "", "1317.700000", "1329.900000,", "2650.100000", "N/A", "2659.000000")
	if off, detail := offPosition(clean, intervals); off {
		t.Fatalf("clean decode flagged: %s", detail)
	}
	// A REMUX with swapped volumes: a window 10 s into one of them came back
	// timed 70 s later, another 35 s later.
	for _, ts := range []string{"1398.290000", "1363.110000"} {
		off, detail := offPosition(frames("0.000000", ts, "2657.000000"), intervals)
		if !off || !strings.Contains(detail, "window at 1328 s") {
			t.Fatalf("frame at %s not flagged: %v %q", ts, off, detail)
		}
	}
	// Windows 100 s apart and a 70 s shift: the misplaced frames fit the next
	// window's range, but arrive before this window has printed anything.
	close := []string{"0%+#48", "3100%+#48", "3200%+#48", "3650%+#48"}
	off, detail := offPosition(frames("0.000000", "0.500000", "3170.290000", "3172.000000", "3235.110000", "3722.760000"), close)
	if !off || !strings.Contains(detail, "window at 3100 s") {
		t.Fatalf("frames that fit the next window not flagged: %v %q", off, detail)
	}
	// Frames from an earlier volume, too.
	if off, _ := offPosition(frames("0.000000", "1250.000000", "2657.000000"), intervals); !off {
		t.Fatal("a frame 78 s early not flagged")
	}
	// The last window returning the second window's frames is not "near some
	// window": each frame is held against the window being printed.
	if off, _ := offPosition(frames("0.000000", "1328.000000", "2657.000000", "1330.000000"), intervals); !off {
		t.Fatal("a frame out of window order not flagged")
	}
	// A file whose timestamps start at 600 s is in place throughout, whether
	// the windows count from its start time or from zero.
	if off, detail := offPosition(frames("600.000000", "1928.000000", "3257.000000"), intervals); off {
		t.Fatalf("a start-time offset flagged: %s", detail)
	}
	if off, detail := offPosition(frames("600.000000", "1328.000000", "2657.000000"), intervals); off {
		t.Fatalf("absolute timestamps on a file with a start time flagged: %s", detail)
	}
	// Short file, windows 88 s apart: a 35 s shift is still caught.
	short := []string{"0%+#48", "88%+#48", "176%+#48", "264%+#48"}
	if off, _ := offPosition(frames("0.000000", "88.000000", "211.000000", "264.000000"), short); !off {
		t.Fatal("a 35 s shift in a short file not flagged")
	}
	if off, detail := offPosition(frames("0.000000", "80.500000", "89.900000", "170.000000", "178.000000", "262.000000"), short); off {
		t.Fatalf("short file in place flagged: %s", detail)
	}
	// ffprobe 4.2 prints the first interval's frames only.
	if off, _ := offPosition(frames("0.000000", "0.500000", "1.900000"), intervals); off {
		t.Fatal("first-interval-only output flagged")
	}
	// One window, or no timestamps, gives nothing to compare.
	if off, _ := offPosition(frames("5000.000000"), intervals[:1]); off {
		t.Fatal("a single window flagged")
	}
	if off, _ := offPosition(frames("N/A", "N/A"), intervals); off {
		t.Fatal("frames without timestamps flagged")
	}
}

func TestFrameOffPositionLeavesFileUnverified(t *testing.T) {
	f := &ffprobeChecker{}
	intervals := []string{"0%+#48", "1328%+#48"}
	stdout := []byte("0.000000\n1474.300000\n")
	cause := &unverifiedCause{}
	ctx := contextWithUnverifiedCause(context.Background(), cause)

	// Only the spread is judged: a head scan reads forward from 0.
	if f.frameOffPosition(ctx, "e", "f.mkv", decodePhaseHead, stdout, intervals, nil) {
		t.Fatal("a head scan was judged by position")
	}
	if !f.frameOffPosition(ctx, "e", "f.mkv", decodePhaseSpread, stdout, intervals, nil) {
		t.Fatal("misplaced frames in a spread not reported")
	}
	if cause.get() != unverifiedDecodeErrors || cause.decodeCause != decodeCauseWrongPosition || cause.detail == "" {
		t.Fatalf("recorded %+v", cause)
	}
}
