package manager

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

const ntscFilm = 24000.0 / 1001

func TestVideoFrameRate(t *testing.T) {
	video := func(avg, r string) ffprobeStream {
		s := stream("video", "h264", 0)
		s.AvgFrameRate, s.RFrameRate = avg, r
		return s
	}
	cover := stream("video", "mjpeg", 1)
	cover.AvgFrameRate, cover.RFrameRate = "90000/1", "90000/1"

	for _, tc := range []struct {
		name    string
		streams []ffprobeStream
		want    float64
	}{
		// Hourcop BMF (VC-1 in VfW mode) on the production install's ffprobe 4.2.7.
		{"avg_frame_rate", []ffprobeStream{video("24000/1001", "24000/1001")}, ntscFilm},
		{"falls back to r_frame_rate", []ffprobeStream{video("0/0", "25/1")}, 25},
		{"skips cover art like V:0", []ffprobeStream{cover, video("24000/1001", "24000/1001")}, ntscFilm},
		{"audio is not video", []ffprobeStream{stream("audio", "dts", 0), video("30000/1001", "")}, 30000.0 / 1001},
		{"implausible rates are unknown", []ffprobeStream{video("1000/1", "0/0")}, 0},
		{"only the V:0 stream is read", []ffprobeStream{video("0/0", "0/0"), video("25/1", "25/1")}, 0},
		{"no video", []ffprobeStream{stream("audio", "aac", 0)}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := videoFrameRate(tc.streams); math.Abs(got-tc.want) > 1e-9 {
				t.Fatalf("videoFrameRate = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDecodeInterval(t *testing.T) {
	for _, tc := range []struct {
		start, span time.Duration
		fps         float64
		want        string
	}{
		{0, 2 * time.Second, 0, "0%+2"},
		{752 * time.Second, 2 * time.Second, ntscFilm, "752%+#48"},
		// Brom's head scan, 2026-09-10: 578.48 s.
		{0, 578480 * time.Millisecond, ntscFilm, "0%+#13870"},
		{10 * time.Second, time.Millisecond, ntscFilm, "10%+#1"},
	} {
		if got := decodeInterval(tc.start, tc.span, tc.fps); got != tc.want {
			t.Errorf("decodeInterval(%v, %v, %v) = %q, want %q", tc.start, tc.span, tc.fps, got, tc.want)
		}
	}
}

// With a frame rate every probe of the tree ends its intervals by frame count,
// so a stream whose packets carry no PTS still stops where it should: the
// detect windows, the spread and the head scan alike.
func TestDecodeWindows_FrameRateEndsEveryIntervalByFrameCount(t *testing.T) {
	window := int(math.Ceil(ffprobeDecodeWindowSpan.Seconds() * ntscFilm))

	for name, outcomes := range map[string]map[string]probeOutcome{
		"seekable":   nil,
		"unseekable": {decodePhaseDetect: unseekable},
	} {
		t.Run(name, func(t *testing.T) {
			parent := verifyBudgetFor(testBigFile)
			f, calls := newTreeChecker(parent, outcomes)
			ctx := contextWithDecodeScope(context.Background(), &decodeScope{fps: ntscFilm})
			f.decodeWindows(ctx, "E", "f.mkv", testDuration, testBigFile, parent)

			if len(*calls) != 2 {
				t.Fatalf("phases = %v, want two probes", phases(*calls))
			}
			for _, c := range *calls {
				for _, iv := range c.intervals {
					frames, _, ok := intervalLength(iv)
					if !ok || frames == 0 {
						t.Fatalf("%s interval %q is not a frame count", c.phase, iv)
					}
					if c.phase != decodePhaseHead && frames != window {
						t.Fatalf("%s interval %q, want %d frames", c.phase, iv, window)
					}
				}
			}
			if name != "unseekable" {
				return
			}
			head := (*calls)[1]
			frames, _, _ := intervalLength(head.intervals[0])
			wantSec := testDuration.Seconds() * float64(defaultDecodeHeadBytes) / float64(testBigFile)
			if got := float64(frames) / ntscFilm; math.Abs(got-wantSec) > 1 {
				t.Fatalf("head interval %q covers %.0fs, want ~%.0fs", head.intervals[0], got, wantSec)
			}
			if !strings.HasPrefix(head.intervals[0], "0%+#") {
				t.Fatalf("head interval %q must start at offset 0", head.intervals[0])
			}
		})
	}
}

// check reads the frame rate from the metadata probe, so the decode windows it
// runs are frame counts - and stay in seconds when the probe gave no rate.
func TestCheck_FrameRateFromMetadataSizesTheWindows(t *testing.T) {
	for _, tc := range []struct {
		name       string
		streams    string
		wantFrames bool
	}{
		{"rate known", `{"codec_type":"video","codec_name":"vc1","avg_frame_rate":"24000/1001","r_frame_rate":"24000/1001"}`, true},
		{"rate unknown", `{"codec_type":"video","codec_name":"vc1","avg_frame_rate":"0/0","r_frame_rate":"0/0"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, calls := newTreeChecker(nil, nil)
			f.binPath = newFakeFFProbeBinary(t, fmt.Sprintf(`{"format":{"duration":"6300.000000"},"streams":[%s,{"codec_type":"audio"}]}`, tc.streams))
			f.baseURL = "http://127.0.0.1:1/"

			ok, reason, _, _ := f.check(context.Background(), "E", "f.mkv",
				expectedRuntime{Seconds: 6300, EpisodeCountConfirmed: true, Bytes: testSmall}, false, verifyBudgetFor(testSmall))
			if !ok {
				t.Fatalf("check = broken (%s), want the fake's clean pass", reason)
			}
			if len(*calls) != 1 || len((*calls)[0].intervals) == 0 {
				t.Fatalf("probes = %v, want one spread", phases(*calls))
			}
			for _, iv := range (*calls)[0].intervals {
				frames, _, _ := intervalLength(iv)
				if (frames > 0) != tc.wantFrames {
					t.Fatalf("interval %q: frame count = %v, want %v", iv, frames > 0, tc.wantFrames)
				}
			}
		})
	}
}

func TestPacketNearEnd(t *testing.T) {
	probed := 5874 * time.Second
	for _, tc := range []struct {
		name    string
		packets []ffprobeTailPacket
		want    bool
	}{
		// VC-1 in VfW mode, as ffprobe 4.2.7 prints it for Hourcop BMF.
		{"VfW packets carry only a DTS", []ffprobeTailPacket{{PtsTime: "N/A", DtsTime: "5850.000000"}}, true},
		{"PTS near the end", []ffprobeTailPacket{{PtsTime: "5860.000000", DtsTime: "5859.958000"}}, true},
		{"PTS wins over DTS", []ffprobeTailPacket{{PtsTime: "100.000000", DtsTime: "5860.000000"}}, false},
		{"nothing near the end", []ffprobeTailPacket{{PtsTime: "N/A", DtsTime: "3000.000000"}}, false},
		{"no timestamps", []ffprobeTailPacket{{PtsTime: "N/A", DtsTime: "N/A"}}, false},
		{"no packets", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := packetNearEnd(tc.packets, probed); got != tc.want {
				t.Fatalf("packetNearEnd = %v, want %v", got, tc.want)
			}
		})
	}
}
