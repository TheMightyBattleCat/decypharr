package manager

import (
	"context"
	"slices"
	"testing"
)

// brokenVerdict is a decode error on an intact body: a real, conclusive verdict.
var brokenVerdict = probeOutcome{ok: false, reason: "ffprobe_decode_error: corrupt", conclusive: true}

// twoPasses runs decodeWindows twice under one context, the way
// checkConfirmed's retry does, and returns the phases both passes ran.
func twoPasses(t *testing.T, ctx context.Context, outcomes map[string]probeOutcome) []string {
	t.Helper()
	parent := verifyBudgetFor(testBigFile)
	f, calls := newTreeChecker(parent, outcomes)
	for range 2 {
		f.decodeWindows(ctx, "E", "f.mkv", testDuration, testBigFile, parent)
	}
	return phases(*calls)
}

// A retry of a broken head-scan verdict goes straight back to the head scan:
// the first pass already paid the detect cut to learn the file cannot seek.
func TestDecodeWindows_RetryReusesUnseekable(t *testing.T) {
	ctx := contextWithDecodeScope(context.Background(), &decodeScope{})
	got := twoPasses(t, ctx, map[string]probeOutcome{
		decodePhaseDetect: unseekable,
		decodePhaseHead:   brokenVerdict,
	})
	want := []string{decodePhaseDetect, decodePhaseHead, decodePhaseHead}
	if !slices.Equal(got, want) {
		t.Fatalf("phases = %v, want %v (the retry must not detect again)", got, want)
	}
}

// A retry of a broken spread verdict goes straight back to the spread.
func TestDecodeWindows_RetryReusesSeekable(t *testing.T) {
	ctx := contextWithDecodeScope(context.Background(), &decodeScope{})
	got := twoPasses(t, ctx, map[string]probeOutcome{decodePhaseSpread: brokenVerdict})
	want := []string{decodePhaseDetect, decodePhaseSpread, decodePhaseSpread}
	if !slices.Equal(got, want) {
		t.Fatalf("phases = %v, want %v (the retry must not detect again)", got, want)
	}
}

// A decode error inside the detect probe classifies nothing, so the retry
// detects again rather than guessing which branch the file belongs to.
func TestDecodeWindows_DetectErrorIsNotRemembered(t *testing.T) {
	ctx := contextWithDecodeScope(context.Background(), &decodeScope{})
	got := twoPasses(t, ctx, map[string]probeOutcome{decodePhaseDetect: brokenVerdict})
	want := []string{decodePhaseDetect, decodePhaseDetect}
	if !slices.Equal(got, want) {
		t.Fatalf("phases = %v, want %v", got, want)
	}
}

// Without a scope nothing carries between calls: each detects afresh.
func TestDecodeWindows_NoScopeDetectsEveryPass(t *testing.T) {
	got := twoPasses(t, context.Background(), map[string]probeOutcome{
		decodePhaseDetect: unseekable,
		decodePhaseHead:   brokenVerdict,
	})
	want := []string{decodePhaseDetect, decodePhaseHead, decodePhaseDetect, decodePhaseHead}
	if !slices.Equal(got, want) {
		t.Fatalf("phases = %v, want %v", got, want)
	}
}

func stream(codecType, codec string, attachedPic int) ffprobeStream {
	var s ffprobeStream
	s.CodecType, s.CodecName, s.Disposition.AttachedPic = codecType, codec, attachedPic
	return s
}

func TestCoverArtDecoders(t *testing.T) {
	cases := []struct {
		name    string
		streams []ffprobeStream
		want    []string
	}{
		{"no cover art", []ffprobeStream{stream("video", "h264", 0), stream("audio", "dts", 0)}, nil},
		{"matroska cover", []ffprobeStream{stream("video", "h264", 0), stream("audio", "dts", 0), stream("video", "mjpeg", 1)}, []string{"mjpeg"}},
		{"two covers", []ffprobeStream{stream("video", "h264", 0), stream("video", "mjpeg", 1), stream("video", "png", 1)}, []string{"mjpeg", "png"}},
		// The cover shares the video's decoder, so its errors cannot be told
		// apart from the video's and nothing may be ignored.
		{"cover shares the video codec", []ffprobeStream{stream("video", "mjpeg", 0), stream("video", "mjpeg", 1)}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := coverArtDecoders(tc.streams)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for _, name := range tc.want {
				if !got[name] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestDropDecoderLines(t *testing.T) {
	cover := map[string]bool{"mjpeg": true}
	cases := []struct {
		name, stderr, kept, dropped string
		decoders                    map[string]bool
	}{
		{
			name:     "only the cover's error",
			stderr:   "[mjpeg @ 0x55fb671ac080] bits 239 is invalid\n",
			decoders: cover,
			dropped:  "[mjpeg @ 0x55fb671ac080] bits 239 is invalid",
		},
		{
			name:     "a video error beside the cover's",
			stderr:   "[mjpeg @ 0x55fb671ac080] bits 239 is invalid\n[h264 @ 0x5622da6b4f00] error while decoding MB 12 30",
			decoders: cover,
			kept:     "[h264 @ 0x5622da6b4f00] error while decoding MB 12 30",
			dropped:  "[mjpeg @ 0x55fb671ac080] bits 239 is invalid",
		},
		{
			name:     "a demuxer error is never the cover's",
			stderr:   "[matroska,webm @ 0x55e40f1cda80] File ended prematurely at pos. 4254545059 (0xfd97c1a3)",
			decoders: cover,
			kept:     "[matroska,webm @ 0x55e40f1cda80] File ended prematurely at pos. 4254545059 (0xfd97c1a3)",
		},
		{
			name:     "an unprefixed line is kept",
			stderr:   "Invalid data found when processing input",
			decoders: cover,
			kept:     "Invalid data found when processing input",
		},
		{
			name:   "no cover art keeps everything",
			stderr: "[mjpeg @ 0x55fb671ac080] bits 239 is invalid",
			kept:   "[mjpeg @ 0x55fb671ac080] bits 239 is invalid",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kept, dropped := dropDecoderLines(tc.stderr, tc.decoders)
			if kept != tc.kept || dropped != tc.dropped {
				t.Fatalf("got kept=%q dropped=%q, want kept=%q dropped=%q", kept, dropped, tc.kept, tc.dropped)
			}
		})
	}
}
