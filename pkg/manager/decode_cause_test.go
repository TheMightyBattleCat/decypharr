package manager

import (
	"context"
	"testing"
)

func first[A, B any](a A, _ B) A { return a }

// Stderr from sweeps on a production install 2026-09-11..15, one or more files per case.
func TestDecodeErrorCause(t *testing.T) {
	for _, tc := range []struct {
		name, stderr, want string
	}{
		{"mmco after a seek (All Still on the Eastern Ridge)",
			"[h264 @ 0x564e76149400] mmco: unref short failure\n", decodeCauseSeekWarnings},
		{"mmco and reference frames",
			"[h264 @ 0x1] mmco: unref short failure\n[h264 @ 0x1] number of reference frames (0+4) exceeds max (3; probably corrupt input), discarding one\n", decodeCauseSeekWarnings},
		{"repeated mmco", "[h264 @ 0x1] mmco: unref short failure\nLast message repeated 2 times\n", decodeCauseSeekWarnings},
		{"MPEG-2 after a seek (Enemy of the State)", "[mpeg2video @ 0x1] ignoring pic cod ext after 0\n", decodeCauseSeekWarnings},
		{"VC-1 after a seek (Rumbles 2)",
			"[vc1 @ 0x55f85e5fef40] warning: first frame is no keyframe\n[vc1 @ 0x55f85e815d40] warning: first frame is no keyframe\n", decodeCauseSeekWarnings},
		{"corrupt article ended the stream (Tower of Coins)",
			"[http @ 0x55f90bb7d940] Stream ends prematurely at 9110903722, should be 27728652537\n[matroska,webm @ 0x55f90bb7cfc0] Read error\n", decodeCauseStreamEnded},
		{"zeros mid-file (Lakeland S02E03)",
			"[matroska,webm @ 0x1] 0x00 at pos 142022280 (0x8771688) invalid as first byte of an EBML number\n[h264 @ 0x2] error while decoding MB 12 40, bytestream -5\n", decodeCauseZeroFilled},
		{"file ended (Tide on Sark S02, one RAR volume served)",
			"[matroska,webm @ 0x5615e7f3a180] File ended prematurely\n", decodeCauseFileEnded},
		{"bad element length (Finding Ada S01E01)",
			"[matroska,webm @ 0x1] Length 5 indicated by an EBML number's first byte 0x0f at pos 459760094 (0x1b6761de) exceeds max length 4.\nTruncating packet of size 1500868329 to 602053385\n", decodeCauseContainer},
		{"packet runs past the data (Lamplight head scan)", "Truncating packet of size 156817019 to 251983\n", decodeCauseContainer},
		{"slice errors (Emberly)",
			"[h264 @ 0x1] top block unavailable for requested intra mode\n[h264 @ 0x1] error while decoding MB 0 0, bytestream 49028\n", decodeCauseCodec},
		{"a seek warning beside a codec error is not only seek warnings",
			"[h264 @ 0x1] mmco: unref short failure\n[h264 @ 0x1] error while decoding MB 0 0, bytestream 49028\n", decodeCauseCodec},
		{"HEVC PPS change (La Belle Odette)", "[hevc @ 0x1] PPS changed between slices.\nLast message repeated 6 times\n", decodeCauseCodec},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := decodeErrorCause(tc.stderr); got != tc.want {
				t.Fatalf("cause = %q, want %q", got, tc.want)
			}
		})
	}
}

// The cause and detail recorded with decoded_with_errors reach the stored
// record, and a later note with another reason drops them.
func TestNoteDecodeErrors(t *testing.T) {
	const line = "[http @ 0x1] Stream ends prematurely at 10, should be 20"
	cause := &unverifiedCause{}
	ctx := contextWithUnverifiedCause(context.Background(), cause)
	noteDecodeErrors(ctx, line+"\n", line)
	noteUnverified(ctx, unverifiedDecodeErrors) // decodeWindows notes the reason again
	if cause.get() != unverifiedDecodeErrors || cause.decodeCause != decodeCauseStreamEnded || cause.detail != line {
		t.Fatalf("after the second note: %+v", *cause)
	}

	files := unverifiedFiles(nil, []fileResult{{name: "a.mkv", healthy: true, unverifiedReason: cause.get(),
		unverifiedCause: cause.decodeCause, unverifiedDetail: cause.detail}})
	if len(files) != 1 || files[0].Cause != decodeCauseStreamEnded || files[0].Detail != line {
		t.Fatalf("stored %+v", files)
	}

	noteUnverified(ctx, unverifiedTimeout)
	if cause.decodeCause != "" || cause.detail != "" {
		t.Fatalf("a timeout kept the decode-error cause: %+v", *cause)
	}
	noteDecodeErrors(context.Background(), "x", "x") // no holder: must not panic
}
