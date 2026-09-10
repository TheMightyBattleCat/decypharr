package reader

import (
	"fmt"

	"github.com/sirrobot01/decypharr/internal/nntp"
)

// partNumberTrust is how many committed articles must carry the yEnc part
// number their NZB segment number promised before a mismatch is taken to mean
// a different upload. A file whose NZB numbers segments some other way never
// gets there, so the part-number check can't reject every article of it.
const partNumberTrust = 2

// articleMismatch explains why a decoded article is not the one seg asked for,
// or returns "" when it fits.
//
// Providers can serve a different upload's article under a reused Message-ID.
// Seen live 2026-09-10 on The Gardeners S01E01 (a 3911-part FLUX post): four
// providers returned parts 143, 413 and 537 of a 630-part upload posted in
// September 2026. Such an article is complete and CRC-valid against its own
// yEnc header, so neither decoder objects, and its bytes used to be committed
// into the slot as a "short" segment and served to players.
//
// Two signals, both conservative:
//   - part number: once this file's articles have been seen to match their
//     segment numbers (numbersTrusted), a different part number is decisive;
//   - size: a part that is not its posted file's last is well under what the
//     slot reads from it. The last part is exempt because a final slot's size
//     can be an estimate.
func articleMismatch(meta *nntp.YencMetadata, seg *SegmentMeta, numbersTrusted bool) string {
	if meta == nil || seg == nil || meta.PartSize <= 0 {
		return "" // no part header to judge by
	}
	if numbersTrusted && meta.Part > 0 && seg.Number > 0 && meta.Part != int64(seg.Number) {
		return fmt.Sprintf("yEnc part %d of %d, want part %d", meta.Part, meta.Total, seg.Number)
	}

	lastPart := (meta.Total > 0 && meta.Part >= meta.Total) || (meta.Size > 0 && meta.End >= meta.Size)
	need := seg.SegmentDataStart + seg.Bytes
	// 90%: a slot's size is at most a few percent off the real part, while the
	// reused-ID parts seen live were 36% and 56% of it.
	if !lastPart && need > 0 && meta.PartSize*10 < need*9 {
		return fmt.Sprintf("yEnc part %d holds %d bytes, slot needs %d", meta.Part, meta.PartSize, need)
	}
	return ""
}
