package reader

import (
	"testing"

	"github.com/sirrobot01/decypharr/internal/nntp"
)

func TestArticleMismatch(t *testing.T) {
	// The Gardeners S01E01 FLUX: 3911 parts of 716800 bytes, slice 72 = part 73.
	slot := &SegmentMeta{Number: 73, Bytes: 716800}
	right := &nntp.YencMetadata{Part: 73, Total: 3911, Size: 2803161788, Offset: 51609600, PartSize: 716800, End: 52326400}
	// What four providers served for that Message-ID on 2026-09-10.
	reused := &nntp.YencMetadata{Part: 143, Total: 630, Size: 215599104, Offset: 48567296, PartSize: 258048, End: 48825344}
	// A reused-ID part that happens to be the slot's size.
	reusedSameSize := &nntp.YencMetadata{Part: 143, Total: 630, Size: 451609600, Offset: 101929600, PartSize: 716800, End: 102646400}

	tests := []struct {
		name       string
		meta       *nntp.YencMetadata
		seg        *SegmentMeta
		trusted    bool
		wantReject bool
	}{
		{name: "the right part", meta: right, seg: slot, trusted: true},
		{name: "reused ID, smaller part, numbering not yet trusted", meta: reused, seg: slot, wantReject: true},
		{name: "reused ID, smaller part, numbering trusted", meta: reused, seg: slot, trusted: true, wantReject: true},
		{name: "reused ID, same size, numbering trusted", meta: reusedSameSize, seg: slot, trusted: true, wantReject: true},
		{name: "part number differs before numbering is trusted", meta: reusedSameSize, seg: slot},
		{
			name:    "last part shorter than an estimated final slot",
			meta:    &nntp.YencMetadata{Part: 3911, Total: 3911, Size: 2803161788, Offset: 2802688000, PartSize: 473788, End: 2803161788},
			seg:     &SegmentMeta{Number: 3911, Bytes: 520000},
			trusted: true,
		},
		{
			name:    "slot reading past an archive header",
			meta:    &nntp.YencMetadata{Part: 1, Total: 70, Size: 50000000, Offset: 0, PartSize: 716800, End: 716800},
			seg:     &SegmentMeta{Number: 1, SegmentDataStart: 20, Bytes: 716780},
			trusted: true,
		},
		{
			name:    "part a few percent under its slot",
			meta:    &nntp.YencMetadata{Part: 73, Total: 3911, Size: 2803161788, Offset: 51609600, PartSize: 700000, End: 52309600},
			seg:     slot,
			trusted: true,
		},
		{
			name:    "single-part post",
			meta:    &nntp.YencMetadata{Size: 300000, PartSize: 300000, End: 300000},
			seg:     &SegmentMeta{Number: 1, Bytes: 300000},
			trusted: true,
		},
		{name: "no yEnc headers", meta: nil, seg: slot, trusted: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason := articleMismatch(tt.meta, tt.seg, tt.trusted)
			if got := reason != ""; got != tt.wantReject {
				t.Fatalf("articleMismatch rejected=%v (reason %q), want rejected=%v", got, reason, tt.wantReject)
			}
		})
	}
}
