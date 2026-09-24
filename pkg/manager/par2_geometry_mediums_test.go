package manager

import (
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// earlyDamagedSliceCheck gates a terminal verdict before any recovery is
// fetched, so it must stay a LOWER bound. On estimated refs a dead article
// really inside one slice can straddle two in the estimate; such files no
// longer count.
func TestEarlyDamagedSliceCheckIsALowerBoundOnEstimatedRefs(t *testing.T) {
	const sliceSize = 64
	content := postedContent(256) // 4 slices; 8 real 32-byte articles
	var slices [][]byte
	for off := 0; off < len(content); off += sliceSize {
		slices = append(slices, content[off:off+sliceSize])
	}
	fileID := [16]byte{0x0c}
	_, ref := uniformPosting(content, 32, []int64{33, 32, 32, 32, 31, 32, 32, 32})
	ref.Size = int64(len(content)) // unique-length match, as MatchFiles' free path
	idx := buildSingleFileIndexWithIFSC(t, sliceSize, fileID, ref.Name, slices)

	// Article 1 is dead: really [32,64), all inside slice 0.
	pending := map[string][]overlay.DeadSegment{ref.Name: {{Index: 1, MessageID: ref.Segments[1].MessageID, Bytes: 32}}}
	trueSlices, _ := idx.DamagedSlices(fileID, 32, 64)
	if early := earlyDamagedSliceCheck(idx, pending, []storage.PostedFileRef{ref}, nil, zerolog.Nop()); early > len(trueSlices) {
		t.Errorf("earlyDamagedSliceCheck = %d over the true %d damaged slices", early, len(trueSlices))
	}

	// With exact (measured) refs the file counts as before.
	exact := ref
	exact.Segments = append([]storage.Par2SegmentRef(nil), ref.Segments...)
	for i := range exact.Segments {
		exact.Segments[i].Bytes = 32
		exact.Segments[i].Real = true
	}
	if early := earlyDamagedSliceCheck(idx, pending, []storage.PostedFileRef{exact}, nil, zerolog.Nop()); early != len(trueSlices) {
		t.Errorf("exact refs: earlyDamagedSliceCheck = %d, want %d", early, len(trueSlices))
	}
}
