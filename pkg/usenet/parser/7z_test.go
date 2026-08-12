package parser

import (
	"io"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/types"
)

// TestBuildSegmentsForRARFile_OutOfOrderVolumeParts proves that
// buildSegmentsForRARFile assembles a file's segments correctly even when its
// VolumeParts are discovered out of physical order — which is exactly what
// happens once header scanning runs across all volumes concurrently instead
// of a single serial pass in volume order.
//
// Fixture: a 3-volume archive, each volume holding one 1000-byte stored part
// of a single 3000-byte file, fed to buildSegmentsForRARFile with its
// VolumeParts deliberately scrambled (PartNumber 2, 0, 1).
func TestBuildSegmentsForRARFile_OutOfOrderVolumeParts(t *testing.T) {
	p := &SevenZParser{logger: zerolog.New(io.Discard)}

	baseSegments := []storage.NZBSegment{
		{Number: 1, MessageID: "seg0-for-part0", Bytes: 1000},
		{Number: 2, MessageID: "seg1-for-part1", Bytes: 1000},
		{Number: 3, MessageID: "seg2-for-part2", Bytes: 1000},
	}
	volumeInfos := []storage.ArchiveVolumeInfo{
		{Name: "archive", Size: 3000, SegmentStart: 0, SegmentEnd: 3},
	}
	rarFileOffsets := map[string]int64{
		"archive.r00": 0,
		"archive.r01": 1000,
		"archive.r02": 2000,
	}

	rarEntry := &RARFileEntry{
		Name:             "movie.mkv",
		UncompressedSize: 3000,
		IsStored:         true,
		VolumeParts: []*types.RARVolumePart{
			{Name: "archive.r02", DataOffset: 0, PackedSize: 1000, UnpackedSize: 1000, Stored: true, PartNumber: 2},
			{Name: "archive.r00", DataOffset: 0, PackedSize: 1000, UnpackedSize: 1000, Stored: true, PartNumber: 0},
			{Name: "archive.r01", DataOffset: 0, PackedSize: 1000, UnpackedSize: 1000, Stored: true, PartNumber: 1},
		},
	}

	segments, err := p.buildSegmentsForRARFile(rarEntry, rarFileOffsets, baseSegments, volumeInfos)
	if err != nil {
		t.Fatalf("buildSegmentsForRARFile returned error: %v", err)
	}

	// (ii) full coverage: summed segment bytes must equal the advertised file size.
	var total int64
	for _, s := range segments {
		total += s.Bytes
	}
	if total != rarEntry.UncompressedSize {
		t.Fatalf("coverage gap: summed segment bytes = %d, want %d (advertised file size)", total, rarEntry.UncompressedSize)
	}

	// (i) monotonic file-offset assignment.
	for i := 1; i < len(segments); i++ {
		if segments[i].StartOffset <= segments[i-1].StartOffset {
			t.Fatalf("segment offsets not strictly monotonic at index %d: %d <= %d", i, segments[i].StartOffset, segments[i-1].StartOffset)
		}
	}

	// The decisive check. Monotonic offsets alone pass even when parts are
	// processed out of order, because buildSegmentsForRARFile assigns
	// StartOffset sequentially regardless of which physical volume it's
	// currently reading — the bug scrambles WHICH bytes land at a position,
	// not whether offsets increase. Only checking content identity at each
	// position reveals whether the geometry is actually correct. Without the
	// (PartNumber, DataOffset) sort, part 2's bytes land at file offset 0
	// instead of part 0's.
	want := []string{"seg0-for-part0", "seg1-for-part1", "seg2-for-part2"}
	if len(segments) != len(want) {
		t.Fatalf("got %d segments, want %d", len(segments), len(want))
	}
	for i, id := range want {
		if segments[i].MessageID != id {
			t.Errorf("segment %d: got MessageID %q at StartOffset %d, want %q (volume parts were not placed in PartNumber order)",
				i, segments[i].MessageID, segments[i].StartOffset, id)
		}
	}
}
