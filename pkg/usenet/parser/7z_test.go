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

// TestObfuscated7zVolumeOrder_StitchesContentOrder proves the fix for
// obfuscated multi-volume RAR archives embedded in a 7z posting: when the
// physical order of the RAR volumes within the 7z (the order
// processRARFilesFromPositions discovers them in, and stamps VolumeParts.PartNumber
// with) differs from their TRUE content order, the volume-order recovery +
// renumber step must correct PartNumber to content order BEFORE
// buildSegmentsForRARFile sorts and stitches — otherwise the sort is keyed on
// posting order and the assembled file is silently corrupt (same segment
// count and total size, wrong byte order; e.g. an MKV that fails EBML
// parsing at import).
//
// Fixture: 3 physical volumes (archive.r00/r01/r02, offsets 0/1000/2000 in
// the 7z), each holding one 1000-byte stored part of a single 3000-byte file.
// Their RAR5 main-header volume numbers (as parsed by parseRAR5Headers, here
// supplied directly as volEntry.num/hasNum to avoid hand-rolling binary RAR5
// header bytes) report the TRUE content order as r01, r02, r00 — i.e.
// physical volume 0 (r00) is actually the LAST volume of the archive.
//
// This exercises the exact call sequence 7z.go's processRARFilesFromPositions
// performs at runtime: resolveVolumeOrder -> buildInversePermutation ->
// renumberVolumeParts -> buildSegmentsForRARFile.
func TestObfuscated7zVolumeOrder_StitchesContentOrder(t *testing.T) {
	rp := &RARParser{logger: zerolog.New(io.Discard)}

	baseSegments := []storage.NZBSegment{
		{Number: 1, MessageID: "seg-for-content-C", Bytes: 1000}, // physical vol0 (r00) payload
		{Number: 2, MessageID: "seg-for-content-A", Bytes: 1000}, // physical vol1 (r01) payload
		{Number: 3, MessageID: "seg-for-content-B", Bytes: 1000}, // physical vol2 (r02) payload
	}
	volumeInfos := []storage.ArchiveVolumeInfo{
		{Name: "archive", Size: 3000, SegmentStart: 0, SegmentEnd: 3},
	}
	rarFileOffsets := map[string]int64{
		"archive.r00": 0,
		"archive.r01": 1000,
		"archive.r02": 2000,
	}

	// VolumeParts as parseRAR5Headers would stamp them: PartNumber = physical
	// posting index (0, 1, 2), i.e. the pre-fix state — content order not yet
	// applied.
	rarEntry := &RARFileEntry{
		Name:             "movie.mkv",
		UncompressedSize: 3000,
		IsStored:         true,
		VolumeParts: []*types.RARVolumePart{
			{Name: "archive.r00", DataOffset: 0, PackedSize: 1000, UnpackedSize: 1000, Stored: true, PartNumber: 0},
			{Name: "archive.r01", DataOffset: 0, PackedSize: 1000, UnpackedSize: 1000, Stored: true, PartNumber: 1},
			{Name: "archive.r02", DataOffset: 0, PackedSize: 1000, UnpackedSize: 1000, Stored: true, PartNumber: 2},
		},
	}

	// Main-header volume numbers recovered per physical volume: r00's header
	// says it's true volume 2 (last), r01 says true volume 0 (first), r02
	// says true volume 1 (middle) — physical order [r00,r01,r02], content
	// order [r01,r02,r00].
	volEntries := []volEntry{
		{idx: 0, num: 2, hasNum: true}, // r00
		{idx: 1, num: 0, hasNum: true}, // r01
		{idx: 2, num: 1, hasNum: true}, // r02
	}

	volumeOrder := rp.resolveVolumeOrder(volEntries, 3)
	if volumeOrder == nil {
		t.Fatalf("resolveVolumeOrder returned nil; expected a non-identity permutation to be recovered from the scrambled volume numbers")
	}
	wantOrder := []int{1, 2, 0}
	if len(volumeOrder) != len(wantOrder) {
		t.Fatalf("volumeOrder = %v, want %v", volumeOrder, wantOrder)
	}
	for i := range wantOrder {
		if volumeOrder[i] != wantOrder[i] {
			t.Fatalf("volumeOrder = %v, want %v", volumeOrder, wantOrder)
		}
	}

	inverse := buildInversePermutation(volumeOrder, 3)
	if inverse == nil {
		t.Fatalf("buildInversePermutation returned nil for a valid permutation %v", volumeOrder)
	}

	rp.renumberVolumeParts([]*RARFileEntry{rarEntry}, inverse)

	segments, err := (&SevenZParser{logger: zerolog.New(io.Discard)}).buildSegmentsForRARFile(rarEntry, rarFileOffsets, baseSegments, volumeInfos)
	if err != nil {
		t.Fatalf("buildSegmentsForRARFile returned error: %v", err)
	}

	want := []string{"seg-for-content-A", "seg-for-content-B", "seg-for-content-C"}
	if len(segments) != len(want) {
		t.Fatalf("got %d segments, want %d", len(segments), len(want))
	}
	for i, id := range want {
		if segments[i].MessageID != id {
			t.Errorf("segment %d: got MessageID %q at StartOffset %d, want %q (obfuscated volumes were not stitched in true content order)",
				i, segments[i].MessageID, segments[i].StartOffset, id)
		}
	}
}
