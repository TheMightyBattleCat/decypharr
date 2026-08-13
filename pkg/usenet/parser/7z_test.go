package parser

import (
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/javi11/sevenzip"
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
	rarFileSizes := map[string]int64{
		"archive.r00": 1000,
		"archive.r01": 1000,
		"archive.r02": 1000,
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

	segments, err := p.buildSegmentsForRARFile(rarEntry, rarFileOffsets, rarFileSizes, baseSegments, volumeInfos)
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
	rarFileSizes := map[string]int64{
		"archive.r00": 1000,
		"archive.r01": 1000,
		"archive.r02": 1000,
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

	segments, err := (&SevenZParser{logger: zerolog.New(io.Discard)}).buildSegmentsForRARFile(rarEntry, rarFileOffsets, rarFileSizes, baseSegments, volumeInfos)
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

// TestSortRARFilesByVolumeOrder_ZeroNumberedObfuscated_OrdersByFilename
// reproduces the real-world case that shipped uncaught in 9a461bc: an
// obfuscated posting where EVERY volume's RAR5 main-header volume number is
// stripped (confirmed live on Coast.of.Windmere.S01E01.1080p.WEB.h264-ETHEL —
// 16/16 volumes hasNum=false), so resolveVolumeOrder has no signal at all and
// declines (canOrder=false at the entry gate, since numbered==0), leaving
// whatever baseline order processRARFilesFromPositions established. Before
// this fix that baseline was raw physical/posting offset — for this release,
// .r00..r14 physically precede .rar (opposite of RAR's logical order, per the
// package comment on sortRARFilesByVolumeOrder) — so the file assembled in
// the wrong order and failed ffprobe on import ("EBML header parsing
// failed"). This test proves sortRARFilesByVolumeOrder itself, the exact
// function processRARFilesFromPositions now calls, fixes the baseline: with
// the old offset-only sort this assertion fails (order stays r00..r14,rar);
// with getRARVolumeOrder driving the sort it passes (.rar first, then
// r00..r14 ascending) — matching the offline harness proof against the real
// NZB (PartNumber 0->.rar, 1->.r00, ..., 15->.r14).
func TestSortRARFilesByVolumeOrder_ZeroNumberedObfuscated_OrdersByFilename(t *testing.T) {
	// Physical/posting order: .r00..r14 (ascending offset), then .rar last —
	// exactly the live posting's layout. No RAR5 volume numbers are involved;
	// sortRARFilesByVolumeOrder never looks at them — it runs before any
	// header is even read.
	rarFiles := []sevenzip.FileInfo{
		{Name: "Coast.of.Windmere.S01E01.1080p.WEB.h264-ETHEL/coast.of.windmere.s01e01.1080p.web.h264-ethel.r00", Offset: 65286963},
		{Name: "Coast.of.Windmere.S01E01.1080p.WEB.h264-ETHEL/coast.of.windmere.s01e01.1080p.web.h264-ethel.r01", Offset: 215286963},
		{Name: "Coast.of.Windmere.S01E01.1080p.WEB.h264-ETHEL/coast.of.windmere.s01e01.1080p.web.h264-ethel.r02", Offset: 365286963},
		{Name: "Coast.of.Windmere.S01E01.1080p.WEB.h264-ETHEL/coast.of.windmere.s01e01.1080p.web.h264-ethel.r03", Offset: 515286963},
		{Name: "Coast.of.Windmere.S01E01.1080p.WEB.h264-ETHEL/coast.of.windmere.s01e01.1080p.web.h264-ethel.r04", Offset: 665286963},
		{Name: "Coast.of.Windmere.S01E01.1080p.WEB.h264-ETHEL/coast.of.windmere.s01e01.1080p.web.h264-ethel.r05", Offset: 815286963},
		{Name: "Coast.of.Windmere.S01E01.1080p.WEB.h264-ETHEL/coast.of.windmere.s01e01.1080p.web.h264-ethel.r06", Offset: 965286963},
		{Name: "Coast.of.Windmere.S01E01.1080p.WEB.h264-ETHEL/coast.of.windmere.s01e01.1080p.web.h264-ethel.r07", Offset: 1115286963},
		{Name: "Coast.of.Windmere.S01E01.1080p.WEB.h264-ETHEL/coast.of.windmere.s01e01.1080p.web.h264-ethel.r08", Offset: 1265286963},
		{Name: "Coast.of.Windmere.S01E01.1080p.WEB.h264-ETHEL/coast.of.windmere.s01e01.1080p.web.h264-ethel.r09", Offset: 1415286963},
		{Name: "Coast.of.Windmere.S01E01.1080p.WEB.h264-ETHEL/coast.of.windmere.s01e01.1080p.web.h264-ethel.r10", Offset: 1565286963},
		{Name: "Coast.of.Windmere.S01E01.1080p.WEB.h264-ETHEL/coast.of.windmere.s01e01.1080p.web.h264-ethel.r11", Offset: 1715286963},
		{Name: "Coast.of.Windmere.S01E01.1080p.WEB.h264-ETHEL/coast.of.windmere.s01e01.1080p.web.h264-ethel.r12", Offset: 1865286963},
		{Name: "Coast.of.Windmere.S01E01.1080p.WEB.h264-ETHEL/coast.of.windmere.s01e01.1080p.web.h264-ethel.r13", Offset: 2015286963},
		{Name: "Coast.of.Windmere.S01E01.1080p.WEB.h264-ETHEL/coast.of.windmere.s01e01.1080p.web.h264-ethel.r14", Offset: 2165286963},
		{Name: "Coast.of.Windmere.S01E01.1080p.WEB.h264-ETHEL/coast.of.windmere.s01e01.1080p.web.h264-ethel.rar", Offset: 2201369905},
	}

	sortRARFilesByVolumeOrder(rarFiles)

	want := []string{"rar", "r00", "r01", "r02", "r03", "r04", "r05", "r06", "r07", "r08", "r09", "r10", "r11", "r12", "r13", "r14"}
	if len(rarFiles) != len(want) {
		t.Fatalf("got %d volumes, want %d", len(rarFiles), len(want))
	}
	for i, wantExt := range want {
		got := rarFiles[i].Name
		if !strings.HasSuffix(got, "."+wantExt) {
			t.Errorf("position %d: got volume %q, want one ending in .%s (posting order was kept instead of content order — the baseline-order fix regressed)", i, got, wantExt)
		}
	}
}

// TestBuildSegmentsForRARFile_ClipsPackedSizeToVolumePhysicalSize proves the
// running-budget clip in buildSegmentsForRARFile. Real RAR5 postings stamp
// every volume's VolumeParts.PackedSize with the WHOLE FILE's size, not that
// volume's own physical share of it (see the note on RARVolumePart.PackedSize
// in rar.go) — this fixture reproduces that exact encoding: 3 volumes, each
// 1000 bytes physically, of a single 3000-byte stored file, with every part
// declaring PackedSize: 3000.
//
// Two things are checked against the SAME fixture:
//
//  1. The naive/pre-fix computation — slicing directly with the uncapped
//     part.PackedSize, exactly what buildSegmentsForRARFile did before this
//     fix — is shown to duplicate data: since all 3 volumes sit back-to-back
//     in one flat 7z byte stream with no per-volume read boundary, each part
//     reads past its own volume into the next one(s). Total bytes returned
//     (6000) overshoots the file's real size (3000), and two of the three
//     source segments are each claimed by more than one part.
//  2. buildSegmentsForRARFile itself (post-fix, clipping every part's read to
//     min(bytes physically left in its own volume, bytes left of the file's
//     advertised total)) is shown to produce exactly the file's 3000 bytes,
//     each of the 3 source segments claimed exactly once, source ranges
//     disjoint.
func TestBuildSegmentsForRARFile_ClipsPackedSizeToVolumePhysicalSize(t *testing.T) {
	baseSegments := []storage.NZBSegment{
		{Number: 1, MessageID: "vol0-data", Bytes: 1000},
		{Number: 2, MessageID: "vol1-data", Bytes: 1000},
		{Number: 3, MessageID: "vol2-data", Bytes: 1000},
	}
	volumeInfos := []storage.ArchiveVolumeInfo{
		{Name: "archive", Size: 3000, SegmentStart: 0, SegmentEnd: 3},
	}
	rarFileOffsets := map[string]int64{
		"archive.r00": 0,
		"archive.r01": 1000,
		"archive.r02": 2000,
	}
	rarFileSizes := map[string]int64{
		"archive.r00": 1000,
		"archive.r01": 1000,
		"archive.r02": 1000,
	}

	// The real-world encoding: every part's PackedSize is the WHOLE FILE's
	// size (3000), not this volume's own 1000-byte share of it.
	volumeParts := []*types.RARVolumePart{
		{Name: "archive.r00", DataOffset: 0, PackedSize: 3000, UnpackedSize: 3000, Stored: true, PartNumber: 0},
		{Name: "archive.r01", DataOffset: 0, PackedSize: 3000, UnpackedSize: 3000, Stored: true, PartNumber: 1},
		{Name: "archive.r02", DataOffset: 0, PackedSize: 3000, UnpackedSize: 3000, Stored: true, PartNumber: 2},
	}
	const uncompressedSize = 3000

	t.Run("pre-clip arithmetic duplicates data", func(t *testing.T) {
		// Exactly what buildSegmentsForRARFile did before this fix: slice
		// using the raw, uncapped part.PackedSize.
		counts := make(map[string]int)
		var total int64
		for _, part := range volumeParts {
			absoluteDataOffset := rarFileOffsets[filepath.Base(part.Name)] + part.DataOffset
			segs, err := sliceSegmentsForRange(baseSegments, volumeInfos, absoluteDataOffset, part.PackedSize)
			if err != nil {
				t.Fatalf("sliceSegmentsForRange: %v", err)
			}
			for _, s := range segs {
				counts[s.MessageID]++
				total += s.Bytes
			}
		}

		if total <= uncompressedSize {
			t.Fatalf("expected the naive computation to OVERSHOOT the file size (duplication), got total=%d, file size=%d", total, uncompressedSize)
		}
		dup := 0
		for id, c := range counts {
			if c > 1 {
				dup++
				t.Logf("source segment %q claimed by %d parts (duplicated)", id, c)
			}
		}
		if dup == 0 {
			t.Fatalf("expected at least one source segment to be claimed by more than one part when PackedSize isn't clipped")
		}
	})

	t.Run("buildSegmentsForRARFile clips and assembles cleanly", func(t *testing.T) {
		p := &SevenZParser{logger: zerolog.New(io.Discard)}
		rarEntry := &RARFileEntry{
			Name:             "movie.mkv",
			UncompressedSize: uncompressedSize,
			IsStored:         true,
			VolumeParts:      volumeParts,
		}

		segments, err := p.buildSegmentsForRARFile(rarEntry, rarFileOffsets, rarFileSizes, baseSegments, volumeInfos)
		if err != nil {
			t.Fatalf("buildSegmentsForRARFile returned error: %v", err)
		}

		var total int64
		seen := make(map[string]int)
		for _, s := range segments {
			total += s.Bytes
			seen[s.MessageID]++
		}
		if total != uncompressedSize {
			t.Fatalf("total bytes = %d, want exactly %d (the file's advertised size, no duplication/overshoot)", total, uncompressedSize)
		}
		for id, c := range seen {
			if c != 1 {
				t.Errorf("source segment %q claimed by %d parts, want exactly 1 (source ranges must be disjoint)", id, c)
			}
		}

		want := []string{"vol0-data", "vol1-data", "vol2-data"}
		if len(segments) != len(want) {
			t.Fatalf("got %d segments, want %d", len(segments), len(want))
		}
		for i, id := range want {
			if segments[i].MessageID != id {
				t.Errorf("segment %d: got MessageID %q, want %q", i, segments[i].MessageID, id)
			}
			if segments[i].Bytes != 1000 {
				t.Errorf("segment %d: got Bytes %d, want 1000 (each volume's own share)", i, segments[i].Bytes)
			}
		}
	})
}
