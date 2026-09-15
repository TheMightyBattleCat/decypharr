package parser

import (
	"context"
	"sync/atomic"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"slices"
	"testing"

	"github.com/Tensai75/nzbparser"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/types"
)

// Geometry from a real stored RAR5 split with -v10m: file data in each full
// volume starts at byte 81 and ends 99 bytes before the volume does.
func threeVolumes() []storage.ArchiveVolumeInfo {
	return []storage.ArchiveVolumeInfo{
		{Name: "t.part1.rar", Size: 10485760},
		{Name: "t.part2.rar", Size: 10485760},
		{Name: "t.part3.rar", Size: 5243412},
	}
}

func part(n int, offset, size int64) *types.RARVolumePart {
	return &types.RARVolumePart{DataOffset: offset, PackedSize: size, UnpackedSize: size, PartNumber: n}
}

func TestCheckPartsInsideVolumes(t *testing.T) {
	vols := threeVolumes()
	cases := []struct {
		name    string
		parts   []*types.RARVolumePart
		wantErr bool
	}{
		{"real archive", []*types.RARVolumePart{part(0, 80, 10485581), part(1, 81, 10485580), part(2, 81, 5243239)}, false},
		{"part ends exactly at its volume end", []*types.RARVolumePart{part(0, 80, 10485680)}, false},
		{"one byte past an inner volume", []*types.RARVolumePart{part(0, 80, 10485581), part(1, 81, 10485680)}, true},
		// The last volume may be an estimate a few KB short; the shortfall check owns it.
		{"past the last volume", []*types.RARVolumePart{part(2, 81, 5243412)}, false},
		{"RAR4 trimmed an inner part to the volume", []*types.RARVolumePart{{DataOffset: 68, PackedSize: 10485692, UnpackedSize: 10485692, PartNumber: 1, TrimmedBytes: 1257}}, true},
		{"RAR4 trimmed the last part", []*types.RARVolumePart{{DataOffset: 68, PackedSize: 5243344, UnpackedSize: 5243344, PartNumber: 2, TrimmedBytes: 1257}}, false},
		{"unknown part number", []*types.RARVolumePart{part(7, 0, 1<<40), part(-1, 0, 1<<40), nil}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkPartsInsideVolumes(&RARFileEntry{Name: "f.mkv", VolumeParts: c.parts}, vols)
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, want error %v", err, c.wantErr)
			}
		})
	}
}

func TestCheckStreamCoversHeader(t *testing.T) {
	const article = 716800
	cases := []struct {
		name          string
		header, found int64
		articleBytes  int64
		wantMissing   int64
		wantErr       bool
	}{
		{"complete", 5_000_000_000, 5_000_000_000, article, 0, false},
		{"stream longer than header", 5_000_000_000, 5_000_000_100, article, 0, false},
		{"estimated final article, 23 KB", 5_000_000_000, 5_000_000_000 - 22841, article, 22841, false},
		{"just under an article", 5_000_000_000, 5_000_000_000 - (article - 1), article, article - 1, false},
		{"one article", 5_000_000_000, 5_000_000_000 - article, article, article, true},
		{"a whole volume (Signal S01E09)", 3_400_000_000, 3_400_000_000 - 52223882, article, 52223882, true},
		{"no article size falls back", 5_000_000_000, 5_000_000_000 - fallbackArticleBytes, 0, fallbackArticleBytes, true},
		{"no article size, under the fallback", 5_000_000_000, 5_000_000_000 - (fallbackArticleBytes - 1), 0, fallbackArticleBytes - 1, false},
		{"unknown header size", 0, 5_000_000_000, article, 0, false},
		{"nothing streamed", 5_000_000_000, 0, article, 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			missing, err := checkStreamCoversHeader(&RARFileEntry{Name: "f.mkv", UncompressedSize: c.header}, c.found, c.articleBytes)
			if missing != c.wantMissing || (err != nil) != c.wantErr {
				t.Fatalf("got (%d, %v), want (%d, error %v)", missing, err, c.wantMissing, c.wantErr)
			}
		})
	}
}

func TestDroppedVolumes(t *testing.T) {
	group := &FileGroup{Files: []nzbparser.NzbFile{{Filename: "a.part1.rar"}, {Filename: "a.part2.rar"}, {Filename: "a.part3.rar"}}}
	got := droppedVolumes(group, []*types.Volume{{Index: 0}, {Index: 2}})
	if !slices.Equal(got, []string{"a.part2.rar"}) {
		t.Fatalf("dropped = %v, want [a.part2.rar]", got)
	}
	if got := droppedVolumes(group, []*types.Volume{{Index: 0}, {Index: 1}, {Index: 2}}); len(got) != 0 {
		t.Fatalf("dropped = %v, want none", got)
	}
}

const (
	fullVolume   = 68 * article // what RAR wrote for every volume but the last
	rarTailBytes = 118          // after a part's data: end block and padding
	yhFinalLast  = 300000
	yhFinal      = 9*article + yhFinalLast
)

// dearJudge replays Dear Judge S01E06 on a production install: 68-article volumes whose
// NZB bytes are decoded sizes, sized from a reference that does not fit them,
// so each gets a 0.97 estimate for its last article, plus a measured final
// volume. Three full volumes stand in for 101.
func dearJudge() (*FileGroup, *RARFileEntry) {
	files := []nzbparser.NzbFile{
		postedVolume(1, "yh.part1.rar", 68, article),
		postedVolume(2, "yh.part2.rar", 68, article),
		postedVolume(3, "yh.part3.rar", 68, article),
		postedVolume(4, "yh.part4.rar", 10, yhFinalLast),
	}
	group := &FileGroup{BaseName: "yh", Files: files}
	group.metadata = &fileAnalysisResult{fileSize: 71 * article, lastFileSize: yhFinal, segmentSize: article}
	group.metadata.measure(files[3], yhFinal)

	file := &RARFileEntry{Name: "yh.mkv", IsStored: true}
	for i := range files {
		offset, volume := int64(103), int64(fullVolume)
		if i == 0 {
			offset = 102
		}
		if i == len(files)-1 {
			volume = yhFinal
		}
		size := volume - offset - rarTailBytes
		file.VolumeParts = append(file.VolumeParts, part(i, offset, size))
		file.UncompressedSize += size
	}
	return group, file
}

func adjacentDuplicateNumbers(segs []storage.NZBSegment) int {
	n := 0
	for i := 1; i < len(segs); i++ {
		if segs[i].Number == segs[i-1].Number {
			n++
		}
	}
	return n
}

func TestMeasureCrossedVolumes_DearJudge(t *testing.T) {
	group, file := dearJudge()
	p := &RARParser{}

	base, vols, _ := buildBaseSegments(group)
	if got, want := vols[0].Size, int64(67*article)+estimate(article); got != want {
		t.Fatalf("estimated full volume = %d, want %d", got, want)
	}
	segs, err := p.buildSegmentsForFile(file, base, buildVolumeOffsetMap(vols))
	if err != nil {
		t.Fatal(err)
	}
	if got := adjacentDuplicateNumbers(segs); got != 3 {
		t.Fatalf("estimated geometry splices %d boundaries, want 3 (the replay is off)", got)
	}
	if err := checkPartsInsideVolumes(file, vols); err == nil {
		t.Fatal("checkPartsInsideVolumes passed a geometry that splices every volume boundary")
	}

	var calls atomic.Int32
	fetch := countingFetch(fakeYencFetch(map[string]*nntp.YencMetadata{
		"<yh.part1.rar-1>": {Size: fullVolume, Begin: 1, End: article},
	}), &calls)
	resized, err := measureCrossedVolumes(context.Background(), group, []*RARFileEntry{file}, vols, fetch)
	if !resized || err != nil || calls.Load() != 1 {
		t.Fatalf("got resized=%v err=%v fetches=%d, want one fetch sizing all three volumes", resized, err, calls.Load())
	}

	base, vols, _ = buildBaseSegments(group)
	if err := checkPartsInsideVolumes(file, vols); err != nil {
		t.Fatalf("after measuring: %v", err)
	}
	segs, err = p.buildSegmentsForFile(file, base, buildVolumeOffsetMap(vols))
	if err != nil {
		t.Fatal(err)
	}
	var stream int64
	for _, s := range segs {
		stream += s.Bytes
	}
	if dups := adjacentDuplicateNumbers(segs); dups != 0 || stream != file.UncompressedSize {
		t.Fatalf("after measuring: %d spliced boundaries, %d of %d bytes; want 0 and all", dups, stream, file.UncompressedSize)
	}

	// Nothing crosses any more: a second pass fetches nothing.
	if resized, err := measureCrossedVolumes(context.Background(), group, []*RARFileEntry{file}, vols, fetch); resized || err != nil || calls.Load() != 1 {
		t.Fatalf("second pass: resized=%v err=%v fetches=%d, want no work", resized, err, calls.Load())
	}
}

// A crossed volume measured from its own header keeps that size when the probe
// for its article count measures differently: the probe's size is only for
// volumes nobody measured.
func TestMeasureCrossedVolumes_KeepsAVolumesOwnMeasurement(t *testing.T) {
	group, file := dearJudge()
	own := int64(fullVolume - 200) // still short of volume 2's part
	group.metadata.measure(group.Files[1], own)
	_, vols, _ := buildBaseSegments(group)

	fetch := fakeYencFetch(map[string]*nntp.YencMetadata{
		"<yh.part1.rar-1>": {Size: fullVolume, Begin: 1, End: article},
	})
	if _, err := measureCrossedVolumes(context.Background(), group, []*RARFileEntry{file}, vols, fetch); err != nil {
		t.Fatal(err)
	}
	if got := group.metadata.measuredSize(group.Files[1]); got != own {
		t.Fatalf("volume 2 measured %d bytes after the probe, want its own %d", got, own)
	}
	if got := group.metadata.measuredSize(group.Files[2]); got != fullVolume {
		t.Fatalf("volume 3 measured %d bytes, want the probe's %d", got, fullVolume)
	}
	_, vols, _ = buildBaseSegments(group)
	if err := checkPartsInsideVolumes(file, vols); err == nil {
		t.Fatal("checkPartsInsideVolumes passed a volume whose own header says it is too short")
	}
}

// The measured final volume shares its subject file number with the inner
// volumes' probe: the probe must still be fetched, not take the final
// volume's size and hand it to every crossed volume.
func TestMeasureCrossedVolumes_ProbeSharingTheFinalVolumesNumber(t *testing.T) {
	group, file := dearJudge()
	group.metadata.measured = nil
	group.Files[3].Number = group.Files[0].Number
	group.metadata.measure(group.Files[3], yhFinal)
	_, vols, _ := buildBaseSegments(group)

	var calls atomic.Int32
	fetch := countingFetch(fakeYencFetch(map[string]*nntp.YencMetadata{
		"<yh.part1.rar-1>": {Size: fullVolume, Begin: 1, End: article},
	}), &calls)
	resized, err := measureCrossedVolumes(context.Background(), group, []*RARFileEntry{file}, vols, fetch)
	if !resized || err != nil || calls.Load() != 1 {
		t.Fatalf("got resized=%v err=%v fetches=%d, want the probe fetched", resized, err, calls.Load())
	}
	for i := 0; i < 3; i++ {
		if got := group.metadata.measuredSize(group.Files[i]); got != fullVolume {
			t.Fatalf("volume %d measured %d bytes, want %d", i+1, got, fullVolume)
		}
	}
	if got := group.metadata.measuredSize(group.Files[3]); got != yhFinal {
		t.Fatalf("final volume measured %d bytes, want %d", got, yhFinal)
	}
	_, vols, _ = buildBaseSegments(group)
	if err := checkPartsInsideVolumes(file, vols); err != nil {
		t.Fatalf("after measuring: %v", err)
	}
}

func TestMeasureCrossedVolumes_FetchFailsLeavesGeometry(t *testing.T) {
	group, file := dearJudge()
	_, vols, _ := buildBaseSegments(group)
	resized, err := measureCrossedVolumes(context.Background(), group, []*RARFileEntry{file}, vols, fakeYencFetch(nil))
	if resized || err == nil {
		t.Fatalf("got resized=%v err=%v, want the fetch error and nothing recorded", resized, err)
	}
	if got := group.metadata.measuredSize(group.Files[1]); got != 0 {
		t.Fatalf("recorded %d for an unmeasured volume", got)
	}
}

func TestMeasureCrossedVolumes_NeedsHeaderMeasuredMetadata(t *testing.T) {
	group, file := dearJudge()
	group.metadata.measured = nil
	_, vols, _ := buildBaseSegments(group)
	var calls atomic.Int32
	resized, err := measureCrossedVolumes(context.Background(), group, []*RARFileEntry{file}, vols, countingFetch(fakeYencFetch(nil), &calls))
	if resized || err != nil || calls.Load() != 0 {
		t.Fatalf("got resized=%v err=%v fetches=%d, want nothing done on estimated metadata", resized, err, calls.Load())
	}
}

func TestUntrimParts(t *testing.T) {
	vols := []storage.ArchiveVolumeInfo{{Size: fullVolume}, {Size: fullVolume}}
	fits := &types.RARVolumePart{DataOffset: 68, PackedSize: fullVolume - 1000, UnpackedSize: fullVolume - 1000, TrimmedBytes: 932}
	tooBig := &types.RARVolumePart{DataOffset: 68, PackedSize: fullVolume - 1000, UnpackedSize: fullVolume - 1000, TrimmedBytes: 933, PartNumber: 1}
	file := &RARFileEntry{PackedSize: 2 * (fullVolume - 1000), VolumeParts: []*types.RARVolumePart{fits, tooBig}}
	untrimParts([]*RARFileEntry{file}, vols)
	if fits.TrimmedBytes != 0 || fits.UnpackedSize != fullVolume-68 || fits.PackedSize != fullVolume-68 {
		t.Fatalf("fitting part not restored: %+v", fits)
	}
	if tooBig.TrimmedBytes != 933 || tooBig.UnpackedSize != fullVolume-1000 {
		t.Fatalf("part still past its volume was restored: %+v", tooBig)
	}
	if file.PackedSize != 2*(fullVolume-1000)+932 {
		t.Fatalf("file packed size = %d", file.PackedSize)
	}
}
