package parser

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Tensai75/nzbparser"

	"github.com/sirrobot01/decypharr/internal/nntp"
)

// article is the decoded article size of the releases the tail clamp hit.
const article = 768000

// postedVolume builds a posted archive volume of n articles whose NZB byte
// counts are the decoded sizes, as the posters whose files were truncated
// declare them: full articles, then a last article of last bytes.
func postedVolume(number int, name string, n, last int) nzbparser.NzbFile {
	segs := make(nzbparser.NzbSegments, n)
	for i := range segs {
		segs[i] = nzbparser.NzbSegment{Number: i + 1, Bytes: article, Id: fmt.Sprintf("<%s-%d>", name, i+1)}
	}
	segs[n-1].Bytes = last
	return nzbparser.NzbFile{Number: number, Filename: name, Segments: segs}
}

func countingFetch(fetch yencHeaderFetchFunc, calls *atomic.Int32) yencHeaderFetchFunc {
	return func(ctx context.Context, id string) (*nntp.YencMetadata, error) {
		calls.Add(1)
		return fetch(ctx, id)
	}
}

const (
	finalLastArticle = 629961
	finalVolumeBytes = 31*article + finalLastArticle
	finalHeaderID    = "<rel.part082.rar-1>"
)

// northernlight replays Northernlight S01E04 on a production install: full 68-article
// volumes and a 32-article final volume whose last article is 629,961 bytes,
// posted out of order, so the file enrichGroupWithFileInfo measured as the
// last one (in NZB order) is a full volume. Two full volumes stand in for 81.
func northernlight() *FileGroup {
	full := int64(68 * article)
	vol1 := postedVolume(1, "rel.part001.rar", 68, article)
	vol2 := postedVolume(82, "rel.part011.rar", 68, article) // last in NZB order
	final := postedVolume(40, "rel.part082.rar", 32, finalLastArticle)
	group := &FileGroup{BaseName: "rel", Files: []nzbparser.NzbFile{vol1, vol2, final}}
	group.metadata = &fileAnalysisResult{fileSize: full, lastFileSize: full, segmentSize: article}
	group.metadata.measure(vol1, full)
	group.metadata.measure(vol2, full)
	return group
}

func finalVolumeFetch() yencHeaderFetchFunc {
	return fakeYencFetch(map[string]*nntp.YencMetadata{
		finalHeaderID: {Size: finalVolumeBytes, Begin: 1, End: article},
	})
}

// estimate is getNZBSegments' fallback for a last article it cannot size.
func estimate(nzbBytes int) int64 { return int64(float64(nzbBytes) * 0.97) }

func TestGetNZBSegments_FinalVolumeReorderedAwayFromTheMeasuredFile(t *testing.T) {
	group := northernlight()
	final := group.Files[2]

	// Unmeasured, the last article is the 0.97 estimate: the 18,899 bytes the
	// served file was short, less the RAR end block.
	_, segs := getNZBSegments(2, final, group)
	if got, want := segs[len(segs)-1].Bytes, estimate(finalLastArticle); got != want {
		t.Fatalf("unmeasured last article = %d, want the %d estimate", got, want)
	}

	n, err := measureUnsizedVolumes(context.Background(), group, finalVolumeFetch())
	if n != 1 || err != nil {
		t.Fatalf("measureUnsizedVolumes = %d, %v; want 1, nil", n, err)
	}
	size, segs := getNZBSegments(2, final, group)
	if size != finalVolumeBytes {
		t.Fatalf("final volume = %d bytes, want %d", size, finalVolumeBytes)
	}
	if got := segs[len(segs)-1].Bytes; got != finalLastArticle {
		t.Fatalf("last article = %d, want %d", got, finalLastArticle)
	}
}

// A measured size belongs to the file it measured, wherever the processor
// puts that file; the file at the last index must not take it by position.
func TestGetNZBSegments_SizesByMeasurementNotPosition(t *testing.T) {
	full := int64(68 * article)
	vol1 := postedVolume(1, "a.part001.rar", 68, article)
	short := postedVolume(3, "a.part003.rar", 32, finalLastArticle) // measured as the NZB-order last file
	vol2 := postedVolume(2, "a.part002.rar", 68, article)
	group := &FileGroup{Files: []nzbparser.NzbFile{vol1, short, vol2}}
	group.metadata = &fileAnalysisResult{fileSize: full, lastFileSize: finalVolumeBytes, segmentSize: article}
	group.metadata.measure(vol1, full)
	group.metadata.measure(short, finalVolumeBytes)

	if size, _ := getNZBSegments(1, short, group); size != finalVolumeBytes {
		t.Fatalf("short volume = %d bytes, want its measured %d", size, finalVolumeBytes)
	}
	size, segs := getNZBSegments(2, vol2, group)
	if size != full || segs[len(segs)-1].Bytes != article {
		t.Fatalf("last-index full volume = %d bytes (last article %d), want %d (%d): it must not take the short volume's size by position",
			size, segs[len(segs)-1].Bytes, full, article)
	}
}

func TestMeasureUnsizedVolumes_SkipsMeasuredFiles(t *testing.T) {
	group := northernlight()
	group.metadata.measure(group.Files[2], finalVolumeBytes)
	var calls atomic.Int32
	n, err := measureUnsizedVolumes(context.Background(), group, countingFetch(fakeYencFetch(nil), &calls))
	if n != 0 || err != nil || calls.Load() != 0 {
		t.Fatalf("got n=%d err=%v fetches=%d, want nothing fetched", n, err, calls.Load())
	}
}

// A header size that cannot belong to a file of this many articles (a
// mixed-subject group) is not recorded.
func TestMeasureUnsizedVolumes_RejectsASizeThatDoesNotFit(t *testing.T) {
	group := northernlight()
	fetch := fakeYencFetch(map[string]*nntp.YencMetadata{
		finalHeaderID: {Size: 68 * article, Begin: 1, End: article},
	})
	n, err := measureUnsizedVolumes(context.Background(), group, fetch)
	if n != 0 || err == nil || !strings.Contains(err.Error(), "does not fit") {
		t.Fatalf("got n=%d err=%v, want a does-not-fit error", n, err)
	}
	if got := group.metadata.measuredSize(group.Files[2]); got != 0 {
		t.Fatalf("recorded %d for the final volume, want nothing", got)
	}
}

func TestMeasureUnsizedVolumes_FetchErrorKeepsTheEstimate(t *testing.T) {
	group := northernlight()
	n, err := measureUnsizedVolumes(context.Background(), group, fakeYencFetch(nil))
	if n != 0 || err == nil {
		t.Fatalf("got n=%d err=%v, want the fetch error", n, err)
	}
	_, segs := getNZBSegments(2, group.Files[2], group)
	if got, want := segs[len(segs)-1].Bytes, estimate(finalLastArticle); got != want {
		t.Fatalf("last article = %d, want the unchanged %d estimate", got, want)
	}
}

// With no header measurements every article size is an estimate, and an exact
// total cannot be combined with estimated articles: leave it all alone.
func TestMeasureUnsizedVolumes_LeavesEstimatedMetadataAlone(t *testing.T) {
	vol1 := postedVolume(1, "e.part1.rar", 68, article)
	final := postedVolume(2, "e.part2.rar", 32, finalLastArticle)
	group := &FileGroup{Files: []nzbparser.NzbFile{vol1, final}}
	guess := group.getMetadata()

	var calls atomic.Int32
	n, err := measureUnsizedVolumes(context.Background(), group, countingFetch(fakeYencFetch(nil), &calls))
	if n != 0 || err != nil || calls.Load() != 0 {
		t.Fatalf("got n=%d err=%v fetches=%d, want nothing fetched", n, err, calls.Load())
	}
	if size, _ := getNZBSegments(1, final, group); size != guess.lastFileSize {
		t.Fatalf("last file = %d bytes, want the position-based guess %d", size, guess.lastFileSize)
	}
}

// The header is fetched from the file's first article, whatever order the NZB
// listed its segments in.
func TestMeasureUnsizedVolumes_FetchesTheFirstArticle(t *testing.T) {
	group := northernlight()
	segs := group.Files[2].Segments
	segs[0], segs[5] = segs[5], segs[0]
	if n, err := measureUnsizedVolumes(context.Background(), group, finalVolumeFetch()); n != 1 || err != nil {
		t.Fatalf("measureUnsizedVolumes = %d, %v; want 1, nil", n, err)
	}
}

func TestMeasureUnsizedVolumes_FinalFirstThenShortestWithinTheCap(t *testing.T) {
	ref := postedVolume(1, "o.ref.rar", 68, article)
	files := []nzbparser.NzbFile{ref}
	for i, n := range []int{60, 20, 50, 10, 40, 30} {
		files = append(files, postedVolume(10+i, fmt.Sprintf("o.short%d.rar", n), n, article))
	}
	// Full length but last: still the likeliest final volume.
	files = append(files, postedVolume(99, "o.final.rar", 68, 5000))
	group := &FileGroup{Files: files}
	group.metadata = &fileAnalysisResult{fileSize: 68 * article, lastFileSize: 68 * article, segmentSize: article}
	group.metadata.measure(ref, 68*article)

	var order []string
	fetch := func(_ context.Context, id string) (*nntp.YencMetadata, error) {
		order = append(order, id)
		return nil, errors.New("offline")
	}
	n, err := measureUnsizedVolumes(context.Background(), group, fetch)
	want := []string{"<o.final.rar-1>", "<o.short10.rar-1>", "<o.short20.rar-1>", "<o.short30.rar-1>"}
	if !slices.Equal(order, want) {
		t.Fatalf("fetched %v, want %v (final first, then shortest, at most %d)", order, want, maxVolumeMeasurements)
	}
	if n != 0 || err == nil || strings.Contains(err.Error(), "\n") || !strings.HasPrefix(err.Error(), "4 of 4 volumes unmeasured") {
		t.Fatalf("got n=%d err=%q, want one single-line error counting all 4 failures", n, err)
	}
}

const (
	gretaFinalLast  = 11161
	gretaFinalBytes = 45*article + gretaFinalLast
)

// greta replays Greta S03E03 on a production install: its NZB's [n/m] subject counters
// are scrambled, so the final volume (46 articles, last 11,161 bytes) shares
// its file number with the 68-article reference volume
// enrichGroupWithFileInfo measured. Two full volumes stand in for 47.
func greta() *FileGroup {
	full := int64(68 * article)
	ref := postedVolume(3, "h.part01.rar", 68, article)
	lastByNumber := postedVolume(9, "h.part02.rar", 68, article)
	final := postedVolume(3, "h.part48.rar", 46, gretaFinalLast)
	group := &FileGroup{BaseName: "h", Files: []nzbparser.NzbFile{ref, lastByNumber, final}}
	group.metadata = &fileAnalysisResult{fileSize: full, lastFileSize: full, segmentSize: article}
	group.metadata.measure(ref, full)
	group.metadata.measure(lastByNumber, full)
	return group
}

func TestFileMetaKey_NamesOneFileWhateverItsNumberOrSegmentOrder(t *testing.T) {
	group := greta()
	ref, final := group.Files[0], group.Files[2]
	if fileMetaKey(ref) == fileMetaKey(final) {
		t.Fatalf("volumes sharing subject number %d share key %q", ref.Number, fileMetaKey(ref))
	}
	key := fileMetaKey(final)
	shuffled := final
	shuffled.Segments = slices.Clone(final.Segments)
	slices.Reverse(shuffled.Segments)
	if got := fileMetaKey(shuffled); got != key {
		t.Fatalf("key changed with segment order: %q, want %q", got, key)
	}
	if got := fileMetaKey(nzbparser.NzbFile{Number: 4, Subject: "s"}); got != "s:s" {
		t.Fatalf("no segments: key %q, want the subject", got)
	}
}

func TestMeasureUnsizedVolumes_FinalVolumeSharingAMeasuredFilesNumber(t *testing.T) {
	group := greta()
	final := group.Files[2]
	if got := group.metadata.measuredSize(final); got != 0 {
		t.Fatalf("final volume reads as measured (%d bytes) before its header was fetched", got)
	}

	var calls atomic.Int32
	fetch := countingFetch(fakeYencFetch(map[string]*nntp.YencMetadata{
		"<h.part48.rar-1>": {Size: gretaFinalBytes, Begin: 1, End: article},
	}), &calls)
	n, err := measureUnsizedVolumes(context.Background(), group, fetch)
	if n != 1 || err != nil || calls.Load() != 1 {
		t.Fatalf("measureUnsizedVolumes = %d, %v after %d fetches; want the final volume measured", n, err, calls.Load())
	}
	size, segs := getNZBSegments(2, final, group)
	if size != gretaFinalBytes {
		t.Fatalf("final volume = %d bytes, want %d", size, gretaFinalBytes)
	}
	if got := segs[len(segs)-1].Bytes; got != gretaFinalLast {
		t.Fatalf("last article = %d, want %d (the estimate is %d)", got, gretaFinalLast, estimate(gretaFinalLast))
	}
	// The reference keeps its own size.
	if size, _ := getNZBSegments(0, group.Files[0], group); size != 68*article {
		t.Fatalf("reference volume = %d bytes, want %d", size, 68*article)
	}
}
