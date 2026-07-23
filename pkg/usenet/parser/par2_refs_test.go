package parser

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/Tensai75/nzbparser"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// fakeYencFetch returns canned yEnc metadata per message ID, so tests never
// need a real, provider-backed NNTP client.
func fakeYencFetch(data map[string]*nntp.YencMetadata) yencHeaderFetchFunc {
	return func(_ context.Context, messageID string) (*nntp.YencMetadata, error) {
		d, ok := data[messageID]
		if !ok {
			return nil, errors.New("fakeYencFetch: no metadata for " + messageID)
		}
		return d, nil
	}
}

func TestBuildPar2Refs(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}

	files := nzbparser.NzbFiles{
		{
			Filename: "release.rar",
			Bytes:    1500,
			Segments: nzbparser.NzbSegments{
				// Deliberately out of order to prove buildPar2Refs sorts by Number.
				{Number: 2, Bytes: 500, Id: "<rar-seg2>"},
				{Number: 1, Bytes: 1000, Id: "<rar-seg1>"},
			},
		},
		{
			Filename: "release.par2",
			Bytes:    5000,
			Segments: nzbparser.NzbSegments{
				{Number: 1, Bytes: 5000, Id: "<par2-idx>"},
			},
		},
		{
			Filename: "release.vol000+01.par2",
			Bytes:    8000,
			Segments: nzbparser.NzbSegments{
				{Number: 1, Bytes: 8000, Id: "<par2-vol>"},
			},
		},
		{
			// No segments - must be skipped entirely.
			Filename: "empty.par2",
			Bytes:    0,
			Segments: nzbparser.NzbSegments{},
		},
	}

	// The XML-declared Bytes above are the yEnc-ENCODED (wire) sizes; real
	// decoded sizes (what buildPar2Refs must actually store) are smaller and
	// deliberately don't match the declared bytes exactly, to prove the fix
	// isn't just echoing the XML attribute back out.
	fetch := fakeYencFetch(map[string]*nntp.YencMetadata{
		// release.rar: only seg1 (first segment) is fetched. Real total=1490,
		// this segment's real decoded length=1000 -> seg2 (last) = 1490-1000=490.
		"<rar-seg1>": {Size: 1490, Begin: 0, End: 999},
		// release.par2: single segment, real size=4854 (not the declared 5000).
		"<par2-idx>": {Size: 4854, Begin: 0, End: 4853},
		// release.vol000+01.par2 deliberately has NO fake metadata registered,
		// forcing a fetch error -> the ~3% XML-bytes fallback estimate.
	})

	par2Files, source, aborted := buildPar2RefsWithFetch(context.Background(), p.logger, 4, files, p.detectFileType, fetch)
	if aborted {
		t.Fatal("aborted = true, want false (only one probe failure, well under the abort threshold)")
	}

	if len(par2Files) != 2 {
		t.Fatalf("par2Files len = %d, want 2 (release.par2, release.vol000+01.par2); got %+v", len(par2Files), par2Files)
	}
	byName := make(map[string]int)
	for i, f := range par2Files {
		byName[f.Name] = i
	}

	idx := par2Files[byName["release.par2"]]
	if idx.Size != 4854 {
		t.Errorf("release.par2 Size = %d, want 4854 (real decoded, not declared 5000)", idx.Size)
	}
	if len(idx.Segments) != 1 || idx.Segments[0].Bytes != 4854 {
		t.Errorf("release.par2 Segments = %+v, want one segment of 4854 bytes", idx.Segments)
	}

	vol := par2Files[byName["release.vol000+01.par2"]]
	wantFallback := int64(float64(8000) * 0.97)
	if vol.Size != wantFallback {
		t.Errorf("release.vol000+01.par2 Size = %d, want fallback estimate %d (fetch error)", vol.Size, wantFallback)
	}

	if len(source) != 1 {
		t.Fatalf("source len = %d, want 1 (release.rar); got %+v", len(source), source)
	}
	rar := source[0]
	if rar.Name != "release.rar" {
		t.Errorf("source[0].Name = %q, want release.rar", rar.Name)
	}
	if rar.Size != 1490 {
		t.Errorf("source[0].Size = %d, want 1490 (real decoded total, not declared 1500)", rar.Size)
	}
	if len(rar.Segments) != 2 {
		t.Fatalf("source[0].Segments len = %d, want 2", len(rar.Segments))
	}
	// Segment order must follow NZB part Number (1, then 2), not input order,
	// and real decoded lengths: seg1=1000 (fetched), seg2=490 (remainder).
	if rar.Segments[0].MessageID != "<rar-seg1>" || rar.Segments[0].Bytes != 1000 {
		t.Errorf("source[0].Segments[0] = %+v, want MessageID=<rar-seg1> Bytes=1000", rar.Segments[0])
	}
	if rar.Segments[1].MessageID != "<rar-seg2>" || rar.Segments[1].Bytes != 490 {
		t.Errorf("source[0].Segments[1] = %+v, want MessageID=<rar-seg2> Bytes=490", rar.Segments[1])
	}
}

func TestBuildPar2RefsNoPar2Files(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	files := nzbparser.NzbFiles{
		{
			Filename: "movie.mkv",
			Bytes:    2000,
			Segments: nzbparser.NzbSegments{{Number: 1, Bytes: 2000, Id: "<mkv-seg1>"}},
		},
	}
	fetch := fakeYencFetch(map[string]*nntp.YencMetadata{
		"<mkv-seg1>": {Size: 1940, Begin: 0, End: 1939},
	})
	par2Files, source, aborted := buildPar2RefsWithFetch(context.Background(), p.logger, 4, files, p.detectFileType, fetch)
	if aborted {
		t.Fatal("aborted = true, want false")
	}
	if len(par2Files) != 0 {
		t.Fatalf("par2Files = %+v, want empty", par2Files)
	}
	if len(source) != 1 || source[0].Name != "movie.mkv" || source[0].Size != 1940 {
		t.Fatalf("source = %+v, want one entry for movie.mkv sized 1940", source)
	}
}

// TestAvailabilityThenPar2RefsShortCircuitsOnStatFailure proves the
// reordering: when the connectivity STAT check fails, availabilityThenPar2Refs
// must return immediately and never invoke the (expensive) yEnc fetch for
// PAR2 source-size resolution - a release with missing segments gets
// rejected regardless of what PAR2 probing would have found, so there's no
// reason to pay for it.
func TestAvailabilityThenPar2RefsShortCircuitsOnStatFailure(t *testing.T) {
	fileGroups := map[string]*FileGroup{
		"release": {
			BaseName:       "release",
			ActualFilename: "release.rar",
			Files: []nzbparser.NzbFile{
				{Filename: "release.rar", Segments: nzbparser.NzbSegments{{Number: 1, Bytes: 1000, Id: "<seg1>"}}},
			},
		},
	}
	rawFiles := nzbparser.NzbFiles{
		{Filename: "release.rar", Bytes: 1000, Segments: nzbparser.NzbSegments{{Number: 1, Bytes: 1000, Id: "<seg1>"}}},
	}

	fetchCalled := false
	fetch := func(_ context.Context, _ string) (*nntp.YencMetadata, error) {
		fetchCalled = true
		return &nntp.YencMetadata{Size: 970, Begin: 0, End: 969}, nil
	}
	statErr := errors.New("article not found")
	stat := func(_ context.Context, _ string) error { return statErr }

	p := &NZBParser{logger: zerolog.Nop(), maxConcurrent: 4}
	par2Files, source, err := availabilityThenPar2Refs(context.Background(), p.logger, p.maxConcurrent, fileGroups, rawFiles, p.detectFileType, stat, fetch)
	if err == nil {
		t.Fatal("expected an error when the availability stat fails")
	}
	if !errors.Is(err, statErr) {
		t.Errorf("error = %v, want it to wrap the stat failure %v", err, statErr)
	}
	if fetchCalled {
		t.Error("yEnc fetch was called despite the availability check failing; PAR2 probing should short-circuit")
	}
	if par2Files != nil || source != nil {
		t.Errorf("par2Files/source = %+v/%+v, want nil on availability failure", par2Files, source)
	}
}

// TestAvailabilityThenPar2RefsRunsProbeAfterSuccessfulStat proves PAR2
// probing still runs, and produces its normal result, once the availability
// check passes.
func TestAvailabilityThenPar2RefsRunsProbeAfterSuccessfulStat(t *testing.T) {
	fileGroups := map[string]*FileGroup{
		"release": {
			BaseName:       "release",
			ActualFilename: "release.rar",
			Files: []nzbparser.NzbFile{
				{Filename: "release.rar", Segments: nzbparser.NzbSegments{{Number: 1, Bytes: 1000, Id: "<seg1>"}}},
			},
		},
	}
	rawFiles := nzbparser.NzbFiles{
		{Filename: "release.rar", Bytes: 1000, Segments: nzbparser.NzbSegments{{Number: 1, Bytes: 1000, Id: "<seg1>"}}},
	}

	statCalled := false
	stat := func(_ context.Context, _ string) error {
		statCalled = true
		return nil
	}
	fetch := fakeYencFetch(map[string]*nntp.YencMetadata{
		"<seg1>": {Size: 970, Begin: 0, End: 969},
	})

	p := &NZBParser{logger: zerolog.Nop(), maxConcurrent: 4}
	_, source, err := availabilityThenPar2Refs(context.Background(), p.logger, p.maxConcurrent, fileGroups, rawFiles, p.detectFileType, stat, fetch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !statCalled {
		t.Error("availability stat was never called")
	}
	if len(source) != 1 || source[0].Size != 970 {
		t.Errorf("source = %+v, want one entry sized 970 (real decoded)", source)
	}
}

// TestRealPar2SegmentRefsFallsBackOnSizeMismatch covers the case where the
// fetched header's declared total is wildly inconsistent with this segment
// count/size (e.g. a mixed-subject false match) - must not trust the derived
// per-segment math in that case.
func TestRealPar2SegmentRefsFallsBackOnSizeMismatch(t *testing.T) {
	segs := nzbparser.NzbSegments{
		{Number: 1, Bytes: 1000, Id: "<s1>"},
		{Number: 2, Bytes: 1000, Id: "<s2>"},
	}
	// segmentSize=1000 (from Begin/End), but declared total Size=50 is wildly
	// inconsistent with 2 segments of ~1000 bytes each.
	fetch := fakeYencFetch(map[string]*nntp.YencMetadata{
		"<s1>": {Size: 50, Begin: 0, End: 999},
	})
	refs, total, fetchFailed, real := realPar2SegmentRefs(context.Background(), zerolog.Nop(), "mismatched.dat", segs, fetch)
	if fetchFailed {
		t.Error("fetchFailed = true, want false (the fetch itself succeeded; only the sanity check rejected it)")
	}
	if real {
		t.Error("real = true, want false (result came from the fallback estimate, not real per-segment data)")
	}
	wantS1 := int64(float64(1000) * yencOverheadEstimate)
	wantS2 := int64(float64(1000) * yencOverheadEstimate)
	if len(refs) != 2 || refs[0].Bytes != wantS1 || refs[1].Bytes != wantS2 {
		t.Fatalf("refs = %+v, want fallback estimate [%d %d]", refs, wantS1, wantS2)
	}
	if total != wantS1+wantS2 {
		t.Errorf("total = %d, want %d", total, wantS1+wantS2)
	}
}

// TestBuildPar2RefsReusesPostingSizeAcrossFiles proves the per-posting-size
// optimization: only the seed file (the first eligible file with more than
// one segment) gets a real yEnc fetch. Every other file whose own segment
// geometry is consistent with the seed's derived article size reuses it
// with zero network round trips.
func TestBuildPar2RefsReusesPostingSizeAcrossFiles(t *testing.T) {
	files := nzbparser.NzbFiles{
		{
			// Seed: 3 full segments, real per-segment size (from the fetch
			// below) = 970, matching its own XML-declared Bytes of 1000
			// scaled by yencOverheadEstimate exactly - a clean baseline.
			Filename: "a.rar",
			Segments: nzbparser.NzbSegments{
				{Number: 1, Bytes: 1000, Id: "<a-seg1>"},
				{Number: 2, Bytes: 1000, Id: "<a-seg2>"},
				{Number: 3, Bytes: 1000, Id: "<a-seg3>"},
			},
		},
		{
			// Consistent geometry (non-final segment Bytes == seed's) - must
			// reuse the seed's derived size with no fetch.
			Filename: "b.rar",
			Segments: nzbparser.NzbSegments{
				{Number: 1, Bytes: 1000, Id: "<b-seg1>"},
				{Number: 2, Bytes: 1000, Id: "<b-seg2>"},
			},
		},
		{
			// Still within postingSizeToleranceFrac (8% high on the
			// non-final segment) - must also reuse, not probe.
			Filename: "c.rar",
			Segments: nzbparser.NzbSegments{
				{Number: 1, Bytes: 1080, Id: "<c-seg1>"},
				{Number: 2, Bytes: 900, Id: "<c-seg2>"},
			},
		},
	}

	var calls int32
	fetch := func(_ context.Context, messageID string) (*nntp.YencMetadata, error) {
		atomic.AddInt32(&calls, 1)
		if messageID == "<a-seg1>" {
			return &nntp.YencMetadata{Size: 2910, Begin: 0, End: 969}, nil // segmentSize=970
		}
		return nil, errors.New("unexpected fetch for " + messageID)
	}

	p := &NZBParser{logger: zerolog.Nop()}
	par2Files, source, aborted := buildPar2RefsWithFetch(context.Background(), p.logger, 4, files, p.detectFileType, fetch)
	if aborted {
		t.Fatal("aborted = true, want false")
	}
	if len(par2Files) != 0 {
		t.Fatalf("par2Files = %+v, want empty (all files are .rar)", par2Files)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("fetch called %d times, want exactly 1 (only the seed file)", got)
	}

	byName := make(map[string]storage.PostedFileRef)
	for _, f := range source {
		byName[f.Name] = f
	}

	segBytesOf := func(f storage.PostedFileRef) []int64 {
		out := make([]int64, len(f.Segments))
		for i, s := range f.Segments {
			out[i] = s.Bytes
		}
		return out
	}

	a := byName["a.rar"]
	if a.Size != 2910 || !equalInt64(segBytesOf(a), []int64{970, 970, 970}) {
		t.Errorf("a.rar = %+v, want size=2910 segBytes=[970 970 970]", a)
	}
	b := byName["b.rar"]
	wantBLast := int64(float64(1000) * yencOverheadEstimate)
	if b.Size != 970+wantBLast || !equalInt64(segBytesOf(b), []int64{970, wantBLast}) {
		t.Errorf("b.rar = %+v, want segBytes=[970 %d] (shared size + XML-estimated last segment)", b, wantBLast)
	}
	c := byName["c.rar"]
	wantCLast := int64(float64(900) * yencOverheadEstimate)
	if c.Size != 970+wantCLast || !equalInt64(segBytesOf(c), []int64{970, wantCLast}) {
		t.Errorf("c.rar = %+v, want segBytes=[970 %d] (shared size + XML-estimated last segment)", c, wantCLast)
	}
}

// TestBuildPar2RefsEarlyAbortAfterKFailures proves the early-abort behavior:
// once par2ProbeMaxFailedFetches real fetches have failed, probing stops for
// every remaining file in the release - no further fetch calls - and the
// release is reported aborted. maxConcurrent=1 makes candidate processing
// deterministic (strictly in input order) so the exact fetch count is
// assertable.
func TestBuildPar2RefsEarlyAbortAfterKFailures(t *testing.T) {
	// All single-segment files, so none qualifies as a multi-segment seed -
	// every file goes through the same per-candidate probe path in order.
	files := make(nzbparser.NzbFiles, 0, 6)
	for i := 0; i < 6; i++ {
		id := "<seg" + string(rune('0'+i)) + ">"
		files = append(files, nzbparser.NzbFile{
			Filename: "file" + string(rune('0'+i)) + ".rar",
			Segments: nzbparser.NzbSegments{{Number: 1, Bytes: 1000, Id: id}},
		})
	}

	var calls int32
	fetch := func(_ context.Context, _ string) (*nntp.YencMetadata, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("simulated dead article")
	}

	p := &NZBParser{logger: zerolog.Nop()}
	par2Files, source, aborted := buildPar2RefsWithFetch(context.Background(), p.logger, 1, files, p.detectFileType, fetch)
	if aborted != true {
		t.Fatal("aborted = false, want true after par2ProbeMaxFailedFetches failures")
	}
	if len(par2Files) != 0 || len(source) != 6 {
		t.Fatalf("par2Files/source = %d/%d, want 0/6", len(par2Files), len(source))
	}
	if got := atomic.LoadInt32(&calls); got != par2ProbeMaxFailedFetches {
		t.Fatalf("fetch called %d times, want exactly %d (probing stops once the threshold trips)", got, par2ProbeMaxFailedFetches)
	}
	// Every file - probed-and-failed or skipped post-abort - falls back to
	// the same XML-bytes estimate, so all 6 results are consistent.
	want := int64(float64(1000) * yencOverheadEstimate)
	for _, f := range source {
		if len(f.Segments) != 1 || f.Segments[0].Bytes != want {
			t.Errorf("%s Segments = %+v, want one segment of %d bytes", f.Name, f.Segments, want)
		}
	}
}

func equalInt64(got, want []int64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
