package parser

import (
	"context"
	"errors"
	"testing"

	"github.com/Tensai75/nzbparser"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/nntp"
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

	par2Files, source := buildPar2RefsWithFetch(context.Background(), p.logger, 4, files, p.detectFileType, fetch)

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
	par2Files, source := buildPar2RefsWithFetch(context.Background(), p.logger, 4, files, p.detectFileType, fetch)
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
	refs, total := realPar2SegmentRefs(context.Background(), zerolog.Nop(), "mismatched.dat", segs, fetch)
	wantS1 := int64(float64(1000) * 0.97)
	wantS2 := int64(float64(1000) * 0.97)
	if len(refs) != 2 || refs[0].Bytes != wantS1 || refs[1].Bytes != wantS2 {
		t.Fatalf("refs = %+v, want fallback estimate [%d %d]", refs, wantS1, wantS2)
	}
	if total != wantS1+wantS2 {
		t.Errorf("total = %d, want %d", total, wantS1+wantS2)
	}
}
