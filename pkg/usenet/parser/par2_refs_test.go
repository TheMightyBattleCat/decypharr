package parser

import (
	"testing"

	"github.com/Tensai75/nzbparser"
)

func TestBuildPar2Refs(t *testing.T) {
	p := &NZBParser{}

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

	par2Files, source := p.buildPar2Refs(files)

	if len(par2Files) != 2 {
		t.Fatalf("par2Files len = %d, want 2 (release.par2, release.vol000+01.par2); got %+v", len(par2Files), par2Files)
	}
	if par2Files[0].Name != "release.par2" || par2Files[0].Size != 5000 {
		t.Errorf("par2Files[0] = %+v", par2Files[0])
	}
	if par2Files[1].Name != "release.vol000+01.par2" || par2Files[1].Size != 8000 {
		t.Errorf("par2Files[1] = %+v", par2Files[1])
	}

	if len(source) != 1 {
		t.Fatalf("source len = %d, want 1 (release.rar); got %+v", len(source), source)
	}
	if source[0].Name != "release.rar" || source[0].Size != 1500 {
		t.Errorf("source[0] = %+v", source[0])
	}
	if len(source[0].Segments) != 2 {
		t.Fatalf("source[0].Segments len = %d, want 2", len(source[0].Segments))
	}
	// Segment order must follow NZB part Number (1, then 2), not input order.
	if source[0].Segments[0].MessageID != "<rar-seg1>" || source[0].Segments[0].Bytes != 1000 {
		t.Errorf("source[0].Segments[0] = %+v, want MessageID=<rar-seg1> Bytes=1000", source[0].Segments[0])
	}
	if source[0].Segments[1].MessageID != "<rar-seg2>" || source[0].Segments[1].Bytes != 500 {
		t.Errorf("source[0].Segments[1] = %+v, want MessageID=<rar-seg2> Bytes=500", source[0].Segments[1])
	}
}

func TestBuildPar2RefsNoPar2Files(t *testing.T) {
	p := &NZBParser{}
	files := nzbparser.NzbFiles{
		{
			Filename: "movie.mkv",
			Bytes:    2000,
			Segments: nzbparser.NzbSegments{{Number: 1, Bytes: 2000, Id: "<mkv-seg1>"}},
		},
	}
	par2Files, source := p.buildPar2Refs(files)
	if len(par2Files) != 0 {
		t.Fatalf("par2Files = %+v, want empty", par2Files)
	}
	if len(source) != 1 || source[0].Name != "movie.mkv" {
		t.Fatalf("source = %+v, want one entry for movie.mkv", source)
	}
}
