package parser

import (
	"testing"

	"github.com/Tensai75/nzbparser"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// A PAR2 file recognised from its yEnc name shares its base name with the
// release's own files. It must stay out of their group whatever the order the
// NZB lists them in: listed first it made the group a PAR2 one and the import
// failed with "no valid files found in NZB".
func TestSniffedPar2StaysOutOfReleaseGroups(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	sniffed := func(subject, yencName string, fileType storage.NZBFileType) contentResult {
		return contentResult{
			file: nzbparser.NzbFile{
				Filename:     subject,
				Basefilename: subject,
				Segments:     nzbparser.NzbSegments{{Number: 1, Id: subject + "@x"}},
			},
			fileType:       fileType,
			actualFilename: yencName,
		}
	}
	index := sniffed("a1", "X.par2", storage.NZBFileTypePar2)
	plusVolume := sniffed("a2", "X.vol00+01.par2", storage.NZBFileTypePar2)
	hyphenVolume := sniffed("a3", "X.vol01-03.par2", storage.NZBFileTypePar2)

	tests := []struct {
		name     string
		files    []contentResult
		wantType storage.NZBFileType
		want     []string
	}{
		{
			name: "7z volumes, index listed first",
			files: []contentResult{
				index,
				sniffed("b1", "X.7z.001", storage.NZBFileTypeSevenZip),
				sniffed("b2", "X.7z.002", storage.NZBFileTypeSevenZip),
				plusVolume, hyphenVolume,
			},
			wantType: storage.NZBFileTypeSevenZip,
			want:     []string{"X.7z.001", "X.7z.002"},
		},
		{
			name: "RAR volumes, index listed last",
			files: []contentResult{
				sniffed("b1", "X.part01.rar", storage.NZBFileTypeRar),
				sniffed("b2", "X.part02.rar", storage.NZBFileTypeRar),
				plusVolume, index,
			},
			wantType: storage.NZBFileTypeRar,
			want:     []string{"X.part01.rar", "X.part02.rar"},
		},
		{
			name:     "media file, index listed first",
			files:    []contentResult{index, sniffed("b1", "X.mkv", storage.NZBFileTypeMedia), plusVolume},
			wantType: storage.NZBFileTypeMedia,
			want:     []string{"X.mkv"},
		},
		{
			// The yEnc name is the only clue to the type here.
			name: "PAR2 typed from its name inside grouping",
			files: []contentResult{
				sniffed("a1", "X.par2", storage.NZBFileTypeUnknown),
				sniffed("b1", "X.mkv", storage.NZBFileTypeMedia),
			},
			wantType: storage.NZBFileTypeMedia,
			want:     []string{"X.mkv"},
		},
	}
	for _, tt := range tests {
		groups := p.groupProcessedFiles(tt.files)
		if len(groups) != 1 {
			t.Errorf("%s: groups = %d, want 1", tt.name, len(groups))
			continue
		}
		g := groups["X"]
		if g == nil || g.Type != tt.wantType {
			t.Errorf("%s: group X = %+v, want type %q", tt.name, g, tt.wantType)
			continue
		}
		var got []string
		for _, f := range g.Files {
			got = append(got, f.Filename)
		}
		if len(got) != len(tt.want) {
			t.Errorf("%s: files = %v, want %v", tt.name, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("%s: files = %v, want %v", tt.name, got, tt.want)
				break
			}
		}
	}

	if groups := p.groupProcessedFiles([]contentResult{index, plusVolume, hyphenVolume}); len(groups) != 0 {
		t.Errorf("PAR2 files only: groups = %d, want 0", len(groups))
	}
}
