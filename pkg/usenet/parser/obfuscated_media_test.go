package parser

import (
	"testing"

	"github.com/Tensai75/nzbparser"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestContentDetectionInfersExtensionForObfuscatedMedia(t *testing.T) {
	p := &NZBParser{}
	tests := []struct {
		name      string
		data      []byte
		extension string
	}{
		{name: "matroska", data: []byte{0x1A, 0x45, 0xDF, 0xA3}, extension: ".mkv"},
		{name: "mp4", data: []byte{0, 0, 0, 0, 'f', 't', 'y', 'p'}, extension: ".mp4"},
		{name: "avi", data: []byte{'R', 'I', 'F', 'F', 0, 0, 0, 0, 'A', 'V', 'I', ' '}, extension: ".avi"},
		{name: "mpeg", data: []byte{0, 0, 1, 0xBA}, extension: ".mpg"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fileType, extension := p.detectFileTypeAndExtensionFromContent(tt.data)
			if fileType != storage.NZBFileTypeMedia || extension != tt.extension {
				t.Fatalf("content classification = (%q, %q), want (%q, %q)", fileType, extension, storage.NZBFileTypeMedia, tt.extension)
			}
		})
	}
	if fileType, extension := p.detectFileTypeAndExtensionFromContent([]byte("Rar!\x1A\x07\x01\x00")); fileType != storage.NZBFileTypeRar || extension != "" {
		t.Fatalf("RAR5 classification = (%q, %q)", fileType, extension)
	}
}

func TestTypeAndNameFromContent(t *testing.T) {
	p := &NZBParser{}
	mkv := []byte{0x1A, 0x45, 0xDF, 0xA3, 0, 0, 0, 0}
	tests := []struct {
		name, yencName, posted string
		snippet                []byte
		wantType               storage.NZBFileType
		wantName               string
	}{
		{"no yEnc name", "", "hBnewXHwWBUEYm24QydJAcpQ4nNpC", mkv, storage.NZBFileTypeMedia, "hBnewXHwWBUEYm24QydJAcpQ4nNpC.mkv"},
		{"opaque yEnc name", "a1b2c3d4", "posted", mkv, storage.NZBFileTypeMedia, "a1b2c3d4.mkv"},
		{"known extension kept", "Episode.mkv", "posted", mkv, storage.NZBFileTypeMedia, "Episode.mkv"},
		{"archive not renamed", "a1b2c3d4", "posted", []byte("Rar!\x1A\x07\x01\x00"), storage.NZBFileTypeRar, "a1b2c3d4"},
		{"unknown content", "a1b2c3d4", "posted", []byte("nothing"), storage.NZBFileTypeUnknown, "a1b2c3d4"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotName := p.typeAndNameFromContent(&nntp.YencMetadata{Name: tt.yencName, Snippet: tt.snippet}, tt.posted)
			if gotType != tt.wantType || gotName != tt.wantName {
				t.Fatalf("typeAndNameFromContent = (%q, %q), want (%q, %q)", gotType, gotName, tt.wantType, tt.wantName)
			}
		})
	}
}

// Grouping takes the inferred name, so the obfuscated file becomes one media
// file with a playable extension.
func TestExtensionlessObfuscatedMediaGroupsAsMedia(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	file := nzbparser.NzbFile{Filename: "hBnewXHwWBUEYm24QydJAcpQ4nNpC", Basefilename: "hBnewXHwWBUEYm24QydJAcpQ4nNpC"}
	groups := p.groupProcessedFiles([]contentResult{{
		file:           file,
		fileType:       storage.NZBFileTypeMedia,
		actualFilename: "hBnewXHwWBUEYm24QydJAcpQ4nNpC.mkv",
	}})
	if len(groups) != 1 {
		t.Fatalf("groups = %d, want 1", len(groups))
	}
	for _, g := range groups {
		if g.Type != storage.NZBFileTypeMedia || len(g.Files) != 1 || g.Files[0].Filename != "hBnewXHwWBUEYm24QydJAcpQ4nNpC.mkv" {
			t.Fatalf("group = type %q, files %+v", g.Type, g.Files)
		}
	}
}
