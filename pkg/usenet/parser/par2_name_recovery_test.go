package parser

import (
	"testing"

	"github.com/Tensai75/nzbparser"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

func recoveredVolume(obfuscated, real string) recoveredFile {
	return recoveredFile{
		file:     nzbparser.NzbFile{Filename: obfuscated, Segments: nzbparser.NzbSegments{{Number: 1, Id: "<" + obfuscated + ">"}}},
		realName: real,
		groups:   map[string]struct{}{"alt.binaries.test": {}},
	}
}

// Every volume of one archive shares its base name; the second volume must
// join the first one's group, not abort as a collision (on a production install every
// recovery since cbb251a aborted this way).
func TestRegroupRecoveredVolumes_OneArchive(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	nfo := &FileGroup{BaseName: "other", Type: storage.NZBFileTypeMedia}
	recovered := []recoveredFile{
		recoveredVolume("x3", "AHdnxms.part03.rar"),
		recoveredVolume("x1", "AHdnxms.part01.rar"),
		recoveredVolume("x2", "AHdnxms.part02.rar"),
	}
	groups, collision := regroupRecoveredVolumes(recovered, []*FileGroup{nfo}, p.getBaseFilename)
	if groups == nil {
		t.Fatalf("aborted on key %q", collision)
	}
	if len(groups) != 2 || groups["other"] != nfo {
		t.Fatalf("groups = %v, want the archive plus the untouched other group", groups)
	}
	var archive *FileGroup
	for k, g := range groups {
		if k != "other" {
			archive = g
		}
	}
	if archive == nil || archive.Type != storage.NZBFileTypeRar || len(archive.Files) != 3 {
		t.Fatalf("archive group = %+v, want one RAR group of 3 volumes", archive)
	}
	for i, want := range []string{"AHdnxms.part01.rar", "AHdnxms.part02.rar", "AHdnxms.part03.rar"} {
		if got := archive.Files[i].Filename; got != want {
			t.Fatalf("volume %d = %q, want %q", i, got, want)
		}
	}
	if _, ok := archive.Groups["alt.binaries.test"]; !ok {
		t.Fatalf("newsgroups not carried: %v", archive.Groups)
	}
	if recovered[0].file.Filename != "x3" {
		t.Fatalf("caller's recovered file renamed in place: %q", recovered[0].file.Filename)
	}
}

func TestRegroupRecoveredVolumes_CollisionWithAnExistingGroup(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	recovered := []recoveredFile{
		recoveredVolume("x1", "Show.S01E01.part01.rar"),
		recoveredVolume("x2", "Show.S01E01.part02.rar"),
	}
	existing := &FileGroup{BaseName: p.getBaseFilename("Show.S01E01.part01.rar"), Type: storage.NZBFileTypeMedia}
	groups, collision := regroupRecoveredVolumes(recovered, []*FileGroup{existing}, p.getBaseFilename)
	if groups != nil || collision != existing.BaseName {
		t.Fatalf("got groups=%v collision=%q, want nil and %q", groups, collision, existing.BaseName)
	}
}

// Two archives recovered from one NZB stay apart.
func TestRegroupRecoveredVolumes_TwoArchives(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	recovered := []recoveredFile{
		recoveredVolume("a", "one.part1.rar"),
		recoveredVolume("b", "two.part1.rar"),
		recoveredVolume("c", "one.part2.rar"),
		recoveredVolume("d", "two.part2.rar"),
	}
	groups, collision := regroupRecoveredVolumes(recovered, nil, p.getBaseFilename)
	if groups == nil || len(groups) != 2 {
		t.Fatalf("got %d groups (collision %q), want 2", len(groups), collision)
	}
	for _, g := range groups {
		if len(g.Files) != 2 {
			t.Fatalf("group %q has %d volumes, want 2", g.BaseName, len(g.Files))
		}
	}
}

// RAR4 volume names (.rar, .r00, .r01) are one archive too.
func TestRegroupRecoveredVolumes_RAR4Names(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	recovered := []recoveredFile{
		recoveredVolume("a", "nora.s01e06.r01"),
		recoveredVolume("b", "nora.s01e06.rar"),
		recoveredVolume("c", "nora.s01e06.r00"),
	}
	groups, collision := regroupRecoveredVolumes(recovered, nil, p.getBaseFilename)
	if groups == nil || len(groups) != 1 {
		t.Fatalf("got %d groups (collision %q), want one archive", len(groups), collision)
	}
	for _, g := range groups {
		if len(g.Files) != 3 {
			t.Fatalf("archive has %d volumes, want 3", len(g.Files))
		}
	}
}
