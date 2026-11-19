package parser

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/Tensai75/nzbparser"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

func mediaGroupFile(name, base string, number int, ids ...string) nzbparser.NzbFile {
	f := nzbparser.NzbFile{Filename: name, Basefilename: base, Number: number, Groups: []string{"alt.binaries.test"}}
	for i, id := range ids {
		f.Segments = append(f.Segments, nzbparser.NzbSegment{Number: i + 1, Id: id, Bytes: 1000})
	}
	return f
}

func processedMediaFiles(t *testing.T, p *NZBParser, groups map[string]*FileGroup) map[string]*storage.NZBFile {
	t.Helper()
	out := make(map[string]*storage.NZBFile)
	for _, g := range groups {
		if g.Type != storage.NZBFileTypeMedia {
			t.Fatalf("group %q has type %q, want media", g.BaseName, g.Type)
		}
		f := p.processMediaFile(g, "")
		if f == nil {
			t.Fatalf("group %q produced no file", g.BaseName)
		}
		out[f.Name] = f
	}
	return out
}

func segmentIDs(f *storage.NZBFile) []string {
	ids := make([]string, 0, len(f.Segments))
	for _, s := range f.Segments {
		ids = append(ids, s.MessageID)
	}
	return ids
}

// Two media files that share a base name (a video and its external audio
// track) are two files, not one with the second appended.
func TestMediaFilesSharingABaseNameStaySeparate(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop(), maxConcurrent: 1}
	files := nzbparser.NzbFiles{
		mediaGroupFile("Movie.2020.mkv", "Movie.2020", 1, "v1@x", "v2@x"),
		mediaGroupFile("Movie.2020.flac", "Movie.2020", 2, "a1@x"),
	}
	groups, _ := p.groupFiles(context.Background(), files)
	got := processedMediaFiles(t, p, groups)
	var names []string
	for n := range got {
		names = append(names, n)
	}
	sort.Strings(names)
	if len(names) != 2 || names[0] != "Movie.2020.flac" || names[1] != "Movie.2020.mkv" {
		t.Fatalf("files = %v, want [Movie.2020.flac Movie.2020.mkv]", names)
	}
	if ids := segmentIDs(got["Movie.2020.mkv"]); len(ids) != 2 || ids[0] != "v1@x" || ids[1] != "v2@x" {
		t.Fatalf("mkv segments = %v, want [v1@x v2@x]", ids)
	}
	if ids := segmentIDs(got["Movie.2020.flac"]); len(ids) != 1 || ids[0] != "a1@x" {
		t.Fatalf("flac segments = %v, want [a1@x]", ids)
	}
}

// The same holds when the names come from the yEnc header of obfuscated
// posts: an external .mka found by content keeps apart from the .mkv.
func TestContentNamedMediaFilesSharingABaseNameStaySeparate(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	groups := p.groupProcessedFiles([]contentResult{
		{file: mediaGroupFile("aaaa", "aaaa", 1, "v1@x"), fileType: storage.NZBFileTypeMedia, actualFilename: "Show.S01E01.mkv"},
		{file: mediaGroupFile("bbbb", "bbbb", 2, "a1@x"), fileType: storage.NZBFileTypeMedia, actualFilename: "Show.S01E01.mka"},
	})
	got := processedMediaFiles(t, p, groups)
	if len(got) != 2 || got["Show.S01E01.mkv"] == nil || got["Show.S01E01.mka"] == nil {
		names := make([]string, 0, len(got))
		for n := range got {
			names = append(names, n)
		}
		t.Fatalf("files = %v, want Show.S01E01.mkv and Show.S01E01.mka", names)
	}
}

// True parts of one file still join: a split .mkv.001/.mkv.002 is one file.
func TestSplitMediaPartsStillJoin(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	groups := p.groupProcessedFiles([]contentResult{
		{file: mediaGroupFile("Movie.mkv.001", "Movie", 1, "p1@x"), fileType: storage.NZBFileTypeMedia, actualFilename: "Movie.mkv.001"},
		{file: mediaGroupFile("Movie.mkv.002", "Movie", 2, "p2@x"), fileType: storage.NZBFileTypeUnknown, actualFilename: "Movie.mkv.002"},
	})
	if len(groups) != 1 {
		t.Fatalf("groups = %d, want 1", len(groups))
	}
	for _, g := range groups {
		if len(g.Files) != 2 {
			t.Fatalf("group %q holds %d files, want 2", g.BaseName, len(g.Files))
		}
	}
}

func mkvAndFlacGroups(p *NZBParser) map[string]*FileGroup {
	return p.groupProcessedFiles([]contentResult{
		{file: mediaGroupFile("Movie.mkv", "Movie", 3, "v1@x"), fileType: storage.NZBFileTypeMedia, actualFilename: "Movie.mkv"},
		{file: mediaGroupFile("Movie.flac", "Movie", 4, "a1@x"), fileType: storage.NZBFileTypeMedia, actualFilename: "Movie.flac"},
	})
}

func assertMkvAndFlacKept(t *testing.T, p *NZBParser, groups map[string]*FileGroup) {
	t.Helper()
	var media int
	names := map[string]bool{}
	for _, g := range groups {
		if g.Type != storage.NZBFileTypeMedia {
			continue
		}
		media++
		if f := p.processMediaFile(g, ""); f != nil {
			names[f.Name] = true
		}
	}
	if media != 2 || !names["Movie.mkv"] || !names["Movie.flac"] {
		t.Fatalf("media groups = %d, files %v, want Movie.mkv and Movie.flac", media, names)
	}
}

// The obfuscated RAR merge rebuilds the group map; the two media groups of
// one base name must both come through it.
func TestMediaFilesSharingABaseNameSurviveTheRarMerge(t *testing.T) {
	probe := &fakeVolumeProbe{volume: map[string]bool{}}
	p := &NZBParser{logger: zerolog.Nop(), maxConcurrent: 2, volumeProbe: probe.probe}
	groups := mkvAndFlacGroups(p)
	rars, raw := singleFileRarGroups(
		postedVolume(1, "k3j9.rar", 5, 100),
		postedVolume(2, "a9xq.rar", 2, 100),
	)
	for k, g := range rars {
		groups[k] = g
	}
	got := p.mergeObfuscatedRarGroups(context.Background(), groups, raw, nil)
	if len(got) != 3 {
		t.Fatalf("groups = %d, want the merged archive and two media files", len(got))
	}
	assertMkvAndFlacKept(t, p, got)
}

// So must they through the PAR2 name recovery regroup.
func TestMediaFilesSharingABaseNameSurviveTheRecoveryRegroup(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	var others []*FileGroup
	for _, g := range mkvAndFlacGroups(p) {
		others = append(others, g)
	}
	got, collision := regroupRecoveredVolumes([]recoveredFile{
		recoveredVolume("x1", "Archive.part01.rar"),
		recoveredVolume("x2", "Archive.part02.rar"),
	}, others, p.getBaseFilename)
	if got == nil {
		t.Fatalf("aborted on key %q", collision)
	}
	if len(got) != 3 {
		t.Fatalf("groups = %d, want the archive and two media files", len(got))
	}
	assertMkvAndFlacKept(t, p, got)
}

func groupSummary(groups map[string]*FileGroup) map[storage.NZBFileType][]string {
	out := map[storage.NZBFileType][]string{}
	for _, g := range groups {
		var names []string
		for _, f := range g.Files {
			names = append(names, f.Filename)
		}
		sort.Strings(names)
		out[g.Type] = append(out[g.Type], g.BaseName+":"+strings.Join(names, ","))
	}
	return out
}

// A RAR volume and the parts of a split media file can share a base name
// ("Movie.rar" and "Movie.mkv.001"): they are two files, and grouped as one
// the media parts would be read as RAR volumes.
func TestSplitMediaPartsKeepApartFromASameBaseRar(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	groups := p.groupProcessedFiles([]contentResult{
		{file: mediaGroupFile("Movie.rar", "Movie", 1, "r1@x"), fileType: storage.NZBFileTypeRar, actualFilename: "Movie.rar"},
		{file: mediaGroupFile("Movie.mkv.001", "Movie", 2, "p1@x"), fileType: storage.NZBFileTypeMedia, actualFilename: "Movie.mkv.001"},
		{file: mediaGroupFile("Movie.mkv.002", "Movie", 3, "p2@x"), fileType: storage.NZBFileTypeUnknown, actualFilename: "Movie.mkv.002"},
	})
	got := groupSummary(groups)
	if len(groups) != 2 ||
		len(got[storage.NZBFileTypeRar]) != 1 || got[storage.NZBFileTypeRar][0] != "Movie:Movie.rar" ||
		len(got[storage.NZBFileTypeMedia]) != 1 || got[storage.NZBFileTypeMedia][0] != "Movie:Movie.mkv.001,Movie.mkv.002" {
		t.Fatalf("groups = %v, want a RAR group of Movie.rar and a media group of both .mkv parts", got)
	}
}

// An unsplit file and numbered parts of the same name stay apart, as fix 65
// made them: the parts must not be appended to the whole file.
func TestSplitMediaPartsKeepApartFromTheUnsplitFile(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	groups := p.groupProcessedFiles([]contentResult{
		{file: mediaGroupFile("Movie.mkv", "Movie", 1, "w1@x"), fileType: storage.NZBFileTypeMedia, actualFilename: "Movie.mkv"},
		{file: mediaGroupFile("Movie.mkv.001", "Movie", 2, "p1@x"), fileType: storage.NZBFileTypeMedia, actualFilename: "Movie.mkv.001"},
	})
	got := groupSummary(groups)
	media := got[storage.NZBFileTypeMedia]
	sort.Strings(media)
	if len(groups) != 2 || len(media) != 2 || media[0] != "Movie:Movie.mkv" || media[1] != "Movie:Movie.mkv.001" {
		t.Fatalf("groups = %v, want Movie.mkv and Movie.mkv.001 in two media groups", got)
	}
}
