package parser

import (
	"context"
	"fmt"
	"testing"

	"github.com/Tensai75/nzbparser"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// obfuscatedFile is a posted file whose subject says nothing about it: n
// articles of 1000 wire bytes with message IDs "<subject-N>".
func obfuscatedFile(subject string, n int) nzbparser.NzbFile {
	f := nzbparser.NzbFile{Filename: subject, Basefilename: subject}
	// Listed last article first: the first article is found by number.
	for i := n; i >= 1; i-- {
		f.Segments = append(f.Segments, nzbparser.NzbSegment{Number: i, Bytes: 1000, Id: fmt.Sprintf("<%s-%d>", subject, i)})
	}
	return f
}

func refNames[T storage.Par2FileRef | storage.PostedFileRef](refs []T, name func(T) string) map[string]bool {
	out := make(map[string]bool, len(refs))
	for _, r := range refs {
		out[name(r)] = true
	}
	return out
}

func par2RefNames(refs []storage.Par2FileRef) map[string]bool {
	return refNames(refs, func(r storage.Par2FileRef) string { return r.Name })
}

func postedRefNames(refs []storage.PostedFileRef) map[string]bool {
	return refNames(refs, func(r storage.PostedFileRef) string { return r.Name })
}

func wantNames(t *testing.T, what string, got map[string]bool, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
	for _, n := range want {
		if !got[n] {
			t.Fatalf("%s = %v, want %v", what, got, want)
		}
	}
}

// The PAR2 files of a release with obfuscated subjects are named only in
// their yEnc headers. They must be kept as the release's PAR2 files, under
// those names (the repair reads a volume's slice range from its name), and
// stay out of the posted files PAR2 protects.
func TestBuildPar2RefsFindsPar2ByYencName(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	files := nzbparser.NzbFiles{
		obfuscatedFile("idx", 1),
		obfuscatedFile("vol1", 3),
		obfuscatedFile("vol2", 3),
		obfuscatedFile("plus", 2),
		obfuscatedFile("hyphen", 5),
	}
	fetch := fakeYencFetch(map[string]*nntp.YencMetadata{
		"<idx-1>":    {Name: "X.par2", Size: 900, Begin: 1, End: 900},
		"<vol1-1>":   {Name: "X.7z.001", Size: 2900, Begin: 1, End: 970},
		"<vol2-1>":   {Name: "X.7z.002", Size: 2500, Begin: 1, End: 970},
		"<plus-1>":   {Name: "X.vol00+01.par2", Size: 1500, Begin: 1, End: 970},
		"<hyphen-1>": {Name: "X.vol01-03.par2", Size: 4500, Begin: 1, End: 970},
	})

	par2Files, source, aborted := buildPar2RefsWithFetch(context.Background(), p.logger, 4, files, p.detectFileType, fetch, nil)
	if aborted {
		t.Fatal("aborted = true, want false")
	}
	wantNames(t, "PAR2 files", par2RefNames(par2Files), "X.par2", "X.vol00+01.par2", "X.vol01-03.par2")
	// The posted files keep their subject names: the repair's match cache and
	// its layout lookups are keyed by them.
	wantNames(t, "posted files", postedRefNames(source), "vol1", "vol2")

	for _, f := range par2Files {
		if f.Name == "X.par2" && (f.Size != 900 || len(f.Segments) != 1 || f.Segments[0].MessageID != "<idx-1>") {
			t.Errorf("X.par2 = %+v, want one article <idx-1> of 900 bytes", f)
		}
		if f.Name == "X.vol01-03.par2" && (len(f.Segments) != 5 || f.Segments[0].MessageID != "<hyphen-1>") {
			t.Errorf("X.vol01-03.par2 articles = %+v, want 5 in number order", f.Segments)
		}
	}
	for _, f := range source {
		if f.Name == "vol2" && f.Size != 2500 {
			t.Errorf("vol2 size = %d, want its own header's 2500", f.Size)
		}
	}
}

// The probe does not run for every file (it stops after enough missing
// articles) and can fail. The yEnc name content detection already read still
// says which files are PAR2.
func TestBuildPar2RefsUsesSniffedPar2NameWithoutProbe(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	files := nzbparser.NzbFiles{
		obfuscatedFile("vol1", 3),
		obfuscatedFile("idx", 1),
		obfuscatedFile("rec", 3),
		obfuscatedFile("other", 1),
	}
	par2Names := map[string]string{
		fileMetaKey(files[1]): "X.par2",
		fileMetaKey(files[2]): "X.vol00+02.par2",
	}
	var fetched []string
	fetch := func(_ context.Context, messageID string) (*nntp.YencMetadata, error) {
		fetched = append(fetched, messageID)
		if messageID == "<vol1-1>" {
			return &nntp.YencMetadata{Name: "X.7z.001", Size: 2900, Begin: 1, End: 970}, nil
		}
		return nil, context.DeadlineExceeded
	}

	par2Files, source, _ := buildPar2RefsWithFetch(context.Background(), p.logger, 1, files, p.detectFileType, fetch, par2Names)
	wantNames(t, "PAR2 files", par2RefNames(par2Files), "X.par2", "X.vol00+02.par2")
	wantNames(t, "posted files", postedRefNames(source), "vol1", "other")
	// A known PAR2 volume whose articles fit the release's article size takes
	// that size and needs no fetch of its own.
	for _, id := range fetched {
		if id == "<rec-1>" {
			t.Errorf("fetched %s: a PAR2 volume known from content detection needs no probe", id)
		}
	}
}

// A file whose subject names it as one of the release's own files is grouped
// as that; a yEnc name ending .par2 does not move it to the PAR2 files.
func TestBuildPar2RefsSubjectTypeOutranksYencName(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	rar := obfuscatedFile("rar", 2)
	rar.Filename = "release.rar"
	files := nzbparser.NzbFiles{rar}
	fetch := fakeYencFetch(map[string]*nntp.YencMetadata{
		"<rar-1>": {Name: "release.par2", Size: 1900, Begin: 1, End: 970},
	})
	par2Files, source, _ := buildPar2RefsWithFetch(context.Background(), p.logger, 1, files, p.detectFileType, fetch,
		map[string]string{fileMetaKey(rar): "release.par2"})
	if len(par2Files) != 0 {
		t.Fatalf("PAR2 files = %+v, want none", par2Files)
	}
	wantNames(t, "posted files", postedRefNames(source), "release.rar")
}

func TestSniffedPar2Names(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	idx, vol, media, bare := obfuscatedFile("a", 1), obfuscatedFile("b", 4), obfuscatedFile("c", 4), obfuscatedFile("d", 1)
	got := sniffedPar2Names([]contentResult{
		{file: idx, fileType: storage.NZBFileTypePar2, actualFilename: "X.par2"},
		{file: vol, fileType: storage.NZBFileTypePar2, actualFilename: "X.vol01-03.PAR2"},
		{file: media, fileType: storage.NZBFileTypeMedia, actualFilename: "X.mkv"},
		{file: bare, fileType: storage.NZBFileTypeRar},
	}, p.detectFileType)
	want := map[string]string{"m:<a-1>": "X.par2", "m:<b-1>": "X.vol01-03.PAR2"}
	if len(got) != len(want) {
		t.Fatalf("sniffedPar2Names = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("sniffedPar2Names = %v, want %v", got, want)
		}
	}
	if sniffedPar2Names(nil, p.detectFileType) != nil {
		t.Fatal("sniffedPar2Names(nil) is not nil")
	}
}
