package usenet

import (
	"context"
	"errors"
	"testing"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func postedRef(name string, ids ...string) storage.PostedFileRef {
	pf := storage.PostedFileRef{Name: name}
	for _, id := range ids {
		pf.Segments = append(pf.Segments, storage.Par2SegmentRef{MessageID: id, Bytes: 100})
		pf.Size += 100
	}
	return pf
}

// obfuscatedRecord is a release imported before the parser recognised a PAR2
// file by its yEnc name: two archive volumes the stored file reads, then the
// PAR2 index and two recovery volumes, all stored as posted files.
func obfuscatedRecord() *storage.NZB {
	return &storage.NZB{
		Files: []storage.NZBFile{{
			Name:     "X.mkv",
			Segments: []storage.NZBSegment{{MessageID: "v1a"}, {MessageID: "v1b"}, {MessageID: "v2a"}},
		}},
		Par2Source: []storage.PostedFileRef{
			postedRef("s1", "idx"),
			postedRef("s2", "v1a", "v1b"),
			postedRef("s3", "v2a"),
			postedRef("s4", "r1a", "r1b"),
			postedRef("s5", "r2a"),
		},
	}
}

func TestPar2RefileCandidates(t *testing.T) {
	nzb := obfuscatedRecord()
	got := par2RefileCandidates(nzb)
	if len(got) != 3 || got[0] != 0 || got[1] != 3 || got[2] != 4 {
		t.Fatalf("candidates = %v, want [0 3 4] (the posted files no stored file reads)", got)
	}
	if !Par2FilesMayBeAmongPosted(nzb) {
		t.Fatal("Par2FilesMayBeAmongPosted = false, want true")
	}

	// A record that has PAR2 files is left alone.
	withPar2 := obfuscatedRecord()
	withPar2.Par2Files = []storage.Par2FileRef{{Name: "X.par2"}}
	if Par2FilesMayBeAmongPosted(withPar2) {
		t.Fatal("a record with PAR2 files has candidates")
	}

	// A posted file whose name gives its type is not a misfiled PAR2 file: a
	// release posted without PAR2 files, with an .nfo no stored file reads.
	named := &storage.NZB{
		Files:      []storage.NZBFile{{Segments: []storage.NZBSegment{{MessageID: "a"}}}},
		Par2Source: []storage.PostedFileRef{postedRef("X.part01.rar", "a"), postedRef("X.nfo", "n"), postedRef("X.part02.rar", "unread")},
	}
	if Par2FilesMayBeAmongPosted(named) {
		t.Fatal("posted files with known names are candidates")
	}
	if Par2FilesMayBeAmongPosted(nil) || Par2FilesMayBeAmongPosted(&storage.NZB{}) {
		t.Fatal("an empty record has candidates")
	}
}

func TestRefilePar2RefsMovesNamedPar2Files(t *testing.T) {
	nzb := obfuscatedRecord()
	yencNames := map[string]string{"idx": "X.par2", "r1a": "X.vol00+02.par2", "r2a": "X.vol02-03.PAR2"}
	var asked []string
	names, err := par2NamesAmongPosted(context.Background(), nzb, par2RefileCandidates(nzb), 1, func(_ context.Context, id string) (string, error) {
		asked = append(asked, id)
		return yencNames[id], nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(asked) != 3 {
		t.Fatalf("fetched %v, want only the three candidates' first articles", asked)
	}
	if moved := refilePar2Refs(nzb, names); moved != 3 {
		t.Fatalf("moved = %d, want 3", moved)
	}
	if len(nzb.Par2Files) != 3 || nzb.Par2Files[0].Name != "X.par2" || nzb.Par2Files[1].Name != "X.vol00+02.par2" || nzb.Par2Files[2].Name != "X.vol02-03.PAR2" {
		t.Fatalf("Par2Files = %+v, want the index and both volumes under their yEnc names", nzb.Par2Files)
	}
	if v := nzb.Par2Files[1]; v.Size != 200 || len(v.Segments) != 2 || v.Segments[1].MessageID != "r1b" {
		t.Fatalf("moved volume = %+v, want its size and articles kept", v)
	}
	if len(nzb.Par2Source) != 2 || nzb.Par2Source[0].Name != "s2" || nzb.Par2Source[1].Name != "s3" {
		t.Fatalf("Par2Source = %+v, want the two archive volumes in order", nzb.Par2Source)
	}
	if Par2FilesMayBeAmongPosted(nzb) {
		t.Fatal("a refiled record still has candidates")
	}
}

func TestPar2NamesAmongPostedFailures(t *testing.T) {
	nzb := obfuscatedRecord()
	candidates := par2RefileCandidates(nzb)

	// A recovery volume no provider holds is passed over; the rest move.
	names, err := par2NamesAmongPosted(context.Background(), nzb, candidates, 2, func(_ context.Context, id string) (string, error) {
		if id == "r2a" {
			return "", &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: "no such article"}
		}
		return map[string]string{"idx": "X.par2", "r1a": "X.vol00+02.par2"}[id], nil
	})
	if err != nil || len(names) != 2 {
		t.Fatalf("names = %v, err = %v; want the two readable PAR2 files", names, err)
	}

	// A fetch that fails for another reason stops the refile: moving part of
	// the set would leave the rest behind for good.
	names, err = par2NamesAmongPosted(context.Background(), nzb, candidates, 2, func(_ context.Context, id string) (string, error) {
		if id == "r1a" {
			return "", errors.New("read tcp: i/o timeout")
		}
		return "X.par2", nil
	})
	if err == nil || names != nil {
		t.Fatalf("names = %v, err = %v; want an error and no names", names, err)
	}

	// No PAR2 names among the candidates: nothing to move, no error.
	names, err = par2NamesAmongPosted(context.Background(), nzb, candidates, 2, func(context.Context, string) (string, error) {
		return "X.7z.003", nil
	})
	if err != nil || len(names) != 0 {
		t.Fatalf("names = %v, err = %v; want none", names, err)
	}
	if moved := refilePar2Refs(nzb, names); moved != 0 || len(nzb.Par2Source) != 5 {
		t.Fatalf("moved = %d with %d posted files left, want the record untouched", moved, len(nzb.Par2Source))
	}
}

// The refiled lists survive the record codec, measured-size marks included.
func TestRefilePar2RefsIsStored(t *testing.T) {
	nzb := obfuscatedRecord()
	nzb.ID, nzb.Name = "r1", "r1"
	nzb.Par2Source[3].Segments[0].Real = true
	names := map[string]string{"idx": "X.par2", "r1a": "X.vol00+02.par2", "r2a": "X.vol02+01.par2"}
	if moved := refilePar2Refs(nzb, names); moved != 3 {
		t.Fatalf("moved = %d, want 3", moved)
	}
	data, err := encodeNZBV2(nzb)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decodeNZBV2(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Par2Files) != 3 || len(got.Par2Source) != 2 {
		t.Fatalf("stored record has %d PAR2 files and %d posted files, want 3 and 2", len(got.Par2Files), len(got.Par2Source))
	}
	if v := got.Par2Files[1]; v.Name != "X.vol00+02.par2" || len(v.Segments) != 2 || !v.Segments[0].Real || v.Segments[1].Real {
		t.Fatalf("stored volume = %+v, want its name, articles and measured-size marks kept", v)
	}
	if hdr, err := decodeNZBV2Header(data); err != nil || len(hdr.Par2Files) != 3 {
		t.Fatalf("header read: %v, PAR2 files %d, want 3", err, len(hdr.Par2Files))
	}
}
