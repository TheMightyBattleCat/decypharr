package parser

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/Tensai75/nzbparser"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

type xmlCopy struct {
	poster, subject, idPrefix string
	numbers                   []int
}

func nzbXML(copies ...xmlCopy) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8" ?>` + "\n" + `<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">` + "\n")
	b.WriteString(`<head><meta type="title">t</meta></head>` + "\n")
	for _, c := range copies {
		fmt.Fprintf(&b, `<file poster="%s" date="1" subject="%s"><groups><group>a.b</group></groups><segments>`, c.poster, strings.ReplaceAll(c.subject, `"`, "&quot;"))
		for _, n := range c.numbers {
			fmt.Fprintf(&b, `<segment bytes="1000" number="%d">%s-%d@x</segment>`, n, c.idPrefix, n)
		}
		b.WriteString("</segments></file>\n")
	}
	b.WriteString("</nzb>\n")
	return b.String()
}

func seq(lo, hi int) []int {
	var out []int
	for i := lo; i <= hi; i++ {
		out = append(out, i)
	}
	return out
}

// TestParseNZB_KeepsOneCopyOfAFilePostedTwice replays Keeper S03E06 SiQ: every
// volume listed twice under one subject, by two posters, with different
// message IDs. Merged, each volume had every article number twice and
// getNZBSegments rejected all of them.
func TestParseNZB_KeepsOneCopyOfAFilePostedTwice(t *testing.T) {
	subj := func(n int) string {
		return fmt.Sprintf(`[%02d/12] - "Keeper.S03E06.mkv.part%d.rar" yEnc (1/4)`, n, n)
	}
	content := nzbXML(
		xmlCopy{"OxwfDc359Ds61]iVy", subj(1), "a1", seq(1, 4)},
		xmlCopy{"iVy", subj(1), "b1", seq(1, 4)},
		xmlCopy{"iVy", subj(2), "b2", seq(1, 4)},
		xmlCopy{"OxwfDc359Ds61]iVy", subj(2), "a2", seq(1, 4)},
	)

	merged, err := nzbparser.Parse(strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := getNZBSegments(0, merged.Files[0], &FileGroup{Files: merged.Files[:1]}); n != 0 {
		t.Fatalf("nzbparser.Parse: getNZBSegments sized the merged copies at %d; the replay is off", n)
	}

	nzb, dropped, err := parseNZB(strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if len(dropped) != 2 || len(nzb.Files) != 2 {
		t.Fatalf("dropped %v, %d files; want one copy of each of the 2 files kept", dropped, len(nzb.Files))
	}
	for i, want := range []string{"a1", "b2"} { // the first copy in document order
		f := nzb.Files[i]
		if len(f.Segments) != 4 || !strings.HasPrefix(f.Segments[0].Id, want+"-") {
			t.Fatalf("file %d = %d segments from %q, want the 4 of copy %s", i, len(f.Segments), f.Segments[0].Id, want)
		}
		if n, segs := getNZBSegments(0, f, &FileGroup{Files: []nzbparser.NzbFile{f}}); n == 0 || len(segs) != 4 {
			t.Fatalf("file %d: getNZBSegments rejected the kept copy", i)
		}
	}
}

// Entries that list one posting in pieces, or repeat the same message IDs,
// still merge; a subject without a complete copy is left to MakeUnique.
func TestParseNZB_StillMergesPiecesAndRepeats(t *testing.T) {
	const s = `"f.rar" yEnc (1/4)`
	cases := []struct {
		name   string
		copies []xmlCopy
	}{
		{"pieces of one posting", []xmlCopy{{"p", s, "a", seq(1, 2)}, {"p", s, "a", seq(3, 4)}}},
		{"the same articles listed twice", []xmlCopy{{"p", s, "a", seq(1, 4)}, {"p", s, "a", seq(1, 4)}}},
		{"no complete copy", []xmlCopy{{"p", s, "a", seq(1, 3)}, {"q", s, "b", seq(2, 4)}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			content := nzbXML(tc.copies...)
			want, err := nzbparser.Parse(strings.NewReader(content))
			if err != nil {
				t.Fatal(err)
			}
			got, dropped, err := parseNZB(strings.NewReader(content))
			if err != nil {
				t.Fatal(err)
			}
			if len(dropped) != 0 || len(got.Files) != 1 || len(got.Files[0].Segments) != len(want.Files[0].Segments) {
				t.Fatalf("dropped %v, files %d; want nzbparser's merge of %d segments", dropped, len(got.Files), len(want.Files[0].Segments))
			}
		})
	}
}

// Posters that scramble the [n/m] counter give many files one number.
// nzbparser.Parse sorted with sort.Sort, which reorders files within a number
// (f20 f35 f25 f15 ... for this NZB), so the order they were posted in was
// lost before anything could fall back to it.
func TestParseNZB_KeepsDocumentOrderForTiedNumbers(t *testing.T) {
	var copies []xmlCopy
	for i := 0; i < 40; i++ {
		subject := fmt.Sprintf(`[%d/11] - "obf%02d" yEnc (1/2)`, i%5+1, i)
		copies = append(copies, xmlCopy{"p", subject, fmt.Sprintf("f%02d", i), seq(1, 2)})
	}
	content := nzbXML(copies...)
	check := func(files nzbparser.NzbFiles) error {
		var want []string
		for n := 0; n < 5; n++ {
			for i := n; i < 40; i += 5 {
				want = append(want, fmt.Sprintf("f%02d-1@x", i))
			}
		}
		for i, f := range files {
			if f.Segments[0].Id != want[i] {
				return fmt.Errorf("file %d is %s, want %s (by number, then document order)", i, f.Segments[0].Id, want[i])
			}
		}
		return nil
	}

	old, err := nzbparser.Parse(strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if check(old.Files) == nil {
		t.Fatal("nzbparser.Parse kept document order; the replay is off")
	}
	nzb, _, err := parseNZB(strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if err := check(nzb.Files); err != nil {
		t.Fatal(err)
	}
	if nzb.Meta["title"] != "t" {
		t.Fatalf("meta = %v, want title t", nzb.Meta)
	}
}

// Obfuscated single-file RAR groups come out of a map. With scrambled subject
// numbers the merge sorted them by number alone, unstably, so volumes sharing
// a number were ordered differently from run to run. They must come out by
// number, then in the order the NZB lists them, every time.
func TestMergeObfuscatedRarGroups_BreaksNumberTiesByNZBOrder(t *testing.T) {
	var raw nzbparser.NzbFiles
	for i := 0; i < 30; i++ {
		raw = append(raw, postedVolume(i%3+1, fmt.Sprintf("%08x", i*7919), 2, 100))
	}
	var want []string
	for n := 1; n <= 3; n++ {
		for _, f := range raw {
			if f.Number == n {
				want = append(want, f.Filename)
			}
		}
	}
	p := &NZBParser{logger: zerolog.Nop()}
	for run := 0; run < 20; run++ {
		groups := map[string]*FileGroup{}
		for _, f := range raw {
			groups[f.Filename] = &FileGroup{BaseName: f.Filename, Type: storage.NZBFileTypeRar, Files: []nzbparser.NzbFile{f}, Groups: map[string]struct{}{}}
		}
		merged := p.mergeObfuscatedRarGroups(context.Background(), groups, raw)
		if len(merged) != 1 {
			t.Fatalf("run %d: %d groups, want 1", run, len(merged))
		}
		for _, g := range merged {
			for i, f := range g.Files {
				if f.Filename != want[i] {
					t.Fatalf("run %d: volume %d is %s, want %s", run, i, f.Filename, want[i])
				}
			}
		}
	}
}

func TestNamesOrderVolumes(t *testing.T) {
	named := []nzbparser.NzbFile{{Filename: "a.rar"}, {Filename: "a.r00"}, {Filename: "a.r01"}}
	if !namesOrderVolumes(named) {
		t.Error("a.rar, a.r00, a.r01: want ordered by name")
	}
	obfuscated := []nzbparser.NzbFile{{Filename: "8f1c.rar"}, {Filename: "03ab.rar"}}
	if namesOrderVolumes(obfuscated) {
		t.Error("two plain .rar names: want not ordered by name")
	}
}

func TestParseNZB_RejectsANonNZBDocument(t *testing.T) {
	if _, _, err := parseNZB(strings.NewReader("<html><body>nope</body></html>")); err == nil {
		t.Fatal("parsed an HTML page as an NZB")
	}
}
