package parser

// nzbparser.NzbSegment.Id is the element's raw inner XML: entities are not
// decoded and line breaks inside it are kept. A line break would send what
// follows it as a second NNTP command; an escaped "&" would be asked for as
// "&amp;". parseNZBWithPasswords cleans each ID and drops a file holding one
// that cannot be sent.

import (
	"strings"
	"testing"
)

func segmentIDTestNZB(files ...string) string {
	var b strings.Builder
	b.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n<nzb xmlns=\"http://www.newzbin.com/DTD/2003/nzb\">\r\n")
	for _, f := range files {
		b.WriteString(f)
	}
	b.WriteString("</nzb>\r\n")
	return b.String()
}

func segmentIDTestFile(name, segment string) string {
	return "<file poster=\"p\" date=\"1\" subject=\"&quot;" + name + "&quot; yEnc (1/1)\">\r\n" +
		"<groups><group>alt.binaries.test</group></groups>\r\n" +
		"<segments><segment bytes=\"100\" number=\"1\">" + segment + "</segment></segments>\r\n" +
		"</file>\r\n"
}

func TestParseNZBCleansSegmentIDs(t *testing.T) {
	nzb := segmentIDTestNZB(
		segmentIDTestFile("one.bin", "a@test>\r\nDATE"),
		segmentIDTestFile("two.bin", "a&amp;b@test"),
		segmentIDTestFile("three.bin", "\r\n        &lt;c@test&gt;\r\n      "),
		segmentIDTestFile("four.bin", "d@test"),
		segmentIDTestFile("five.bin", "e@te st"),
		segmentIDTestFile("six.bin", "   "),
	)
	raw, _, badIDs, _, err := parseNZBWithPasswords(strings.NewReader(nzb))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got := map[string]string{}
	for _, f := range raw.Files {
		got[f.Filename] = f.Segments[0].Id
	}
	want := map[string]string{
		"two.bin":   "a&b@test", // asked for as "a&amp;b@test" before: a 430
		"three.bin": "c@test",   // pretty-printed, bracketed: trimmed and unwrapped
		"four.bin":  "d@test",   // unchanged
	}
	for name, id := range want {
		if got[name] != id {
			t.Errorf("%s: segment id %q, want %q", name, got[name], id)
		}
	}
	for _, name := range []string{"one.bin", "five.bin", "six.bin"} {
		if id, ok := got[name]; ok {
			t.Errorf("%s kept with segment id %q; want the file dropped", name, id)
		}
	}
	if len(raw.Files) != len(want) {
		t.Errorf("%d files kept, want %d", len(raw.Files), len(want))
	}
	if len(badIDs) != 3 {
		t.Fatalf("badIDs = %q, want the 3 dropped files", badIDs)
	}
	for i, name := range []string{"one.bin", "five.bin", "six.bin"} {
		if !strings.Contains(badIDs[i], name) {
			t.Errorf("badIDs[%d] = %q, want it to name %s", i, badIDs[i], name)
		}
	}
}

// An NZB whose every file holds a bad ID fails the parse instead of
// reaching the news server.
func TestParseNZBWithOnlyBadSegmentIDsFails(t *testing.T) {
	nzb := segmentIDTestNZB(segmentIDTestFile("one.bin", "a@test>\r\nDATE"))
	if _, _, badIDs, _, err := parseNZBWithPasswords(strings.NewReader(nzb)); err == nil || len(badIDs) != 1 {
		t.Fatalf("err = %v, badIDs = %q; want a parse error naming the file", err, badIDs)
	}
}

func TestCleanMessageID(t *testing.T) {
	for _, tc := range []struct {
		raw, want string
		ok        bool
	}{
		{"part1of10.AbC$xYz@poster.local", "part1of10.AbC$xYz@poster.local", true},
		{"<a@test>", "a@test", true},
		{"&lt;a@test&gt;", "a@test", true},
		{"a&#39;b@test", "a'b@test", true},
		{" &#10;a@test&#10; ", "a@test", true},
		{"a&#10;b@test", "", false},
		{"a@test>\r\nDATE", "", false},
		{"a\x00b@test", "", false},
		{"a<b@test", "", false},
		{"<<a@test>>", "", false},
		{"<>", "", false},
		{"", "", false},
		{"aé@test", "", false},
	} {
		got, err := cleanMessageID(tc.raw)
		if (err == nil) != tc.ok || got != tc.want {
			t.Errorf("cleanMessageID(%q) = %q, %v; want %q, ok %v", tc.raw, got, err, tc.want, tc.ok)
		}
	}
}
