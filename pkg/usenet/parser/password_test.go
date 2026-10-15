package parser

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestSplitNamePassword(t *testing.T) {
	for _, test := range []struct{ in, name, password string }{
		{"Show.S01.nzb", "Show.S01.nzb", ""},
		{"Show.S01{{secret}}.nzb", "Show.S01.nzb", "secret"},
		{"Show.S01 {{p w}}", "Show.S01", "p w"},
		{"Show{{a}}.S01{{b}}.nzb", "Show{{a}}.S01.nzb", "b"},
		{"Show{{}}.nzb", "Show{{}}.nzb", ""},
	} {
		name, password := SplitNamePassword(test.in)
		if name != test.name || password != test.password {
			t.Errorf("SplitNamePassword(%q) = %q, %q; want %q, %q", test.in, name, password, test.name, test.password)
		}
	}
}

const passwordTestFiles = `<file poster="p" date="1" subject="&quot;a.mkv&quot; yEnc (1/1)"><groups><group>a.b</group></groups><segments><segment bytes="10" number="1">a@x</segment></segments></file></nzb>`

func TestWithPasswordMeta(t *testing.T) {
	const tricky = `a&b<c"d'e`
	for _, test := range []struct{ name, doc string }{
		{"no head", `<?xml version="1.0"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">` + passwordTestFiles},
		{"empty head", `<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"><head/>` + passwordTestFiles},
		{"head with title", `<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"><head><meta type="title">T &amp; U</meta></head>` + passwordTestFiles},
		{"head with a password", `<nzb><head><meta type="password">first</meta></head>` + passwordTestFiles},
	} {
		t.Run(test.name, func(t *testing.T) {
			out := WithPasswordMeta([]byte(test.doc), tricky)
			nzb, _, passwords, err := parseNZBWithPasswords(strings.NewReader(string(out)))
			if err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
			if len(nzb.Files) != 1 {
				t.Fatalf("files = %d\n%s", len(nzb.Files), out)
			}
			if passwords[len(passwords)-1] != tricky {
				t.Fatalf("passwords = %q\n%s", passwords, out)
			}
			if strings.Contains(test.doc, "first") && passwords[0] != "first" {
				t.Fatalf("existing password moved: %q", passwords)
			}
			if strings.Contains(test.doc, "title") && nzb.Meta["title"] != "T & U" {
				t.Fatalf("title = %q", nzb.Meta["title"])
			}
			if again := WithPasswordMeta(out, tricky); string(again) != string(out) {
				t.Fatalf("a password already listed was added again:\n%s", again)
			}
		})
	}
	if out := WithPasswordMeta([]byte("not an nzb"), "pw"); string(out) != "not an nzb" {
		t.Fatalf("non-NZB content changed: %s", out)
	}
	// A subject quoting "<head>" lies past the first <file> and is left alone.
	doc := `<nzb><file subject="&lt;head&gt;"/></nzb>`
	if out := string(WithPasswordMeta([]byte(doc), "pw")); !strings.HasPrefix(out, `<nzb><head><meta type="password">pw</meta></head><file`) {
		t.Fatalf("got %s", out)
	}
}

func TestMovePasswordIntoNZB(t *testing.T) {
	doc := []byte(`<nzb>` + passwordTestFiles)
	name, out := MovePasswordIntoNZB("Show.S01{{from name}}.nzb", doc, "from field")
	if name != "Show.S01.nzb" {
		t.Fatalf("name = %q", name)
	}
	_, _, passwords, err := parseNZBWithPasswords(strings.NewReader(string(out)))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(passwords, []string{"from name", "from field"}) {
		t.Fatalf("passwords = %q", passwords)
	}
	if name, out := MovePasswordIntoNZB("Plain.nzb", doc); name != "Plain.nzb" || string(out) != string(doc) {
		t.Fatalf("plain name changed: %q %s", name, out)
	}
}

func TestParseNZBKeepsEveryPassword(t *testing.T) {
	doc := `<nzb><head><meta type="password">one</meta><meta type="Password"> two </meta><meta type="password"></meta><meta type="password"><![CDATA[th&ree]]></meta></head>` + passwordTestFiles
	nzb, _, passwords, err := parseNZBWithPasswords(strings.NewReader(doc))
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"one", "two", "th&ree"}; !reflect.DeepEqual(passwords, want) {
		t.Fatalf("passwords = %q, want %q", passwords, want)
	}
	if nzb.Meta["password"] != "th&ree" {
		t.Fatalf("Meta[password] = %q", nzb.Meta["password"])
	}
}

func TestPasswordCandidates(t *testing.T) {
	got := passwordCandidates([]string{"a", " b ", ""}, []string{"a"}, []string{"c", "b"})
	if want := []string{"a", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestTryPasswords(t *testing.T) {
	p := &NZBParser{logger: zerolog.New(io.Discard)}
	group := &FileGroup{BaseName: "g"}
	opens := func(right string) func(string) ([]*storage.NZBFile, error) {
		return func(password string) ([]*storage.NZBFile, error) {
			file := &storage.NZBFile{Name: "a.mkv", IsEncrypted: true, Password: password}
			if password == right {
				file.EncryptionKey = make([]byte, 32)
			}
			return []*storage.NZBFile{file}, nil
		}
	}

	files, err := p.tryPasswords(group, []string{"x", "y", "right"}, opens("right"))
	if err != nil || len(files) != 1 || files[0].Password != "right" {
		t.Fatalf("files=%v err=%v", files, err)
	}
	if _, err := p.tryPasswords(group, []string{"x", "y"}, opens("right")); !errors.Is(err, ErrArchivePassword) {
		t.Fatalf("err = %v, want ErrArchivePassword", err)
	}
	if _, err := p.tryPasswords(group, nil, opens("right")); !errors.Is(err, ErrArchivePassword) {
		t.Fatalf("no candidates: err = %v, want ErrArchivePassword", err)
	}

	// Any other failure is final: the next candidate would only repeat it.
	calls := 0
	_, err = p.tryPasswords(group, []string{"x", "y"}, func(string) ([]*storage.NZBFile, error) {
		calls++
		return nil, fmt.Errorf("compressed media")
	})
	if err == nil || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}
