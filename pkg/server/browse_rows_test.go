package server

import (
	"encoding/json"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"

	"github.com/sirrobot01/decypharr/internal/utils"
)

// Hostile names as a poster (archive member names) or a debrid account
// (torrent names, not cleaned) can deliver them.
var browseHostileNames = []string{
	"Show.S01E01&#39;);pwned=1;(&#39;.mkv", // HTML-decoded to ' inside an attribute
	"Show.S01E01');pwned=2;('.mkv",         // plain apostrophe
	`Movie "Cut".mkv`,                      // double quote
	"<img src=x onerror=pwned=3>",          // markup (a torrent name)
}

type browseTestEntry struct {
	Name      string `json:"name"`
	Path      string `json:"path"`
	IsDir     bool   `json:"is_dir"`
	Kind      string `json:"kind"`
	InfoHash  string `json:"info_hash,omitempty"`
	CanDelete bool   `json:"can_delete,omitempty"`
	Size      int64  `json:"size"`
}

func browseTestEntries() []browseTestEntry {
	var out []browseTestEntry
	for _, n := range browseHostileNames {
		// A file row inside a release (no info_hash: its id is its path) and a
		// release row (a directory with every menu action).
		out = append(out,
			browseTestEntry{Name: n, Path: "/__all__/Show.S01.1080p/" + n, Kind: "file", Size: 1 << 20},
			browseTestEntry{Name: n, Path: "/__all__/" + n, IsDir: true, Kind: "entry", InfoHash: "abc123", CanDelete: true},
		)
	}
	return out
}

// runBrowseJS evaluates browse.js (only the FileBrowser class, no top-level
// code) followed by script in JavaScriptCore and returns the last value.
func runBrowseJS(t *testing.T, script string) string {
	t.Helper()
	if _, err := exec.LookPath("osascript"); err != nil {
		t.Skip("needs osascript (JavaScriptCore)")
	}
	src, err := os.ReadFile("assets/js/browse.js")
	if err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(t.TempDir(), "browse_test.js")
	if err := os.WriteFile(f, []byte(string(src)+"\n;\n"+script), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("osascript", "-l", "JavaScript", f).CombinedOutput()
	if err != nil {
		t.Fatalf("osascript: %v: %s", err, out)
	}
	return strings.TrimSpace(string(out))
}

// A release or file name is data wherever the Browse page writes it: no event
// handler holds any part of it, each data-name and data-path decodes to the
// exact value, and markup in a name creates no element.
func TestBrowseRowHTMLKeepsNamesOutOfHandlers(t *testing.T) {
	for _, n := range browseHostileNames[:2] {
		if got := utils.RemoveInvalidChars(path.Base(n)); got != n {
			t.Fatalf("test setup: the parser's name cleaning changes %q to %q", n, got)
		}
	}
	entries := browseTestEntries()
	entriesJSON, _ := json.Marshal(entries)

	out := runBrowseJS(t, `
var fb = Object.create(FileBrowser.prototype);
fb.state = {selectedEntries: new Set(), health: new Map()};
JSON.stringify(`+string(entriesJSON)+`.map(function (e) { return fb.rowHtml(e); }))`)
	var rows []string
	if err := json.Unmarshal([]byte(out), &rows); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if len(rows) != len(entries) {
		t.Fatalf("got %d rows for %d entries", len(rows), len(entries))
	}

	for i, e := range entries {
		ctx := &html.Node{Type: html.ElementNode, Data: "tbody", DataAtom: atom.Tbody}
		nodes, err := html.ParseFragment(strings.NewReader(rows[i]), ctx)
		if err != nil {
			t.Fatalf("%s row %q: parse: %v", e.Kind, e.Name, err)
		}
		var handlers []string
		var names, paths, entryJSON []string
		var imgs int
		var walk func(*html.Node)
		walk = func(n *html.Node) {
			if n.Type == html.ElementNode {
				if n.Data == "img" {
					imgs++
				}
				for _, a := range n.Attr {
					switch {
					case strings.HasPrefix(a.Key, "on"):
						handlers = append(handlers, a.Key+"="+a.Val)
					case a.Key == "data-name":
						names = append(names, a.Val)
					case a.Key == "data-path":
						paths = append(paths, a.Val)
					case a.Key == "data-entry":
						entryJSON = append(entryJSON, a.Val)
					}
				}
			}
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				walk(c)
			}
		}
		for _, n := range nodes {
			walk(n)
		}

		label := e.Kind + " row " + e.Name
		if imgs != 0 {
			t.Errorf("%s: markup in the name created %d img element(s)", label, imgs)
		}
		for _, h := range handlers {
			if strings.Contains(h, "pwned") || strings.Contains(h, "Show.S01") || strings.Contains(h, "Movie") {
				t.Errorf("%s: an event handler holds part of the name: %s", label, h)
			}
		}
		// Run every handler the row still has, as the browser would (the
		// parser has already decoded its character references).
		for _, h := range handlers {
			code := h[strings.Index(h, "=")+1:]
			codeJSON, _ := json.Marshal(code)
			got := runBrowseJS(t, `
var pwned = 0;
var window = {fileBrowser: new Proxy({}, {get: function () { return function () {}; }})};
var event = {stopPropagation: function () {}, preventDefault: function () {}};
try { (new Function('event', `+string(codeJSON)+`)).call({checked: true}, event); } catch (e) { pwned = 'error: ' + e; }
String(pwned)`)
			if got != "0" {
				t.Errorf("%s: handler %q ran with pwned = %s", label, h, got)
			}
		}
		if len(names) == 0 {
			t.Errorf("%s: no data-name attribute", label)
		}
		for _, v := range names {
			if v != e.Name {
				t.Errorf("%s: data-name decodes to %q", label, v)
			}
		}
		if len(paths) == 0 {
			t.Errorf("%s: no data-path attribute", label)
		}
		for _, v := range paths {
			if v != e.Path {
				t.Errorf("%s: data-path decodes to %q", label, v)
			}
		}
		if len(entryJSON) != 1 {
			t.Fatalf("%s: %d data-entry attributes", label, len(entryJSON))
		}
		var back browseTestEntry
		if err := json.Unmarshal([]byte(entryJSON[0]), &back); err != nil || back != e {
			t.Errorf("%s: data-entry decodes to %+v (%v)", label, back, err)
		}
	}
}

// The delegated listeners call the row actions with the row's own values: a
// file whose name holds an apostrophe can be selected, its name downloads it
// (is_dir false, not read as a string), and a release row's menu rechecks and
// deletes it by its exact name and hash.
func TestBrowseDelegatedRowActions(t *testing.T) {
	entries := browseTestEntries()
	entriesJSON, _ := json.Marshal(entries)

	out := runBrowseJS(t, `
var calls = [];
var listeners = {};
var list = {
  addEventListener: function (type, fn) { listeners[type] = (listeners[type] || []).concat([fn]); },
  contains: function () { return true; },
};
var fb = Object.create(FileBrowser.prototype);
fb.state = {selectedEntries: new Set(), health: new Map()};
fb.refs = {fileBrowserList: list};
fb.navigate = function (p) { calls.push(['navigate', p]); };
fb.downloadFile = function (p, n) { calls.push(['download', p, n]); };
fb.recheckEntry = function (n) { calls.push(['recheck', n]); };
fb.deleteTorrent = function (h, n) { calls.push(['delete', h, n]); };
fb.handleEntrySelect = function (id, checked, entry) { calls.push(['select', id, checked, entry.name]); };
fb.showContextMenu = function (ev, entry) { calls.push(['context', entry.name]); };
fb.bindEntryListEvents();

// The row's attributes as the browser decodes them from rowHtml's output.
function decode(s) {
  return s.replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
}
function attr(html, name) {
  var m = html.match(new RegExp(name + "=(?:\"([^\"]*)\"|'([^']*)')"));
  return m ? decode(m[1] !== undefined ? m[1] : m[2]) : undefined;
}
function fire(type, entry, action, checked) {
  var html = fb.rowHtml(entry);
  var row = {dataset: {entry: attr(html, 'data-entry'), entryId: attr(html, 'data-entry-id')}};
  var el = action ? {dataset: {action: action}, checked: checked} : null;
  var target = {closest: function (sel) { return sel === '[data-action]' ? el : row; }};
  listeners[type].forEach(function (fn) { fn({target: target, preventDefault: function () {}, stopPropagation: function () {}}); });
}
var entries = `+string(entriesJSON)+`;
entries.forEach(function (e) {
  if (e.kind === 'file') {
    fire('change', e, 'select', true);
    fire('click', e, 'open');
    fire('click', e, 'download');
  } else {
    fire('click', e, 'open');
    fire('click', e, 'recheck');
    fire('click', e, 'delete');
  }
  fire('contextmenu', e, null);
});
JSON.stringify(calls)`)

	var calls [][]any
	if err := json.Unmarshal([]byte(out), &calls); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	var want [][]any
	for _, e := range entries {
		if e.Kind == "file" {
			want = append(want,
				[]any{"select", e.Path, true, e.Name},
				[]any{"download", e.Path, e.Name},
				[]any{"download", e.Path, e.Name},
			)
		} else {
			want = append(want,
				[]any{"navigate", e.Path},
				[]any{"recheck", e.Name},
				[]any{"delete", e.InfoHash, e.Name},
			)
		}
		want = append(want, []any{"context", e.Name})
	}
	gotJSON, _ := json.Marshal(calls)
	wantJSON, _ := json.Marshal(want)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("row actions:\n got  %s\n want %s", gotJSON, wantJSON)
	}
}
