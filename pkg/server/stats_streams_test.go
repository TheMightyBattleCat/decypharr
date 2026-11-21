package server

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"

	"golang.org/x/net/html"
)

// The Stats page and the Repair runs table write server-supplied strings into
// innerHTML. A stream's client is the HTTP User-Agent of whoever reads a file
// over WebDAV, and entry and file names come from torrents, so none of them
// may become markup. Each test runs the page's own function in JavaScriptCore
// and parses what it produced.

func jsRender(t *testing.T, script string) string {
	t.Helper()
	if _, err := exec.LookPath("osascript"); err != nil {
		t.Skip("needs osascript (JavaScriptCore)")
	}
	out, err := exec.Command("osascript", "-l", "JavaScript", "-e", script).CombinedOutput()
	if err != nil {
		t.Fatalf("osascript: %v: %s", err, out)
	}
	return strings.TrimSpace(string(out))
}

// liveMarkup lists elements carrying an event-handler attribute and any img
// element in rendered, so injected markup shows up as a failure.
func liveMarkup(t *testing.T, rendered string) []string {
	t.Helper()
	doc, err := html.Parse(strings.NewReader(rendered))
	if err != nil {
		t.Fatal(err)
	}
	var live []string
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode {
			if n.Data == "img" || n.Data == "script" {
				live = append(live, "<"+n.Data+">")
			}
			for _, a := range n.Attr {
				if strings.HasPrefix(a.Key, "on") {
					live = append(live, "<"+n.Data+" "+a.Key+"="+a.Val+">")
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)
	return live
}

func visibleText(rendered string) string {
	doc, _ := html.Parse(strings.NewReader(rendered))
	var b strings.Builder
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.TextNode {
			b.WriteString(n.Data)
			b.WriteString("\n")
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)
	return b.String()
}

const jsEscapeHtml = `function(text){ var map = {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}; return text ? text.replace(/[&<>"']/g, function(m){ return map[m]; }) : ''; }`

func TestStatsPageEscapesStreamStrings(t *testing.T) {
	const ua = `"><img src=x onerror=alert(document.domain)>`
	const entryName = `<b>Show</b><img src=x onerror=pwned=1>`
	const fileName = `"><script>pwned=2</script>.mkv`
	streams := []map[string]any{{
		"source": "torrent", "debrid": `<img src=x onerror=pwned=3>`,
		"client": ua, "entry_name": entryName, "file_name": fileName,
		"file_size": 1 << 20, "started_at": 1700000000,
	}}
	payload, _ := json.Marshal(map[string]any{"count": 1, "streams": streams})

	src, err := os.ReadFile("templates/stats.html")
	if err != nil {
		t.Fatal(err)
	}
	fn := extractFunction(t, string(src), "function updateActiveStreams(activeStreamsData) {")
	esc := extractFunction(t, string(src), "function escapeText(value) {")
	script := "var els = {}; var document = {getElementById: function(id){ if (!els[id]) els[id] = {textContent:'', className:'', innerHTML:''}; return els[id]; }};\n" +
		"var window = {decypharrUtils: {formatBytes: function(n){ return String(n); }, escapeHtml: " + jsEscapeHtml + "}};\n" +
		"function formatUptime(s){ return s + 's'; }\n" +
		esc + "\n" + fn + "\n" +
		"updateActiveStreams(" + string(payload) + ");\n" +
		"els['streams-content'].innerHTML"
	rendered := jsRender(t, script)

	if live := liveMarkup(t, rendered); len(live) > 0 {
		t.Fatalf("stream strings became live markup: %v\n%s", live, rendered)
	}
	text := visibleText(rendered)
	for _, want := range []string{entryName, fileName} {
		if !strings.Contains(text, want) {
			t.Errorf("visible text lacks %q:\n%s", want, text)
		}
	}
	// The client is longer than 30 chars, so the badge shows the leading
	// token; the full User-Agent must be intact in the title attribute.
	doc, _ := html.Parse(strings.NewReader(rendered))
	var title string
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "span" {
			for _, a := range n.Attr {
				if a.Key == "title" && strings.Contains(a.Val, "alert") {
					title = a.Val
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)
	if title != ua {
		t.Errorf("client title = %q, want the exact User-Agent %q", title, ua)
	}
}

func TestRepairRunsTableEscapesRunStrings(t *testing.T) {
	src, err := os.ReadFile("assets/js/repair.js")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	history := extractFunction(t, s, "renderHistory(runs) {")
	badge := extractFunction(t, s, "statusBadge(status) {")
	esc := extractFunction(t, s, "escape(s) {")
	const bad = `<img src=x onerror=pwned=1>`
	runs, _ := json.Marshal([]map[string]any{{
		"id": "r1", "trigger": bad + "-trigger", "status": bad + "-status",
		"error": bad + ": no Arr owns its broken files, nothing to re-search",
		"stats": map[string]any{"probed": 1},
	}})
	// A minimal stub DOM: createElement('div') escapes textContent into
	// innerHTML, 'tr' records innerHTML; the table body collects the rows.
	script := `
var rows = [];
function mkEl(tag) {
  var el = {tag: tag, _text: '', innerHTML: '', classList: {add: function(){}, remove: function(){}}};
  Object.defineProperty(el, 'textContent', {
    set: function(v) { this._text = v; this.innerHTML = String(v).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); },
    get: function() { return this._text; }});
  el.querySelector = function() { return null; };
  el.appendChild = function(c) { rows.push(c); };
  return el;
}
var document = {createElement: mkEl, getElementById: function(){ return mkEl('x'); }};
var ctl = {
  ` + history + `,
  ` + badge + `,
  ` + esc + `,
  formatDuration: function(){ return '-'; }
};
ctl.renderHistory(` + string(runs) + `);
rows.map(function(r){ return '<table><tbody>' + r.innerHTML + '</tbody></table>'; }).join('\n')`
	rendered := jsRender(t, script)
	if rendered == "" {
		t.Fatal("no row rendered")
	}
	if live := liveMarkup(t, rendered); len(live) > 0 {
		t.Fatalf("a run's strings became live markup: %v\n%s", live, rendered)
	}
	if !strings.Contains(visibleText(rendered), bad+": no Arr owns its broken files") {
		t.Errorf("error text not shown verbatim:\n%s", rendered)
	}
}

// extractFunction returns src from head to the brace that closes it.
func extractFunction(t *testing.T, src, head string) string {
	t.Helper()
	i := strings.Index(src, head)
	if i < 0 {
		t.Fatalf("no %q", head)
	}
	depth := 0
	for j := i; j < len(src); j++ {
		switch src[j] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return src[i : j+1]
			}
		}
	}
	t.Fatalf("unbalanced braces after %q", head)
	return ""
}
