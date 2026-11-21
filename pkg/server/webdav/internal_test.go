package webdav

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"golang.org/x/crypto/bcrypt"
)

// The paths below are spelled out, not built from manager.EntryInternalFolder,
// so they pin the route our own ffprobe checks open.
const (
	splitNZBID   = "parent-nzb-id"
	splitParent  = "Show.S01-S02.1080p"
	splitSeason  = "Show.S01.1080p"
	splitFile    = "Show.S01E01.mkv"
	splitFileLen = 4321
)

// newInternalServer serves the real WebDAV routes behind the auth middleware
// (not the readiness gate: the manager is never started). The store holds a
// multi-season NZB as it is after a season was split out and completed: the
// parent under its NZB ID, keeping the files, and the season entry under its
// own ID, whose files carry the parent's NZB ID.
func newInternalServer(t *testing.T) (*httptest.Server, *manager.Manager) {
	t.Helper()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)

	m := manager.New()
	t.Cleanup(func() { _ = m.Storage().Close() })

	s1 := func() *storage.File {
		return &storage.File{Name: splitFile, Size: splitFileLen, InfoHash: splitNZBID}
	}
	s2 := &storage.File{Name: "Show.S02E01.mkv", Size: 1 << 20, InfoHash: splitNZBID}
	parent := &storage.Entry{
		InfoHash: splitNZBID, Name: splitParent, Protocol: config.ProtocolNZB,
		Files: map[string]*storage.File{splitFile: s1(), s2.Name: s2},
	}
	season := &storage.Entry{
		InfoHash: "season-1-id", Name: splitSeason, Protocol: config.ProtocolNZB,
		Files: map[string]*storage.File{splitFile: s1()}, IsComplete: true,
	}
	for _, e := range []*storage.Entry{parent, season} {
		if err := m.Storage().AddOrUpdate(e); err != nil {
			t.Fatal(err)
		}
	}
	if m.InternalToken() == "" {
		t.Fatal("test setup: manager has no internal token")
	}

	h := NewHandler(m)
	r := chi.NewRouter()
	r.Use(h.authMiddleware)
	h.mountRoutes(r)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv, m
}

func doReq(t *testing.T, method, url string, set func(*http.Request)) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if set != nil {
		set(req)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp, string(body)
}

func bearer(token string) func(*http.Request) {
	return func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) }
}

// A season split's file resolves on the internal path by the NZB ID its
// files carry, with no folder name in the URL.
func TestInternalPathResolvesSeasonSplitByInfoHash(t *testing.T) {
	srv, m := newInternalServer(t)

	for _, hash := range []string{splitNZBID, "season-1-id"} {
		resp, _ := doReq(t, http.MethodHead, srv.URL+"/__internal__/"+hash+"/"+splitFile, bearer(m.InternalToken()))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("HEAD by %s: status %d, want 200", hash, resp.StatusCode)
		}
		if got := resp.Header.Get("Content-Length"); got != "4321" {
			t.Fatalf("HEAD by %s: Content-Length %q, want the file's size 4321", hash, got)
		}
	}
	// Folder names are not internal keys.
	for _, name := range []string{splitParent, splitSeason} {
		resp, _ := doReq(t, http.MethodHead, srv.URL+"/__internal__/"+name+"/"+splitFile, bearer(m.InternalToken()))
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("HEAD by folder name %s: status %d, want 404", name, resp.StatusCode)
		}
	}
	// A file another entry holds, an unknown hash, a deleted file: 404.
	for _, path := range []string{
		"/__internal__/" + splitNZBID + "/Other.mkv",
		"/__internal__/0000/" + splitFile,
	} {
		resp, _ := doReq(t, http.MethodHead, srv.URL+path, bearer(m.InternalToken()))
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("HEAD %s: status %d, want 404", path, resp.StatusCode)
		}
	}
	entry, err := m.Storage().Get(splitNZBID)
	if err != nil {
		t.Fatal(err)
	}
	entry.Files[splitFile].Deleted = true
	if err := m.Storage().AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	resp, _ := doReq(t, http.MethodHead, srv.URL+"/__internal__/"+splitNZBID+"/"+splitFile, bearer(m.InternalToken()))
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("HEAD of a deleted file: status %d, want 404", resp.StatusCode)
	}
}

// Only the internal token opens the path: not with auth off, not with a
// valid WebDAV password, not with a wrong token.
func TestInternalPathRefusedWithoutToken(t *testing.T) {
	srv, _ := newInternalServer(t)
	target := srv.URL + "/__internal__/" + splitNZBID + "/" + splitFile

	cfg := config.Get()
	cfg.UseAuth, cfg.EnableWebdavAuth = false, false
	for name, set := range map[string]func(*http.Request){
		"no credentials": nil,
		"wrong token":    bearer("not-the-token"),
	} {
		for _, method := range []string{http.MethodHead, http.MethodGet} {
			resp, _ := doReq(t, method, target, set)
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("auth off, %s, %s: status %d, want 401", name, method, resp.StatusCode)
			}
		}
	}

	hash, err := bcrypt.GenerateFromPassword([]byte("pw"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	cfg.UseAuth, cfg.EnableWebdavAuth = true, true
	cfg.Auth = &config.Auth{Username: "user", Password: string(hash)}
	basic := func(r *http.Request) { r.SetBasicAuth("user", "pw") }
	// Control: the password opens the ordinary path.
	if resp, _ := doReq(t, http.MethodHead, srv.URL+"/__all__/"+splitParent+"/"+splitFile, basic); resp.StatusCode != http.StatusOK {
		t.Fatalf("test setup: password refused on __all__: status %d", resp.StatusCode)
	}
	for name, set := range map[string]func(*http.Request){
		"no credentials": nil,
		"password":       basic,
	} {
		resp, _ := doReq(t, http.MethodHead, target, set)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("auth on, %s: status %d, want 401", name, resp.StatusCode)
		}
	}
}

// The internal path reads and nothing else; it lists nothing, and is not
// listed anywhere.
func TestInternalPathIsReadOnlyAndUnlisted(t *testing.T) {
	srv, m := newInternalServer(t)
	tok := bearer(m.InternalToken())
	// Login off, so the handler's own method check answers, not the login's
	// refusal of a write (TestWebDAVInternalTokenCannotWrite covers that).
	config.Get().UseAuth = false

	for _, method := range []string{http.MethodDelete, "PROPFIND", "MOVE", http.MethodPut} {
		resp, _ := doReq(t, method, srv.URL+"/__internal__/"+splitNZBID+"/"+splitFile, tok)
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("%s: status %d, want 405", method, resp.StatusCode)
		}
	}
	if _, err := m.Storage().Get(splitNZBID); err != nil {
		t.Fatalf("entry gone after refused writes: %v", err)
	}

	for _, path := range []string{"/__internal__", "/__internal__/" + splitNZBID} {
		resp, _ := doReq(t, "PROPFIND", srv.URL+path, tok)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("PROPFIND %s: status %d, want 404", path, resp.StatusCode)
		}
	}
	for _, path := range []string{"/", "/__all__"} {
		resp, body := doReq(t, "PROPFIND", srv.URL+path, tok)
		if resp.StatusCode != http.StatusMultiStatus {
			t.Fatalf("PROPFIND %s: status %d, want 207", path, resp.StatusCode)
		}
		if strings.Contains(body, "__internal__") {
			t.Fatalf("PROPFIND %s lists the internal path:\n%s", path, body)
		}
	}
}

// Every other path keeps working: the ordinary file path, and a group named
// "internal" (a user may name a virtual folder that).
func TestInternalPathLeavesOtherPathsAlone(t *testing.T) {
	srv, _ := newInternalServer(t)
	for _, path := range []string{
		"/__all__/" + splitParent + "/" + splitFile,
		"/__all__/" + splitSeason + "/" + splitFile,
		"/internal/" + splitParent + "/" + splitFile,
	} {
		resp, _ := doReq(t, http.MethodHead, srv.URL+path, nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("HEAD %s: status %d, want 200", path, resp.StatusCode)
		}
	}
}
