package webdav

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"golang.org/x/crypto/bcrypt"
)

// newWriteAuthServer serves the production middleware chain of Routes()
// minus the readiness gate and gzip: commonMiddleware, then authMiddleware.
func newWriteAuthServer(t *testing.T) (*httptest.Server, *manager.Manager) {
	t.Helper()
	base, m := newInternalServer(t)
	base.Close()
	h := NewHandler(m)
	r := chi.NewRouter()
	r.Use(h.commonMiddleware)
	r.Use(h.authMiddleware)
	h.mountRoutes(r)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv, m
}

func setLogin(t *testing.T) {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte("pw"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Get()
	cfg.UseAuth = true
	cfg.Auth = &config.Auth{Username: "user", Password: string(hash)}
}

// With the UI login on and enable_webdav_auth at its default (off), a WebDAV
// request that changes anything needs the UI credentials; reads stay open,
// and no other origin is let in through CORS.
func TestWebDAVWritesNeedCredentialsWhileLoginOn(t *testing.T) {
	srv, m := newWriteAuthServer(t)
	setLogin(t)
	if config.Get().EnableWebdavAuth {
		t.Fatal("test setup: enable_webdav_auth should be at its default (false)")
	}
	first := srv.URL + "/__all__/" + splitParent + "/" + splitFile
	second := srv.URL + "/__all__/" + splitParent + "/Show.S02E01.mkv"

	pre, _ := doReq(t, http.MethodOptions, first, func(r *http.Request) {
		r.Header.Set("Origin", "https://page.example")
		r.Header.Set("Access-Control-Request-Method", "DELETE")
	})
	if got := pre.Header.Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("preflight from another origin: Access-Control-Allow-Origin %q, want none", got)
	}
	if pre.StatusCode != http.StatusOK {
		t.Fatalf("OPTIONS with no credentials: status %d, want 200", pre.StatusCode)
	}

	for _, target := range []string{first, second} {
		resp, _ := doReq(t, http.MethodDelete, target, nil)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("DELETE %s with no credentials: status %d, want 401", target[len(srv.URL):], resp.StatusCode)
		}
		if resp.Header.Get("WWW-Authenticate") == "" {
			t.Fatalf("DELETE 401 carries no WWW-Authenticate challenge")
		}
	}
	for _, method := range []string{"MOVE", "COPY"} {
		resp, _ := doReq(t, method, first, func(r *http.Request) {
			r.Header.Set("Destination", srv.URL+"/__all__/elsewhere/"+splitFile)
		})
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s with no credentials: status %d, want 401", method, resp.StatusCode)
		}
	}
	if _, err := m.Storage().Get(splitNZBID); err != nil {
		t.Fatalf("entry gone after refused deletes: %v", err)
	}

	// Reads answer as before, with no credentials.
	if resp, _ := doReq(t, http.MethodHead, first, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD with no credentials: status %d, want 200", resp.StatusCode)
	}
	if resp, _ := doReq(t, http.MethodGet, first, func(r *http.Request) { r.Header.Set("Range", "bytes=0-0") }); resp.StatusCode == http.StatusUnauthorized {
		t.Fatalf("GET with no credentials: 401, want it served as before")
	}
	if resp, _ := doReq(t, PROPFIND, srv.URL+"/__all__", func(r *http.Request) { r.Header.Set("Depth", "1") }); resp.StatusCode != http.StatusMultiStatus {
		t.Fatalf("PROPFIND with no credentials: status %d, want 207", resp.StatusCode)
	}

	// Control: the UI credential deletes.
	resp, _ := doReq(t, http.MethodDelete, second, func(r *http.Request) { r.SetBasicAuth("user", "pw") })
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE with the UI credential: status %d, want 204", resp.StatusCode)
	}
	if resp, _ := doReq(t, http.MethodDelete, first, func(r *http.Request) { r.SetBasicAuth("user", "wrong") }); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("DELETE with a wrong password: status %d, want 401", resp.StatusCode)
	}
}

// The internal token reads (GET, HEAD) and nothing else, even with both
// toggles on.
func TestWebDAVInternalTokenCannotWrite(t *testing.T) {
	srv, m := newWriteAuthServer(t)
	setLogin(t)
	config.Get().EnableWebdavAuth = true
	first := srv.URL + "/__all__/" + splitParent + "/" + splitFile
	tok := bearer(m.InternalToken())

	if resp, _ := doReq(t, http.MethodHead, first, tok); resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD with the internal token: status %d, want 200", resp.StatusCode)
	}
	for _, method := range []string{http.MethodDelete, "MOVE", PROPFIND} {
		resp, _ := doReq(t, method, first, tok)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s with the internal token: status %d, want 401", method, resp.StatusCode)
		}
	}
	if _, err := m.Storage().Get(splitNZBID); err != nil {
		t.Fatalf("entry gone after a token DELETE: %v", err)
	}
	// Reads with both toggles on still need credentials.
	if resp, _ := doReq(t, http.MethodHead, first, nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("HEAD with no credentials, both toggles on: status %d, want 401", resp.StatusCode)
	}
}

// Control: with no login, a DELETE works as before.
func TestWebDAVDeleteWithoutLoginUnchanged(t *testing.T) {
	srv, _ := newWriteAuthServer(t)
	config.Get().UseAuth = false
	second := srv.URL + "/__all__/" + splitParent + "/Show.S02E01.mkv"
	if resp, _ := doReq(t, http.MethodDelete, second, nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE with no login configured: status %d, want 204", resp.StatusCode)
	}
	if resp, _ := doReq(t, http.MethodHead, second, nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("HEAD after DELETE: status %d, want 404", resp.StatusCode)
	}
}
