package server

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gorilla/sessions"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"golang.org/x/crypto/bcrypt"
)

func postRegister(s *Server, username, password string) *httptest.ResponseRecorder {
	form := url.Values{"username": {username}, "password": {password}, "confirmPassword": {password}}
	req := httptest.NewRequest(http.MethodPost, "/register", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	s.RegisterHandler(rec, req)
	return rec
}

// Before the guard, a POST to /register overwrote the stored username and
// password with no credential, so anyone who could reach the port could take
// over the web login.
func TestRegisterClosesOnceACredentialExists(t *testing.T) {
	cfg := config.Get()
	prevUseAuth, prevAuth := cfg.UseAuth, cfg.Auth
	t.Cleanup(func() { cfg.UseAuth, cfg.Auth = prevUseAuth, prevAuth })

	s := &Server{logger: zerolog.Nop(), cookie: sessions.NewCookieStore([]byte("test-secret"))}

	// Auth on, nothing stored yet: registration is open, but not with an
	// empty username or password.
	cfg.UseAuth = true
	cfg.Auth = &config.Auth{APIToken: "tok"}
	if rec := postRegister(s, "", "secret"); rec.Code != http.StatusBadRequest {
		t.Fatalf("empty username: status = %d, want 400", rec.Code)
	}
	if rec := postRegister(s, "admin", "secret"); rec.Code != http.StatusSeeOther {
		t.Fatalf("first registration: status = %d, want 303", rec.Code)
	}
	if cfg.Auth.Username != "admin" || bcrypt.CompareHashAndPassword([]byte(cfg.Auth.Password), []byte("secret")) != nil {
		t.Fatalf("first registration did not store the credential")
	}

	// A credential exists: a second POST is refused and changes nothing.
	if rec := postRegister(s, "attacker", "owned"); rec.Code != http.StatusForbidden {
		t.Fatalf("second registration: status = %d, want 403", rec.Code)
	}
	if cfg.Auth.Username != "admin" {
		t.Fatalf("stored username = %q, want admin", cfg.Auth.Username)
	}
	rec := httptest.NewRecorder()
	s.RegisterHandler(rec, httptest.NewRequest(http.MethodGet, "/register", nil))
	if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/" {
		t.Fatalf("GET /register = %d %q, want 303 /", rec.Code, rec.Header().Get("Location"))
	}

	// Auth off: registration is closed too (it used to dereference a nil auth).
	cfg.UseAuth = false
	if rec := postRegister(s, "attacker", "owned"); rec.Code != http.StatusForbidden {
		t.Fatalf("auth off: status = %d, want 403", rec.Code)
	}
}

func TestVerifyToken(t *testing.T) {
	cfg := config.Get()
	prevUseAuth, prevAuth := cfg.UseAuth, cfg.Auth
	t.Cleanup(func() { cfg.UseAuth, cfg.Auth = prevUseAuth, prevAuth })

	cfg.UseAuth = true
	cfg.Auth = &config.Auth{Username: "admin", Password: "hash", APIToken: "api-token"}
	for token, want := range map[string]bool{"api-token": true, "api-toke": false, "": false, "api-token ": false} {
		if got := config.VerifyToken(token); got != want {
			t.Errorf("VerifyToken(%q) = %t, want %t", token, got, want)
		}
	}
	cfg.Auth = &config.Auth{Username: "admin", Password: "hash"}
	if config.VerifyToken("") {
		t.Error("VerifyToken accepted an empty token with no token configured")
	}
}
