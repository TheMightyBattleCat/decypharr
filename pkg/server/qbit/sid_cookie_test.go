package qbit

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	"golang.org/x/crypto/bcrypt"
)

const (
	sidTestUser     = "admin"
	sidTestPassword = "ui-password-1"
	sidTestToken    = "api-token-1"
)

// newSIDTestQBit turns the login on with a real password and API token,
// saved the way the settings page saves them.
func newSIDTestQBit(t *testing.T) (*QBit, http.Handler) {
	t.Helper()
	q := newAuthenticationTestQBit(t)
	cfg := config.Get()
	cfg.UseAuth = true
	saveSIDTestAuth(t, sidTestPassword, sidTestToken)
	return q, q.Routes()
}

func saveSIDTestAuth(t *testing.T, password, token string) {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if err := config.Get().SaveAuth(&config.Auth{Username: sidTestUser, Password: string(hash), APIToken: token}); err != nil {
		t.Fatalf("save auth: %v", err)
	}
}

func sidLogin(t *testing.T, routes http.Handler, username, password string) *http.Cookie {
	t.Helper()
	form := url.Values{"username": {username}, "password": {password}}
	req := httptest.NewRequest(http.MethodPost, "/auth/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	routes.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "Ok." {
		t.Fatalf("login status = %d body %q, want 200 Ok.", rec.Code, rec.Body.String())
	}
	cookies := rec.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != "SID" || cookies[0].Value == "" {
		t.Fatalf("login cookies = %#v, want one SID cookie", cookies)
	}
	return cookies[0]
}

func sidGet(routes http.Handler, name, value string) int {
	req := httptest.NewRequest(http.MethodGet, "/app/preferences", nil)
	req.AddCookie(&http.Cookie{Name: name, Value: value})
	rec := httptest.NewRecorder()
	routes.ServeHTTP(rec, req)
	return rec.Code
}

// assertSIDHidesCredentials fails if the cookie value, read as it stands or
// decoded as base64 or hex, contains any of the secrets.
func assertSIDHidesCredentials(t *testing.T, value string, secrets ...string) {
	t.Helper()
	views := []string{value}
	for _, enc := range []*base64.Encoding{base64.URLEncoding, base64.StdEncoding, base64.RawURLEncoding, base64.RawStdEncoding} {
		if b, err := enc.DecodeString(value); err == nil {
			views = append(views, string(b))
		}
	}
	if b, err := hex.DecodeString(value); err == nil {
		views = append(views, string(b))
	}
	for _, view := range views {
		for _, secret := range secrets {
			if strings.Contains(view, secret) {
				t.Fatalf("SID %q carries the credential %q", value, secret)
			}
		}
	}
}

func TestQBitLoginSIDIsOpaqueAndHttpOnly(t *testing.T) {
	_, routes := newSIDTestQBit(t)
	for _, password := range []string{sidTestPassword, sidTestToken} {
		cookie := sidLogin(t, routes, sidTestUser, password)
		assertSIDHidesCredentials(t, cookie.Value, sidTestPassword, sidTestToken)
		if b, err := hex.DecodeString(cookie.Value); err != nil || len(b) != 32 {
			t.Fatalf("SID %q is not 32 random bytes in hex", cookie.Value)
		}
		if !cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode {
			t.Fatalf("SID flags: HttpOnly=%v SameSite=%v, want HttpOnly and Strict", cookie.HttpOnly, cookie.SameSite)
		}
		for _, name := range []string{"SID", "sid"} {
			if code := sidGet(routes, name, cookie.Value); code != http.StatusOK {
				t.Fatalf("request with %s cookie: status = %d, want 200", name, code)
			}
		}
	}
}

func TestQBitSIDEndsWhenTheLoginOrTokenChanges(t *testing.T) {
	_, routes := newSIDTestQBit(t)
	byPassword := sidLogin(t, routes, sidTestUser, sidTestPassword)
	byToken := sidLogin(t, routes, sidTestUser, sidTestToken)

	// Rotating only the token ends the sessions of both kinds: each save of
	// the credentials ends every session issued before it.
	saveSIDTestAuth(t, sidTestPassword, "api-token-2")
	for _, c := range []*http.Cookie{byPassword, byToken} {
		if code := sidGet(routes, "SID", c.Value); code != http.StatusForbidden {
			t.Fatalf("SID after a token change: status = %d, want 403 so the client logs in again", code)
		}
	}
	fresh := sidLogin(t, routes, sidTestUser, sidTestPassword)
	if code := sidGet(routes, "SID", fresh.Value); code != http.StatusOK {
		t.Fatalf("SID from a new login: status = %d, want 200", code)
	}

	saveSIDTestAuth(t, "ui-password-2", "api-token-2")
	if code := sidGet(routes, "SID", fresh.Value); code != http.StatusForbidden {
		t.Fatalf("SID after a password change: status = %d, want 403", code)
	}
}

func TestQBitForgedOrOldFormatSIDIsForbidden(t *testing.T) {
	_, routes := newSIDTestQBit(t)
	// The old self-contained format: base64(user|password|mac). Without the
	// secret key the mac cannot be right, so a forged one and a real one from
	// before the change are both just unknown session IDs now.
	oldFormat := base64.URLEncoding.EncodeToString(fmt.Appendf(nil, "%s|%s|%s", sidTestUser, sidTestPassword, "0123456789abcdef"))
	for _, value := range []string{oldFormat, strings.Repeat("ab", 32), "not-a-session"} {
		if code := sidGet(routes, "SID", value); code != http.StatusForbidden {
			t.Fatalf("SID %q: status = %d, want 403", value, code)
		}
	}
	// No credentials at all is still 401, as before.
	rec := httptest.NewRecorder()
	routes.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/app/preferences", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no credentials: status = %d, want 401", rec.Code)
	}
}

func TestQBitSIDFromBeforeTheLoginWasTurnedOnIsForbidden(t *testing.T) {
	_, routes := newSIDTestQBit(t)
	config.Get().UseAuth = false
	cookie := sidLogin(t, routes, "homarr-user", "homarr-password")
	if code := sidGet(routes, "SID", cookie.Value); code != http.StatusOK {
		t.Fatalf("login off: status = %d, want 200", code)
	}
	config.Get().UseAuth = true
	if code := sidGet(routes, "SID", cookie.Value); code != http.StatusForbidden {
		t.Fatalf("login turned on: status = %d, want 403", code)
	}
}
