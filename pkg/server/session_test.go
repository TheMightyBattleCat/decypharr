package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/sessions"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"golang.org/x/crypto/bcrypt"
)

// useTestAuth turns auth on with admin/password and restores the shared
// config afterwards.
func useTestAuth(t *testing.T, password, token string) *config.Config {
	t.Helper()
	cfg := config.Get()
	prevUseAuth, prevAuth := cfg.UseAuth, cfg.Auth
	t.Cleanup(func() { cfg.UseAuth, cfg.Auth = prevUseAuth, prevAuth })
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	cfg.UseAuth = true
	if err := cfg.SaveAuth(&config.Auth{Username: "admin", Password: string(hash), APIToken: token}); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestCredentialChangesInvalidateBrowserSessions(t *testing.T) {
	for _, change := range []string{"password", "token"} {
		t.Run(change, func(t *testing.T) {
			cfg := useTestAuth(t, "old-password", "old-token")
			s := &Server{logger: zerolog.Nop(), cookie: sessions.NewCookieStore([]byte(cfg.SecretKey()))}

			login := httptest.NewRecorder()
			s.LoginHandler(login, httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"username":"admin","password":"old-password"}`)))
			if login.Code != http.StatusSeeOther {
				t.Fatalf("login status = %d: %s", login.Code, login.Body.String())
			}
			cookies := login.Result().Cookies()
			if len(cookies) != 1 {
				t.Fatalf("login set %d cookies, want 1", len(cookies))
			}
			handler := s.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}))
			request := httptest.NewRequest(http.MethodGet, "/api/test", nil)
			request.AddCookie(cookies[0])
			before := httptest.NewRecorder()
			handler.ServeHTTP(before, request)
			if before.Code != http.StatusNoContent {
				t.Fatalf("fresh session status = %d", before.Code)
			}

			switch change {
			case "password":
				hash, _ := bcrypt.GenerateFromPassword([]byte("new-password"), bcrypt.MinCost)
				auth := *cfg.GetAuth()
				auth.Password = string(hash)
				if err := cfg.SaveAuth(&auth); err != nil {
					t.Fatal(err)
				}
			case "token":
				if _, err := s.refreshAPIToken(); err != nil {
					t.Fatal(err)
				}
			}
			after := httptest.NewRecorder()
			handler.ServeHTTP(after, request)
			if after.Code != http.StatusUnauthorized {
				t.Fatalf("old session status = %d, want 401", after.Code)
			}
		})
	}
}

// A session cookie from before this change has no auth_version; it must log
// in again.
func TestSessionWithoutVersionIsRefused(t *testing.T) {
	cfg := useTestAuth(t, "password", "token")
	store := sessions.NewCookieStore([]byte(cfg.SecretKey()))
	s := &Server{logger: zerolog.Nop(), cookie: store}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	session, _ := store.Get(req, "auth-session")
	session.Values["authenticated"] = true
	session.Values["username"] = "admin"
	if err := session.Save(req, rec); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/test", nil)
	request.AddCookie(rec.Result().Cookies()[0])
	response := httptest.NewRecorder()
	s.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", response.Code)
	}
}

// Only a credential change logs sessions out; an ordinary settings save must
// not, since it does not save auth.json.
func TestSettingsSaveKeepsBrowserSession(t *testing.T) {
	cfg := useTestAuth(t, "password", "token")
	s := &Server{logger: zerolog.Nop(), manager: newTestManager(t), cookie: sessions.NewCookieStore([]byte(cfg.SecretKey()))}

	login := httptest.NewRecorder()
	s.LoginHandler(login, httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"username":"admin","password":"password"}`)))
	if login.Code != http.StatusSeeOther {
		t.Fatalf("login status = %d", login.Code)
	}
	cookie := login.Result().Cookies()[0]

	save := httptest.NewRecorder()
	body := `{"bind_address":"0.0.0.0","port":"8282","download_folder":"/tmp/downloads"}`
	s.handleUpdateConfig(save, httptest.NewRequest(http.MethodPost, "/api/config", strings.NewReader(body)))
	if save.Code != http.StatusOK {
		t.Fatalf("save status = %d: %s", save.Code, save.Body.String())
	}

	request := httptest.NewRequest(http.MethodGet, "/api/test", nil)
	request.AddCookie(cookie)
	response := httptest.NewRecorder()
	s.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("session after a settings save: status = %d, want 204", response.Code)
	}
}
