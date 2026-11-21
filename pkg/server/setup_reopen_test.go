package server

import (
	"html/template"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/sessions"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"golang.org/x/crypto/bcrypt"
)

// installWithLogin sets up an installed instance: one Usenet provider, a
// download folder and a stored login with API token "tok". It returns the
// config directory and the download folder.
func installWithLogin(t *testing.T) (string, string) {
	t.Helper()
	config.Reset()
	dir := t.TempDir()
	config.SetConfigPath(dir)
	t.Cleanup(config.Reset)

	hash, err := bcrypt.GenerateFromPassword([]byte("pw"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	downloads := t.TempDir()
	if _, err := config.Update(func(c *config.Config) error {
		c.DownloadFolder = downloads
		c.Usenet.Providers = []config.UsenetProvider{{Host: "news.example", Port: 563, Username: "u", Password: "p", SSL: true, MaxConnections: 10}}
		c.UseAuth = true
		return c.SaveAuth(&config.Auth{Username: "admin", Password: string(hash), APIToken: "tok"})
	}); err != nil {
		t.Fatal(err)
	}
	if err := config.Get().SetupComplete(); err != nil {
		t.Fatalf("test setup: install not complete: %v", err)
	}
	return dir, downloads
}

// A settings save that would leave no provider is refused with the reason,
// and the install stays complete.
func TestSettingsSaveThatLeavesNoProviderIsRefused(t *testing.T) {
	installWithLogin(t)
	s := &Server{logger: zerolog.Nop(), manager: newTestManager(t), cookie: sessions.NewCookieStore([]byte("k"))}

	// What the settings page sends once the last provider card is removed.
	rec := httptest.NewRecorder()
	s.handleUpdateConfig(rec, httptest.NewRequest(http.MethodPost, "/api/config",
		strings.NewReader(`{"usenet":{"providers":[]},"debrids":[]}`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("save with no provider: status %d %q, want 400", rec.Code, strings.TrimSpace(rec.Body.String()))
	}
	if !strings.Contains(rec.Body.String(), "provider must be configured") {
		t.Errorf("400 body %q does not give the reason", strings.TrimSpace(rec.Body.String()))
	}
	if err := config.Get().SetupComplete(); err != nil {
		t.Fatalf("the refused save still changed the config: SetupComplete = %v", err)
	}
	if got := config.Get().Usenet.Providers; len(got) != 1 || got[0].Host != "news.example" {
		t.Fatalf("providers after a refused save = %+v, want news.example kept", got)
	}
}

// Once a login is stored, the setup wizard's write routes need it even when
// the config stops validating (a config-history revert, an older build's
// save): no anonymous caller turns the login off or installs its own
// provider. The owner can still reach /login and /settings to undo it.
func TestSetupRoutesNeedTheStoredLoginWhenTheConfigStopsValidating(t *testing.T) {
	for _, reload := range []bool{false, true} {
		name := "login in memory"
		if reload {
			name = "login loaded from auth.json after a restart"
		}
		t.Run(name, func(t *testing.T) {
			dir, downloads := installWithLogin(t)
			if reload {
				config.Reset()
				config.SetConfigPath(dir)
			}
			// An invalid config forced in past the settings save.
			if _, err := config.Update(func(c *config.Config) error {
				c.Usenet.Providers = nil
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if config.Get().SetupComplete() == nil {
				t.Fatal("test setup: config still validates")
			}

			s := &Server{logger: zerolog.Nop(), manager: newTestManager(t), cookie: sessions.NewCookieStore([]byte("k")),
				templates: template.Must(template.ParseFS(content, "templates/*.html"))}
			router := s.WebRoutes()

			attack := `{"auth":{"skip_auth":true},` +
				`"usenet":{"host":"nntp.attacker.example","port":563,"username":"x","password":"y","ssl":true},` +
				`"download":{"download_folder":"` + downloads + `"},"mount":{"mount_type":"none"}}`
			for _, path := range []string{"/api/setup/complete", "/skip-auth", "/api/setup/restore/snapshots", "/api/setup/restore", "/api/setup/restore/upload"} {
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(attack)))
				// /skip-auth is also sent to the wizard page by the setup
				// redirect, before any guard; either way it must not run.
				if rec.Code != http.StatusUnauthorized && rec.Header().Get("Location") != "/setup" {
					t.Errorf("anonymous POST %s: status %d Location %q, want 401", path, rec.Code, rec.Header().Get("Location"))
				}
			}
			after := config.Get()
			if !after.UseAuth || len(after.Usenet.Providers) != 0 {
				t.Fatalf("anonymous setup calls changed the config: use_auth=%v providers=%+v", after.UseAuth, after.Usenet.Providers)
			}

			// The stored API token passes the guard (the handler then refuses
			// the malformed body itself).
			req := httptest.NewRequest(http.MethodPost, "/api/setup/complete", strings.NewReader("{"))
			req.Header.Set("Authorization", "Bearer tok")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code == http.StatusUnauthorized {
				t.Errorf("POST /api/setup/complete with the API token: 401, want the guard passed")
			}

			login := httptest.NewRecorder()
			router.ServeHTTP(login, httptest.NewRequest(http.MethodGet, "/login", nil))
			if login.Code != http.StatusOK {
				t.Errorf("GET /login: %d Location %q, want 200", login.Code, login.Header().Get("Location"))
			}
			settings := httptest.NewRecorder()
			router.ServeHTTP(settings, httptest.NewRequest(http.MethodGet, "/settings", nil))
			if loc := settings.Header().Get("Location"); loc != "/login" {
				t.Errorf("anonymous GET /settings: %d Location %q, want the login page, not the wizard", settings.Code, loc)
			}
		})
	}
}

// Control: on a fresh install (no login stored yet) the wizard still
// completes with no credentials.
func TestFreshInstallCompletesTheWizardAnonymously(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	if !config.Get().NeedsAuth() {
		t.Fatal("test setup: a fresh install already has a login")
	}
	downloads := t.TempDir()

	s := &Server{logger: zerolog.Nop(), manager: newTestManager(t), cookie: sessions.NewCookieStore([]byte("k"))}
	body := `{"auth":{"username":"owner","password":"secret"},` +
		`"usenet":{"host":"news.example","port":563,"username":"u","password":"p","ssl":true},` +
		`"download":{"download_folder":"` + downloads + `"},"mount":{"mount_type":"none"}}`
	rec := httptest.NewRecorder()
	s.WebRoutes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/setup/complete", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("fresh-install POST /api/setup/complete: %d %q, want 200", rec.Code, strings.TrimSpace(rec.Body.String()))
	}
	if err := config.Get().SetupComplete(); err != nil {
		t.Fatalf("wizard finished but setup incomplete: %v", err)
	}
	if auth := config.Get().GetAuth(); auth == nil || auth.Username != "owner" {
		t.Fatalf("wizard's login not stored: %+v", auth)
	}
}
