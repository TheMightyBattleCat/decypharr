package server

import (
	"encoding/json"
	"html/template"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/sessions"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
)

// tokenScopeServer is an installed instance (login admin/pw, API token
// "tok") behind the real router, plus a signed-in session cookie.
func tokenScopeServer(t *testing.T) (http.Handler, *http.Cookie, *Server) {
	t.Helper()
	installWithLogin(t)
	s := &Server{logger: zerolog.Nop(), manager: newTestManager(t), cookie: sessions.NewCookieStore([]byte("k")),
		templates: template.Must(template.ParseFS(content, "templates/*.html"))}
	router := s.WebRoutes()
	return router, signIn(t, router), s
}

func signIn(t *testing.T, router http.Handler) *http.Cookie {
	t.Helper()
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"username":"admin","password":"pw"}`)))
	cookies := rec.Result().Cookies()
	if rec.Code != http.StatusSeeOther || len(cookies) != 1 {
		t.Fatalf("login: status %d, %d cookies", rec.Code, len(cookies))
	}
	return cookies[0]
}

func callWithToken(router http.Handler, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func callWithSession(router http.Handler, cookie *http.Cookie, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// The API token is also the password the Arrs store for the qBittorrent and
// SABnzbd APIs. A caller holding only it reads the config with every stored
// secret blank; the owner's session still reads them all. The live config
// keeps them either way.
func TestTokenOnlyConfigReadHasNoSecrets(t *testing.T) {
	router, cookie, s := tokenScopeServer(t)
	if _, err := config.Update(func(c *config.Config) error {
		c.Debrids = []config.Debrid{{Provider: "realdebrid", Name: "rd", APIKey: "debrid-key", DownloadAPIKeys: []string{"download-key"}, RcPass: "debrid-rc-pass"}}
		c.Usenet.Providers[0].Password = "news-pass"
		c.Usenet.ArchivePasswords = []string{"archive-pass"}
		c.SMB.Password = "smb-pass"
		c.Mount.ExternalRclone.RCPassword = "rclone-rc-pass"
		c.Plex.Token = "plex-token"
		c.Strm.Secret = "strm-secret"
		c.WebhookToken = "webhook-token"
		c.Notifications.WebhookURL = "https://discord.invalid/api/webhooks/1/discord-key"
		c.Notifications.CallbackURL = "https://callback.invalid/?key=callback-key"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	s.manager.Arr().AddOrUpdate(arr.New("sonarr", "http://sonarr.invalid", "arr-key", false, nil, "", string(arr.SourceManual)))
	secrets := []string{"debrid-key", "download-key", "debrid-rc-pass", "news-pass", "archive-pass", "smb-pass",
		"rclone-rc-pass", "plex-token", "strm-secret", "webhook-token", "discord-key", "callback-key", "arr-key", `"tok"`, config.Get().SessionSecret}

	token := callWithToken(router, http.MethodGet, "/api/config", "")
	if token.Code != http.StatusOK {
		t.Fatalf("token GET /api/config: %d %s", token.Code, token.Body.String())
	}
	for _, secret := range secrets {
		if strings.Contains(token.Body.String(), secret) {
			t.Errorf("token GET /api/config contains %s", secret)
		}
	}
	// The answer is still the config, not an empty object.
	var shown struct {
		Debrids []struct {
			Name string `json:"name"`
		} `json:"debrids"`
		Arrs []struct {
			Name string `json:"name"`
		} `json:"arrs"`
		Usenet struct {
			Providers []struct {
				Host string `json:"host"`
			} `json:"providers"`
		} `json:"usenet"`
	}
	if err := json.Unmarshal(token.Body.Bytes(), &shown); err != nil {
		t.Fatal(err)
	}
	if len(shown.Debrids) != 1 || shown.Debrids[0].Name != "rd" || len(shown.Arrs) != 1 || shown.Arrs[0].Name != "sonarr" ||
		len(shown.Usenet.Providers) != 1 || shown.Usenet.Providers[0].Host != "news.example" {
		t.Errorf("token GET /api/config lost the settings themselves: %+v", shown)
	}

	arrs := callWithToken(router, http.MethodGet, "/api/arrs", "")
	if arrs.Code != http.StatusOK || strings.Contains(arrs.Body.String(), "arr-key") || !strings.Contains(arrs.Body.String(), "sonarr") {
		t.Errorf("token GET /api/arrs: %d %s, want the Arr without its key", arrs.Code, arrs.Body.String())
	}

	session := callWithSession(router, cookie, http.MethodGet, "/api/config", "")
	if session.Code != http.StatusOK {
		t.Fatalf("session GET /api/config: %d", session.Code)
	}
	for _, secret := range secrets[:len(secrets)-1] { // the session secret is never sent
		if !strings.Contains(session.Body.String(), secret) {
			t.Errorf("session GET /api/config lacks %s", secret)
		}
	}
	if got := callWithSession(router, cookie, http.MethodGet, "/api/arrs", ""); !strings.Contains(got.Body.String(), "arr-key") {
		t.Errorf("session GET /api/arrs lacks the Arr key: %s", got.Body.String())
	}

	live := config.Get()
	if live.Debrids[0].APIKey != "debrid-key" || live.Debrids[0].DownloadAPIKeys[0] != "download-key" ||
		live.Usenet.Providers[0].Password != "news-pass" || live.Usenet.ArchivePasswords[0] != "archive-pass" ||
		live.Plex.Token != "plex-token" || live.Strm.Secret != "strm-secret" || live.WebhookToken != "webhook-token" ||
		s.manager.Arr().Get("sonarr").Token != "arr-key" {
		t.Fatalf("the token read blanked the live config: %+v", live)
	}
}

// A caller holding only the API token cannot turn the login off, rotate the
// token, replace the config, run the setup wizard or restore a backup over
// the config; the owner's session can.
func TestTokenOnlyCannotChangeLoginOrConfig(t *testing.T) {
	router, cookie, _ := tokenScopeServer(t)
	before := config.Get()
	webhookBefore := before.WebhookToken

	for _, c := range []struct{ method, path, body string }{
		{http.MethodPost, "/api/update-auth", `{"username":"","password":""}`},
		{http.MethodPost, "/api/refresh-token", ""},
		{http.MethodPost, "/api/webhook-token/refresh", ""},
		{http.MethodPost, "/api/config", `{"download_folder":"/tmp/attacker"}`},
		{http.MethodPost, "/api/setup/complete", `{"auth":{"skip_auth":true}}`},
		{http.MethodPost, "/api/backups/config-history/1/revert", ""},
		{http.MethodPost, "/api/backups/snapshots/1/restore/db", ""},
		{http.MethodPost, "/api/backups/snapshots/1/restore/nzb", `{"entry":"x"}`},
		{http.MethodPost, "/api/backups/snapshots/1/restore/links/apply", `{"plan_id":"p"}`},
		{http.MethodPost, "/api/backups/removed/k/restore", `{}`},
		{http.MethodGet, "/api/backups/snapshots/1/download?component=config", ""},
	} {
		rec := callWithToken(router, c.method, c.path, c.body)
		if rec.Code != http.StatusForbidden {
			t.Errorf("token %s %s: %d %s, want 403", c.method, c.path, rec.Code, strings.TrimSpace(rec.Body.String()))
		}
	}
	after := config.Get()
	var auth config.Auth
	if a := after.GetAuth(); a != nil {
		auth = *a
	}
	if !after.UseAuth || auth.Username != "admin" || auth.APIToken != "tok" ||
		after.DownloadFolder != before.DownloadFolder || after.WebhookToken != webhookBefore {
		t.Fatalf("token calls changed the config: use_auth=%v user=%q token rotated=%v folder=%q webhook token rotated=%v",
			after.UseAuth, auth.Username, auth.APIToken != "tok", after.DownloadFolder, after.WebhookToken != webhookBefore)
	}

	// Controls: the token still reaches the rest of /api.
	if rec := callWithToken(router, http.MethodGet, "/api/torrents", ""); rec.Code != http.StatusOK {
		t.Errorf("token GET /api/torrents: %d, want 200", rec.Code)
	}

	// The owner's session does each of them.
	save := callWithSession(router, cookie, http.MethodPost, "/api/config", `{"log_level":"debug"}`)
	if save.Code != http.StatusOK || config.Get().LogLevel != "debug" {
		t.Fatalf("session POST /api/config: %d %s (log_level %q)", save.Code, save.Body.String(), config.Get().LogLevel)
	}
	if rec := callWithSession(router, cookie, http.MethodPost, "/api/refresh-token", ""); rec.Code != http.StatusOK || config.Get().GetAuth().APIToken == "tok" {
		t.Fatalf("session POST /api/refresh-token: %d, token rotated %v", rec.Code, config.Get().GetAuth().APIToken != "tok")
	}
	cookie = signIn(t, router) // rotating the token ends every session
	if rec := callWithSession(router, cookie, http.MethodPost, "/api/update-auth", `{"username":"","password":""}`); rec.Code != http.StatusOK || config.Get().UseAuth {
		t.Fatalf("session POST /api/update-auth: %d, use_auth %v", rec.Code, config.Get().UseAuth)
	}
}
