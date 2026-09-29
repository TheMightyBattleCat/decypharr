package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/server/qbit"
	"github.com/sirrobot01/decypharr/pkg/server/sabnzbd"
)

// With auth on, the qBittorrent and SABnzbd APIs accept the web login, the
// API token as the password, or the exact host and key of an Arr configured
// in Settings. They never probe the caller's host, and an auto-detected Arr's
// stored host and key are not a credential.
func TestCompatibilityAPIsAuthenticateBeforeProbing(t *testing.T) {
	useTestAuth(t, "ui-password", "server-token")
	mgr := newTestManager(t)

	var probes atomic.Int64
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes.Add(1)
		_, _ = w.Write([]byte(`{"appName":"Sonarr"}`))
	}))
	t.Cleanup(endpoint.Close)
	mgr.Arr().AddOrUpdate(arr.New("manual", endpoint.URL, "arr-token", false, nil, "", string(arr.SourceManual)))
	mgr.Arr().AddOrUpdate(arr.New("configured", endpoint.URL, "arr-token", false, nil, "", ""))
	mgr.Arr().AddOrUpdate(arr.New("discovered", endpoint.URL, "arr-token", false, nil, "", string(arr.SourceAuto)))

	for _, protocol := range []struct {
		name    string
		handler http.Handler
	}{
		{name: "qbit", handler: qbit.New(mgr).Routes()},
		{name: "sabnzbd", handler: sabnzbd.New(mgr).Routes()},
	} {
		t.Run(protocol.name, func(t *testing.T) {
			for _, tc := range []struct {
				name, category, username, password string
				wantStatus                         int
			}{
				{"untrusted endpoint", "unknown", endpoint.URL, "arr-token", http.StatusUnauthorized},
				{"wrong configured token", "manual", endpoint.URL, "wrong", http.StatusUnauthorized},
				{"wrong configured host", "manual", "http://untrusted.invalid", "arr-token", http.StatusUnauthorized},
				{"discovered credentials", "discovered", endpoint.URL, "arr-token", http.StatusUnauthorized},
				{"manual credentials", "manual", endpoint.URL, "arr-token", http.StatusOK},
				{"settings credentials", "configured", endpoint.URL, "arr-token", http.StatusOK},
				{"api token", "discovered", endpoint.URL, "server-token", http.StatusOK},
				{"web login", "discovered", "admin", "ui-password", http.StatusOK},
				{"wrong web password", "discovered", "admin", "nope", http.StatusUnauthorized},
			} {
				t.Run(tc.name, func(t *testing.T) {
					query := url.Values{"category": {tc.category}}
					path := "/torrents/categories"
					if protocol.name == "sabnzbd" {
						path = "/api/"
						query.Set("mode", "version")
						query.Set("ma_username", tc.username)
						query.Set("ma_password", tc.password)
					}
					req := httptest.NewRequest(http.MethodGet, path+"?"+query.Encode(), nil)
					req.SetBasicAuth(tc.username, tc.password)
					response := httptest.NewRecorder()
					protocol.handler.ServeHTTP(response, req)
					if response.Code != tc.wantStatus {
						t.Fatalf("status = %d, want %d: %s", response.Code, tc.wantStatus, response.Body.String())
					}
					if got := probes.Load(); got != 0 {
						t.Fatalf("authentication sent %d probe requests", got)
					}
				})
			}
		})
	}

	// A login with the API token must leave the stored Arr's own host and key
	// alone: the wanted search and the import checks call the Arr with them.
	stored := mgr.Arr().Get("discovered")
	if stored.Host != endpoint.URL || stored.Token != "arr-token" {
		t.Fatalf("stored Arr = host %q token %q, want it unchanged", stored.Host, stored.Token)
	}
}

func TestConfigAPIKeepsSessionSecretPrivate(t *testing.T) {
	cfg := useTestAuth(t, "ui-password", "server-token")
	secret := cfg.SessionSecret
	if secret == "" {
		t.Fatal("no session secret generated")
	}
	s := &Server{logger: zerolog.Nop(), manager: newTestManager(t)}

	response := httptest.NewRecorder()
	s.handleGetConfig(response, httptest.NewRequest(http.MethodGet, "/api/config", nil))
	var publicConfig map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &publicConfig); err != nil {
		t.Fatal(err)
	}
	if _, exposed := publicConfig["session_secret"]; exposed {
		t.Fatal("the config API exposed the session signing key")
	}

	// A settings save, even one that sends a session_secret, keeps the
	// stored one.
	body := `{"bind_address":"0.0.0.0","port":"8282","download_folder":"/tmp/downloads","session_secret":"attacker"}`
	rec := httptest.NewRecorder()
	s.handleUpdateConfig(rec, httptest.NewRequest(http.MethodPost, "/api/config", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("save status = %d: %s", rec.Code, rec.Body.String())
	}
	raw, err := os.ReadFile(cfg.JsonFile())
	if err != nil {
		t.Fatal(err)
	}
	var saved struct {
		SessionSecret string `json:"session_secret"`
	}
	if err := json.Unmarshal(raw, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.SessionSecret != secret {
		t.Fatalf("saved session_secret = %q, want the original", saved.SessionSecret)
	}
}
