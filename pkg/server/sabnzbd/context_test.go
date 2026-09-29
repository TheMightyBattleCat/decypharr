package sabnzbd

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

func newAuthenticationTestSABnzbd(t *testing.T) *SABnzbd {
	t.Helper()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	config.Get().UseAuth = false

	mgr := manager.New()
	t.Cleanup(func() {
		if err := mgr.Stop(); err != nil {
			t.Error(err)
		}
	})
	return &SABnzbd{manager: mgr}
}

func TestAuthenticateDoesNotOverwriteArrWithClientCredentials(t *testing.T) {
	s := newAuthenticationTestSABnzbd(t)
	existing := arr.New("sonarr", "http://sonarr:8989", "arr-api-key", false, nil, "", string(arr.SourceAuto))
	s.manager.Arr().AddOrUpdate(existing)

	got, err := s.authenticate("sonarr", "some-user", "some-password")
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if got.Host != existing.Host || got.Token != existing.Token {
		t.Fatalf("authenticated Arr = host %q token %q, want host %q token %q", got.Host, got.Token, existing.Host, existing.Token)
	}
	stored := s.manager.Arr().Get("sonarr")
	if stored.Host != "http://sonarr:8989" || stored.Token != "arr-api-key" {
		t.Fatalf("stored Arr = host %q token %q, want it unchanged", stored.Host, stored.Token)
	}
}

func TestAuthenticateDiscoversValidatedArrCredentials(t *testing.T) {
	s := newAuthenticationTestSABnzbd(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/health" || r.Header.Get("X-Api-Key") != "arr-api-key" {
			http.Error(w, "unexpected Arr validation request", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	got, err := s.authenticate("sonarr", server.URL, "arr-api-key")
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if got.Host != server.URL || got.Token != "arr-api-key" || got.Source != arr.SourceAuto {
		t.Fatalf("authenticated Arr = %#v", got)
	}
	stored := s.manager.Arr().Get("sonarr")
	if stored == nil || stored.Host != server.URL || stored.Token != "arr-api-key" {
		t.Fatalf("stored Arr = %#v", stored)
	}
}
