package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

// TestHandleUpdateConfig_PreservesWebhookToken guards against the config
// save wiping webhook_token: the settings form has no field for it, so a
// PUT /api/config that omits it decodes to an empty string unless
// handleUpdateConfig explicitly preserves the live value the same way it
// already does for Auth.
//
// The assertion reads back the persisted config.json rather than the live
// in-memory singleton: Save() writes newConfig's fields to disk
// unconditionally, while the singleton is only mutated via ApplyRuntime,
// which is skipped whenever RequiresRestart is true - as it would be here,
// since a wiped WebhookToken alone differs enough from the live config to
// trigger it. Checking the singleton would pass even with the bug present.
func TestHandleUpdateConfig_PreservesWebhookToken(t *testing.T) {
	config.Get().WebhookToken = "existing-secret-token"
	t.Cleanup(func() { config.Get().WebhookToken = "" })

	s := &Server{
		logger:  zerolog.Nop(),
		manager: manager.New(),
	}

	// A payload shaped like what the current settings form actually submits:
	// no webhook_token field at all, same as every other field the form
	// doesn't render.
	body := `{"bind_address":"0.0.0.0","port":"8282","download_folder":"/tmp/downloads"}`
	req := httptest.NewRequest(http.MethodPost, "/api/config", strings.NewReader(body))
	rec := httptest.NewRecorder()

	s.handleUpdateConfig(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	raw, err := os.ReadFile(config.Get().JsonFile())
	if err != nil {
		t.Fatalf("reading persisted config.json: %v", err)
	}
	var persisted struct {
		WebhookToken string `json:"webhook_token"`
	}
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatalf("unmarshaling persisted config.json: %v", err)
	}

	if persisted.WebhookToken != "existing-secret-token" {
		t.Errorf("persisted webhook_token = %q, want %q (preserved)", persisted.WebhookToken, "existing-secret-token")
	}
}
