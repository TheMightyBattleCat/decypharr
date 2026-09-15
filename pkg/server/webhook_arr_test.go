package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
)

// TestMain gives internal/config a throwaway config.json location before any
// test in this package calls config.Get() (handleArrWebhook's token check
// does, via config.Get().WebhookToken) - without this, the first call would
// create config.json in the process's working directory instead of a
// throwaway one.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "decypharr-server-test-*")
	if err == nil {
		config.SetConfigPath(dir)
		defer os.RemoveAll(dir)
	}
	os.Exit(m.Run())
}

// These cover only the request-handling branches of handleArrWebhook that
// return before ever touching s.manager (the token check, the "Test" event,
// and the unhandled-eventType default) - so a Server with a nil manager is
// safe to use here. The branches that resolve and tear down an entry
// (MovieFileDelete/EpisodeFileDelete) are covered directly against
// Manager.HandleArrWebhookCleanup in pkg/manager/webhook_cleanup_test.go,
// which needs a real, storage-backed Manager to mean anything.
func newTestServerForWebhook() *Server {
	return &Server{logger: zerolog.Nop()}
}

func TestHandleArrWebhook_TestEvent_ReturnsOK(t *testing.T) {
	s := newTestServerForWebhook()

	req := httptest.NewRequest(http.MethodPost, "/webhooks/arr", strings.NewReader(`{"eventType":"Test"}`))
	rec := httptest.NewRecorder()

	s.handleArrWebhook(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestHandleArrWebhook_UnknownEventType_ReturnsOK(t *testing.T) {
	s := newTestServerForWebhook()

	req := httptest.NewRequest(http.MethodPost, "/webhooks/arr", strings.NewReader(`{"eventType":"SeriesDelete"}`))
	rec := httptest.NewRecorder()

	s.handleArrWebhook(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestHandleArrWebhook_Token(t *testing.T) {
	config.Get().WebhookToken = "s3cr3t"
	t.Cleanup(func() { config.Get().WebhookToken = "" })

	s := newTestServerForWebhook()

	t.Run("mismatched token is rejected", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/webhooks/arr?token=wrong", strings.NewReader(`{"eventType":"Test"}`))
		rec := httptest.NewRecorder()

		s.handleArrWebhook(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}
	})

	t.Run("matching token is accepted", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/webhooks/arr?token=s3cr3t", strings.NewReader(`{"eventType":"Test"}`))
		rec := httptest.NewRecorder()

		s.handleArrWebhook(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
		}
	})

	t.Run("missing token is rejected", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/webhooks/arr", strings.NewReader(`{"eventType":"Test"}`))
		rec := httptest.NewRecorder()

		s.handleArrWebhook(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}
	})
}

func TestHandleArrWebhook_WrongMethod(t *testing.T) {
	s := newTestServerForWebhook()

	req := httptest.NewRequest(http.MethodGet, "/webhooks/arr", nil)
	rec := httptest.NewRecorder()

	s.handleArrWebhook(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}
