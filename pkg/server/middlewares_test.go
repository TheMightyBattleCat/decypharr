package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/sessions"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
)

func TestIsAPIRequest(t *testing.T) {
	tests := []struct {
		name    string
		urlBase string
		path    string
		want    bool
	}{
		{name: "API", urlBase: "/", path: "/api/repair/run", want: true},
		{name: "webhook", urlBase: "/", path: "/webhooks/tautulli", want: true},
		{name: "web page", urlBase: "/", path: "/login", want: false},
		{name: "API under URL base", urlBase: "/decypharr/", path: "/decypharr/api/repair/run", want: true},
		{name: "webhook under URL base", urlBase: "/decypharr/", path: "/decypharr/webhooks/tautulli", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{urlBase: tt.urlBase}
			r := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if got := s.isAPIRequest(r); got != tt.want {
				t.Fatalf("isAPIRequest() = %t, want %t", got, tt.want)
			}
		})
	}
}

// The Tautulli webhook can start a repair sweep, so with auth on it needs the
// API token, a web session, or the webhook token as ?token=.
func TestTautulliWebhookRequiresAuth(t *testing.T) {
	cfg := config.Get()
	prevUseAuth, prevAuth, prevWebhookToken := cfg.UseAuth, cfg.Auth, cfg.WebhookToken
	t.Cleanup(func() {
		cfg.UseAuth, cfg.Auth, cfg.WebhookToken = prevUseAuth, prevAuth, prevWebhookToken
	})
	cfg.UseAuth = true
	cfg.Auth = &config.Auth{Username: "user", Password: "hash", APIToken: "api-token"}
	cfg.WebhookToken = "webhook-token"

	s := &Server{logger: zerolog.Nop(), urlBase: "/", cookie: sessions.NewCookieStore([]byte("test-secret"))}
	reached := false
	handler := s.tautulliAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	tests := []struct {
		name       string
		target     string
		header     string
		wantStatus int
	}{
		{name: "no credential", target: "/webhooks/tautulli", wantStatus: http.StatusUnauthorized},
		{name: "wrong webhook token", target: "/webhooks/tautulli?token=nope", wantStatus: http.StatusUnauthorized},
		{name: "webhook token", target: "/webhooks/tautulli?token=webhook-token", wantStatus: http.StatusOK},
		{name: "API token as bearer", target: "/webhooks/tautulli", header: "Bearer api-token", wantStatus: http.StatusOK},
		{name: "webhook token as bearer", target: "/webhooks/tautulli", header: "Bearer webhook-token", wantStatus: http.StatusUnauthorized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reached = false
			req := httptest.NewRequest(http.MethodPost, tt.target, nil)
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if reached != (tt.wantStatus == http.StatusOK) {
				t.Fatalf("handler reached = %t", reached)
			}
		})
	}
}

func TestArrWebhookRejectsWrongToken(t *testing.T) {
	cfg := config.Get()
	prev := cfg.WebhookToken
	t.Cleanup(func() { cfg.WebhookToken = prev })
	cfg.WebhookToken = "webhook-token"

	s := newTestServerForWebhook()
	for target, want := range map[string]int{
		"/webhooks/arr":                     http.StatusUnauthorized,
		"/webhooks/arr?token=webhook-toke":  http.StatusUnauthorized,
		"/webhooks/arr?token=webhook-token": http.StatusOK,
	} {
		req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(`{"eventType":"Test"}`))
		rec := httptest.NewRecorder()
		s.handleArrWebhook(rec, req)
		if rec.Code != want {
			t.Fatalf("%s: status = %d, want %d", target, rec.Code, want)
		}
	}
}
