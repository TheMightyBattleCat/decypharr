package server

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/backup"
	"github.com/sirrobot01/decypharr/pkg/backup/backuptest"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

func newBackupTestServer(t *testing.T) (*Server, *backuptest.Service) {
	t.Helper()
	fake := backuptest.NewService()
	mgr := &manager.Manager{}
	mgr.SetBackup(fake)
	return &Server{
		logger:    zerolog.Nop(),
		manager:   mgr,
		templates: template.Must(template.ParseFS(content, "templates/*.html")),
	}, fake
}

func TestBackupAndLinksPagesRender(t *testing.T) {
	s, _ := newBackupTestServer(t)
	for path, h := range map[string]http.HandlerFunc{"/backups": s.BackupsHandler, "/links": s.LinksHandler} {
		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s = %d, want 200", path, rec.Code)
		}
		body := rec.Body.String()
		for _, want := range []string{"assets/js" + path + ".js\"", path[1:] + `" class=`} {
			if !strings.Contains(body, want) {
				t.Errorf("GET %s: page lacks %q", path, want)
			}
		}
	}
}

func TestBackupRoutesAreRegistered(t *testing.T) {
	s, _ := newBackupTestServer(t)
	have := map[string]bool{}
	err := chi.Walk(s.WebRoutes().(chi.Routes), func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		have[method+" "+route] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"GET /backups", "GET /links",
		"GET /api/backups/status", "GET /api/backups/snapshots", "GET /api/backups/snapshots/{id}",
		"POST /api/backups/jobs/{name}/run", "POST /api/backups/snapshots/{id}/verify",
		"GET /api/backups/snapshots/{id}/download", "POST /api/backups/snapshots/{id}/restore/db",
		"POST /api/backups/snapshots/{id}/restore/nzb", "POST /api/backups/snapshots/{id}/restore/links/plan",
		"POST /api/backups/snapshots/{id}/restore/links/apply", "DELETE /api/backups/snapshots/{id}",
		"GET /api/backups/config-history", "GET /api/backups/config-history/{id}/diff",
		"POST /api/backups/config-history/{id}/revert", "GET /api/backups/removed",
		"POST /api/backups/removed/{key}/restore", "POST /api/backups/retention/preview",
		"POST /api/backups/schedule/check",
		"GET /api/links/status", "POST /api/links/check", "GET /api/links", "GET /api/links/missing",
		"POST /api/links/cleanup/plan", "POST /api/links/cleanup/apply",
	} {
		if !have[want] {
			t.Errorf("route %q is not registered", want)
		}
	}
}

func TestBackupHandlersAnswer501UntilImplemented(t *testing.T) {
	s, _ := newBackupTestServer(t)
	for name, h := range map[string]http.HandlerFunc{
		"status": s.handleBackupStatus, "snapshots": s.handleBackupListSnapshots,
		"links status": s.handleLinksStatus, "links": s.handleLinksList, "history": s.handleBackupConfigHistory,
		"removed": s.handleBackupListRemoved,
	} {
		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != http.StatusNotImplemented {
			t.Errorf("%s = %d, want 501", name, rec.Code)
		}
	}
}

func TestBackupErrorMapping(t *testing.T) {
	s, fake := newBackupTestServer(t)
	fake.GetSnapshotFn = func(context.Context, string) (*backup.Snapshot, error) { return nil, backup.ErrNotFound }
	rec := httptest.NewRecorder()
	s.handleBackupGetSnapshot(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("not found = %d", rec.Code)
	}
	fake.RunJobFn = func(context.Context, string) (backup.RunJobResponse, error) {
		return backup.RunJobResponse{}, backup.ErrBusy
	}
	rec = httptest.NewRecorder()
	s.handleBackupRunJob(rec, httptest.NewRequest(http.MethodPost, "/", nil))
	if rec.Code != http.StatusConflict {
		t.Fatalf("busy = %d", rec.Code)
	}
}

func TestBackupScheduleCheck(t *testing.T) {
	s, _ := newBackupTestServer(t)
	post := func(body string) (int, string) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		s.handleBackupScheduleCheck(rec, req)
		return rec.Code, rec.Body.String()
	}
	code, body := post(`{"schedule":"0 */6 * * *"}`)
	if code != 200 || !strings.Contains(body, `"valid": true`) || strings.Count(body, "T") < 3 {
		t.Fatalf("valid cron: %d %s", code, body)
	}
	code, body = post(`{"schedule":"6h"}`)
	if code != 200 || strings.Contains(body, `"valid": true`) || !strings.Contains(body, "error") {
		t.Fatalf("interval: %d %s", code, body)
	}
	if code, _ = post(`{bad`); code != http.StatusBadRequest {
		t.Fatalf("bad body = %d", code)
	}
}

func TestPreActionCallsServiceAndSurvivesFailure(t *testing.T) {
	s, fake := newBackupTestServer(t)
	fake.PreActionFn = func(context.Context, string) (string, error) { return "", errors.New("disk full") }
	s.preAction(context.Background(), backup.ActionDeleteEntry) // must not panic or abort
	s.preAction(context.Background(), backup.ActionClearRepairRuns)
	got := fake.PreActions()
	if len(got) != 2 || got[0] != backup.ActionDeleteEntry || got[1] != backup.ActionClearRepairRuns {
		t.Fatalf("pre-actions = %v", got)
	}
}

func TestRefusedPreActionAnswers409AndStopsHandler(t *testing.T) {
	s, fake := newBackupTestServer(t)
	fake.PreActionFn = func(context.Context, string) (string, error) {
		return "", fmt.Errorf("%w: disk full", backup.ErrPreActionRefused)
	}
	rec := httptest.NewRecorder()
	// The handler must stop before it touches storage (the test manager has none).
	s.handleClearRepairRuns(rec, httptest.NewRequest(http.MethodDelete, "/", nil))
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "safety snapshot") {
		t.Fatalf("refused = %d %s", rec.Code, rec.Body.String())
	}
	if err := s.preAction(context.Background(), backup.ActionDeleteEntry); !errors.Is(err, backup.ErrPreActionRefused) {
		t.Fatalf("preAction error = %v", err)
	}
}

type holdFake struct {
	*backuptest.Service
	held bool
}

func (h *holdFake) HoldActive() bool   { return h.held }
func (h *holdFake) ReleaseHold() error { h.held = false; return nil }

func TestBackupReleaseHoldRoute(t *testing.T) {
	s, fake := newBackupTestServer(t)
	rec := httptest.NewRecorder()
	s.handleBackupReleaseHold(rec, httptest.NewRequest(http.MethodPost, "/", nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("service without a hold = %d", rec.Code)
	}
	hf := &holdFake{Service: fake, held: true}
	s.manager.SetBackup(hf)
	rec = httptest.NewRecorder()
	s.handleBackupReleaseHold(rec, httptest.NewRequest(http.MethodPost, "/", nil))
	if rec.Code != http.StatusOK || hf.held || !strings.Contains(rec.Body.String(), "false") {
		t.Fatalf("release = %d %s held=%v", rec.Code, rec.Body.String(), hf.held)
	}
}
