package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gorilla/sessions"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/backup"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

// routeSvc answers every Service method the backup routes call with a value
// the tests can recognise, and records the arguments that matter.
type routeSvc struct {
	backup.Service
	calls map[string]any
}

func (f *routeSvc) rec(name string, v any) { f.calls[name] = v }

func (f *routeSvc) Status(context.Context) (backup.StatusResponse, error) {
	return backup.StatusResponse{Configured: true, Snapshots: 3}, nil
}
func (f *routeSvc) RunJob(_ context.Context, n string) (backup.RunJobResponse, error) {
	f.rec("run", n)
	return backup.RunJobResponse{Job: n, Started: true}, nil
}
func (f *routeSvc) ListSnapshots(context.Context) ([]backup.SnapshotInfo, error) { return nil, nil }
func (f *routeSvc) GetSnapshot(_ context.Context, id string) (*backup.Snapshot, error) {
	return &backup.Snapshot{ID: id}, nil
}
func (f *routeSvc) DeleteSnapshot(_ context.Context, id string) error {
	f.rec("delete", id)
	return nil
}
func (f *routeSvc) VerifySnapshot(_ context.Context, id string, full bool) (backup.VerifyReport, error) {
	f.rec("verify", full)
	return backup.VerifyReport{OK: true, Snapshots: 1}, nil
}
func (f *routeSvc) Download(_ context.Context, id string, c backup.Component) (backup.DownloadResult, error) {
	f.rec("download", c)
	return backup.DownloadResult{Filename: id + ".tar.zst", ContentType: "application/zstd", Body: io.NopCloser(strings.NewReader("payload"))}, nil
}
func (f *routeSvc) RestoreDB(context.Context, string) (backup.RestoreDBResponse, error) {
	return backup.RestoreDBResponse{StagedDir: "staged", RestartRequired: true}, nil
}
func (f *routeSvc) RestoreNZB(_ context.Context, _ string, r backup.RestoreNZBRequest) (backup.RestoreNZBResponse, error) {
	return backup.RestoreNZBResponse{Entry: r.Entry, Restored: 2}, nil
}
func (f *routeSvc) PlanLinksRestore(_ context.Context, _ string, r backup.LinksPlanRequest) (backup.LinkPlan, error) {
	return backup.LinkPlan{PlanID: "p1", Scope: r.Scope}, nil
}
func (f *routeSvc) ApplyLinksRestore(_ context.Context, _ string, r backup.LinksApplyRequest) (backup.LinksApplyResponse, error) {
	f.rec("apply", r)
	return backup.LinksApplyResponse{Created: 4}, nil
}
func (f *routeSvc) LinksStatus(context.Context) (backup.LinksStatusResponse, error) {
	return backup.LinksStatusResponse{Missing: 7}, nil
}
func (f *routeSvc) LinksCheck(context.Context) (backup.LinksStatusResponse, error) {
	return backup.LinksStatusResponse{Running: true}, nil
}
func (f *routeSvc) ListLinks(_ context.Context, q backup.LinksQuery) (backup.LinksPage, error) {
	f.rec("links", q)
	return backup.LinksPage{Total: 9, Page: q.Page}, nil
}
func (f *routeSvc) LinksMissing(context.Context) ([]backup.MissingLink, error) { return nil, nil }
func (f *routeSvc) PlanLinksCleanup(context.Context, backup.LinksCleanupPlanRequest) (backup.CleanupPlan, error) {
	return backup.CleanupPlan{PlanID: "c1", Total: 2}, nil
}
func (f *routeSvc) ApplyLinksCleanup(_ context.Context, r backup.LinksCleanupApplyRequest) (backup.CleanupResult, error) {
	f.rec("cleanup", r)
	return backup.CleanupResult{Removed: 2}, nil
}
func (f *routeSvc) ListRemoved(context.Context, backup.RemovedQuery) ([]backup.RemovedEntry, error) {
	return nil, nil
}
func (f *routeSvc) RestoreRemoved(_ context.Context, k string, r backup.RestoreRemovedRequest) (backup.RestoreRemovedResponse, error) {
	return backup.RestoreRemovedResponse{Key: k, Applied: !r.DryRun}, nil
}
func (f *routeSvc) ListConfigHistory(context.Context) ([]backup.ConfigVersion, error) {
	return nil, nil
}
func (f *routeSvc) DiffConfigHistory(_ context.Context, id, against string) (backup.ConfigDiff, error) {
	return backup.ConfigDiff{ID: id, Against: against}, nil
}
func (f *routeSvc) RevertConfig(context.Context, string) (backup.RevertResponse, error) {
	return backup.RevertResponse{NewID: "v2", RestartRequired: true}, nil
}
func (f *routeSvc) RetentionPreview(_ context.Context, r backup.RetentionPreviewRequest) (backup.PrunePlan, error) {
	return backup.PrunePlan{KeepSnapshots: r.Policy.Last}, nil
}
func (f *routeSvc) PreAction(_ context.Context, a string) (string, error) {
	f.rec("pre:"+a, true)
	return "snap-pre", nil
}

func newRouteServer(t *testing.T) (http.Handler, *routeSvc) {
	t.Helper()
	svc := &routeSvc{Service: backup.NotImplemented(), calls: map[string]any{}}
	return newRouteServerFor(t, svc), svc
}

// newRouteServerFor serves the web routes in front of svc.
func newRouteServerFor(t *testing.T, svc backup.Service) http.Handler {
	t.Helper()
	// The setup middleware answers 503 until a provider is configured.
	cfg := config.Get()
	prevProviders, prevUseAuth, prevAuth := cfg.Usenet.Providers, cfg.UseAuth, cfg.Auth
	t.Cleanup(func() { cfg.Usenet.Providers, cfg.UseAuth, cfg.Auth = prevProviders, prevUseAuth, prevAuth })
	cfg.UseAuth = false
	cfg.Usenet.Providers = []config.UsenetProvider{{Host: "news.example.invalid", Port: 563, Username: "u", Password: "p", MaxConnections: 1}}
	s, _ := newBackupTestServer(t)
	mgr := &manager.Manager{}
	mgr.SetBackup(svc)
	s.manager = mgr
	s.urlBase = "/"
	s.cookie = sessions.NewCookieStore([]byte("test-secret"))
	return s.WebRoutes()
}

// One case per route: method, path, body, expected status and a fragment of
// the JSON body that proves the service result reached the response.
func TestBackupAndLinksRoutesEndToEnd(t *testing.T) {
	h, svc := newRouteServer(t)
	cases := []struct {
		method, path, body string
		status             int
		want               string
	}{
		{"GET", "/api/backups/status", "", 200, `"snapshots": 3`},
		{"GET", "/api/backups/snapshots", "", 200, `[]`},
		{"GET", "/api/backups/snapshots/abc", "", 200, `"id": "abc"`},
		{"POST", "/api/backups/jobs/quick/run", "", 202, `"started": true`},
		{"POST", "/api/backups/snapshots/abc/verify?full=true", "", 200, `"ok": true`},
		{"GET", "/api/backups/snapshots/abc/download?component=db", "", 200, `payload`},
		{"POST", "/api/backups/snapshots/abc/restore/db", "", 200, `"restart_required": true`},
		{"POST", "/api/backups/snapshots/abc/restore/nzb", `{"entry":"e1"}`, 200, `"restored_files": 2`},
		{"POST", "/api/backups/snapshots/abc/restore/links/plan", `{"scope":{"root":"/tv"}}`, 200, `"plan_id": "p1"`},
		{"POST", "/api/backups/snapshots/abc/restore/links/apply", `{"plan_id":"p1","retarget":true}`, 200, `"created": 4`},
		{"DELETE", "/api/backups/snapshots/abc", "", 204, ``},
		{"GET", "/api/backups/config-history", "", 200, `[]`},
		{"GET", "/api/backups/config-history/v1/diff", "", 200, `"against": "previous"`},
		{"POST", "/api/backups/config-history/v1/revert", "", 200, `"new_id": "v2"`},
		{"GET", "/api/backups/removed", "", 200, `[]`},
		{"POST", "/api/backups/removed/k1/restore", `{"dry_run":true}`, 200, `"key": "k1"`},
		{"POST", "/api/backups/retention/preview", `{"policy":{"last":5}}`, 200, `"keep_snapshots": 5`},
		{"POST", "/api/backups/schedule/check", `{"schedule":"0 3 * * *"}`, 200, `"valid": true`},
		{"GET", "/api/links/status", "", 200, `"missing": 7`},
		{"POST", "/api/links/check", "", 200, `"running": true`},
		{"GET", "/api/links?state=live&page=2", "", 200, `"total": 9`},
		{"GET", "/api/links/missing", "", 200, `[]`},
		{"POST", "/api/links/cleanup/plan", `{"filter":{"root":"/tv"}}`, 200, `"plan_id": "c1"`},
		{"POST", "/api/links/cleanup/apply", `{"plan_id":"c1","groups":["g"]}`, 200, `"removed": 2`},
	}
	for _, c := range cases {
		t.Run(c.method+" "+c.path, func(t *testing.T) {
			req := httptest.NewRequest(c.method, c.path, strings.NewReader(c.body))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != c.status {
				t.Fatalf("status = %d, want %d: %s", rec.Code, c.status, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), c.want) {
				t.Fatalf("body %q lacks %q", rec.Body.String(), c.want)
			}
		})
	}
	if svc.calls["verify"] != true || svc.calls["run"] != "quick" || svc.calls["delete"] != "abc" {
		t.Errorf("arguments did not reach the service: %v", svc.calls)
	}
	if q, _ := svc.calls["links"].(backup.LinksQuery); q.State != backup.LinkLive || q.Page != 2 {
		t.Errorf("links query = %+v", q)
	}
	if a, _ := svc.calls["apply"].(backup.LinksApplyRequest); a.PlanID != "p1" || !a.Retarget {
		t.Errorf("apply request = %+v", a)
	}
	if _, ok := svc.calls["pre:"+backup.ActionLinksCleanup]; !ok {
		t.Errorf("links cleanup apply took no pre-action snapshot: %v", svc.calls)
	}
}

func TestBackupRoutesValidateAndMapErrors(t *testing.T) {
	h, _ := newRouteServer(t)
	cases := []struct {
		method, path, body string
		status             int
	}{
		{"GET", "/api/backups/snapshots/abc/download?component=nzb", "", 400},
		{"GET", "/api/backups/snapshots/abc/download", "", 400},
		{"POST", "/api/backups/snapshots/abc/restore/nzb", `{}`, 400},
		{"POST", "/api/backups/snapshots/abc/restore/nzb", `{bad`, 400},
		{"POST", "/api/backups/snapshots/abc/restore/links/apply", `{}`, 400},
		{"GET", "/api/backups/removed?since=yesterday", "", 400},
		{"GET", "/api/backups/config-history/v1/diff?against=current", "", 200},
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(c.method, c.path, strings.NewReader(c.body)))
		if rec.Code != c.status {
			t.Errorf("%s %s = %d, want %d", c.method, c.path, rec.Code, c.status)
		}
	}
}

func TestBackupDownloadHeaders(t *testing.T) {
	h, _ := newRouteServer(t)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/api/backups/snapshots/abc/download?component=config", nil))
	if got := rec.Header().Get("Content-Disposition"); got != `attachment; filename="abc.tar.zst"` {
		t.Errorf("Content-Disposition = %q", got)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/zstd" {
		t.Errorf("Content-Type = %q", got)
	}
}

// Downloads and every other backup route sit behind the authenticated
// router: without a session or token the answer is 401 and the service is
// never reached; with the API token the download streams.
func TestBackupRoutesNeedAuthentication(t *testing.T) {
	h, svc := newRouteServer(t)
	cfg := config.Get()
	cfg.UseAuth = true // restored by newRouteServer's cleanup
	cfg.Auth = &config.Auth{Username: "user", Password: "hash", APIToken: "api-token"}
	for _, p := range []string{
		"/api/backups/snapshots/abc/download?component=db", "/api/backups/status", "/api/links/status",
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", p, nil))
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("GET %s without credentials = %d, want 401", p, rec.Code)
		}
	}
	if len(svc.calls) != 0 {
		t.Errorf("service was reached without authentication: %v", svc.calls)
	}
	req := httptest.NewRequest("GET", "/api/backups/snapshots/abc/download?component=db", nil)
	req.Header.Set("Authorization", "Bearer api-token")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "payload" {
		t.Errorf("authenticated download = %d %q", rec.Code, rec.Body.String())
	}
}

// The history route reaches the real service and its history, not a fake.
func TestConfigHistoryRouteThroughRealService(t *testing.T) {
	dir := t.TempDir()
	blobs, err := backup.OpenRepo(filepath.Join(dir, "history"))
	if err != nil {
		t.Fatal(err)
	}
	h := backup.NewHistory(backup.HistoryOptions{Dir: filepath.Join(dir, "history"), Blobs: blobs})
	if _, _, err := h.Record(context.Background(), []byte(`{"log_level":"info"}`), "settings"); err != nil {
		t.Fatal(err)
	}
	svc := backup.NewService(backup.Deps{DataDir: dir})
	svc.History = h

	rec := httptest.NewRecorder()
	newRouteServerFor(t, svc).ServeHTTP(rec, httptest.NewRequest("GET", "/api/backups/config-history", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "settings") {
		t.Fatalf("GET config-history = %d %s", rec.Code, rec.Body.String())
	}
}
