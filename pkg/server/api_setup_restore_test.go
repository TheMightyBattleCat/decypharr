package server

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/backup"
)

func setupRestoreRepo(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	repo, err := backup.OpenRepo(dir)
	if err != nil {
		t.Fatal(err)
	}
	ref, _, err := repo.PutBlob(context.Background(), strings.NewReader(`{"restored":true}`), backup.CompressDefault)
	if err != nil {
		t.Fatal(err)
	}
	snap := &backup.Snapshot{ID: "20260101-000000-abcd", CreatedAt: time.Now(), Trigger: "manual:full",
		Components: map[backup.Component]backup.ComponentEntry{
			backup.ComponentState: {Files: map[string]string{"arr_library_paths.json": ref.SHA256}},
		}}
	if err := repo.WriteSnapshot(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	return dir, snap.ID
}

func postJSON(h http.HandlerFunc, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)))
	return rec
}

func TestSetupRestoreListsAndRestores(t *testing.T) {
	data := t.TempDir()
	config.SetConfigPath(data)
	t.Cleanup(config.Reset)
	if err := config.Get().SetupComplete(); err == nil {
		t.Skip("a fresh config counts as set up")
	}
	repo, id := setupRestoreRepo(t)
	s := &Server{}

	rec := postJSON(s.setupRestoreListHandler, `{"dir":"`+repo+`"}`)
	var list setupRestoreResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil || rec.Code != 200 || len(list.Snapshots) != 1 {
		t.Fatalf("list: %d %s", rec.Code, rec.Body)
	}

	rec = postJSON(s.setupRestoreHandler, `{"dir":"`+repo+`","snapshot_id":"latest"}`)
	if rec.Code != 200 {
		t.Fatalf("restore: %d %s", rec.Code, rec.Body)
	}
	var out setupRestoreResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if out.SnapshotID != id || out.Restored["state"] != 1 || !out.RestartRequired {
		t.Fatalf("response = %+v", out)
	}
	if b, _ := os.ReadFile(filepath.Join(data, "arr_library_paths.json")); string(b) != `{"restored":true}` {
		t.Fatalf("state file = %q", b)
	}
}

func TestSetupRestoreRefusedOnceSetupIsDone(t *testing.T) {
	data := t.TempDir()
	config.SetConfigPath(data)
	t.Cleanup(config.Reset)
	if _, err := config.Update(func(c *config.Config) error {
		c.DownloadFolder = data
		c.Usenet.Providers = []config.UsenetProvider{{Host: "news.example.org", Port: 119, Username: "u", Password: "p", MaxConnections: 1}}
		return nil
	}); err != nil || config.Get().SetupComplete() != nil {
		t.Skip("cannot build a completed config here")
	}
	repo, _ := setupRestoreRepo(t)
	if rec := postJSON((&Server{}).setupRestoreHandler, `{"dir":"`+repo+`"}`); rec.Code != http.StatusForbidden {
		t.Fatalf("code = %d", rec.Code)
	}
}

func TestSetupRestoreNeverCreatesARepository(t *testing.T) {
	data := t.TempDir()
	config.SetConfigPath(data)
	t.Cleanup(config.Reset)
	if err := config.Get().SetupComplete(); err == nil {
		t.Skip("a fresh config counts as set up")
	}
	s := &Server{}
	missing := filepath.Join(t.TempDir(), "nowhere", "backups")
	empty := t.TempDir()

	for name, h := range map[string]http.HandlerFunc{"list": s.setupRestoreListHandler, "restore": s.setupRestoreHandler} {
		for _, dir := range []string{missing, empty} {
			if rec := postJSON(h, `{"dir":"`+dir+`"}`); rec.Code != http.StatusBadRequest {
				t.Fatalf("%s %s: code = %d", name, dir, rec.Code)
			}
		}
	}
	if _, err := os.Stat(filepath.Dir(missing)); !os.IsNotExist(err) {
		t.Fatalf("a folder was created for the missing path: %v", err)
	}
	if entries, _ := os.ReadDir(empty); len(entries) != 0 {
		t.Fatalf("the empty folder was turned into a repository: %v", entries)
	}
}

func TestSetupCompleteRefusedWhileRestorePending(t *testing.T) {
	data := t.TempDir()
	config.SetConfigPath(data)
	t.Cleanup(config.Reset)
	if err := config.Get().SetupComplete(); err == nil {
		t.Skip("a fresh config counts as set up")
	}
	body := `{"debrid":{"provider":"realdebrid","api_key":"k"},"auth":{"skip_auth":true}}`
	s := &Server{}

	if err := os.WriteFile(filepath.Join(data, backup.PendingRestoreFile), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(filepath.Join(data, "config.json"))
	if rec := postJSON(s.setupCompleteHandler, body); rec.Code != http.StatusConflict {
		t.Fatalf("code = %d, want 409 (%s)", rec.Code, rec.Body)
	}
	if after, _ := os.ReadFile(filepath.Join(data, "config.json")); string(after) != string(before) {
		t.Fatal("config.json changed while a restore was pending")
	}

	// Without the pending marker the handler gets past the guard.
	_ = os.Remove(filepath.Join(data, backup.PendingRestoreFile))
	if rec := postJSON(s.setupCompleteHandler, body); rec.Code == http.StatusConflict {
		t.Fatalf("still refused without a pending restore: %s", rec.Body)
	}
}

func setupArchive(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(zw)
	for name, body := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		_, _ = tw.Write([]byte(body))
	}
	if err := errors.Join(tw.Close(), zw.Close()); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func uploadRequest(t *testing.T, field string, data []byte) *http.Request {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, err := mw.CreateFormFile(field, "backup.tar.zst")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fw.Write(data)
	_ = mw.Close()
	req := httptest.NewRequest(http.MethodPost, "/", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return req
}

func TestSetupRestoreUploadRestoresAndCleansUp(t *testing.T) {
	data := t.TempDir()
	config.SetConfigPath(data)
	t.Cleanup(config.Reset)
	if err := config.Get().SetupComplete(); err == nil {
		t.Skip("a fresh config counts as set up")
	}
	s := &Server{}
	arch := setupArchive(t, map[string]string{"arr_library_paths.json": `{"up":true}`})

	rec := httptest.NewRecorder()
	s.setupRestoreUploadHandler(rec, uploadRequest(t, "file", arch))
	if rec.Code != 200 {
		t.Fatalf("upload: %d %s", rec.Code, rec.Body)
	}
	var out setupRestoreResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if out.Restored["state"] != 1 || !out.RestartRequired {
		t.Fatalf("response = %+v", out)
	}
	if b, _ := os.ReadFile(filepath.Join(data, "arr_library_paths.json")); string(b) != `{"up":true}` {
		t.Fatalf("state file = %q", b)
	}
	if left, _ := filepath.Glob(filepath.Join(data, ".setup-upload-*")); len(left) != 0 {
		t.Fatalf("temp upload left behind: %v", left)
	}
	if left, _ := filepath.Glob(filepath.Join(data, ".restore-archive-*")); len(left) != 0 {
		t.Fatalf("scratch left behind: %v", left)
	}

	// No file field, a bad archive and an oversize upload are refused and
	// leave no temp file either.
	rec = httptest.NewRecorder()
	s.setupRestoreUploadHandler(rec, uploadRequest(t, "other", arch))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("no file field: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	s.setupRestoreUploadHandler(rec, uploadRequest(t, "file", []byte("not an archive")))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad archive: %d", rec.Code)
	}
	old := setupUploadLimit
	setupUploadLimit = 64
	t.Cleanup(func() { setupUploadLimit = old })
	rec = httptest.NewRecorder()
	s.setupRestoreUploadHandler(rec, uploadRequest(t, "file", arch))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize: %d %s", rec.Code, rec.Body)
	}
	if left, _ := filepath.Glob(filepath.Join(data, ".setup-upload-*")); len(left) != 0 {
		t.Fatalf("temp upload left behind: %v", left)
	}
}

func TestSetupRestoreUploadRefusedOnceSetupIsDone(t *testing.T) {
	data := t.TempDir()
	config.SetConfigPath(data)
	t.Cleanup(config.Reset)
	if _, err := config.Update(func(c *config.Config) error {
		c.DownloadFolder = data
		c.Usenet.Providers = []config.UsenetProvider{{Host: "news.example.org", Port: 119, Username: "u", Password: "p", MaxConnections: 1}}
		return nil
	}); err != nil || config.Get().SetupComplete() != nil {
		t.Skip("cannot build a completed config here")
	}
	rec := httptest.NewRecorder()
	(&Server{}).setupRestoreUploadHandler(rec, uploadRequest(t, "file", setupArchive(t, map[string]string{"config.json": "{}"})))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("code = %d", rec.Code)
	}
	if left, _ := filepath.Glob(filepath.Join(data, ".setup-upload-*")); len(left) != 0 {
		t.Fatalf("temp upload left behind: %v", left)
	}
}

func TestSetupRestoresRefuseWhileAnotherRestoreRuns(t *testing.T) {
	data := t.TempDir()
	config.SetConfigPath(data)
	t.Cleanup(config.Reset)
	if err := config.Get().SetupComplete(); err == nil {
		t.Skip("a fresh config counts as set up")
	}
	s := &Server{}
	setupRestoreMu.Lock()
	t.Cleanup(setupRestoreMu.Unlock)

	rec := postJSON(s.setupRestoreHandler, `{"dir":"`+data+`"}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("folder restore: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	s.setupRestoreUploadHandler(rec, uploadRequest(t, "file", setupArchive(t, map[string]string{"config.json": "{}"})))
	if rec.Code != http.StatusConflict {
		t.Fatalf("upload: %d", rec.Code)
	}
	if b, _ := os.ReadFile(filepath.Join(data, "config.json")); string(b) == "{}" {
		t.Fatal("config replaced while another restore ran")
	}
}

func multiUploadRequest(t *testing.T, archives ...[]byte) *http.Request {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	for _, a := range archives {
		fw, err := mw.CreateFormFile("file", "backup.tar.zst")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = fw.Write(a)
	}
	_ = mw.Close()
	req := httptest.NewRequest(http.MethodPost, "/", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return req
}

func TestSetupRestoreUploadOfSeveralFilesRefusesAllOrNone(t *testing.T) {
	data := t.TempDir()
	config.SetConfigPath(data)
	t.Cleanup(config.Reset)
	if err := config.Get().SetupComplete(); err == nil {
		t.Skip("a fresh config counts as set up")
	}
	s := &Server{}
	cfg := setupArchive(t, map[string]string{"config.json": `{"new":true}`})
	badDB := setupArchive(t, map[string]string{
		"backup-info.json": `{"format":1,"component":"db","db_format":999999}`,
		"db/entries":       "x",
	})
	before, _ := os.ReadFile(filepath.Join(data, "config.json"))
	for name, order := range map[string][][]byte{"config first": {cfg, badDB}, "db first": {badDB, cfg}} {
		rec := httptest.NewRecorder()
		s.setupRestoreUploadHandler(rec, multiUploadRequest(t, order...))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: code = %d %s", name, rec.Code, rec.Body)
		}
		if after, _ := os.ReadFile(filepath.Join(data, "config.json")); string(after) != string(before) {
			t.Fatalf("%s: config.json changed to %q", name, after)
		}
	}
	if left, _ := filepath.Glob(filepath.Join(data, ".setup-upload-*")); len(left) != 0 {
		t.Fatalf("temp upload left behind: %v", left)
	}

	state := setupArchive(t, map[string]string{"arr_library_paths.json": `{"s":1}`})
	rec := httptest.NewRecorder()
	s.setupRestoreUploadHandler(rec, multiUploadRequest(t, cfg, state))
	if rec.Code != 200 {
		t.Fatalf("good pair: %d %s", rec.Code, rec.Body)
	}
	if b, _ := os.ReadFile(filepath.Join(data, "config.json")); string(b) != `{"new":true}` {
		t.Fatalf("config = %q", b)
	}
}
