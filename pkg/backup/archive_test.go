//go:build !windows

package backup_test

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/backup"
	"github.com/sirrobot01/decypharr/pkg/backup/backuptest"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

type tarEntry struct {
	h    tar.Header
	body string
}

func reg(name, body string) tarEntry {
	return tarEntry{h: tar.Header{Name: name, Mode: 0o600, Typeflag: tar.TypeReg}, body: body}
}

func infoEntry(t *testing.T, dbFormat int) tarEntry {
	t.Helper()
	b, err := json.Marshal(backup.ArchiveInfo{Format: 1, SnapshotID: "snapX", Component: backup.ComponentDB, DBFormat: dbFormat})
	if err != nil {
		t.Fatal(err)
	}
	return reg(backup.ArchiveInfoName, string(b))
}

func buildArchive(t *testing.T, entries ...tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(zw)
	for _, e := range entries {
		h := e.h
		if h.Typeflag == tar.TypeReg {
			h.Size = int64(len(e.body))
		}
		if err := tw.WriteHeader(&h); err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			if _, err := io.WriteString(tw, e.body); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func dirEntries(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		out = append(out, e.Name())
	}
	return out
}

func downloadBytes(t *testing.T, svc *backup.BackupService, id string, c backup.Component) []byte {
	t.Helper()
	res, err := svc.Download(context.Background(), id, c)
	if err != nil {
		t.Fatal(c, err)
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestRestoreFromArchiveRoundTripsDownload(t *testing.T) {
	st := newSourceStore(t)
	repo := backuptest.NewMemRepo()
	ctx := context.Background()
	dbEntry, err := backup.NewDBExporter(st)(ctx, repo, nil)
	if err != nil {
		t.Fatal(err)
	}
	put := func(body string) string {
		ref, _, err := repo.PutBlob(ctx, strings.NewReader(body), backup.CompressDefault)
		if err != nil {
			t.Fatal(err)
		}
		return ref.SHA256
	}
	snap := &backup.Snapshot{
		ID: "20260101-000000-abcd", CreatedAt: time.Now(), Trigger: "manual:full",
		Decypharr: backup.BuildInfo{Commit: "abc1234", DBFormat: storage.DBFormat},
		Components: map[backup.Component]backup.ComponentEntry{
			backup.ComponentDB:     dbEntry,
			backup.ComponentConfig: {Files: map[string]string{"config.json": put(`{"port":"8282"}`)}},
			backup.ComponentState:  {Files: map[string]string{"arr_library_paths.json": put(`{"a":1}`)}},
		},
	}
	if err := repo.WriteSnapshot(ctx, snap); err != nil {
		t.Fatal(err)
	}
	svc := backup.NewService(backup.Deps{
		DataDir:  t.TempDir(),
		Config:   func() config.Backup { return config.Backup{Dir: "repo"} },
		OpenRepo: func(string) (backup.Repo, error) { return repo, nil },
	})

	data := t.TempDir()
	if err := os.WriteFile(filepath.Join(data, "config.json"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, c := range []backup.Component{backup.ComponentConfig, backup.ComponentState, backup.ComponentDB} {
		rep, err := backup.RestoreFromArchive(ctx, bytes.NewReader(downloadBytes(t, svc, snap.ID, c)),
			backup.ArchiveRestoreOptions{DataDir: data})
		if err != nil {
			t.Fatal(c, err)
		}
		if rep.Restored[c] == 0 {
			t.Fatalf("%s: report %+v", c, rep)
		}
	}
	if b, _ := os.ReadFile(filepath.Join(data, "config.json")); string(b) != `{"port":"8282"}` {
		t.Fatalf("config = %q", b)
	}
	if olds, _ := filepath.Glob(filepath.Join(data, "config.json.before-restore-*")); len(olds) != 1 {
		t.Fatalf("old config not kept: %v", olds)
	}
	if b, _ := os.ReadFile(filepath.Join(data, "arr_library_paths.json")); string(b) != `{"a":1}` {
		t.Fatalf("state = %q", b)
	}
	if !backup.PendingRestoreExists(data) {
		t.Fatal("database not staged")
	}
	if err := backup.ApplyPendingRestore(data); err != nil {
		t.Fatal(err)
	}
	if !backup.PostRestoreHoldActive(data) {
		t.Fatal("post-restore hold not on")
	}
	restored, err := storage.NewStorage(filepath.Join(data, "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if restored.StoreCounts()["entries"] != 2 {
		t.Fatalf("counts = %v", restored.StoreCounts())
	}
	for _, left := range dirEntries(t, data) {
		if strings.HasPrefix(left, ".restore-archive-") {
			t.Fatalf("scratch folder left behind: %s", left)
		}
	}
}

func TestRestoreFromArchiveRejectsUnsafeEntriesWritingNothing(t *testing.T) {
	good := reg("config.json", "new")
	cases := map[string][]tarEntry{
		"traversal":        {good, reg("../evil", "x")},
		"nested traversal": {good, reg("db/../../evil", "x")},
		"absolute":         {good, reg("/etc/passwd", "x")},
		"dot segment":      {good, reg("./auth.json", "x")},
		"symlink":          {good, {h: tar.Header{Name: "auth.json", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd"}}},
		"hardlink":         {good, {h: tar.Header{Name: "auth.json", Typeflag: tar.TypeLink, Linkname: "config.json"}}},
		"device":           {good, {h: tar.Header{Name: "auth.json", Typeflag: tar.TypeChar}}},
		"fifo":             {good, {h: tar.Header{Name: "auth.json", Typeflag: tar.TypeFifo}}},
		"unknown name":     {good, reg("usenet/meta/x.meta", "x")},
		"duplicate":        {good, reg("config.json", "again")},
	}
	for name, entries := range cases {
		t.Run(name, func(t *testing.T) {
			data := t.TempDir()
			_, err := backup.RestoreFromArchive(context.Background(), bytes.NewReader(buildArchive(t, entries...)),
				backup.ArchiveRestoreOptions{DataDir: data})
			if !errors.Is(err, backup.ErrInvalid) {
				t.Fatalf("err = %v", err)
			}
			if left := dirEntries(t, data); len(left) != 0 {
				t.Fatalf("data folder not empty: %v", left)
			}
		})
	}
}

func TestRestoreFromArchiveCapsSizeAndCount(t *testing.T) {
	data := t.TempDir()
	big := buildArchive(t, reg("config.json", strings.Repeat("x", 4096)))
	_, err := backup.RestoreFromArchive(context.Background(), bytes.NewReader(big),
		backup.ArchiveRestoreOptions{DataDir: data, MaxBytes: 1024})
	if !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("oversize: %v", err)
	}
	many := buildArchive(t, reg("config.json", "a"), reg("auth.json", "b"), reg("plex_reap_jobs.json", "c"))
	_, err = backup.RestoreFromArchive(context.Background(), bytes.NewReader(many),
		backup.ArchiveRestoreOptions{DataDir: data, MaxEntries: 2})
	if !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("too many entries: %v", err)
	}
	if left := dirEntries(t, data); len(left) != 0 {
		t.Fatalf("data folder not empty: %v", left)
	}
	if _, err := backup.RestoreFromArchive(context.Background(), strings.NewReader("not zstd at all"),
		backup.ArchiveRestoreOptions{DataDir: data}); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("garbage: %v", err)
	}
}

func TestRestoreFromArchiveRefusedDBLeavesConfigUntouched(t *testing.T) {
	cases := map[string][]tarEntry{
		"newer format":   {infoEntry(t, storage.DBFormat+1), reg("config.json", "new"), reg("db/entries", "x")},
		"no format info": {reg("config.json", "new"), reg("db/entries", "x")},
	}
	for name, entries := range cases {
		t.Run(name, func(t *testing.T) {
			data := t.TempDir()
			if err := os.WriteFile(filepath.Join(data, "config.json"), []byte("old"), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := backup.RestoreFromArchive(context.Background(), bytes.NewReader(buildArchive(t, entries...)),
				backup.ArchiveRestoreOptions{DataDir: data})
			if !errors.Is(err, backup.ErrInvalid) {
				t.Fatalf("err = %v", err)
			}
			if b, _ := os.ReadFile(filepath.Join(data, "config.json")); string(b) != "old" {
				t.Fatalf("config = %q", b)
			}
			if left := dirEntries(t, data); len(left) != 1 {
				t.Fatalf("data folder = %v", left)
			}
			if backup.PendingRestoreExists(data) {
				t.Fatal("pending marker written")
			}
		})
	}
}

func writeArchiveFile(t *testing.T, entries ...tarEntry) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "x.tar.zst")
	if err := os.WriteFile(p, buildArchive(t, entries...), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCLIRestoreFile(t *testing.T) {
	file := writeArchiveFile(t, reg("config.json", `{"p":1}`))
	data := t.TempDir()
	code, out, e := runCLI("restore", "--file", file, "--config", data)
	if code != 0 {
		t.Fatalf("restore: %d %q %q", code, out, e)
	}
	if b, _ := os.ReadFile(filepath.Join(data, "config.json")); string(b) != `{"p":1}` {
		t.Fatalf("config = %q", b)
	}

	if code, _, e := runCLI("restore", "--file", file, "--repo", t.TempDir(), "--config", data); code != 2 || !strings.Contains(e, "not both") {
		t.Fatalf("--file with --repo: %d %q", code, e)
	}
	if code, _, _ := runCLI("restore", "--file", file, "--config", data, "--only", "links"); code == 0 {
		t.Fatal("links restored from an archive")
	}
}

func TestCLIRestoreFileRefusesWhileStoresLocked(t *testing.T) {
	file := writeArchiveFile(t, reg("config.json", "new"))
	data := t.TempDir()
	if err := os.MkdirAll(filepath.Join(data, "db"), 0o755); err != nil {
		t.Fatal(err)
	}
	lock, err := os.OpenFile(filepath.Join(data, "db", "entries.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	code, _, e := runCLI("restore", "--file", file, "--config", data)
	if code == 0 || !strings.Contains(e, "running Decypharr") {
		t.Fatalf("restore went ahead: %d %q", code, e)
	}
	if _, err := os.Stat(filepath.Join(data, "config.json")); err == nil {
		t.Fatal("restore wrote files while the stores were locked")
	}
}

func TestRestoreFromArchiveRefusesHugeZstdWindow(t *testing.T) {
	// A frame header declaring a 256 MB window, followed by an empty last block.
	frame := []byte{0x28, 0xB5, 0x2F, 0xFD, 0x00, 18 << 3, 0x01, 0x00, 0x00}
	data := t.TempDir()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := backup.RestoreFromArchive(context.Background(), bytes.NewReader(frame),
		backup.ArchiveRestoreOptions{DataDir: data})
	runtime.ReadMemStats(&after)
	if !errors.Is(err, backup.ErrInvalid) || !strings.Contains(err.Error(), "window") {
		t.Fatalf("huge window: %v", err)
	}
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 128<<20 {
		t.Fatalf("allocated %d bytes for a refused frame", grew)
	}
	// An archive written by this build still restores.
	ok := buildArchive(t, reg("config.json", "{}"))
	if _, err := backup.RestoreFromArchive(context.Background(), bytes.NewReader(ok),
		backup.ArchiveRestoreOptions{DataDir: data}); err != nil {
		t.Fatalf("real archive: %v", err)
	}
}

func TestRestoreFromArchiveFilesChecksTheDatabaseFirst(t *testing.T) {
	badDB := writeArchiveFile(t, infoEntry(t, storage.DBFormat+1), reg("db/entries", "x"))
	cfg := writeArchiveFile(t, reg("config.json", "new"))
	state := writeArchiveFile(t, reg("arr_library_paths.json", "s"))
	for name, paths := range map[string][]string{
		"config then db":  {cfg, badDB},
		"db then config":  {badDB, cfg},
		"state, db, cfg":  {state, badDB, cfg},
		"two db archives": {badDB, badDB},
	} {
		t.Run(name, func(t *testing.T) {
			data := t.TempDir()
			if err := os.WriteFile(filepath.Join(data, "config.json"), []byte("old"), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := backup.RestoreFromArchiveFiles(context.Background(), paths, backup.ArchiveRestoreOptions{DataDir: data})
			if !errors.Is(err, backup.ErrInvalid) {
				t.Fatalf("err = %v", err)
			}
			if b, _ := os.ReadFile(filepath.Join(data, "config.json")); string(b) != "old" {
				t.Fatalf("config = %q", b)
			}
			if left := dirEntries(t, data); len(left) != 1 {
				t.Fatalf("data folder = %v", left)
			}
		})
	}

	data := t.TempDir()
	rep, err := backup.RestoreFromArchiveFiles(context.Background(), []string{cfg, state}, backup.ArchiveRestoreOptions{DataDir: data})
	if err != nil || rep.Restored[backup.ComponentConfig] != 1 || rep.Restored[backup.ComponentState] != 1 {
		t.Fatalf("good set: %+v %v", rep, err)
	}
}
