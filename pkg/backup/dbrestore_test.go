package backup_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/backup"
	"github.com/sirrobot01/decypharr/pkg/backup/backuptest"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func exportSnapshot(t *testing.T, repo *backuptest.MemRepo, st *storage.Storage, dbFormat int) string {
	t.Helper()
	ce, err := backup.NewDBExporter(st)(context.Background(), repo, nil)
	if err != nil {
		t.Fatal(err)
	}
	snap := &backup.Snapshot{
		ID:         "snap1",
		Decypharr:  backup.BuildInfo{Commit: "abc1234", DBFormat: dbFormat},
		Components: map[backup.Component]backup.ComponentEntry{backup.ComponentDB: ce},
	}
	if err := repo.WriteSnapshot(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	return snap.ID
}

func newSourceStore(t *testing.T) *storage.Storage {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	st, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	for _, e := range []*storage.Entry{
		{InfoHash: "aaa", Name: "One", Category: "movies", Status: debridTypes.TorrentStatus("downloaded")},
		{InfoHash: "bbb", Name: "Two", Category: "tv", Status: debridTypes.TorrentStatus("downloaded")},
	} {
		if err := st.AddOrUpdate(e); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

func TestStageAndApplyDBRestoreKeepsOldFolder(t *testing.T) {
	st := newSourceStore(t)
	repo := backuptest.NewMemRepo()
	id := exportSnapshot(t, repo, st, storage.DBFormat)

	main := t.TempDir()
	live := filepath.Join(main, "db")
	if err := os.MkdirAll(live, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(live, "sentinel"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	resp, err := backup.StageDBRestore(repo, id, main)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.RestartRequired || resp.Counts["entries"] != 2 {
		t.Fatalf("response = %+v", resp)
	}
	if !backup.PendingRestoreExists(main) {
		t.Fatal("no pending marker")
	}
	if _, err := os.Stat(filepath.Join(live, "sentinel")); err != nil {
		t.Fatal("staging touched the live database")
	}

	if err := backup.ApplyPendingRestore(main); err != nil {
		t.Fatal(err)
	}
	if backup.PendingRestoreExists(main) || !backup.PostRestoreHoldActive(main) {
		t.Fatal("markers wrong after swap")
	}
	if _, err := os.Stat(filepath.Join(live, "sentinel")); err == nil {
		t.Fatal("old database still in db/")
	}
	olds, _ := filepath.Glob(filepath.Join(main, "db.before-restore-*", "sentinel"))
	if len(olds) != 1 {
		t.Fatalf("old folder not kept: %v", olds)
	}
	restored, err := storage.NewStorage(live)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if restored.StoreCounts()["entries"] != 2 {
		t.Fatalf("counts = %v", restored.StoreCounts())
	}
	if err := backup.ReleasePostRestoreHold(main); err != nil || backup.PostRestoreHoldActive(main) {
		t.Fatal("hold not released")
	}
	// A second start does nothing.
	if err := backup.ApplyPendingRestore(main); err != nil {
		t.Fatal(err)
	}
}

func TestStageDBRestoreRefusesNewerFormat(t *testing.T) {
	st := newSourceStore(t)
	repo := backuptest.NewMemRepo()
	id := exportSnapshot(t, repo, st, storage.DBFormat+1)

	main := t.TempDir()
	_, err := backup.StageDBRestore(repo, id, main)
	if !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("err = %v", err)
	}
	if backup.PendingRestoreExists(main) {
		t.Fatal("marker written for refused snapshot")
	}
	if ents, _ := filepath.Glob(filepath.Join(main, "db.restore-*")); len(ents) != 0 {
		t.Fatalf("staged folder left behind: %v", ents)
	}
}

func TestApplyPendingRestoreWithoutLiveDB(t *testing.T) {
	st := newSourceStore(t)
	repo := backuptest.NewMemRepo()
	id := exportSnapshot(t, repo, st, storage.DBFormat)
	main := t.TempDir()
	if _, err := backup.StageDBRestore(repo, id, main); err != nil {
		t.Fatal(err)
	}
	if err := backup.ApplyPendingRestore(main); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(main, "db", "entries.db")); err != nil {
		t.Fatalf("db missing: %v", err)
	}
}

type failingBlobs struct{ *backuptest.MemRepo }

func (failingBlobs) PutBlob(_ context.Context, r io.Reader, _ backup.CompressionLevel) (backup.BlobRef, bool, error) {
	_, _ = io.Copy(io.Discard, r)
	return backup.BlobRef{}, false, errors.New("disk full")
}

func TestDBExportReturnsWhenPutBlobFails(t *testing.T) {
	st := newSourceStore(t)
	done := make(chan error, 1)
	go func() {
		_, err := backup.NewDBExporter(st)(context.Background(), failingBlobs{backuptest.NewMemRepo()}, nil)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected an error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("export hung after PutBlob failed")
	}
}

func TestApplyPendingRestoreKeepsMarkerWhenHoldCannotBeWritten(t *testing.T) {
	st := newSourceStore(t)
	repo := backuptest.NewMemRepo()
	id := exportSnapshot(t, repo, st, storage.DBFormat)
	main := t.TempDir()
	if err := os.MkdirAll(filepath.Join(main, "db"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := backup.StageDBRestore(repo, id, main); err != nil {
		t.Fatal(err)
	}
	// A directory in the hold marker's place makes the write fail.
	if err := os.MkdirAll(filepath.Join(main, backup.PostRestoreHoldFile, "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := backup.ApplyPendingRestore(main); err == nil {
		t.Fatal("expected an error")
	}
	if !backup.PendingRestoreExists(main) {
		t.Fatal("pending marker removed although nothing was swapped")
	}
	if _, err := os.Stat(filepath.Join(main, "db")); err != nil {
		t.Fatal("live db moved")
	}
}
