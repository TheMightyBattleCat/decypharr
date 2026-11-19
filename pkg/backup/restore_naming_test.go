//go:build !windows

package backup_test

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/backup"
	"github.com/sirrobot01/decypharr/pkg/backup/backuptest"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func setNaming(t *testing.T, naming config.WebDavFolderNaming) {
	t.Helper()
	if _, err := config.Update(func(c *config.Config) error {
		c.FolderNaming = naming
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// namingSnapshot backs up an install that runs under infohash naming: its
// database and a config.json that says so.
func namingSnapshot(t *testing.T) (backup.Repo, *backup.BackupService, string) {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	setNaming(t, config.WebdavUseHash)
	st, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	for _, e := range []*storage.Entry{
		{InfoHash: "aaa", Name: "One", Category: "movies", Status: "downloaded", Protocol: config.ProtocolTorrent,
			Files: map[string]*storage.File{"one.mkv": {Name: "one.mkv", InfoHash: "aaa", Size: 1}}},
		{InfoHash: "bbb", Name: "Two", Category: "tv", Status: "downloaded", Protocol: config.ProtocolNZB,
			Files: map[string]*storage.File{"two.mkv": {Name: "two.mkv", InfoHash: "bbb", Size: 1}}},
	} {
		if err := st.AddOrUpdate(e); err != nil {
			t.Fatal(err)
		}
	}
	repo := backuptest.NewMemRepo()
	ctx := context.Background()
	dbEntry, err := backup.NewDBExporter(st)(ctx, repo, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, _, err := repo.PutBlob(ctx, strings.NewReader(`{"folder_naming":"infohash"}`), backup.CompressDefault)
	if err != nil {
		t.Fatal(err)
	}
	snap := &backup.Snapshot{
		ID: "20260101-000000-naming", CreatedAt: time.Now(), Trigger: "manual:full",
		Decypharr: backup.BuildInfo{Commit: "abc1234", DBFormat: storage.DBFormat},
		Components: map[backup.Component]backup.ComponentEntry{
			backup.ComponentDB:     dbEntry,
			backup.ComponentConfig: {Files: map[string]string{"config.json": ref.SHA256}},
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
	return repo, svc, snap.ID
}

// assertIndexedUnderHash restarts under the restored config (infohash) and
// checks that the folder name each entry is indexed under is its item's key.
func assertIndexedUnderHash(t *testing.T, data string) {
	t.Helper()
	setNaming(t, config.WebdavUseHash)
	st, err := storage.NewStorage(filepath.Join(data, "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var names []string
	if err := st.ForEachMeta(func(m *storage.EntryMetaInfo) error {
		names = append(names, m.Name)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	sort.Strings(names)
	var items []string
	for k := range st.GetEntryItems() {
		items = append(items, k)
	}
	sort.Strings(items)
	want := []string{"aaa", "bbb"}
	if !reflect.DeepEqual(names, want) || !reflect.DeepEqual(items, want) {
		t.Fatalf("indexed folder names = %v, items = %v; want both %v", names, items, want)
	}
}

// The setup wizard on a fresh install (default naming) restores the database
// archive and the config archive together. The database must be indexed
// under the restored config's naming, not the running one.
func TestArchiveRestoreIndexesUnderRestoredConfigNaming(t *testing.T) {
	_, svc, id := namingSnapshot(t)
	dbFile := filepath.Join(t.TempDir(), "db.tar.zst")
	cfgFile := filepath.Join(t.TempDir(), "config.tar.zst")
	if err := os.WriteFile(dbFile, downloadBytes(t, svc, id, backup.ComponentDB), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfgFile, downloadBytes(t, svc, id, backup.ComponentConfig), 0o600); err != nil {
		t.Fatal(err)
	}

	setNaming(t, "")
	data := t.TempDir()
	if _, err := backup.RestoreFromArchiveFiles(context.Background(), []string{cfgFile, dbFile},
		backup.ArchiveRestoreOptions{DataDir: data, ApplyDB: true}); err != nil {
		t.Fatal(err)
	}
	assertIndexedUnderHash(t, data)
}

// The same through a repository restore of the database and the config.
func TestRepoRestoreIndexesUnderRestoredConfigNaming(t *testing.T) {
	repo, _, id := namingSnapshot(t)
	setNaming(t, "")
	data := t.TempDir()
	if _, err := backup.RestoreToDataDir(context.Background(), repo, backup.RestoreOptions{
		SnapshotID: id, DataDir: data, ApplyDB: true,
		Only: []backup.Component{backup.ComponentDB, backup.ComponentConfig},
	}); err != nil {
		t.Fatal(err)
	}
	assertIndexedUnderHash(t, data)
}

func hasNamingWarning(warnings []string) bool {
	for _, w := range warnings {
		if strings.Contains(w, `folder_naming "infohash"`) {
			return true
		}
	}
	return false
}

// A database-only restore keeps the running naming. When the snapshot's
// config used another one, the restore says so; when it used the same, it
// says nothing.
func TestDBOnlyRestoreWarnsWhenSnapshotNamingDiffers(t *testing.T) {
	repo, _, id := namingSnapshot(t)

	setNaming(t, "")
	resp, err := backup.StageDBRestore(repo, id, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !hasNamingWarning(resp.Warnings) {
		t.Errorf("warnings = %q, want one naming the snapshot's folder_naming", resp.Warnings)
	}

	setNaming(t, config.WebdavUseHash)
	resp, err = backup.StageDBRestore(repo, id, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if hasNamingWarning(resp.Warnings) {
		t.Errorf("warnings = %q, want none under the same naming", resp.Warnings)
	}
}

// The same through a repository restore of the database alone; restoring the
// config with it gives no warning.
func TestRepoDBOnlyRestoreWarnsWhenSnapshotNamingDiffers(t *testing.T) {
	repo, _, id := namingSnapshot(t)
	setNaming(t, "")
	for _, tc := range []struct {
		only []backup.Component
		warn bool
	}{
		{[]backup.Component{backup.ComponentDB}, true},
		{[]backup.Component{backup.ComponentDB, backup.ComponentConfig}, false},
	} {
		rep, err := backup.RestoreToDataDir(context.Background(), repo, backup.RestoreOptions{
			SnapshotID: id, DataDir: t.TempDir(), Only: tc.only,
		})
		if err != nil {
			t.Fatal(tc.only, err)
		}
		if got := hasNamingWarning(rep.Warnings); got != tc.warn {
			t.Errorf("%v: warnings = %q, want naming warning %v", tc.only, rep.Warnings, tc.warn)
		}
	}
}
