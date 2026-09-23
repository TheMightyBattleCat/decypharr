package manager

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// inUseMount is a stub MountManager that also answers dfsEntryCacheUse.
type inUseMount struct {
	stubMountManager
	inUse map[string]bool
}

func (m *inUseMount) EntryCacheInUse(entryName string) bool { return m.inUse[entryName] }

// withDFSCacheDir points the live config at a temp DFS cache dir for the
// test, restoring the previous mount settings afterwards.
func withDFSCacheDir(t *testing.T) string {
	t.Helper()
	cfg := config.Get()
	prevType, prevDir := cfg.Mount.Type, cfg.Mount.DFS.CacheDir
	dir := t.TempDir()
	cfg.Mount.Type = config.MountTypeDFS
	cfg.Mount.DFS.CacheDir = dir
	t.Cleanup(func() {
		cfg.Mount.Type = prevType
		cfg.Mount.DFS.CacheDir = prevDir
	})
	return dir
}

func newCacheDirTestManager(t *testing.T) (*Manager, *storage.Storage) {
	t.Helper()
	m, strg := newTestManagerForReapVerdict(t)
	m.logger = zerolog.Nop()
	m.repair = &Repair{manager: m, logger: zerolog.Nop()}
	return m, strg
}

func addCacheDirEntry(t *testing.T, strg *storage.Storage, hash, name, file string) *storage.Entry {
	t.Helper()
	e := &storage.Entry{
		InfoHash: hash,
		Name:     name,
		Protocol: config.ProtocolNZB,
		Files: map[string]*storage.File{
			file: {Name: file, InfoHash: hash, Size: 100, AddedOn: time.Now()},
		},
	}
	if err := strg.AddOrUpdate(e); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}
	return e
}

func mkCacheDir(t *testing.T, base, name string) string {
	t.Helper()
	dir := filepath.Join(base, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "file.mkv"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestDeleteEntryRemovesDFSCacheDir(t *testing.T) {
	base := withDFSCacheDir(t)
	m, strg := newCacheDirTestManager(t)
	e := addCacheDirEntry(t, strg, "h1", "Show.S01E01", "file.mkv")
	dir := mkCacheDir(t, base, e.GetFolder())

	if err := m.deleteEntry("h1", false); err != nil {
		t.Fatalf("deleteEntry: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("cache dir should be gone after delete, stat err=%v", err)
	}
}

func TestDeleteEntryKeepsCacheDirOfSameNameTwin(t *testing.T) {
	base := withDFSCacheDir(t)
	m, strg := newCacheDirTestManager(t)
	old := addCacheDirEntry(t, strg, "old", "Show.S01E01", "file.mkv")
	addCacheDirEntry(t, strg, "new", "Show.S01E01", "other.mkv")
	dir := mkCacheDir(t, base, old.GetFolder())

	if err := m.deleteEntry("old", false); err != nil {
		t.Fatalf("deleteEntry: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("cache dir still claimed by a same-name twin was removed: %v", err)
	}
}

func TestRemoveEntryCacheDirLeavesOpenCache(t *testing.T) {
	base := withDFSCacheDir(t)
	m, strg := newCacheDirTestManager(t)
	e := addCacheDirEntry(t, strg, "h1", "Show.S01E01", "file.mkv")
	dir := mkCacheDir(t, base, e.GetFolder())
	m.SetMountManager(&inUseMount{inUse: map[string]bool{e.GetFolder(): true}})

	if _, ok := m.repair.RemoveEntryCacheDir(e.GetFolder(), "h1"); ok {
		t.Fatal("removed a cache dir with a file open")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("open cache dir removed: %v", err)
	}
}
