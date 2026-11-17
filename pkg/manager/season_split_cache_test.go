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

// evictMount is a stub MountManager whose EvictCachedFile removes the cached
// file from base, as the DFS cache does.
type evictMount struct {
	stubMountManager
	base string
}

func (m *evictMount) EvictCachedFile(entryName, filename string) (int64, bool) {
	p := filepath.Join(m.base, entryName, filename)
	info, err := os.Stat(p)
	if os.IsNotExist(err) {
		return 0, true
	}
	if err != nil {
		return 0, false
	}
	if err := os.Remove(p); err != nil {
		return 0, false
	}
	return info.Size(), true
}

// seasonSplitCacheFixture stores a multi-season NZB's parent entry and its
// two season entries, and caches one file of each season under the parent's
// folder, where the split seasons' files are served and cached.
func seasonSplitCacheFixture(t *testing.T) (m *Manager, s1 *storage.Entry, s1File, s2File, folderDir string) {
	t.Helper()
	base := withDFSCacheDir(t)
	m, strg := newCacheDirTestManager(t)
	m.SetMountManager(&evictMount{base: base})

	const nzbID = "nzb-pack"
	now := time.Now()
	file := func(name string) *storage.File {
		return &storage.File{Name: name, InfoHash: nzbID, Size: 100, AddedOn: now}
	}
	parent := &storage.Entry{
		InfoHash: nzbID,
		Name:     "Show.S01-S02.1080p",
		Protocol: config.ProtocolNZB,
		Files: map[string]*storage.File{
			"Show.S01E01.mkv": file("Show.S01E01.mkv"),
			"Show.S02E01.mkv": file("Show.S02E01.mkv"),
		},
	}
	s1 = &storage.Entry{
		InfoHash: generateSeasonHash(nzbID, 1),
		Name:     "Show.S01.1080p",
		Protocol: config.ProtocolNZB,
		SavePath: t.TempDir(),
		Files:    map[string]*storage.File{"Show.S01E01.mkv": file("Show.S01E01.mkv")},
	}
	s2 := &storage.Entry{
		InfoHash: generateSeasonHash(nzbID, 2),
		Name:     "Show.S02.1080p",
		Protocol: config.ProtocolNZB,
		SavePath: t.TempDir(),
		Files:    map[string]*storage.File{"Show.S02E01.mkv": file("Show.S02E01.mkv")},
	}
	for _, e := range []*storage.Entry{parent, s1, s2} {
		if err := strg.AddOrUpdate(e); err != nil {
			t.Fatalf("AddOrUpdate %s: %v", e.Name, err)
		}
	}

	folderDir = filepath.Join(base, parent.GetFolder())
	if err := os.MkdirAll(folderDir, 0o755); err != nil {
		t.Fatal(err)
	}
	s1File = filepath.Join(folderDir, "Show.S01E01.mkv")
	s2File = filepath.Join(folderDir, "Show.S02E01.mkv")
	for _, p := range []string{s1File, s2File} {
		if err := os.WriteFile(p, []byte("cached bytes"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return m, s1, s1File, s2File, folderDir
}

func assertSeasonCacheFreed(t *testing.T, s1File, s2File, folderDir string) {
	t.Helper()
	if _, err := os.Stat(s1File); !os.IsNotExist(err) {
		t.Errorf("season 1's cached file survived, stat err=%v", err)
	}
	if _, err := os.Stat(s2File); err != nil {
		t.Errorf("sibling season 2's cached file was removed: %v", err)
	}
	if _, err := os.Stat(folderDir); err != nil {
		t.Errorf("parent cache folder still used by season 2 was removed: %v", err)
	}
}

// Deleting one season of a split frees that season's cache in the parent's
// folder; the sibling season's cache and the shared folder survive.
func TestDeleteSeasonSplitEntryFreesOnlyItsCache(t *testing.T) {
	m, s1, s1File, s2File, folderDir := seasonSplitCacheFixture(t)

	if err := m.deleteEntry(s1.InfoHash, false); err != nil {
		t.Fatalf("deleteEntry: %v", err)
	}
	assertSeasonCacheFreed(t, s1File, s2File, folderDir)
}

// The same for a season whose import is rejected.
func TestRejectSeasonSplitImportFreesOnlyItsCache(t *testing.T) {
	m, s1, s1File, s2File, folderDir := seasonSplitCacheFixture(t)

	d := &Downloader{manager: m, logger: zerolog.Nop()}
	d.cleanupRejectedImport(s1)
	assertSeasonCacheFreed(t, s1File, s2File, folderDir)
}
