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

// TestMain gives internal/config a throwaway config.json location before any
// test in this package touches config.Get(): entry.GetFolder() (used by
// storage.AddOrUpdate/GetEntryItem, both exercised below) reads
// config.Get().FolderNaming, and config.Get()'s first call creates a config
// file at GetMainPath() if none exists yet. Without this, that first call
// would create config.json in the process's working directory instead of a
// throwaway one.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "decypharr-manager-test-*")
	if err == nil {
		config.SetConfigPath(dir)
		defer os.RemoveAll(dir)
	}
	os.Exit(m.Run())
}

// newTestManager builds a Manager with just enough real state to exercise
// HandleArrWebhookCleanup: a real, temp-dir-backed Storage (so
// AddOrUpdate/GetEntryItem/DeleteEntryHealth behave exactly as they do in
// production), a non-nil config (RefreshMount, called async off DeleteEntry,
// dereferences it), and an initialized EntryCache (RefreshEntries needs it).
// usenet is deliberately left nil - every call site already guards for that
// (an install with usenet unconfigured), and none of the tests here reach
// finishUsenetTeardown/config.Get() inside removeWebhookCacheDir, which is
// the only path that would need it.
func newTestManager(t *testing.T) *Manager {
	t.Helper()
	s, err := storage.NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("failed to create test storage: %v", err)
	}
	m := &Manager{
		storage: s,
		logger:  zerolog.Nop(),
		config:  &config.Config{},
	}
	m.initEntryCache()
	return m
}

// addTestEntry saves an entry (and thereby its merged EntryItem) so
// GetEntry/GetEntryItem/RemoveTorrentFile all see it exactly as they would
// for a real download.
func addTestEntry(t *testing.T, m *Manager, entry *storage.Entry) {
	t.Helper()
	if entry.Files == nil {
		entry.Files = make(map[string]*storage.File)
	}
	if entry.Providers == nil {
		entry.Providers = make(map[string]*storage.ProviderEntry)
	}
	if err := m.storage.AddOrUpdate(entry); err != nil {
		t.Fatalf("failed to save test entry %q: %v", entry.Name, err)
	}
}

func TestHandleArrWebhookCleanup_MultiFileUsenetEntry_RemovesOnlyNamedFile(t *testing.T) {
	m := newTestManager(t)

	entry := &storage.Entry{
		InfoHash: "usenet-season-pack-hash",
		Name:     "Test.Show.S01",
		Protocol: config.ProtocolNZB,
		Files: map[string]*storage.File{
			"Test.Show.S01E01.mkv": {Name: "Test.Show.S01E01.mkv", InfoHash: "usenet-season-pack-hash", AddedOn: time.Now()},
			"Test.Show.S01E02.mkv": {Name: "Test.Show.S01E02.mkv", InfoHash: "usenet-season-pack-hash", AddedOn: time.Now()},
		},
	}
	addTestEntry(t, m, entry)

	action, entryName, matchedBy := m.HandleArrWebhookCleanup(ArrWebhookEvent{
		DownloadID:   "usenet-season-pack-hash",
		DeleteReason: "upgrade",
		FileName:     "Test.Show.S01E01.mkv",
	})

	if action != "file-removed" {
		t.Errorf("action = %q, want %q", action, "file-removed")
	}
	if entryName != "Test.Show.S01" {
		t.Errorf("entryName = %q, want %q", entryName, "Test.Show.S01")
	}
	if matchedBy != "downloadId" {
		t.Errorf("matchedBy = %q, want %q", matchedBy, "downloadId")
	}

	// The entry itself must still exist - only the named file is gone.
	if exists, _ := m.storage.Exists(entry.InfoHash); !exists {
		t.Fatal("entry was fully deleted, want it to survive with its other episode intact")
	}
	item, err := m.storage.GetEntryItem("Test.Show.S01")
	if err != nil {
		t.Fatalf("GetEntryItem: %v", err)
	}
	if f, ok := item.Files["Test.Show.S01E01.mkv"]; !ok || !f.Deleted {
		t.Errorf("named file should be marked deleted, got %+v", f)
	}
	if f, ok := item.Files["Test.Show.S01E02.mkv"]; !ok || f.Deleted {
		t.Errorf("other episode should be untouched, got %+v", f)
	}
}

func TestHandleArrWebhookCleanup_TorrentEntry_NeverTouched(t *testing.T) {
	m := newTestManager(t)

	entry := &storage.Entry{
		InfoHash: "torrent-hash",
		Name:     "Test.Movie.2024",
		Protocol: config.ProtocolTorrent,
		Files: map[string]*storage.File{
			"Test.Movie.2024.mkv": {Name: "Test.Movie.2024.mkv", InfoHash: "torrent-hash", AddedOn: time.Now()},
		},
	}
	addTestEntry(t, m, entry)

	action, entryName, matchedBy := m.HandleArrWebhookCleanup(ArrWebhookEvent{
		DownloadID:   "torrent-hash",
		DeleteReason: "upgrade",
		FileName:     "Test.Movie.2024.mkv",
	})

	if action != "torrent-skipped" {
		t.Errorf("action = %q, want %q", action, "torrent-skipped")
	}
	if entryName != "Test.Movie.2024" {
		t.Errorf("entryName = %q, want %q", entryName, "Test.Movie.2024")
	}
	if matchedBy != "downloadId" {
		t.Errorf("matchedBy = %q, want %q", matchedBy, "downloadId")
	}

	// Zero teardown: the entry and its file must be completely untouched.
	if exists, _ := m.storage.Exists(entry.InfoHash); !exists {
		t.Fatal("torrent entry was deleted, want it fully untouched")
	}
	item, err := m.storage.GetEntryItem("Test.Movie.2024")
	if err != nil {
		t.Fatalf("GetEntryItem: %v", err)
	}
	if f, ok := item.Files["Test.Movie.2024.mkv"]; !ok || f.Deleted {
		t.Errorf("torrent's file should be completely untouched, got %+v", f)
	}
}

func TestHandleArrWebhookCleanup_MissingEntry_NoMatch(t *testing.T) {
	m := newTestManager(t)

	action, entryName, matchedBy := m.HandleArrWebhookCleanup(ArrWebhookEvent{
		DownloadID:   "does-not-exist",
		DeleteReason: "upgrade",
		FileName:     "Some.File.That.Does.Not.Exist.mkv",
	})

	if action != "no-match" {
		t.Errorf("action = %q, want %q", action, "no-match")
	}
	if entryName != "" {
		t.Errorf("entryName = %q, want empty", entryName)
	}
	if matchedBy != "" {
		t.Errorf("matchedBy = %q, want empty", matchedBy)
	}
}
