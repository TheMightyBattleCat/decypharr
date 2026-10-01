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
// dereferences it), an initialized EntryCache (InvalidateEntryCache needs it), and
// its own temp-file-backed arrLibraryMap (isolated per test, rather than
// sharing the package's one throwaway config dir across every subtest).
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
		storage:       s,
		logger:        zerolog.Nop(),
		config:        &config.Config{},
		arrLibraryMap: newTestArrLibraryMap(t),
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

// TestHandleArrWebhookCleanup_LibraryMatchTiers covers the tiers added on top
// of the original downloadId/filename matching, using the two real payloads
// Sonarr/Radarr are known to send when they delete or upgrade a file: the
// library file name they report is the renamed, post-import name, sharing
// nothing with the release name decypharr stored the entry under - except
// sceneName, which Arr preserves unchanged.
func TestHandleArrWebhookCleanup_LibraryMatchTiers(t *testing.T) {
	const (
		wintercatsEntry = "The.WinterCats.1970.BluRay.1080p.REMUX.AVC.DTS-HD.MA.5.1-LEGi0N"
		wintercatsFile  = "The.WinterCats.1970.BluRay.1080p.REMUX.AVC.DTS-HD.MA.5.1-LEGi0N.mkv"
		inheritorsEntry = "Inheritors.S02E07.iTALiAN.1080p.WEB.H264-CHEOPE"
		inheritorsFile  = "Inheritors.S02E07.iTALiAN.1080p.WEB.H264-CHEOPE.mkv"

		// The actual post-import, renamed library file name Arr reports in
		// its delete webhook (from the real logs this feature is fixing) -
		// used as the event's FileName in the path-tier cases below so the
		// existing downloadId/filename tier (which only ever matches
		// decypharr's own, unrenamed file names) can't accidentally hit
		// first and mask what's actually being tested.
		renamedWintercatsFile = "The Wintercats (1970) {imdb-tt0000042} [Remux-1080p][DTS-HD MA 5.1][AVC]-LEGi0N.mkv"
	)

	// Every entry below carries a second, untouched file so a match always
	// tears down just the one named file ("file-removed") rather than the
	// whole entry - keeping every case's assertions independent of the
	// DFS-cache-dir/full-delete path exercised elsewhere.
	twoFileEntry := func(infoHash, name, mainFile string, createdAt time.Time, downloading bool, protocol config.Protocol) *storage.Entry {
		return &storage.Entry{
			InfoHash:      infoHash,
			Name:          name,
			Protocol:      protocol,
			CreatedAt:     createdAt,
			IsDownloading: downloading,
			Files: map[string]*storage.File{
				mainFile:      {Name: mainFile, InfoHash: infoHash, AddedOn: createdAt},
				"sibling.mkv": {Name: "sibling.mkv", InfoHash: infoHash, AddedOn: createdAt},
			},
		}
	}

	old := time.Now().Add(-time.Hour)

	tests := []struct {
		name          string
		setup         func(t *testing.T, m *Manager)
		event         ArrWebhookEvent
		wantAction    string
		wantMatchedBy string
		wantEntryName string
	}{
		{
			name: "movie: reported path exact-matches the recorded symlink path",
			setup: func(t *testing.T, m *Manager) {
				addTestEntry(t, m, twoFileEntry("wintercats-hash", wintercatsEntry, wintercatsFile, old, false, config.ProtocolNZB))
				m.arrLibraryMap.record(wintercatsEntry, wintercatsFile,
					"/downloads/nzb/"+wintercatsEntry+"/"+wintercatsFile,
					"/mnt/rclone/__all__/"+wintercatsEntry+"/"+wintercatsFile,
				)
			},
			event: ArrWebhookEvent{
				DeleteReason: "upgrade",
				FileName:     renamedWintercatsFile,
				LibraryPath:  "/downloads/nzb/" + wintercatsEntry + "/" + wintercatsFile,
			},
			wantAction:    "file-removed",
			wantMatchedBy: "symlink_path",
			wantEntryName: wintercatsEntry,
		},
		{
			name: "movie: renamed reported path falls back to a recorded basename match",
			setup: func(t *testing.T, m *Manager) {
				addTestEntry(t, m, twoFileEntry("wintercats-hash-2", wintercatsEntry, wintercatsFile, old, false, config.ProtocolNZB))
				m.arrLibraryMap.record(wintercatsEntry, wintercatsFile,
					"/downloads/nzb/"+wintercatsEntry+"/"+wintercatsFile,
				)
			},
			event: ArrWebhookEvent{
				DeleteReason: "upgrade",
				FileName:     renamedWintercatsFile,
				// A different directory (the real Arr library folder), same basename.
				LibraryPath: "/media/Movies/The Wintercats (1970) {imdb-tt0000042}/" + wintercatsFile,
			},
			wantAction:    "file-removed",
			wantMatchedBy: "symlink_basename",
			wantEntryName: wintercatsEntry,
		},
		{
			name: "episode: sceneName fallback matches when no mapping was ever recorded",
			setup: func(t *testing.T, m *Manager) {
				addTestEntry(t, m, twoFileEntry("inheritors-hash", inheritorsEntry, inheritorsFile, old, false, config.ProtocolNZB))
			},
			event: ArrWebhookEvent{
				DeleteReason: "manual",
				// The renamed library file Arr actually reports - unmatchable
				// by path or basename against anything decypharr recorded.
				FileName:    "Inheritors - S02E07 - Return WEBDL-1080p.mkv",
				LibraryPath: "/media/TV/Inheritors/Season 02/Inheritors - S02E07 - Return WEBDL-1080p.mkv",
				SceneName:   inheritorsEntry,
			},
			wantAction:    "file-removed",
			wantMatchedBy: "scene_name",
			wantEntryName: inheritorsEntry,
		},
		{
			name: "ambiguous sceneName match across two entries yields no-match",
			setup: func(t *testing.T, m *Manager) {
				addTestEntry(t, m, twoFileEntry("dup-hash-1", "Ambiguous.Release-GROUP", "Ambiguous.Release-GROUP.mkv", old, false, config.ProtocolNZB))
				addTestEntry(t, m, twoFileEntry("dup-hash-2", "Ambiguous.Release-GROUP", "Ambiguous.Release-GROUP.mkv", old, false, config.ProtocolNZB))
			},
			event: ArrWebhookEvent{
				DeleteReason: "manual",
				FileName:     "Renamed.In.Library.mkv",
				SceneName:    "Ambiguous.Release-GROUP",
			},
			wantAction:    "no-match",
			wantMatchedBy: "",
			wantEntryName: "",
		},
		{
			name: "freshly-added entry is skipped, not resolved, by sceneName",
			setup: func(t *testing.T, m *Manager) {
				addTestEntry(t, m, twoFileEntry("fresh-hash", "Fresh.Upgrade.Release-GROUP", "Fresh.Upgrade.Release-GROUP.mkv", time.Now(), false, config.ProtocolNZB))
			},
			event: ArrWebhookEvent{
				DeleteReason: "upgrade",
				FileName:     "Renamed.In.Library.mkv",
				SceneName:    "Fresh.Upgrade.Release-GROUP",
			},
			wantAction:    "no-match",
			wantMatchedBy: "",
			wantEntryName: "",
		},
		{
			name: "currently-downloading entry is skipped, not resolved, by sceneName",
			setup: func(t *testing.T, m *Manager) {
				addTestEntry(t, m, twoFileEntry("downloading-hash", "Downloading.Release-GROUP", "Downloading.Release-GROUP.mkv", old, true, config.ProtocolNZB))
			},
			event: ArrWebhookEvent{
				DeleteReason: "upgrade",
				FileName:     "Renamed.In.Library.mkv",
				SceneName:    "Downloading.Release-GROUP",
			},
			wantAction:    "no-match",
			wantMatchedBy: "",
			wantEntryName: "",
		},
		{
			name: "torrent-protocol entry resolved by sceneName is still never touched",
			setup: func(t *testing.T, m *Manager) {
				addTestEntry(t, m, twoFileEntry("torrent-hash", "Torrent.Release-GROUP", "Torrent.Release-GROUP.mkv", old, false, config.ProtocolTorrent))
			},
			event: ArrWebhookEvent{
				DeleteReason: "manual",
				FileName:     "Renamed.In.Library.mkv",
				SceneName:    "Torrent.Release-GROUP",
			},
			wantAction:    "torrent-skipped",
			wantMatchedBy: "scene_name",
			wantEntryName: "Torrent.Release-GROUP",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newTestManager(t)
			tt.setup(t, m)

			action, entryName, matchedBy := m.HandleArrWebhookCleanup(tt.event)

			if action != tt.wantAction {
				t.Errorf("action = %q, want %q", action, tt.wantAction)
			}
			if matchedBy != tt.wantMatchedBy {
				t.Errorf("matchedBy = %q, want %q", matchedBy, tt.wantMatchedBy)
			}
			if entryName != tt.wantEntryName {
				t.Errorf("entryName = %q, want %q", entryName, tt.wantEntryName)
			}
		})
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
