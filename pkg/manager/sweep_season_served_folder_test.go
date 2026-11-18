package manager

import (
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Once a season split out of a multi-season NZB is marked complete, the
// repair sweep probes it at webdav/__all__/<sweep candidate name>/<file>,
// where the name is the season's own folder (managed sweep) or the parent's
// folder its symlinks point into (Arr sweep).
// That URL must resolve through the same lookups the WebDAV handler makes
// (handleTorrentFile -> GetTorrentFile, handleDownload -> GetEntryByName):
// a 404 there would leave every split season unverifiable by the sweep.
func TestSweepProbe_SeasonSplitResolvesAfterImport(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	cfg := config.Get()

	store, err := storage.NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	m := &Manager{
		storage: store, queue: newQueue(store, ""), config: cfg, ctx: t.Context(),
		arr: arr.NewStorage(), logger: zerolog.Nop(),
		clients: xsync.NewMap[string, debrid.Client](), processingEntries: xsync.NewMap[string, struct{}](),
	}
	d := NewDownloadManager(m)

	const nzbID = "parent-nzb-id"
	s1 := &storage.File{Name: "Show.S01E01.mkv", Size: 1 << 20, InfoHash: nzbID}
	s2 := &storage.File{Name: "Show.S02E01.mkv", Size: 1 << 20, InfoHash: nzbID}
	parent := &storage.Entry{
		InfoHash: nzbID, Name: "Show.S01-S02.1080p", Protocol: config.ProtocolNZB,
		Files:     map[string]*storage.File{s1.Name: s1, s2.Name: s2},
		Providers: map[string]*storage.ProviderEntry{},
	}
	// processor.go writes the parent to the main store before download runs.
	if err := m.AddOrUpdate(parent, nil); err != nil {
		t.Fatal(err)
	}
	seasons := convertToMultiSeason(parent, []SeasonInfo{
		{SeasonNumber: 1, Files: []*storage.File{s1}, InfoHash: "season-1-id", Name: "Show.S01.1080p"},
		{SeasonNumber: 2, Files: []*storage.File{s2}, InfoHash: "season-2-id", Name: "Show.S02.1080p"},
	})
	season := seasons[0]
	if season.GetFolder() == parent.GetFolder() {
		t.Fatalf("test setup: season folder %q equals the parent's", parent.GetFolder())
	}

	// Before completion the season is not a served folder (fix 16's case).
	if _, err := m.GetTorrentFile(season.GetFolder(), s1.Name); err == nil {
		t.Fatalf("test setup: season folder %q already served before completion", season.GetFolder())
	}

	d.markAsCompleted(season)

	// The sweep's managed candidates are the entry-item names.
	r := &Repair{manager: m, logger: zerolog.Nop()}
	cands, err := r.enumerateManagedCandidates(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	c, ok := cands[season.GetFolder()]
	if !ok {
		names := make([]string, 0, len(cands))
		for n := range cands {
			names = append(names, n)
		}
		t.Fatalf("season %q not a sweep candidate after completion; candidates: %v", season.GetFolder(), names)
	}

	// The Arr sweep names its candidate after the symlink's target folder
	// (collectArrFiles), which for a split season is the parent's folder.
	probes := map[string]string{"managed": c.name, "arr": parent.GetFolder()}
	for kind, name := range probes {
		// Parse the folder and file back out of the URL the sweep's ffprobe
		// opens, as the WebDAV router's {torrent}/{file} params would be.
		checker := &ffprobeChecker{baseURL: "http://127.0.0.1:1/webdav/"}
		target := checker.probeTarget(name, s1.Name)
		parts := strings.Split(strings.TrimPrefix(target, checker.baseURL+EntryAllFolder+"/"), "/")
		if len(parts) != 2 {
			t.Fatalf("%s: unexpected probe URL shape %q", kind, target)
		}
		folder, _ := url.PathUnescape(parts[0])
		file, _ := url.PathUnescape(parts[1])

		info, err := m.GetTorrentFile(folder, file)
		if err != nil || info == nil {
			t.Fatalf("%s: sweep probe %s would 404 (handleTorrentFile): %v", kind, target, err)
		}
		if !info.IsRemote() {
			t.Fatalf("%s: sweep probe %s resolved to a local file, want the streamed one", kind, target)
		}
		entry, err := m.GetEntryByName(info.Parent(), info.Name())
		if err != nil || entry == nil {
			t.Fatalf("%s: sweep probe %s would fail in handleDownload: entry=%v err=%v", kind, target, entry, err)
		}
		if _, ok := entry.Files[s1.Name]; !ok {
			t.Fatalf("%s: sweep probe %s streams from entry %q, which has no %s", kind, target, entry.Name, s1.Name)
		}
	}
}
