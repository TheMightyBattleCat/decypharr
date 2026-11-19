package manager

import (
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
// repair sweep finds it under the season's own folder (managed sweep) or the
// parent's folder its symlinks point into (Arr sweep). Either way it probes
// webdav/__internal__/<infohash>/<file> with the hash the candidate's file
// carries, and that must resolve through Manager.InternalFile, the lookup
// the WebDAV handler makes: a 404 there would leave every split season
// unverifiable by the sweep.
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
		// probeFile takes the hash from the candidate's item, as here.
		item, err := m.GetEntryItem(name)
		if err != nil || item == nil || item.Files[s1.Name] == nil {
			t.Fatalf("%s: candidate %q has no item holding %s: %v", kind, name, s1.Name, err)
		}
		infoHash := item.Files[s1.Name].InfoHash

		const base = "http://127.0.0.1:1/webdav/"
		checker := &ffprobeChecker{baseURL: base}
		target := checker.probeTarget(infoHash, s1.Name)
		hash, file := internalTargetParts(t, base, target)
		for _, folder := range []string{season.GetFolder(), parent.GetFolder()} {
			if strings.Contains(target, folder) {
				t.Fatalf("%s: probe URL %s still names folder %q", kind, target, folder)
			}
		}
		entry, f, err := m.InternalFile(hash, file)
		if err != nil {
			t.Fatalf("%s: sweep probe %s would 404 on the internal path: %v", kind, target, err)
		}
		if entry.InfoHash != nzbID || f.Name != s1.Name {
			t.Fatalf("%s: sweep probe %s streams entry %q file %q, want %q %q", kind, target, entry.InfoHash, f.Name, nzbID, s1.Name)
		}
	}
}
