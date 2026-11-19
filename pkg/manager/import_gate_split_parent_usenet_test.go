package manager

import (
	"path/filepath"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/notifications"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// A multi-season NZB is split into one entry per season, and each season's
// completion runs the import gates over its own files. The parent is then
// completed only to leave the queue: its files are the seasons' files,
// already checked, so its completion must ask the news server nothing (the
// STAT gate asks about every segment, the availability gate about a sample).
func TestDownload_SplitParentAsksNewsServerNothing(t *testing.T) {
	config.Reset()
	t.Cleanup(config.Reset)
	servers, fx := newP2eEnv(t, 1) // sets the config path and the provider
	srv := servers[0]
	cfg := config.Get()
	cfg.Repair.FFProbeOnImport = false // row 73w's test covers the ffprobe gate

	const nzbID = "split-parent-nzb"
	seasonNames := []string{"Show.S01E01.mkv", "Show.S02E01.mkv"}
	nzb := fx.nzb(nzbID)
	nzb.Files = nzb.Files[:2]
	for i := range nzb.Files {
		nzb.Files[i].Name = seasonNames[i]
	}

	u, err := usenet.New()
	if err != nil {
		t.Fatalf("usenet.New: %v", err)
	}
	t.Cleanup(func() { _ = u.Close() })
	if err := u.AddNZBForTest(nzb); err != nil {
		t.Fatalf("AddNZBForTest: %v", err)
	}

	store, err := storage.NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	m := &Manager{
		storage: store, config: cfg, ctx: t.Context(), queue: newQueue(store, ""),
		arr: arr.NewStorage(), logger: zerolog.Nop(), usenet: u,
		Notifications: notifications.New(&cfg.Notifications, zerolog.Nop()),
		clients:       xsync.NewMap[string, debrid.Client](), processingEntries: xsync.NewMap[string, struct{}](),
	}
	d := NewDownloadManager(m)
	m.downloader = d

	files := map[string]*storage.File{}
	for _, f := range nzb.Files {
		files[f.Name] = &storage.File{Name: f.Name, Size: f.Size, InfoHash: nzbID}
	}

	// Baseline: what the two gates that read through Usenet ask the server
	// for one pass over each season file.
	for _, name := range seasonNames {
		one := &storage.Entry{
			InfoHash: "baseline-" + name, Name: name, Protocol: config.ProtocolNZB,
			Files:     map[string]*storage.File{name: files[name]},
			Providers: map[string]*storage.ProviderEntry{},
		}
		if err := d.importAvailabilityGate(one); err != nil {
			t.Fatalf("baseline availability gate on %s: %v", name, err)
		}
		if err := d.statImportGate(one); err != nil {
			t.Fatalf("baseline STAT gate on %s: %v", name, err)
		}
	}
	baseline := srv.counts()
	if baseline.Stat == 0 {
		t.Fatal("baseline gates sent no STAT: the gates did not reach the server")
	}
	srv.resetCounters()

	parent := &storage.Entry{
		InfoHash: nzbID, Name: "Show.S01-S02.1080p", Protocol: config.ProtocolNZB,
		State: storage.EntryStateDownloading, Action: config.DownloadActionNone, SavePath: t.TempDir(),
		Files:     files,
		Providers: map[string]*storage.ProviderEntry{},
	}
	if err := store.AddOrUpdate(parent); err != nil {
		t.Fatal(err)
	}
	if err := m.queue.Add(parent); err != nil {
		t.Fatal(err)
	}

	if err := d.download(parent); err != nil {
		t.Fatalf("download: %v", err)
	}

	got := srv.counts()
	t.Logf("baseline (one pass per season file): STAT %d BODY %d; download: STAT %d BODY %d",
		baseline.Stat, baseline.Body, got.Stat, got.Body)
	if got.Stat != baseline.Stat {
		t.Errorf("download sent %d STAT, want %d (each season's gates once, nothing for the parent)", got.Stat, baseline.Stat)
	}
	if got.Body != baseline.Body {
		t.Errorf("download sent %d BODY, want %d", got.Body, baseline.Body)
	}

	saved, err := store.Get(nzbID)
	if err != nil {
		t.Fatalf("parent not in storage: %v", err)
	}
	if !saved.IsComplete {
		t.Errorf("parent not complete after download (state %v)", saved.State)
	}
}
