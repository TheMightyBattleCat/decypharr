package manager

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// A queued NZB that was staged but never parsed when the process stopped is
// handed to a job unparsed after the restart, so the job parses it under the
// same retry and rejection rules as a new add, instead of the restore parsing
// it once and failing it (which the Arr blocklists) on a passing error.
func TestRestoreHandsAStagedNZBToTheJobUnparsed(t *testing.T) {
	oldPath := config.GetMainPath()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(func() { config.SetConfigPath(oldPath) })

	cfg := config.Get()
	oldUsenet := cfg.Usenet
	cfg.Usenet = config.Usenet{
		Providers: []config.UsenetProvider{{
			Host:           "127.0.0.1",
			Port:           1,
			MaxConnections: 1,
		}},
		MaxConnections:           1,
		ProcessingMaxConnections: 1,
		DiskBufferPath:           t.TempDir(),
	}
	t.Cleanup(func() { cfg.Usenet = oldUsenet })

	usenetClient, err := usenet.New()
	if err != nil {
		t.Fatalf("create usenet client: %v", err)
	}
	t.Cleanup(func() { _ = usenetClient.Close() })

	store, err := storage.NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	m := &Manager{
		usenet: usenetClient,
		queue:  newQueue(store, ""),
		arr:    arr.NewStorage(),
		config: cfg,
		logger: zerolog.Nop(),
		ctx:    t.Context(),
	}
	received := make(chan *Job, 1)
	m.jobQueue = NewJobQueue(t.Context(), 1, func(_ context.Context, job *Job) { received <- job })
	t.Cleanup(m.jobQueue.Close)

	staged := filepath.Join(t.TempDir(), "nzb-1.queued")
	if err := os.WriteFile(staged, []byte(`<nzb><file subject="x"></file></nzb>`), 0o644); err != nil {
		t.Fatal(err)
	}
	entry := &storage.Entry{
		InfoHash: "nzb-1", Name: "release.nzb", OriginalFilename: "release.nzb",
		Protocol: config.ProtocolNZB, Magnet: staged, Category: "sonarr",
		SavePath: filepath.Join(t.TempDir(), "sonarr"),
		Status:   debridTypes.TorrentStatusQueued, State: storage.EntryStateDownloading,
		AddedOn:   time.Now(),
		Providers: map[string]*storage.ProviderEntry{}, Files: map[string]*storage.File{},
	}
	if err := m.queue.Add(entry); err != nil {
		t.Fatal(err)
	}

	m.restoreActiveDownloadJobs()

	select {
	case job := <-received:
		if job.Request == nil || job.NZBMeta != nil {
			t.Fatalf("restored job request=%v meta=%v, want an unparsed job with its request", job.Request != nil, job.NZBMeta != nil)
		}
		if job.Request.Id != entry.InfoHash || job.Request.Name != "release.nzb" {
			t.Fatalf("restored request id=%q name=%q", job.Request.Id, job.Request.Name)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the staged NZB was not handed to a job")
	}

	saved, err := m.queue.GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if saved.State != storage.EntryStateDownloading || saved.Status != debridTypes.TorrentStatusQueued || saved.Magnet != staged {
		t.Fatalf("restored entry state=%s status=%s magnet=%q, want it still queued on its staged file", saved.State, saved.Status, saved.Magnet)
	}
}
