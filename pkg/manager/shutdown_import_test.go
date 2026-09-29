package manager

import (
	"context"
	"errors"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// The import gates let an import through when their check is cut short. A
// shutdown cuts every check short, so completeEntry must not report the import
// done then: it returns the cancellation, and processAction keeps the entry to
// import and check again after the restart.
func TestCompleteEntryDuringShutdownDoesNotReportTheImport(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	cfg := config.Get()
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	m := &Manager{
		storage: store, config: cfg, ctx: ctx, cancelDownloads: cancel,
		queue: newQueue(store, ""), arr: arr.NewStorage(), logger: zerolog.Nop(),
		clients: xsync.NewMap[string, debrid.Client](), processingEntries: xsync.NewMap[string, struct{}](),
	}
	m.downloader = NewDownloadManager(m)
	entry := &storage.Entry{
		InfoHash: "0123456789012345678901234567890123456789", Name: "release",
		Protocol: config.ProtocolTorrent, State: storage.EntryStateDownloading, IsDownloading: true,
		Action: config.DownloadActionSymlink, SavePath: t.TempDir(),
		Files: map[string]*storage.File{}, Providers: map[string]*storage.ProviderEntry{},
	}
	if err := m.queue.Add(entry); err != nil {
		t.Fatal(err)
	}

	cancel() // Stop cancels the manager's context first
	if err := m.downloader.completeEntry(entry); !errors.Is(err, context.Canceled) {
		t.Fatalf("completeEntry during shutdown = %v, want context.Canceled", err)
	}
	saved, err := m.queue.GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if saved.IsComplete || saved.State != storage.EntryStateDownloading {
		t.Fatalf("entry after an interrupted import = complete %v, state %s; want it left to resume", saved.IsComplete, saved.State)
	}
}
