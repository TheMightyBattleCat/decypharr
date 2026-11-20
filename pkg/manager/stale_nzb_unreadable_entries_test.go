package manager

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// newStaleNZBTestRepair is a Repair with NZB record storage and one Arr with
// an empty library, so the stale-NZB passes get past the Arr reference set.
// It stores one NZB entry and leaves an old .meta file named after it: a file
// an entry owns, never an orphan.
func newStaleNZBTestRepair(t *testing.T) (repair *Repair, metaFile string) {
	t.Helper()
	repair = newTestRepairForFix(t)
	store, err := overlay.NewStore(filepath.Join(config.GetMainPath(), "usenet", "overlay"), zerolog.Nop())
	if err != nil {
		t.Fatalf("overlay.NewStore: %v", err)
	}
	u, err := usenet.NewWithNZBStorageForTest(store)
	if err != nil {
		t.Fatalf("NewWithNZBStorageForTest: %v", err)
	}
	repair.manager.usenet = u

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
	t.Cleanup(srv.Close)
	repair.manager.arr.AddOrUpdate(arr.New("radarr", srv.URL, "key", false, nil, "", ""))

	owned := oldNZBEntry("nzb-owned", "Owned.Movie.2026")
	addEntries(t, repair, owned)

	metaFile = filepath.Join(u.NZBStorage().MetaDir(), owned.InfoHash+".meta")
	if err := os.WriteFile(metaFile, []byte("meta"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * staleNZBMinAge)
	if err := os.Chtimes(metaFile, old, old); err != nil {
		t.Fatal(err)
	}
	return repair, metaFile
}

// Control: with the entries readable, the owned file is no orphan.
func TestStaleNZBPreviewKeepsAnOwnedFileOutOfTheOrphans(t *testing.T) {
	repair, _ := newStaleNZBTestRepair(t)
	preview, err := repair.PreviewStaleNZBs(context.Background())
	if err != nil {
		t.Fatalf("PreviewStaleNZBs: %v", err)
	}
	if len(preview.Orphans) != 0 {
		t.Fatalf("orphans %+v, want none: the entry owns the file", preview.Orphans)
	}
}

// When the entries cannot be read (closed or failing storage), every file
// would look ownerless: the preview must list no orphans.
func TestStaleNZBPreviewListsNoOrphansWhenEntriesCannotBeRead(t *testing.T) {
	repair, _ := newStaleNZBTestRepair(t)
	if err := repair.manager.storage.Close(); err != nil {
		t.Fatal(err)
	}
	preview, err := repair.PreviewStaleNZBs(context.Background())
	if len(preview.Orphans) != 0 {
		t.Fatalf("preview lists %d orphan(s) %+v on unreadable entries", len(preview.Orphans), preview.Orphans)
	}
	if err == nil {
		t.Fatal("preview on unreadable entries returned no error")
	}
}

// The cleanup re-checks each requested orphan against the entries; when they
// cannot be read it deletes nothing.
func TestStaleNZBCleanupDeletesNoOrphanWhenEntriesCannotBeRead(t *testing.T) {
	repair, metaFile := newStaleNZBTestRepair(t)
	if err := repair.manager.storage.Close(); err != nil {
		t.Fatal(err)
	}
	result, _ := repair.CleanupStaleNZBs(context.Background(), nil, []string{"nzb-owned"})
	if _, err := os.Stat(metaFile); err != nil {
		t.Fatalf("owned .meta file deleted on unreadable entries: %v (result %+v)", err, result)
	}
	if result.Deleted != 0 {
		t.Fatalf("cleanup deleted %d, want 0", result.Deleted)
	}
}

// A settings restart whose storage reopen fails reports it: the caller must
// not start the services again on the closed store.
func TestResetReturnsTheStorageReopenError(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	dbDir := filepath.Join(config.GetMainPath(), "db")
	strg, err := storage.NewStorage(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{storage: strg, logger: zerolog.Nop()}
	if err := m.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	entries := filepath.Join(dbDir, "entries.db")
	f, err := os.OpenFile(entries, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(make([]byte, 64)); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	if err := m.Reset(); err == nil {
		t.Fatal("Reset over a damaged entries store returned no error")
	}
}
