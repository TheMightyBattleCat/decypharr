package manager

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// newTestManagerForReap builds a Manager backed by both a real
// storage.Storage (for GetEntry/GetEntryItem - see
// newTestManagerForReapVerdict in overlay_reap_verdict_test.go) and a real
// *usenet.Usenet wired to its own overlay.Store on disk via
// usenet.NewWithOverlayForTest. Unlike OverlayReapVerdict, ReapOverlay's
// execute=true path actually deletes overlay records via
// Usenet.OverlayDeleteFile/OverlayDeleteEntry, so proving "the manifest dir
// is gone on disk" needs the real store, not a stub - Usenet's overlay
// field is unexported, so there's no way to fake it from this package.
//
// NewWithOverlayForTest builds a Usenet with no providers, no nntp client,
// and no background goroutines - nothing in ReapOverlay's overlay-store
// calls touches NNTP, so the full New constructor's machinery isn't needed
// here.
func newTestManagerForReap(t *testing.T) (m *Manager, configDir string) {
	t.Helper()
	configDir = t.TempDir()
	config.SetConfigPath(configDir)

	strg, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("storage.NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = strg.Close() })

	overlayRoot := filepath.Join(configDir, "usenet", "overlay")
	store, err := overlay.NewStore(overlayRoot, zerolog.Nop())
	if err != nil {
		t.Fatalf("overlay.NewStore: %v", err)
	}

	u := usenet.NewWithOverlayForTest(store)

	return &Manager{storage: strg, usenet: u}, configDir
}

// overlayDirFor mirrors overlay.Store.entryDir's layout (root/nzbID) so
// tests can assert on-disk presence/absence without an exported accessor.
func overlayDirFor(configDir, nzbID string) string {
	return filepath.Join(configDir, "usenet", "overlay", nzbID)
}

func assertOverlayDirExists(t *testing.T, configDir, nzbID string) {
	t.Helper()
	if _, err := os.Stat(overlayDirFor(configDir, nzbID)); err != nil {
		t.Fatalf("overlay dir for %q should still exist, stat: %v", nzbID, err)
	}
}

func assertOverlayDirGone(t *testing.T, configDir, nzbID string) {
	t.Helper()
	if _, err := os.Stat(overlayDirFor(configDir, nzbID)); !os.IsNotExist(err) {
		t.Fatalf("overlay dir for %q should be gone, stat returned: %v", nzbID, err)
	}
}

// TestReapOverlayRefusesLiveEntryEvenWithExecute is the load-bearing proof
// for the whole route: a LIVE entry (WouldReap=false) must be refused even
// when execute=true - this is the only thing preventing /overlay/reap from
// deleting an overlay record that's still the current owner of its slot.
func TestReapOverlayRefusesLiveEntryEvenWithExecute(t *testing.T) {
	m, configDir := newTestManagerForReap(t)

	liveHash := "live-hash-1"
	liveName := "My.Show.S01E01"
	liveFile := "My.Show.S01E01.mkv"
	if err := m.storage.AddOrUpdate(&storage.Entry{
		InfoHash: liveHash,
		Name:     liveName,
		Files: map[string]*storage.File{
			liveFile: {Name: liveFile, InfoHash: liveHash, Size: 100},
		},
	}); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}
	if err := m.Usenet().RecordOverlayDead(liveHash, liveFile, 0, "<msg-0>", 1000); err != nil {
		t.Fatalf("RecordOverlayDead: %v", err)
	}
	assertOverlayDirExists(t, configDir, liveHash)

	result, err := m.ReapOverlay(liveHash, liveFile, true)
	if err != nil {
		t.Fatalf("ReapOverlay: %v", err)
	}
	if result.Mode != ReapModeRefusedLive {
		t.Fatalf("Mode=%q, want %q", result.Mode, ReapModeRefusedLive)
	}
	if result.Removed {
		t.Fatalf("Removed=true, want false - a live record must never be deleted")
	}
	assertOverlayDirExists(t, configDir, liveHash)
}

// TestReapOverlayDryRunLeavesSupersededRecordUntouched proves execute=false
// never deletes anything, even when the verdict allows reaping.
func TestReapOverlayDryRunLeavesSupersededRecordUntouched(t *testing.T) {
	m, configDir := newTestManagerForReap(t)

	oldHash := "old-hash-X"
	newHash := "new-hash-Y"
	name := "My.Show.S01E02"
	file := "My.Show.S01E02.mkv"
	now := time.Now()
	if err := m.storage.AddOrUpdate(&storage.Entry{
		InfoHash: oldHash,
		Name:     name,
		Files: map[string]*storage.File{
			file: {Name: file, InfoHash: oldHash, Size: 100, AddedOn: now},
		},
	}); err != nil {
		t.Fatalf("AddOrUpdate(old): %v", err)
	}
	if err := m.storage.AddOrUpdate(&storage.Entry{
		InfoHash: newHash,
		Name:     name,
		Files: map[string]*storage.File{
			file: {Name: file, InfoHash: newHash, Size: 100, AddedOn: now.Add(time.Second)},
		},
	}); err != nil {
		t.Fatalf("AddOrUpdate(new): %v", err)
	}
	if err := m.Usenet().RecordOverlayDead(oldHash, file, 0, "<msg-0>", 1000); err != nil {
		t.Fatalf("RecordOverlayDead: %v", err)
	}
	assertOverlayDirExists(t, configDir, oldHash)

	result, err := m.ReapOverlay(oldHash, file, false)
	if err != nil {
		t.Fatalf("ReapOverlay: %v", err)
	}
	if result.Mode != ReapModeDryRun {
		t.Fatalf("Mode=%q, want %q", result.Mode, ReapModeDryRun)
	}
	if !result.Verdict.WouldReap {
		t.Fatalf("Verdict.WouldReap=false, want true - the slot has moved to a different InfoHash")
	}
	if result.Removed {
		t.Fatalf("Removed=true, want false - execute=false must never delete anything")
	}
	assertOverlayDirExists(t, configDir, oldHash)
}

// TestReapOverlayExecutesSupersededFileDelete proves the actual per-file
// reap path: execute=true on a superseded orphan deletes the overlay record
// and - since this was the only file recorded under oldHash - prunes the
// whole manifest directory (Store.DeleteFile's last-file branch), with no
// extra pruning code required on the ReapOverlay side.
func TestReapOverlayExecutesSupersededFileDelete(t *testing.T) {
	m, configDir := newTestManagerForReap(t)

	oldHash := "old-hash-X"
	newHash := "new-hash-Y"
	name := "My.Show.S01E02"
	file := "My.Show.S01E02.mkv"
	now := time.Now()
	if err := m.storage.AddOrUpdate(&storage.Entry{
		InfoHash: oldHash,
		Name:     name,
		Files: map[string]*storage.File{
			file: {Name: file, InfoHash: oldHash, Size: 100, AddedOn: now},
		},
	}); err != nil {
		t.Fatalf("AddOrUpdate(old): %v", err)
	}
	if err := m.storage.AddOrUpdate(&storage.Entry{
		InfoHash: newHash,
		Name:     name,
		Files: map[string]*storage.File{
			file: {Name: file, InfoHash: newHash, Size: 100, AddedOn: now.Add(time.Second)},
		},
	}); err != nil {
		t.Fatalf("AddOrUpdate(new): %v", err)
	}
	if err := m.Usenet().RecordOverlayDead(oldHash, file, 0, "<msg-0>", 1000); err != nil {
		t.Fatalf("RecordOverlayDead: %v", err)
	}
	assertOverlayDirExists(t, configDir, oldHash)

	result, err := m.ReapOverlay(oldHash, file, true)
	if err != nil {
		t.Fatalf("ReapOverlay: %v", err)
	}
	if result.Mode != ReapModeReapedFile {
		t.Fatalf("Mode=%q, want %q", result.Mode, ReapModeReapedFile)
	}
	if !result.Removed {
		t.Fatalf("Removed=false, want true")
	}
	assertOverlayDirGone(t, configDir, oldHash)
}

// TestReapOverlayExecutesEntryGoneDelete proves the whole-entry reap path:
// when nzbID's backing storage.Entry no longer exists at all, execute=true
// deletes the entire overlay record via OverlayDeleteEntry, and the
// manifest directory is gone on disk afterward.
func TestReapOverlayExecutesEntryGoneDelete(t *testing.T) {
	m, configDir := newTestManagerForReap(t)

	nzbID := "orphaned-nzb-id"
	file := "whatever.mkv"
	// Deliberately no storage.Entry for nzbID - only the overlay record.
	if err := m.Usenet().RecordOverlayDead(nzbID, file, 0, "<msg-0>", 1000); err != nil {
		t.Fatalf("RecordOverlayDead: %v", err)
	}
	assertOverlayDirExists(t, configDir, nzbID)

	result, err := m.ReapOverlay(nzbID, file, true)
	if err != nil {
		t.Fatalf("ReapOverlay: %v", err)
	}
	if result.Mode != ReapModeReapedEntry {
		t.Fatalf("Mode=%q, want %q", result.Mode, ReapModeReapedEntry)
	}
	if !result.Verdict.EntryGone {
		t.Fatalf("Verdict.EntryGone=false, want true")
	}
	if !result.Removed {
		t.Fatalf("Removed=false, want true")
	}
	assertOverlayDirGone(t, configDir, nzbID)
}

// TestReapOverlayExecuteWithNilUsenetErrorsWithoutPanic proves the
// execute=true dispatch guards against a nil Usenet handle instead of
// nil-dereferencing OverlayDeleteEntry/OverlayDeleteFile: given a verdict
// that would otherwise reap (here, EntryGone because there's no backing
// storage.Entry at all), a Manager with no usenet client returns a non-nil
// error and Removed=false, and does not panic.
func TestReapOverlayExecuteWithNilUsenetErrorsWithoutPanic(t *testing.T) {
	strg, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("storage.NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = strg.Close() })
	m := &Manager{storage: strg}

	nzbID := "orphaned-nzb-id-no-usenet"
	file := "whatever.mkv"

	result, err := m.ReapOverlay(nzbID, file, true)
	if err == nil {
		t.Fatalf("ReapOverlay: want non-nil error for nil Usenet, got nil")
	}
	if result.Removed {
		t.Fatalf("Removed=true, want false - nothing should be deleted without a Usenet client")
	}
}

// TestReapOverlayDryRunWithNilUsenetNeedsNoUsenet proves the dry-run path
// (execute=false) never touches Usenet at all, so a Manager with no usenet
// client still reports cleanly instead of erroring or panicking.
func TestReapOverlayDryRunWithNilUsenetNeedsNoUsenet(t *testing.T) {
	strg, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("storage.NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = strg.Close() })
	m := &Manager{storage: strg}

	nzbID := "orphaned-nzb-id-no-usenet-dry-run"
	file := "whatever.mkv"

	result, err := m.ReapOverlay(nzbID, file, false)
	if err != nil {
		t.Fatalf("ReapOverlay: %v", err)
	}
	if result.Mode != ReapModeDryRun {
		t.Fatalf("Mode=%q, want %q", result.Mode, ReapModeDryRun)
	}
	if result.Removed {
		t.Fatalf("Removed=true, want false")
	}
}
