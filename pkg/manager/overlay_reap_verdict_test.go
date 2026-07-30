package manager

import (
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// newTestManagerForReapVerdict builds a minimal, storage-backed Manager -
// OverlayReapVerdict only ever touches m.storage (via GetEntry/GetEntryItem),
// so it doesn't need the heavy manager.New() init path (debrid clients, arr
// connections, a real usenet/NNTP client). Mirrors newTestPar2Repair's
// rationale (par2_repair_registry_test.go).
func newTestManagerForReapVerdict(t *testing.T) (*Manager, *storage.Storage) {
	t.Helper()
	config.SetConfigPath(t.TempDir())

	strg, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("storage.NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = strg.Close() })

	return &Manager{storage: strg}, strg
}

// TestOverlayReapVerdict is the load-bearing proof for the reap guard: a
// LIVE entry (fixture i) must come back WouldReap=false, since that's the
// only thing standing between a future /reap route and deleting an overlay
// record that's still the current owner of its slot. The other three cases
// (superseded, entry-gone, missing-file) all resolve to WouldReap=true.
func TestOverlayReapVerdict(t *testing.T) {
	m, strg := newTestManagerForReapVerdict(t)
	now := time.Now()

	// Fixture (i) LIVE: name resolves via GetEntryItem -> GetFile -> InfoHash
	// back to the SAME value used as the overlay nzbID. Must refuse to reap.
	liveHash := "live-hash-1"
	liveName := "My.Show.S01E01"
	liveFile := "My.Show.S01E01.mkv"
	if err := strg.AddOrUpdate(&storage.Entry{
		InfoHash: liveHash,
		Name:     liveName,
		Files: map[string]*storage.File{
			liveFile: {Name: liveFile, InfoHash: liveHash, Size: 100, AddedOn: now},
		},
	}); err != nil {
		t.Fatalf("AddOrUpdate(live): %v", err)
	}

	// Fixture (ii) SUPERSEDED: an overlay nzbID (oldHash) whose Name now
	// resolves to a DIFFERENT InfoHash (newHash) - mimicking fixer.go:302
	// moving the per-file InfoHash to a replacement on re-grab. oldHash's
	// own Entry record is left in place (as it would be pre-reap), but the
	// name index (EntryItem) has moved on to newHash because it was written
	// later.
	oldHash := "old-hash-X"
	newHash := "new-hash-Y"
	supersededName := "My.Show.S01E02"
	supersededFile := "My.Show.S01E02.mkv"
	if err := strg.AddOrUpdate(&storage.Entry{
		InfoHash: oldHash,
		Name:     supersededName,
		Files: map[string]*storage.File{
			supersededFile: {Name: supersededFile, InfoHash: oldHash, Size: 100, AddedOn: now},
		},
	}); err != nil {
		t.Fatalf("AddOrUpdate(superseded old): %v", err)
	}
	if err := strg.AddOrUpdate(&storage.Entry{
		InfoHash: newHash,
		Name:     supersededName,
		Files: map[string]*storage.File{
			supersededFile: {Name: supersededFile, InfoHash: newHash, Size: 100, AddedOn: now.Add(time.Second)},
		},
	}); err != nil {
		t.Fatalf("AddOrUpdate(superseded new): %v", err)
	}

	t.Run("live entry refuses to reap", func(t *testing.T) {
		v := m.OverlayReapVerdict(liveHash, liveFile)
		if v.EntryGone {
			t.Fatalf("EntryGone=true, want false")
		}
		if v.Name != liveName {
			t.Fatalf("Name=%q, want %q", v.Name, liveName)
		}
		if v.ResolvedInfoHash != liveHash {
			t.Fatalf("ResolvedInfoHash=%q, want %q", v.ResolvedInfoHash, liveHash)
		}
		if v.WouldReap {
			t.Fatalf("WouldReap=true, want false - this is the load-bearing assertion: a live, still-owning overlay record must never be reaped")
		}
	})

	t.Run("superseded entry proceeds to reap", func(t *testing.T) {
		v := m.OverlayReapVerdict(oldHash, supersededFile)
		if v.EntryGone {
			t.Fatalf("EntryGone=true, want false - the stale Entry record for oldHash still exists")
		}
		if v.Name != supersededName {
			t.Fatalf("Name=%q, want %q", v.Name, supersededName)
		}
		if v.ResolvedInfoHash != newHash {
			t.Fatalf("ResolvedInfoHash=%q, want %q (the replacement)", v.ResolvedInfoHash, newHash)
		}
		if !v.WouldReap {
			t.Fatalf("WouldReap=false, want true - the slot has moved to a different InfoHash")
		}
	})

	t.Run("entry gone proceeds to reap", func(t *testing.T) {
		v := m.OverlayReapVerdict("nonexistent-hash", "whatever.mkv")
		if !v.EntryGone {
			t.Fatalf("EntryGone=false, want true")
		}
		if !v.WouldReap {
			t.Fatalf("WouldReap=false, want true")
		}
	})

	t.Run("missing file proceeds to reap", func(t *testing.T) {
		v := m.OverlayReapVerdict(liveHash, "nonexistent-file.mkv")
		if v.EntryGone {
			t.Fatalf("EntryGone=true, want false - the entry itself still exists")
		}
		if !v.WouldReap {
			t.Fatalf("WouldReap=false, want true - the file is no longer part of the entry item")
		}
	})
}
