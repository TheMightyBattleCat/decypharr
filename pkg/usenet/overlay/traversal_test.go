package overlay

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

// The overlay root is <data>/usenet/overlay. An nzbID that is not a single
// path element must be refused: ".." names <data>/usenet (the NZB records)
// and "../.." the data directory. POST /api/overlay/reap passes the caller's
// nzb_id through to DeleteEntry.
func TestStoreRefusesNZBIDOutsideRoot(t *testing.T) {
	data := t.TempDir()
	usenet := filepath.Join(data, "usenet")
	meta := filepath.Join(usenet, "meta")
	if err := os.MkdirAll(meta, 0o755); err != nil {
		t.Fatal(err)
	}
	record := filepath.Join(meta, "abc.meta")
	if err := os.WriteFile(record, []byte("an NZB record"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := filepath.Join(data, "config.json")
	if err := os.WriteFile(cfg, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := NewStore(filepath.Join(usenet, "overlay"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}

	for _, id := range []string{"", ".", "..", "../..", "../meta", "a/b", `a\b`, `..\..`} {
		if err := store.DeleteEntry(id); !errors.Is(err, ErrInvalidNZBID) {
			t.Errorf("DeleteEntry(%q) err = %v, want ErrInvalidNZBID", id, err)
		}
		if store.EntryExists(id) {
			t.Errorf("EntryExists(%q) = true, want false", id)
		}
		if err := store.RecordDead(id, "f.mkv", 0, "<m@x>", 10); err == nil {
			t.Errorf("RecordDead(%q) succeeded, want an error", id)
		}
		if err := store.WritePatch(id, "f.mkv", 0, []byte{1}); err == nil {
			t.Errorf("WritePatch(%q) succeeded, want an error", id)
		}
		if _, err := store.DeleteFile(id, "f.mkv"); err == nil {
			t.Errorf("DeleteFile(%q) succeeded, want an error", id)
		}
		if _, _, err := store.DiskUsage(id); err == nil {
			t.Errorf("DiskUsage(%q) succeeded, want an error", id)
		}
	}

	if _, err := os.Stat(record); err != nil {
		t.Errorf("the NZB record directory was removed: %v", err)
	}
	if _, err := os.Stat(cfg); err != nil {
		t.Errorf("the data directory's config.json was removed: %v", err)
	}

	// An ordinary ID still works end to end.
	const id = "nzb-ok"
	if err := store.RecordDead(id, "f.mkv", 0, "<m@x>", 10); err != nil {
		t.Fatal(err)
	}
	if !store.EntryExists(id) {
		t.Fatal("EntryExists = false after RecordDead")
	}
	if err := store.DeleteEntry(id); err != nil {
		t.Fatal(err)
	}
	if store.EntryExists(id) {
		t.Fatal("entry still on disk after DeleteEntry")
	}
	if _, err := os.Stat(filepath.Join(usenet, "overlay")); err != nil {
		t.Errorf("the overlay root itself was removed: %v", err)
	}
}
