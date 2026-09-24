package overlay

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// A fetcher that passed RecordDead's rejected check and was waiting on the
// entry lock while the teardown ran must not re-create the manifest.
func TestRecordDeadWaitingThroughTeardownDoesNotRecreate(t *testing.T) {
	s, err := NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	const id = "nzb-rejected"
	if err := s.RecordDead(id, "movie.mkv", 5, "<a@x>", 1024); err != nil {
		t.Fatal(err)
	}

	mu := s.lockFor(id)
	mu.Lock() // another holder of the entry lock
	done := make(chan error, 1)
	go func() { done <- s.RecordDead(id, "movie.mkv", 6, "<b@x>", 1024) }()
	time.Sleep(50 * time.Millisecond) // the fetcher is past the first check, parked on mu

	s.MarkRejected(id)
	if err := os.RemoveAll(s.entryDir(id)); err != nil {
		t.Fatal(err)
	}
	mu.Unlock()

	if err := <-done; err != nil {
		t.Fatalf("RecordDead: %v", err)
	}
	if _, err := os.Stat(s.manifestPath(id)); err == nil {
		t.Error("manifest re-created for a rejected, deleted nzbID")
	}
}

// A PAR2 pass finishing after its entry was deleted must not re-create the
// overlay directory; it is told the entry is gone.
func TestWritePatchRefusesRejectedEntry(t *testing.T) {
	s, err := NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	const id = "nzb-deleted"
	if err := s.RecordDead(id, "movie.mkv", 5, "<a@x>", 4); err != nil {
		t.Fatal(err)
	}
	s.MarkRejected(id)
	if err := s.DeleteEntry(id); err != nil {
		t.Fatal(err)
	}
	if err := s.WritePatch(id, "movie.mkv", 5, []byte{1, 2, 3, 4}); !errors.Is(err, ErrEntryRejected) {
		t.Errorf("WritePatch err = %v, want ErrEntryRejected", err)
	}
	if _, err := os.Stat(s.entryDir(id)); err == nil {
		t.Error("WritePatch re-created the overlay dir of a deleted nzbID")
	}

	// Re-added (reviveNZB clears the rejection): writes work again, under the
	// same mutex DeleteEntry left in place.
	s.ClearRejected(id)
	if s.lockFor(id) == nil {
		t.Fatal("no mutex")
	}
	if err := s.RecordDead(id, "movie.mkv", 5, "<a@x>", 4); err != nil {
		t.Fatal(err)
	}
	if err := s.WritePatch(id, "movie.mkv", 5, []byte{1, 2, 3, 4}); err != nil {
		t.Errorf("WritePatch after revive: %v", err)
	}
}

// DeleteEntry keeps the per-nzbID mutex, so a holder of the old one and a
// later lockFor serialize on the same lock.
func TestDeleteEntryKeepsMutex(t *testing.T) {
	s, err := NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	before := s.lockFor("nzb-x")
	if err := s.DeleteEntry("nzb-x"); err != nil {
		t.Fatal(err)
	}
	if after := s.lockFor("nzb-x"); after != before {
		t.Error("DeleteEntry dropped the mutex; a second one was minted for the same nzbID")
	}
}
