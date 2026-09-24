package overlay

import (
	"testing"

	"github.com/rs/zerolog"
)

// EntryExists reports the directory itself: the "removed overlay dir" log
// and ReapOverlay's Removed used a manifest load, which is never nil.
func TestEntryExists(t *testing.T) {
	s, err := NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if s.EntryExists("nzb-x") {
		t.Fatal("no directory yet")
	}
	if err := s.RecordDead("nzb-x", "f.mkv", 1, "<a@x>", 10); err != nil {
		t.Fatal(err)
	}
	if !s.EntryExists("nzb-x") {
		t.Fatal("directory not seen after a record")
	}
	if err := s.DeleteEntry("nzb-x"); err != nil {
		t.Fatal(err)
	}
	if s.EntryExists("nzb-x") {
		t.Fatal("directory still seen after delete")
	}
}
