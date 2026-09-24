package usenet

import (
	"errors"
	"os"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// A read-modify-write goes through Update under the storage lock: it applies
// the change, and a record deleted in the meantime stays deleted instead of
// having its .meta re-created by the write.
func TestNZBStorageUpdate(t *testing.T) {
	s := &NZBStorage{metaDir: t.TempDir(), logger: zerolog.Nop()}
	if err := s.AddNZB(&storage.NZB{ID: "n1", Name: "N1"}); err != nil {
		t.Fatal(err)
	}
	refs := []storage.Par2MatchRef{{PostedName: "a.r00"}}
	if err := s.Update("n1", func(n *storage.NZB) (bool, error) { n.Par2Match = refs; return true, nil }); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetNZB("n1")
	if err != nil || len(got.Par2Match) != 1 {
		t.Fatalf("update not applied: %+v %v", got, err)
	}

	if err := s.DeleteNZB("n1"); err != nil {
		t.Fatal(err)
	}
	err = s.Update("n1", func(n *storage.NZB) (bool, error) { n.Par2Match = nil; return true, nil })
	if !errors.Is(err, ErrNZBGone) {
		t.Fatalf("update of a deleted record: err = %v, want ErrNZBGone", err)
	}
	if _, serr := os.Stat(s.metaFilePath("n1")); !os.IsNotExist(serr) {
		t.Fatal("update re-created a deleted record's meta file")
	}
}
