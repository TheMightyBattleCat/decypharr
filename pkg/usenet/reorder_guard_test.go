package usenet

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// A volume reorder decides from a record it read before its network phase.
// The guard is asked again under the storage lock, before the write: when it
// refuses (an overlay record or a PAR2 pass appeared in between), the record
// and the backup folder are left exactly as they were.
func TestReorderFileSegmentsGuardRefusesUnderTheLock(t *testing.T) {
	root := t.TempDir()
	metaDir := filepath.Join(root, "meta")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}
	store := &NZBStorage{metaDir: metaDir, logger: zerolog.Nop()}
	u := &Usenet{logger: zerolog.Nop(), nzbStorage: store}

	f := storage.NZBFile{Name: "a.rar", Size: 3000, FileType: storage.NZBFileTypeRar}
	for i := 0; i < 3; i++ {
		start := int64(i) * 1000
		f.Segments = append(f.Segments, storage.NZBSegment{Number: i + 1, MessageID: fmt.Sprintf("m%d@x", i), Bytes: 1000, StartOffset: start, EndOffset: start + 999})
	}
	if err := store.AddNZB(&storage.NZB{ID: "rel", Name: "Rel", Files: []storage.NZBFile{f}}); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}
	// The articles of volumes 2 and 3 swapped, offsets kept.
	segs := append([]storage.NZBSegment(nil), f.Segments...)
	segs[1].MessageID, segs[2].MessageID = segs[2].MessageID, segs[1].MessageID
	key := func(*storage.NZBFile) string { return "k" }
	ids := func() []string {
		nzb, err := store.GetNZB("rel")
		if err != nil {
			t.Fatalf("GetNZB: %v", err)
		}
		var out []string
		for _, s := range nzb.GetFileByName("a.rar").Segments {
			out = append(out, s.MessageID)
		}
		return out
	}
	backupDir := filepath.Join(root, "volume-order-backup")

	errRecord := errors.New("the release has repair overlay records")
	asked := 0
	_, err := u.ReorderFileSegments("rel", "a.rar", "k", key, func() error { asked++; return errRecord }, segs)
	if err != errRecord {
		t.Fatalf("refused reorder returned %v, want the guard's error as it is (a recorded refusal keeps its own reason)", err)
	}
	if asked != 1 {
		t.Fatalf("guard asked %d times, want once", asked)
	}
	if got := fmt.Sprint(ids()); got != "[m0@x m1@x m2@x]" {
		t.Fatalf("record changed by a refused reorder: %s", got)
	}
	if _, err := os.Stat(backupDir); !os.IsNotExist(err) {
		t.Fatalf("a refused reorder left a backup folder (stat err %v)", err)
	}

	// Control: a guard that passes lets the same reorder through.
	backup, err := u.ReorderFileSegments("rel", "a.rar", "k", key, func() error { return nil }, segs)
	if err != nil {
		t.Fatalf("reorder with a passing guard: %v", err)
	}
	if got := fmt.Sprint(ids()); got != "[m0@x m2@x m1@x]" {
		t.Fatalf("record after the reorder: %s", got)
	}
	if _, err := os.Stat(backup); err != nil {
		t.Fatalf("no backup kept: %v", err)
	}
}
