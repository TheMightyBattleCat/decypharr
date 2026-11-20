package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/appendstore"
	"github.com/sirrobot01/decypharr/internal/config"
)

// newDamageTestStorage writes one entry and one repair run, closes the
// storage and returns its folder.
func newDamageTestStorage(t *testing.T) string {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	dir := t.TempDir()
	s, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	e := &Entry{InfoHash: "abc123", Name: "Some.Release", Protocol: "nzb", AddedOn: time.Now(),
		Files: map[string]*File{"a.mkv": {Name: "a.mkv", Size: 100}}}
	if err := s.AddOrUpdate(e); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveRepairRun(&RepairRun{ID: "run-1", StartedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

// appendZeros extends a store file with zero bytes: the size grew but the
// data never reached the disk.
func appendZeros(t *testing.T, path string, n int) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(make([]byte, n)); err != nil {
		t.Fatal(err)
	}
}

// movedAsideSuffix is the suffix start-up gives a damaged store it moves
// aside: <name>.db.corrupt-<time>.
const movedAsideSuffix = ".corrupt-"

func corruptCopies(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.db"+movedAsideSuffix+"*"))
	if err != nil {
		t.Fatal(err)
	}
	return matches
}

// A damaged store that holds only history or rebuildable state is moved
// aside and started empty: the program starts, the damaged bytes are kept.
func TestDamagedRebuildableStoreIsMovedAsideAndStartedEmpty(t *testing.T) {
	for _, name := range []string{"repair_runs", "par2_repair_attempts", "regrab_guard"} {
		t.Run(name, func(t *testing.T) {
			dir := newDamageTestStorage(t)
			path := filepath.Join(dir, name+".db")
			appendZeros(t, path, 64)
			damaged, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}

			s, err := NewStorage(dir)
			if err != nil {
				t.Fatalf("NewStorage with a damaged %s.db: %v", name, err)
			}
			defer s.Close()

			copies := corruptCopies(t, dir)
			if len(copies) != 1 || !strings.HasPrefix(filepath.Base(copies[0]), name+".db"+movedAsideSuffix) {
				t.Fatalf("moved-aside copies %v, want one %s.db%s<time>", copies, name, movedAsideSuffix)
			}
			kept, err := os.ReadFile(copies[0])
			if err != nil || !bytes.Equal(kept, damaged) {
				t.Fatalf("moved-aside copy differs from the damaged file (err %v)", err)
			}

			if name == "repair_runs" {
				runs, err := s.ListRepairRuns()
				if err != nil || len(runs) != 0 {
					t.Fatalf("repair runs after the move-aside: %d, %v; want an empty history", len(runs), err)
				}
			}
			if got, err := s.Get("abc123"); err != nil || got == nil {
				t.Fatalf("entry lost after a damaged %s.db: %v", name, err)
			}
			if err := s.SaveRepairRun(&RepairRun{ID: "run-2", StartedAt: time.Now()}); err != nil {
				t.Fatalf("write after the move-aside: %v", err)
			}
		})
	}
}

// A damaged store holding the library stops the start with an error that
// names the file and the way back (a backup), and leaves the file in place.
func TestDamagedLibraryStoreStopsStartAndNamesTheFile(t *testing.T) {
	cases := []struct {
		name   string
		damage func(t *testing.T, path string)
	}{
		{"entries", func(t *testing.T, path string) {
			// The last record has its full length but its checksum bytes
			// never reached the disk.
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			copy(data[len(data)-4:], []byte{0, 0, 0, 0})
			if err := os.WriteFile(path, data, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"queue", func(t *testing.T, path string) { appendZeros(t, path, 64) }},
		{"pending_deletes", func(t *testing.T, path string) { appendZeros(t, path, 64) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := newDamageTestStorage(t)
			path := filepath.Join(dir, tc.name+".db")
			tc.damage(t, path)
			damaged, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}

			s, err := NewStorage(dir)
			if err == nil {
				_ = s.Close()
				t.Fatalf("NewStorage opened a damaged %s.db", tc.name)
			}
			if !errors.Is(err, appendstore.ErrCorruptedData) {
				t.Errorf("error %v does not wrap ErrCorruptedData", err)
			}
			if !strings.Contains(err.Error(), path) || !strings.Contains(err.Error(), "restore the db folder from a backup") {
				t.Errorf("error %q does not name %s and say to restore the db folder from a backup", err, path)
			}
			if after, rerr := os.ReadFile(path); rerr != nil || !bytes.Equal(after, damaged) {
				t.Errorf("%s.db was changed or moved (err %v)", tc.name, rerr)
			}
			if copies := corruptCopies(t, dir); len(copies) != 0 {
				t.Errorf("library store moved aside: %v", copies)
			}
		})
	}
}

// Only damage moves a store aside. A store another process holds open
// (the offline backup command racing the server) is left alone.
func TestLockedStoreIsNotMovedAside(t *testing.T) {
	dir := newDamageTestStorage(t)
	open, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer open.Close()

	s, err := NewStorage(dir)
	if err == nil {
		_ = s.Close()
		t.Fatal("second NewStorage on an open folder succeeded")
	}
	if !errors.Is(err, appendstore.ErrStoreLocked) {
		t.Errorf("error %v, want ErrStoreLocked", err)
	}
	if copies := corruptCopies(t, dir); len(copies) != 0 {
		t.Fatalf("locked store moved aside: %v", copies)
	}
	runs, err := open.ListRepairRuns()
	if err != nil || len(runs) != 1 {
		t.Fatalf("repair runs in the open storage: %d, %v; want 1", len(runs), err)
	}
}
