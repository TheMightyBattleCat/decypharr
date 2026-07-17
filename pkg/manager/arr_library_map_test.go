package manager

import (
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

func newTestArrLibraryMap(t *testing.T) *arrLibraryMap {
	t.Helper()
	return &arrLibraryMap{
		path:   filepath.Join(t.TempDir(), "arr_library_paths.json"),
		byPath: make(map[string]arrLibraryMapEntry),
		logger: zerolog.Nop(),
	}
}

func TestArrLibraryMap_NilSafe(t *testing.T) {
	var a *arrLibraryMap
	a.record("entry", "file.mkv", "/some/path")
	a.removeEntry("entry")
	if _, ok := a.lookup("/some/path"); ok {
		t.Error("lookup on nil map should never find anything")
	}
	if got := a.lookupBasename("file.mkv"); got != nil {
		t.Errorf("lookupBasename on nil map = %v, want nil", got)
	}
}

func TestArrLibraryMap_RecordLookupRemove(t *testing.T) {
	a := newTestArrLibraryMap(t)

	symlinkPath := "/downloads/nzb/Some.Release.Name/Some.Release.Name.mkv"
	mountPath := "/mnt/rclone/__all__/Some.Release.Name/Some.Release.Name.mkv"
	a.record("Some.Release.Name", "Some.Release.Name.mkv", symlinkPath, mountPath)

	for _, p := range []string{symlinkPath, mountPath} {
		e, ok := a.lookup(p)
		if !ok {
			t.Fatalf("lookup(%q) not found", p)
		}
		if e.EntryName != "Some.Release.Name" || e.FileName != "Some.Release.Name.mkv" {
			t.Errorf("lookup(%q) = %+v, want entry/file names", p, e)
		}
	}

	matches := a.lookupBasename("Some.Release.Name.mkv")
	if len(matches) != 1 || matches[0].EntryName != "Some.Release.Name" {
		t.Errorf("lookupBasename = %+v, want a single match", matches)
	}

	a.removeEntry("Some.Release.Name")
	if _, ok := a.lookup(symlinkPath); ok {
		t.Error("mapping should be gone after removeEntry")
	}
	if _, ok := a.lookup(mountPath); ok {
		t.Error("mapping should be gone after removeEntry")
	}
}

// TestArrLibraryMap_SurvivesRestart writes through one arrLibraryMap instance
// and reloads from disk with a second one pointed at the same file, the same
// way decypharr restarting would - the mapping must not be lost.
func TestArrLibraryMap_SurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "arr_library_paths.json")

	first := &arrLibraryMap{path: path, byPath: make(map[string]arrLibraryMapEntry), logger: zerolog.Nop()}
	symlinkPath := "/downloads/nzb/Succession.S02E07.iTALiAN.1080p.WEB.H264-CHEOPE/Succession.S02E07.iTALiAN.1080p.WEB.H264-CHEOPE.mkv"
	first.record("Succession.S02E07.iTALiAN.1080p.WEB.H264-CHEOPE", "Succession.S02E07.iTALiAN.1080p.WEB.H264-CHEOPE.mkv", symlinkPath)

	restarted := &arrLibraryMap{path: path, byPath: make(map[string]arrLibraryMapEntry), logger: zerolog.Nop()}
	restarted.load()

	e, ok := restarted.lookup(symlinkPath)
	if !ok {
		t.Fatal("mapping did not survive restart")
	}
	if e.EntryName != "Succession.S02E07.iTALiAN.1080p.WEB.H264-CHEOPE" || e.FileName != "Succession.S02E07.iTALiAN.1080p.WEB.H264-CHEOPE.mkv" {
		t.Errorf("reloaded entry = %+v", e)
	}
}
