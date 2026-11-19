package overlay

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// A crash between CreateTemp and Rename leaves patch-*.tmp / manifest-*.tmp
// behind in an entry directory. Opening the store removes those older than an
// hour and leaves everything else: newer temp files (a write may be in
// flight), real patches, the manifest, and files that only look similar.
func TestNewStoreRemovesStaleTempFilesOnly(t *testing.T) {
	root := t.TempDir()
	entry := filepath.Join(root, "nzb1")
	other := filepath.Join(root, "nzb2")
	for _, d := range []string{entry, other} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-2 * time.Hour)
	write := func(path string, mtime time.Time) {
		t.Helper()
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}

	stale := []string{
		filepath.Join(entry, "patch-123.tmp"),
		filepath.Join(entry, "manifest-456.tmp"),
		filepath.Join(other, "patch-789.tmp"),
	}
	kept := []string{
		filepath.Join(entry, "patch_abcdef_3.bin"), // a real patch, old
		filepath.Join(entry, manifestFileName),     // the manifest, old
		filepath.Join(entry, "notes-1.tmp"),        // other pattern, old
		filepath.Join(entry, "patch-1.tmp.bak"),    // not a .tmp, old
		filepath.Join(root, "patch-top.tmp"),       // not inside an entry dir
	}
	fresh := []string{
		filepath.Join(entry, "patch-new.tmp"),
		filepath.Join(other, "manifest-new.tmp"),
	}
	for _, p := range append(append([]string{}, stale...), kept...) {
		write(p, old)
	}
	for _, p := range fresh {
		write(p, time.Now())
	}
	// A directory named like a temp file is not a temp file.
	dirLike := filepath.Join(entry, "patch-dir.tmp")
	if err := os.Mkdir(dirLike, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(dirLike, old, old); err != nil {
		t.Fatal(err)
	}

	if _, err := NewStore(root, zerolog.Nop()); err != nil {
		t.Fatal(err)
	}

	for _, p := range stale {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("stale temp file %s still present (err=%v)", p, err)
		}
	}
	for _, p := range append(append(append([]string{}, kept...), fresh...), dirLike) {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("%s was removed or unreadable: %v", p, err)
		}
	}
}
