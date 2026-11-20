//go:build linux || darwin

package storage

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/sirrobot01/appendstore"
)

// A full disk cuts one append part way; the next, shorter append succeeds;
// at the next start the store must open with what was written. The append
// runs in a child process under a file size limit (RLIMIT_FSIZE) standing in
// for the full disk.
func TestFullDiskAppendLeavesAStoreThatOpens(t *testing.T) {
	if os.Getenv("DAMAGED_STORE_CHILD") != "" {
		t.Skip("parent only")
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestFullDiskAppendChild$", "-test.v")
	cmd.Env = append(os.Environ(), "DAMAGED_STORE_CHILD=1", "DAMAGED_STORE_DIR="+dir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("child failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "append on the full disk failed") {
		t.Fatalf("the child's append did not fail on the full disk:\n%s", out)
	}
	s, err := appendstore.Open(filepath.Join(dir, "repair_runs.db"), appendstore.Options{})
	if err != nil {
		t.Fatalf("store does not open after a failed append on a full disk and one later write: %v", err)
	}
	defer s.Close()
	for key, want := range map[string]string{"run-1": "first run", "run-3": "x"} {
		if got, err := s.Get(key); err != nil || string(got) != want {
			t.Errorf("%s = %q, %v; want %q", key, got, err, want)
		}
	}
	if _, err := s.Get("run-2"); err == nil {
		t.Error("the cut record is readable")
	}
}

func TestFullDiskAppendChild(t *testing.T) {
	if os.Getenv("DAMAGED_STORE_CHILD") == "" {
		t.Skip("child only")
	}
	path := filepath.Join(os.Getenv("DAMAGED_STORE_DIR"), "repair_runs.db")
	s, err := appendstore.Open(path, appendstore.Options{SyncInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("run-1", []byte("first run"), nil); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
		t.Fatal(err)
	}
	cut := limit
	cut.Cur = uint64(info.Size()) + 100
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &cut); err != nil {
		t.Fatal(err)
	}
	putErr := s.Put("run-2", make([]byte, 400), nil)
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
		t.Fatal(err)
	}
	if putErr == nil {
		t.Fatal("append past the file size limit succeeded")
	}
	t.Logf("append on the full disk failed: %v", putErr)
	if err := s.Put("run-3", []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
