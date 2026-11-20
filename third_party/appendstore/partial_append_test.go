//go:build linux || darwin

package appendstore

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// A write cut short by a full disk (here a file size limit) must not leave
// part of the record in the file: after one more, shorter append the store
// has to reopen with every record that was written.
func TestFailedAppendLeavesNoPartialRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("first", []byte("first value"), nil); err != nil {
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
		t.Skipf("cannot set a file size limit: %v", err)
	}
	putErr := s.Put("cut", make([]byte, 400), nil)
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
		t.Fatal(err)
	}
	if putErr == nil {
		t.Fatal("the append past the file size limit succeeded")
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != info.Size() {
		t.Fatalf("file is %d bytes after the failed append, want %d", after.Size(), info.Size())
	}

	if err := s.Put("short", []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("reopen after a failed append: %v", err)
	}
	defer reopened.Close()
	for key, want := range map[string]string{"first": "first value", "short": "x"} {
		got, err := reopened.Get(key)
		if err != nil || string(got) != want {
			t.Errorf("%s = %q, %v; want %q", key, got, err, want)
		}
	}
	if _, err := reopened.Get("cut"); err == nil {
		t.Error("the failed record is readable after the reopen")
	}
}
