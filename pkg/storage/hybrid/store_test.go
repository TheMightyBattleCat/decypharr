package hybrid

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// TestMain points config at a throwaway directory: New's logger reads the
// config, which would otherwise be created in the working directory.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "decypharr-hybrid-test-*")
	if err == nil {
		config.SetConfigPath(dir)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(Config{DataPath: filepath.Join(t.TempDir(), "store.log"), SyncInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// raceWriterIntoGet runs write on another goroutine at the point where Get has
// read a value but not yet cached it, gives it time to finish if nothing
// blocks it, and waits for it after Get returns.
func raceWriterIntoGet(t *testing.T, s *Store, key string, write func()) {
	t.Helper()
	var wg sync.WaitGroup
	testHookBeforeCachePut = func() {
		testHookBeforeCachePut = nil // first Get only
		wg.Add(1)
		go func() {
			defer wg.Done()
			write()
		}()
		time.Sleep(100 * time.Millisecond)
	}
	t.Cleanup(func() { testHookBeforeCachePut = nil })
	if _, err := s.Get(key); err != nil {
		t.Fatalf("first Get: %v", err)
	}
	wg.Wait()
}

func TestGetDoesNotCacheAValueReplacedDuringTheRead(t *testing.T) {
	s := newTestStore(t)
	if err := s.Put("k", []byte("old"), nil); err != nil {
		t.Fatal(err)
	}
	raceWriterIntoGet(t, s, "k", func() {
		if err := s.Put("k", []byte("new"), nil); err != nil {
			t.Error(err)
		}
	})
	got, err := s.Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Errorf("Get after a concurrent Put = %q, want %q (stale cache entry)", got, "new")
	}
}

func TestGetDoesNotResurrectAKeyDeletedDuringTheRead(t *testing.T) {
	s := newTestStore(t)
	if err := s.Put("k", []byte("v"), nil); err != nil {
		t.Fatal(err)
	}
	raceWriterIntoGet(t, s, "k", func() {
		if err := s.Delete("k"); err != nil {
			t.Error(err)
		}
	})
	if got, err := s.Get("k"); !errors.Is(err, errKeyNotFound) {
		t.Errorf("Get after a concurrent Delete = %q, %v; want key not found", got, err)
	}
}
