package overlay

import (
	"bytes"
	"sort"
	"sync"
	"testing"
	"time"
)

type clearedCall struct {
	nzbID   string
	removed map[string][]int // file -> segment indices
}

// recordCleared installs a damage-cleared notifier that records each call.
// It reads the entry's manifest from inside the callback, which deadlocks
// (and fails the test on its timeout) if the store still holds the lock.
func recordCleared(t *testing.T, s *Store) func() []clearedCall {
	t.Helper()
	var mu sync.Mutex
	var calls []clearedCall
	s.SetDamageClearedNotifier(func(nzbID string, removed map[string][]DeadSegment) {
		done := make(chan struct{})
		go func() {
			_, _ = s.GetManifest(nzbID)
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Errorf("notifier for %s called with the entry's lock held", nzbID)
		}
		c := clearedCall{nzbID: nzbID, removed: map[string][]int{}}
		for f, segs := range removed {
			for _, d := range segs {
				c.removed[f] = append(c.removed[f], d.Index)
			}
			sort.Ints(c.removed[f])
		}
		mu.Lock()
		calls = append(calls, c)
		mu.Unlock()
	})
	return func() []clearedCall {
		mu.Lock()
		defer mu.Unlock()
		return append([]clearedCall(nil), calls...)
	}
}

// padSegs pads segments idx of file (a large file size keeps them under
// the default caps).
func padSegs(t *testing.T, s *Store, nzbID, file string, idx ...int) {
	t.Helper()
	for _, i := range idx {
		if d, _ := s.Decide(nzbID, file, i, "<m@test>", 1024, 1<<30, 0); d != DecisionPad {
			t.Fatalf("setup: Decide(%s/%d) = %v, want pad", file, i, d)
		}
	}
}

func onlyCall(t *testing.T, calls []clearedCall, nzbID string, want map[string][]int) {
	t.Helper()
	if len(calls) != 1 {
		t.Fatalf("notifier calls = %+v, want one", calls)
	}
	c := calls[0]
	if c.nzbID != nzbID || len(c.removed) != len(want) {
		t.Fatalf("notifier call = %+v, want %s %v", c, nzbID, want)
	}
	for f, idx := range want {
		got := c.removed[f]
		if len(got) != len(idx) {
			t.Fatalf("removed[%s] = %v, want %v", f, got, idx)
		}
		for i := range idx {
			if got[i] != idx[i] {
				t.Fatalf("removed[%s] = %v, want %v", f, got, idx)
			}
		}
	}
}

// Each path that removes dead/padded records reports them (and only them:
// a patched record is real bytes, not zero-fill) after releasing the lock.
func TestDamageClearedNotifierReportsRemovedRecords(t *testing.T) {
	patch := bytes.Repeat([]byte{7}, 1024)

	t.Run("ClearFileDamage", func(t *testing.T) {
		s := newTestStore(t)
		calls := recordCleared(t, s)
		padSegs(t, s, "n1", "a.mkv", 1, 2, 3)
		if err := s.WritePatch("n1", "a.mkv", 2, patch); err != nil {
			t.Fatalf("WritePatch: %v", err)
		}
		if _, err := s.ClearFileDamage("n1", "a.mkv"); err != nil {
			t.Fatalf("ClearFileDamage: %v", err)
		}
		onlyCall(t, calls(), "n1", map[string][]int{"a.mkv": {1, 3}})
	})

	t.Run("DeleteFile", func(t *testing.T) {
		s := newTestStore(t)
		calls := recordCleared(t, s)
		padSegs(t, s, "n2", "a.mkv", 4)
		padSegs(t, s, "n2", "b.mkv", 5)
		if removed, err := s.DeleteFile("n2", "a.mkv"); err != nil || !removed {
			t.Fatalf("DeleteFile = %v, %v", removed, err)
		}
		onlyCall(t, calls(), "n2", map[string][]int{"a.mkv": {4}})
	})

	t.Run("DeleteEntry", func(t *testing.T) {
		s := newTestStore(t)
		calls := recordCleared(t, s)
		padSegs(t, s, "n3", "a.mkv", 0)
		padSegs(t, s, "n3", "b.mkv", 6, 7)
		if err := s.DeleteEntry("n3"); err != nil {
			t.Fatalf("DeleteEntry: %v", err)
		}
		onlyCall(t, calls(), "n3", map[string][]int{"a.mkv": {0}, "b.mkv": {6, 7}})
	})

	t.Run("patched only: no call", func(t *testing.T) {
		s := newTestStore(t)
		calls := recordCleared(t, s)
		padSegs(t, s, "n4", "a.mkv", 1)
		if err := s.WritePatch("n4", "a.mkv", 1, patch); err != nil {
			t.Fatalf("WritePatch: %v", err)
		}
		if _, err := s.ClearFileDamage("n4", "a.mkv"); err != nil {
			t.Fatalf("ClearFileDamage: %v", err)
		}
		if removed, err := s.DeleteFile("n4", "a.mkv"); err != nil || !removed {
			t.Fatalf("DeleteFile = %v, %v", removed, err)
		}
		if got := calls(); len(got) != 0 {
			t.Fatalf("notifier calls = %+v, want none (no zero-fill was removed)", got)
		}
	})

	t.Run("nothing recorded: no call", func(t *testing.T) {
		s := newTestStore(t)
		calls := recordCleared(t, s)
		_, _ = s.ClearFileDamage("n5", "a.mkv")
		_, _ = s.DeleteFile("n5", "a.mkv")
		_ = s.DeleteEntry("n5")
		if got := calls(); len(got) != 0 {
			t.Fatalf("notifier calls = %+v, want none", got)
		}
	})
}
