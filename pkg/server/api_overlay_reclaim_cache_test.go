package server

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

type fakeCacheRemover struct {
	gotEntry *storage.Entry
}

func (f *fakeCacheRemover) RemoveEntryCache(e *storage.Entry) (int64, bool) {
	f.gotEntry = e
	return 7, true
}

// The reclaim routes free a reclaimed entry's cache through RemoveEntryCache,
// the only call that understands a season split's shared parent folder.
func TestRemoveReclaimedEntryCacheUsesEntryCache(t *testing.T) {
	r := &fakeCacheRemover{}
	e := &storage.Entry{InfoHash: "season-hash", Name: "Show.S01"}
	freed, ok := removeReclaimedEntryCache(r, e)
	if r.gotEntry != e {
		t.Fatalf("RemoveEntryCache called with %v, want the reclaimed entry", r.gotEntry)
	}
	if freed != 7 || !ok {
		t.Fatalf("result = %d, %v; want 7, true", freed, ok)
	}
}
