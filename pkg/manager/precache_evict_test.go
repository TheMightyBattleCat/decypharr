package manager

import (
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

type fakeEvictMount struct {
	stubMountManager
	open    bool
	evicted []string
}

func (f *fakeEvictMount) EvictCachedFile(entryName, filename string) (int64, bool) {
	if f.open {
		return 0, false
	}
	f.evicted = append(f.evicted, entryName+"/"+filename)
	return 100, true
}

// "Evict after watched" fires at 90% while the viewer still holds the file,
// so the DFS eviction is queued and retried until the handle closes.
func TestWatchedEvictRetriesUntilClosed(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	m := &Manager{}
	mount := &fakeEvictMount{open: true}
	m.SetMountManager(mount)
	p := NewPrecache(m)
	p.storeReadiness(EpisodeReadiness{InfoHash: "h", Filename: "e.mkv", Clean: true})

	p.queueWatchedEvict("h", "Show.S01E01", "e.mkv")
	p.evictWatchedDue()
	if len(mount.evicted) != 0 || len(p.evictQueue) != 1 {
		t.Fatalf("evicted %v with the file open; queue=%d", mount.evicted, len(p.evictQueue))
	}

	mount.open = false
	p.evictWatchedDue()
	if len(mount.evicted) != 1 || mount.evicted[0] != "Show.S01E01/e.mkv" {
		t.Fatalf("evicted = %v, want Show.S01E01/e.mkv once closed", mount.evicted)
	}
	if len(p.evictQueue) != 0 {
		t.Fatal("eviction still queued after it succeeded")
	}
	if _, ok := p.readiness["h:e.mkv"]; ok {
		t.Fatal("readiness row kept for an evicted episode")
	}
}

func TestWatchedEvictSkipsFileBeingWritten(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	m := &Manager{}
	mount := &fakeEvictMount{}
	m.SetMountManager(mount)
	p := NewPrecache(m)
	p.markInflight("h:e.mkv")

	p.queueWatchedEvict("h", "Show", "e.mkv")
	p.evictWatchedDue()
	if len(mount.evicted) != 0 {
		t.Fatal("evicted a file a burst is writing")
	}
	p.unmarkInflight("h:e.mkv")
	p.evictWatchedDue()
	if len(mount.evicted) != 1 {
		t.Fatal("not evicted once the burst finished")
	}
}
