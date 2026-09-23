package manager

import (
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// A re-grabbed entry someone is streaming is deleted only once the stream
// closes: deleting it ends the viewer's stream.
func TestDeleteEntryWhenIdleWaitsForStream(t *testing.T) {
	prev := idleDeletePoll
	idleDeletePoll = 5 * time.Millisecond
	t.Cleanup(func() { idleDeletePoll = prev })

	m, strg := newTestManagerForReapVerdict(t)
	m.logger = zerolog.Nop()
	m.activeStreams = xsync.NewMap[string, *ActiveStream]()
	r := &Repair{manager: m, logger: zerolog.Nop(), deleteEntryFn: func(h string) error { return m.deleteEntry(h, false) }}
	m.repair = r
	e := &storage.Entry{InfoHash: "old", Name: "Show.S01E01", Protocol: config.ProtocolTorrent,
		Files: map[string]*storage.File{"e.mkv": {Name: "e.mkv", InfoHash: "old", Size: 1, AddedOn: time.Now()}}}
	if err := strg.AddOrUpdate(e); err != nil {
		t.Fatal(err)
	}
	stream := m.TrackStream(e, "e.mkv", "DFS")

	deleted, err := r.deleteEntryWhenIdle("old", e.Name)
	if err != nil || deleted {
		t.Fatalf("deleteEntryWhenIdle = %v, %v; want deferred", deleted, err)
	}
	time.Sleep(30 * time.Millisecond)
	if ok, _ := m.EntryExists("old"); !ok {
		t.Fatal("entry deleted while it was being streamed")
	}

	m.UntrackStream(stream)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ok, _ := m.EntryExists("old"); !ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("entry not deleted after its stream closed")
}

func TestDeleteEntryWhenIdleDeletesUnwatchedNow(t *testing.T) {
	m, strg := newTestManagerForReapVerdict(t)
	m.logger = zerolog.Nop()
	r := &Repair{manager: m, logger: zerolog.Nop(), deleteEntryFn: func(h string) error { return m.deleteEntry(h, false) }}
	if err := strg.AddOrUpdate(&storage.Entry{InfoHash: "h", Name: "X", Protocol: config.ProtocolTorrent,
		Files: map[string]*storage.File{"f": {Name: "f", InfoHash: "h", Size: 1, AddedOn: time.Now()}}}); err != nil {
		t.Fatal(err)
	}
	if deleted, err := r.deleteEntryWhenIdle("h", "X"); err != nil || !deleted {
		t.Fatalf("deleteEntryWhenIdle = %v, %v; want deleted now", deleted, err)
	}
}
