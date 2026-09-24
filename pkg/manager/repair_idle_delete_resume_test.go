package manager

import (
	"context"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// A deferred delete interrupted by a restart is replayed on the next start:
// the Arr row and broken record are gone by then, so nothing else would ever
// come back for the superseded grab.
func TestDeferredDeleteSurvivesRestart(t *testing.T) {
	prev := idleDeletePoll
	idleDeletePoll = 5 * time.Millisecond
	t.Cleanup(func() { idleDeletePoll = prev })

	m, strg := newTestManagerForReapVerdict(t)
	m.logger = zerolog.Nop()
	m.activeStreams = xsync.NewMap[string, *ActiveStream]()
	e := &storage.Entry{InfoHash: "old", Name: "Show.S01E01", Protocol: config.ProtocolTorrent,
		Files: map[string]*storage.File{"e.mkv": {Name: "e.mkv", InfoHash: "old", Size: 1, AddedOn: time.Now()}}}
	if err := strg.AddOrUpdate(e); err != nil {
		t.Fatal(err)
	}

	// Before the restart: streamed, so the delete waits; then shutdown.
	ctx, stop := context.WithCancel(context.Background())
	r1 := &Repair{manager: m, logger: zerolog.Nop(), parentCtx: ctx, deleteEntryFn: func(h string) error { return m.deleteEntry(h, false) }}
	m.repair = r1
	stream := m.TrackStream(e, "e.mkv", "DFS")
	if deleted, err := r1.deleteEntryWhenIdle("old", e.Name); err != nil || deleted {
		t.Fatalf("deleteEntryWhenIdle = %v, %v; want deferred", deleted, err)
	}
	stop()
	// Wait for r1's waiter to exit (it reads idleDeletePoll, which the
	// cleanup above restores).
	for deadline := time.Now().Add(2 * time.Second); ; {
		if _, pending := r1.idleDeletes.Load("old"); !pending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("r1's deferred-delete waiter did not exit on shutdown")
		}
		time.Sleep(5 * time.Millisecond)
	}
	m.UntrackStream(stream)
	if ok, _ := m.EntryExists("old"); !ok {
		t.Fatal("setup: entry deleted before the restart")
	}

	// After the restart: nothing streams; the replay deletes it.
	r2 := &Repair{manager: m, logger: zerolog.Nop(), parentCtx: context.Background(), deleteEntryFn: func(h string) error { return m.deleteEntry(h, false) }}
	m.repair = r2
	r2.resumeIdleDeletes()
	if ok, _ := m.EntryExists("old"); ok {
		t.Fatal("deferred delete not resumed after the restart")
	}
	n := 0
	_ = strg.ForEachPendingDelete(func(*storage.PendingDelete) { n++ })
	if n != 0 {
		t.Fatalf("%d pending delete record(s) left after the resume", n)
	}
}
