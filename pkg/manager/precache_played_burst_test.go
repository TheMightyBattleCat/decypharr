package manager

import (
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Playing a file a walk already burst skips readAhead (it stays triggered
// for precacheTriggeredTTL), and repairAhead ran only from readAhead - so
// damage the burst found was never queued ahead of the playhead. The first
// play past the threshold must now reach playedBurst with the playhead, once.
func TestObserveOnBurstCachedFileRunsPlayedBurstOnce(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	withPrecacheReadAhead(t)
	p := NewPrecache(&Manager{})
	calls := make(chan int64, 4)
	p.playedBurst = func(_ *storage.Entry, _ string, from int64) { calls <- from }

	entry := &storage.Entry{InfoHash: "h", Name: "Show.S01E02"}
	p.triggered["h:e02.mkv"] = time.Now() // burst by the walk

	const size = 1000
	p.Observe(entry, "e02.mkv", 950, size)
	p.Observe(entry, "e02.mkv", 960, size)

	select {
	case from := <-calls:
		if from != 950 {
			t.Fatalf("playedBurst from = %d, want the playhead 950", from)
		}
	case <-time.After(time.Second):
		t.Fatal("playedBurst not called for a burst-cached file played past the threshold")
	}
	select {
	case <-calls:
		t.Fatal("playedBurst called twice for one play")
	case <-time.After(50 * time.Millisecond):
	}
}

// A fully cached episode is read from the DFS cache and never reaches
// Stream, so without Plex a finite-depth walk never moved on. The mount now
// reports those reads; a usenet entry's must reach Observe.
func TestObserveMountReadReachesPrecache(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	withPrecacheReadAhead(t)
	m := &Manager{}
	p := NewPrecache(m)
	m.precache = p
	calls := make(chan string, 4)
	p.playedBurst = func(e *storage.Entry, _ string, _ int64) { calls <- e.InfoHash }
	p.triggered["nzb:e.mkv"] = time.Now()
	p.triggered["torrent:e.mkv"] = time.Now()

	m.ObserveMountRead(&storage.Entry{InfoHash: "torrent", Protocol: config.ProtocolTorrent}, "e.mkv", 950, 1000)
	m.ObserveMountRead(&storage.Entry{InfoHash: "nzb", Protocol: config.ProtocolNZB}, "e.mkv", 950, 1000)

	select {
	case got := <-calls:
		if got != "nzb" {
			t.Fatalf("walk moved for %q, want only the usenet entry", got)
		}
	case <-time.After(time.Second):
		t.Fatal("a cached read of a walked usenet episode did not reach pre-cache")
	}
	select {
	case got := <-calls:
		t.Fatalf("unexpected second report for %q", got)
	case <-time.After(50 * time.Millisecond):
	}
}
