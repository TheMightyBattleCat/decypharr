package manager

import (
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// In threshold mode a pad below the minimum was never repaired, even while
// someone watched it; a watched release now always queues.
func TestAutoEnqueueAllowed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mode    config.Par2RepairMode
		dead    int
		watched bool
		want    bool
	}{
		{"manual never queues", config.Par2RepairModeManual, 50, true, false},
		{"threshold, below min, nobody watching", config.Par2RepairModeAutoThreshold, 1, false, false},
		{"threshold, below min, watched", config.Par2RepairModeAutoThreshold, 1, true, true},
		{"threshold, at min", config.Par2RepairModeAutoThreshold, 8, false, true},
		{"auto all", config.Par2RepairModeAutoAll, 1, false, true},
	} {
		if got := autoEnqueueAllowed(tc.mode, 8, tc.dead, tc.watched); got != tc.want {
			t.Errorf("%s: autoEnqueueAllowed = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestStreamingMatchesInfoHashAndFile(t *testing.T) {
	m := &Manager{activeStreams: xsync.NewMap[string, *ActiveStream]()}
	e := &storage.Entry{InfoHash: "h", Name: "Show", Protocol: config.ProtocolNZB,
		Files: map[string]*storage.File{"e.mkv": {Name: "e.mkv", Size: 1, AddedOn: time.Now()}}}
	id := m.TrackStream(e, "e.mkv", "DFS")

	if !m.Streaming("h", "") || !m.Streaming("h", "e.mkv") {
		t.Fatal("open stream not reported")
	}
	if m.Streaming("h", "other.mkv") || m.Streaming("other", "") {
		t.Fatal("stream reported for a different file or release")
	}
	m.UntrackStream(id)
	if m.Streaming("h", "") {
		t.Fatal("closed stream still reported")
	}
}

// A same-name replacement's stream must not overwrite or remove the old
// grab's record.
func TestStreamRecordsKeptApartForSameNameGrabs(t *testing.T) {
	m := &Manager{activeStreams: xsync.NewMap[string, *ActiveStream]()}
	files := map[string]*storage.File{"e.mkv": {Name: "e.mkv", Size: 1, AddedOn: time.Now()}}
	old := m.TrackStream(&storage.Entry{InfoHash: "old", Name: "Show", Files: files}, "e.mkv", "DFS")
	repl := m.TrackStream(&storage.Entry{InfoHash: "new", Name: "Show", Files: files}, "e.mkv", "DFS")
	if !m.Streaming("old", "") || !m.Streaming("new", "") {
		t.Fatal("one grab's stream replaced the other's")
	}
	m.UntrackStream(repl)
	if !m.Streaming("old", "") {
		t.Fatal("closing the replacement's stream removed the old grab's")
	}
	m.UntrackStream(old)
}
