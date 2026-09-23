package manager

import (
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// After a streamed file's repair has acted, later viewer pads on it (the
// entry lingers until the stream closes) must not start it again. Repair is
// an empty struct here, so reaching handleAutoDamage would panic.
func TestRepairStreamedFailureQuietAfterActing(t *testing.T) {
	m := &Manager{activeStreams: xsync.NewMap[string, *ActiveStream](), repair: &Repair{}}
	e := &storage.Entry{InfoHash: "h", Name: "Show", Protocol: config.ProtocolNZB,
		Files: map[string]*storage.File{"e.mkv": {Name: "e.mkv", Size: 1, AddedOn: time.Now()}}}
	m.TrackStream(e, "e.mkv", "DFS")

	m.streamedRepaired.Store("h:e.mkv", time.Now())
	m.repairStreamedFailure("h", "Show", "e.mkv")

	m.streamedRepaired.Delete("h:e.mkv")
	m.streamedRepairs.Store("h:e.mkv", struct{}{}) // one already running
	m.repairStreamedFailure("h", "Show", "e.mkv")
}
