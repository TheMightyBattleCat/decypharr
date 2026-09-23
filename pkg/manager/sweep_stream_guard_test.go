package manager

import (
	"sync"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func newStreamGuardRepair(t *testing.T, streamed string) *Repair {
	t.Helper()
	m := &Manager{activeStreams: xsync.NewMap[string, *ActiveStream]()}
	if streamed != "" {
		e := &storage.Entry{InfoHash: "pack", Name: "Show.S01", Protocol: config.ProtocolNZB,
			Files: map[string]*storage.File{streamed: {Name: streamed, Size: 1, AddedOn: time.Now()}}}
		m.TrackStream(e, streamed, "DFS")
	}
	return &Repair{manager: m, logger: zerolog.Nop()}
}

func TestHealWouldInterruptStream(t *testing.T) {
	broken := func(files ...string) *storage.EntryHealth {
		h := &storage.EntryHealth{EntryName: "Show.S01", FileCount: 3, BrokenCount: len(files)}
		for _, f := range files {
			h.BrokenFiles = append(h.BrokenFiles, storage.BrokenFile{FileName: f, InfoHash: "pack"})
		}
		return h
	}
	for _, tc := range []struct {
		name     string
		streamed string
		h        *storage.EntryHealth
		want     bool
	}{
		{"broken episode being watched", "e02.mkv", broken("e02.mkv"), true},
		{"another episode watched, partial heal keeps the entry", "e01.mkv", broken("e02.mkv"), false},
		{"whole entry broken, any episode watched", "e01.mkv", broken("e01.mkv", "e02.mkv", "e03.mkv"), true},
		{"nothing streamed", "", broken("e01.mkv", "e02.mkv", "e03.mkv"), false},
	} {
		r := newStreamGuardRepair(t, tc.streamed)
		if _, got := r.healWouldInterruptStream(tc.h); got != tc.want {
			t.Errorf("%s: healWouldInterruptStream = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The sweep must leave a file being watched alone. regrabGuard is nil here,
// so reaching the re-grab path would panic.
func TestSweepHealSkipsStreamedFile(t *testing.T) {
	r := newStreamGuardRepair(t, "e02.mkv")
	h := &storage.EntryHealth{EntryName: "Show.S01", FileCount: 3, BrokenCount: 1,
		BrokenFiles: []storage.BrokenFile{{FileName: "e02.mkv", InfoHash: "pack"}}}
	var mu sync.Mutex
	r.healBrokenEntryGuarded(t.Context(), &storage.RepairRun{}, &mu, "Show.S01", h, false)
}
