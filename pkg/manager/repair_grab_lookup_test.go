package manager

import (
	"context"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// repairArrFiles blocklists the grab that produced the broken file (matched by
// its download ID), not the newest grab of the movie: that can be an upgrade
// still downloading, or a grab already rejected at import. The newest grab is
// used only when the Arr has no record for the file's download ID.
func TestRepairArrFilesBlocklistsTheBrokenFilesOwnGrab(t *testing.T) {
	cases := []struct {
		name       string
		infoHash   string
		byDownload map[string][]arr.HistoryRecord
		wantFailed []string
	}{
		{
			name:     "newest grab is another download: the broken file's grab is failed",
			infoHash: "nzb-dead",
			byDownload: map[string][]arr.HistoryRecord{
				"nzb-dead": {{ID: 5, DownloadID: "nzb-dead", MovieID: 1}},
			},
			wantFailed: []string{"POST /api/v3/history/failed/5"},
		},
		{
			name:       "no record for the download ID: falls back to the newest grab",
			infoHash:   "split-generated",
			wantFailed: []string{"POST /api/v3/history/failed/9"},
		},
		{
			name:       "no download ID known: falls back to the newest grab",
			wantFailed: []string{"POST /api/v3/history/failed/9"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Grab 9 is the newest for movie 1, from a different download.
			fake := &fakeRadarr{historyID: map[string]int{"1": 9}, byDownload: c.byDownload}
			srv := httptest.NewServer(fake)
			defer srv.Close()
			a := arr.New("radarr", srv.URL, "token", false, nil, "", "")
			repair := newTestRepairForClaims(t)
			var mu sync.Mutex
			files := []arr.ContentFile{{Id: 1, FileId: 101, Name: "dead.mkv", IsBroken: true, InfoHash: c.infoHash}}
			if _, cancelled := repair.repairArrFiles(context.Background(), &storage.RepairRun{}, &mu, a, files, nil); cancelled {
				t.Fatal("cancelled")
			}
			var failed []string
			fake.mu.Lock()
			for _, call := range fake.calls {
				if strings.HasPrefix(call, "POST /api/v3/history/failed/") {
					failed = append(failed, call)
				}
			}
			fake.mu.Unlock()
			if !slices.Equal(failed, c.wantFailed) {
				t.Fatalf("blocklist calls %v, want %v", failed, c.wantFailed)
			}
		})
	}
}

// healBrokenEntry hands each broken file's download ID down to the Arr call,
// so the sweep and playback paths blocklist the file's own grab.
func TestHealBrokenEntryBlocklistsTheBrokenFilesOwnGrab(t *testing.T) {
	fake := &fakeRadarr{
		historyID:  map[string]int{"1": 9}, // newest grab: another download
		byDownload: map[string][]arr.HistoryRecord{"nzb-dead": {{ID: 5, DownloadID: "nzb-dead", MovieID: 1}}},
	}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	repair := newTestRepairForFix(t)
	repair.manager.arr.AddOrUpdate(arr.New("radarr", srv.URL, "token", false, nil, "", ""))

	h := &storage.EntryHealth{
		EntryName: "Entry",
		Status:    storage.HealthBroken,
		FileCount: 2, // not fully broken: the entry itself is kept
		BrokenFiles: []storage.BrokenFile{
			{FileName: "dead.mkv", Reason: "usenet_segment_missing", ArrName: "radarr", MediaID: 1, ArrFileID: 101, InfoHash: "nzb-dead"},
		},
	}
	h.BrokenCount = len(h.BrokenFiles)
	var mu sync.Mutex
	repair.healBrokenEntry(context.Background(), &storage.RepairRun{}, &mu, "Entry", h, true)

	if fake.count("POST /api/v3/history/failed/") != 1 || fake.count("POST /api/v3/history/failed/5") != 1 {
		t.Fatalf("blocklist calls: %v, want only failed/5", fake.calls)
	}
}
