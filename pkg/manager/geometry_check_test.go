package manager

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

const geoArticle = 768000

// matroskaHead builds a Matroska head: a 31-byte EBML header body, then a
// Segment with an 8-byte size vint. The Segment ends at 48 + segmentSize.
func matroskaHead(segmentSize int64) []byte {
	b := []byte{0x1A, 0x45, 0xDF, 0xA3, 0x9F}
	b = append(b, make([]byte, 31)...)
	b = append(b, 0x18, 0x53, 0x80, 0x67)
	size := make([]byte, 8)
	binary.BigEndian.PutUint64(size, uint64(segmentSize))
	size[0] = 0x01 // 8-byte vint marker; sizes here fit in 56 bits
	b = append(b, size...)
	return append(b, make([]byte, 200)...)
}

func geoFile(size int64, numbers ...int) *storage.NZBFile {
	f := &storage.NZBFile{Name: "f.mkv", Size: size}
	for _, n := range numbers {
		f.Segments = append(f.Segments, storage.NZBSegment{Number: n, Bytes: geoArticle})
	}
	return f
}

func TestMatroskaSegmentEnd(t *testing.T) {
	if end, ok := matroskaSegmentEnd(matroskaHead(5_000_000_000)); !ok || end != 5_000_000_048 {
		t.Fatalf("got (%d, %v), want (5000000048, true)", end, ok)
	}
	unknown := matroskaHead(0)
	copy(unknown[40:48], []byte{0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	if _, ok := matroskaSegmentEnd(unknown); ok {
		t.Fatal("a Segment of unknown size gave an end")
	}
	cluster := matroskaHead(1000)
	copy(cluster[36:40], []byte{0x1F, 0x43, 0xB6, 0x75})
	if _, ok := matroskaSegmentEnd(cluster); ok {
		t.Fatal("an element other than Segment after the EBML header gave an end")
	}
	if _, ok := matroskaSegmentEnd([]byte("RIFF\x00\x00\x00\x00AVI LIST")); ok {
		t.Fatal("a non-Matroska head gave an end")
	}
	if _, ok := matroskaSegmentEnd(matroskaHead(1)[:30]); ok {
		t.Fatal("a head cut inside the EBML header gave an end")
	}
}

func TestClassifyGeometry(t *testing.T) {
	const size = 5_000_000_048
	cases := []struct {
		name    string
		file    *storage.NZBFile
		head    []byte
		reason  string
		tail    bool
		short   int64
		splices int
		single  bool
	}{
		{"intact", geoFile(size, 1, 2, 3, 1, 2), matroskaHead(size - 48), "", false, 0, 0, false},
		// Dear Judge S01E06: the next volume's first article sorted in after the part's own.
		{"spliced, no head needed", geoFile(size, 1, 1, 2, 3, 1, 1, 2), nil, reasonSplicedVolumes, false, 0, 2, false},
		// Man of Sun Dao: the final volume took a trailing single-article file.
		{"one splice is flagged only", geoFile(size, 1, 2, 3, 1, 1, 2), matroskaHead(size - 48), "", false, 0, 1, true},
		{"tail clamp, 22,841 B", geoFile(size-22841, 1, 2, 3), matroskaHead(size - 48), "", true, 22841, 0, false},
		{"just under an article", geoFile(size-(geoArticle-1), 1, 2, 3), matroskaHead(size - 48), "", true, geoArticle - 1, 0, false},
		{"one article", geoFile(size-geoArticle, 1, 2, 3), matroskaHead(size - 48), reasonMissingVolume, false, geoArticle, 0, false},
		{"a dropped volume (Signal S01E09)", geoFile(size-52223882, 1, 2, 3), matroskaHead(size - 48), reasonMissingVolume, false, 52223882, 0, false},
		{"served 1 B longer", geoFile(size+1, 1, 2, 3), matroskaHead(size - 48), "", false, 0, 0, false},
		{"no head", geoFile(size-22841, 1, 2, 3), nil, "", false, 0, 0, false},
		{"not Matroska", geoFile(size-22841, 1, 2, 3), []byte("RIFF...."), "", false, 0, 0, false},
		{"no slices", &storage.NZBFile{Size: size}, matroskaHead(size), "", false, 0, 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v := classifyGeometry(c.file, c.head)
			if v.reason != c.reason || v.tailTruncated != c.tail || v.shortBytes != c.short || v.splices != c.splices || v.singleSplice != c.single {
				t.Fatalf("got %+v, want reason=%q tail=%v short=%d splices=%d single=%v", v, c.reason, c.tail, c.short, c.splices, c.single)
			}
		})
	}
}

func TestKeepReleaseReason(t *testing.T) {
	for _, r := range []string{reasonSplicedVolumes, reasonMissingVolume} {
		if !keepReleaseReason(r) {
			t.Errorf("%s should keep the release", r)
		}
	}
	for _, r := range []string{"usenet_segment_missing", "ffprobe_decode_error", "sweep_dead_segment", ""} {
		if keepReleaseReason(r) {
			t.Errorf("%q must blocklist", r)
		}
	}
}

// fakeRadarr answers the calls repairArrFiles makes and records them.
type fakeRadarr struct {
	mu        sync.Mutex
	calls     []string
	historyID map[string]int // movieIds query value -> grab history ID
}

func (f *fakeRadarr) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.calls = append(f.calls, r.Method+" "+r.URL.Path)
	f.mu.Unlock()
	if r.Method == http.MethodGet && r.URL.Path == "/api/v3/history" {
		var records []arr.HistoryRecord
		if id := f.historyID[r.URL.Query().Get("movieIds")]; id != 0 {
			records = append(records, arr.HistoryRecord{ID: id})
		}
		_ = json.NewEncoder(w).Encode(arr.HistorySchema{Records: records})
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("{}"))
}

func (f *fakeRadarr) count(prefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if strings.HasPrefix(c, prefix) {
			n++
		}
	}
	return n
}

func TestRepairArrFilesKeepsReleaseForImportFaults(t *testing.T) {
	movies := []arr.ContentFile{
		{Id: 1, FileId: 101, Name: "spliced.mkv", IsBroken: true},
		{Id: 2, FileId: 102, Name: "dead.mkv", IsBroken: true},
	}
	cases := []struct {
		name       string
		files      []arr.ContentFile
		keep       map[int]bool
		history    map[string]int
		wantFailed []string
		wantSearch int
	}{
		{"import fault only: delete + search, no blocklist", movies[:1], map[int]bool{101: true}, map[string]int{"1": 7}, nil, 1},
		{"damaged file still blocklists", movies[1:], nil, map[string]int{"2": 8}, []string{"POST /api/v3/history/failed/8"}, 0},
		{"mixed entry: only the damaged grab is blocklisted", movies, map[int]bool{101: true}, map[string]int{"1": 7, "2": 8}, []string{"POST /api/v3/history/failed/8"}, 1},
		{"shared grab is blocklisted anyway", movies, map[int]bool{101: true}, map[string]int{"1": 9, "2": 9}, []string{"POST /api/v3/history/failed/9"}, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeRadarr{historyID: c.history}
			srv := httptest.NewServer(fake)
			defer srv.Close()
			a := arr.New("radarr", srv.URL, "token", false, nil, "", "")
			repair := newTestRepairForClaims(t)
			var mu sync.Mutex
			files := slices.Clone(c.files)
			actioned, cancelled := repair.repairArrFiles(context.Background(), &storage.RepairRun{}, &mu, a, files, c.keep)
			if !actioned || cancelled {
				t.Fatalf("actioned=%v cancelled=%v", actioned, cancelled)
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
			if got := fake.count("POST /api/v3/command"); got != c.wantSearch {
				t.Fatalf("search commands = %d, want %d (calls %v)", got, c.wantSearch, fake.calls)
			}
			if fake.count("DELETE ") == 0 {
				t.Fatalf("no delete call: %v", fake.calls)
			}
		})
	}
}

// healBrokenEntry carries each broken file's reason down to the Arr call: an
// import fault is re-searched, a damaged file in the same entry blocklisted.
func TestHealBrokenEntryKeepsReleaseByReason(t *testing.T) {
	fake := &fakeRadarr{historyID: map[string]int{"1": 7, "2": 8}}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	repair := newTestRepairForFix(t)
	repair.manager.arr.AddOrUpdate(arr.New("radarr", srv.URL, "token", false, nil, "", ""))

	h := &storage.EntryHealth{
		EntryName: "Entry",
		Status:    storage.HealthBroken,
		FileCount: 3, // not fully broken: the entry itself is kept
		BrokenFiles: []storage.BrokenFile{
			{FileName: "spliced.mkv", Reason: reasonSplicedVolumes, ArrName: "radarr", MediaID: 1, ArrFileID: 101},
			{FileName: "dead.mkv", Reason: "usenet_segment_missing", ArrName: "radarr", MediaID: 2, ArrFileID: 102},
		},
	}
	h.BrokenCount = len(h.BrokenFiles)
	var mu sync.Mutex
	repair.healBrokenEntry(context.Background(), &storage.RepairRun{}, &mu, "Entry", h, true)

	if got := fake.count("POST /api/v3/history/failed/"); got != 1 || fake.count("POST /api/v3/history/failed/8") != 1 {
		t.Fatalf("blocklist calls: %v, want only failed/8", fake.calls)
	}
	if fake.count("POST /api/v3/command") != 1 {
		t.Fatalf("want one search for the import fault: %v", fake.calls)
	}
}
