package manager

import (
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

func TestEntryNZBID(t *testing.T) {
	now := time.Now()
	plain := &storage.Entry{InfoHash: "nzb-1", Files: map[string]*storage.File{
		"a.mkv": {Name: "a.mkv", InfoHash: "nzb-1", AddedOn: now},
	}}
	if got := entryNZBID(plain); got != "nzb-1" {
		t.Fatalf("plain entry: got %q, want nzb-1", got)
	}

	// convertToMultiSeason: the season entry's own hash is generated, each
	// file keeps the NZB's.
	season := &storage.Entry{InfoHash: generateSeasonHash("nzb-pack", 1), Files: map[string]*storage.File{
		"S01E01.mkv": {Name: "S01E01.mkv", InfoHash: "nzb-pack", AddedOn: now},
		"S01E02.mkv": {Name: "S01E02.mkv", InfoHash: "nzb-pack", AddedOn: now},
	}}
	if got := entryNZBID(season); got != "nzb-pack" {
		t.Fatalf("season entry: got %q, want nzb-pack", got)
	}
	if got := fileNZBID(season, season.Files["S01E01.mkv"]); got != "nzb-pack" {
		t.Fatalf("season file: got %q, want nzb-pack", got)
	}

	legacy := &storage.Entry{InfoHash: "nzb-2", Files: map[string]*storage.File{"b.mkv": {Name: "b.mkv"}}}
	if got := entryNZBID(legacy); got != "nzb-2" {
		t.Fatalf("files without InfoHash: got %q, want nzb-2", got)
	}
	if got := fileNZBID(legacy, legacy.Files["b.mkv"]); got != "nzb-2" {
		t.Fatalf("file without InfoHash: got %q, want nzb-2", got)
	}
}

// Rejecting one season of a split multi-season NZB used to key the overlay
// on the season's generated hash: it missed the NZB's overlay entirely. Now
// it removes that season's records only, and leaves the NZB able to record
// damage for its sibling seasons.
func TestCleanupRejectedSeasonImportKeepsSiblingOverlay(t *testing.T) {
	_, repair := newTestPar2Repair(t)
	m := repair.manager
	store, err := overlay.NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("overlay.NewStore: %v", err)
	}
	m.usenet = usenet.NewWithOverlayForTest(store)

	const nzbID = "nzb-pack"
	now := time.Now()
	s1 := &storage.Entry{
		InfoHash: generateSeasonHash(nzbID, 1),
		Name:     "Show.S01.1080p",
		Protocol: config.ProtocolNZB,
		SavePath: t.TempDir(),
		Files: map[string]*storage.File{
			"Show.S01E01.mkv": {Name: "Show.S01E01.mkv", InfoHash: nzbID, Size: 1 << 20, AddedOn: now},
		},
	}
	if err := store.RecordDead(nzbID, "Show.S01E01.mkv", 3, "<a@test>", 1024); err != nil {
		t.Fatalf("RecordDead S01: %v", err)
	}
	if err := store.RecordDead(nzbID, "Show.S02E01.mkv", 5, "<b@test>", 1024); err != nil {
		t.Fatalf("RecordDead S02: %v", err)
	}

	d := &Downloader{manager: m, logger: zerolog.Nop()}
	d.cleanupRejectedImport(s1)

	pending, err := store.PendingRepair(nzbID)
	if err != nil {
		t.Fatalf("PendingRepair: %v", err)
	}
	if _, ok := pending["Show.S01E01.mkv"]; ok {
		t.Fatalf("rejected season's overlay record survived: %v", pending)
	}
	if _, ok := pending["Show.S02E01.mkv"]; !ok {
		t.Fatalf("sibling season's overlay record was removed: %v", pending)
	}

	// The NZB is not marked rejected: the sibling season still records damage.
	if err := store.RecordDead(nzbID, "Show.S02E01.mkv", 9, "<c@test>", 1024); err != nil {
		t.Fatalf("RecordDead after cleanup: %v", err)
	}
	pending, _ = store.PendingRepair(nzbID)
	if got := len(pending["Show.S02E01.mkv"]); got != 2 {
		t.Fatalf("sibling season dead segments = %d, want 2 (the NZB must not be marked rejected)", got)
	}
}
