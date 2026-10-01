package manager

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// BackfillPar2Refs rebuilds only a record with neither PAR2 list. With one
// retained (a release posted without PAR2 files keeps its source list),
// par2Usable said usable whenever the .nzb was still on disk, and the pass
// that followed failed terminal "no PAR2 data available" - one wasted pass
// before the re-grab (review C-N3).
func TestPar2UsableNeedsBothRefListsOrABackfill(t *testing.T) {
	p, _ := newTestPar2Repair(t)
	cfg := config.Get()
	prev := cfg.Repair.Par2Repair
	on := true
	cfg.Repair.Par2Repair = &on
	t.Cleanup(func() { cfg.Repair.Par2Repair = prev })

	store, err := overlay.NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("overlay.NewStore: %v", err)
	}
	u, err := usenet.NewWithNZBStorageForTest(store)
	if err != nil {
		t.Fatalf("NewWithNZBStorageForTest: %v", err)
	}
	p.manager.usenet = u

	nzbPath := filepath.Join(t.TempDir(), "release.nzb")
	if err := os.WriteFile(nzbPath, []byte("<nzb/>"), 0o644); err != nil {
		t.Fatalf("write nzb: %v", err)
	}
	source := []storage.PostedFileRef{{Name: "movie.mkv"}}
	obfuscated := []storage.PostedFileRef{{Name: "a8f3", Segments: []storage.Par2SegmentRef{{MessageID: "idx"}}}}

	cases := []struct {
		id       string
		source   []storage.PostedFileRef
		files    []storage.Par2FileRef
		wantUsed bool
	}{
		{"no-par2-files", source, nil, false},
		{"neither-list", nil, nil, true}, // backfillable from the .nzb on disk
		// An obfuscated posted file with articles that no stored file reads may
		// be a PAR2 file stored as a posted file: with damage pending the pass
		// looks; with none there is nothing for a pass to repair.
		{"par2-among-posted", obfuscated, nil, true},
		{"par2-among-posted-nothing-pending", obfuscated, nil, false},
	}
	if err := store.RecordDead("par2-among-posted", "movie.mkv", 4, "<a@test>", 1024); err != nil {
		t.Fatalf("RecordDead: %v", err)
	}
	for _, c := range cases {
		if err := u.AddNZBForTest(&storage.NZB{ID: c.id, Name: c.id, Path: nzbPath, Par2Source: c.source, Par2Files: c.files}); err != nil {
			t.Fatalf("add %s: %v", c.id, err)
		}
		if got, reason := p.par2Usable(c.id); got != c.wantUsed {
			t.Fatalf("%s: par2Usable = %v (%q), want %v", c.id, got, reason, c.wantUsed)
		}
	}
}

// A stored match cache par2MatchFromCache refuses (one pairing a FileID
// twice, from before 0e4f177) was never replaced - SaveNZBPar2Match is
// write-once - so every pass redid the MD5-16k tie-break (review C-N4).
func TestSaveNZBPar2MatchReplacesOnlyTheRefusedCache(t *testing.T) {
	newTestPar2Repair(t) // points config at a temp dir
	store, err := overlay.NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("overlay.NewStore: %v", err)
	}
	u, err := usenet.NewWithNZBStorageForTest(store)
	if err != nil {
		t.Fatalf("NewWithNZBStorageForTest: %v", err)
	}
	var a, b [16]byte
	a[0], b[0] = 1, 2
	stale := []storage.Par2MatchRef{{PostedName: "x.r00", FileID: a}, {PostedName: "y.r00", FileID: a}}
	fresh := []storage.Par2MatchRef{{PostedName: "x.r00", FileID: a}, {PostedName: "y.r01", FileID: b}}
	other := []storage.Par2MatchRef{{PostedName: "z", FileID: b}}
	if err := u.AddNZBForTest(&storage.NZB{ID: "m1", Name: "m1", Par2Match: stale}); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}
	get := func() []storage.Par2MatchRef {
		nzb, err := u.GetNZB("m1")
		if err != nil {
			t.Fatalf("GetNZB: %v", err)
		}
		return nzb.Par2Match
	}

	for _, refused := range [][]storage.Par2MatchRef{nil, other} {
		if err := u.SaveNZBPar2Match("m1", fresh, refused); err != nil {
			t.Fatalf("SaveNZBPar2Match: %v", err)
		}
		if got := get(); len(got) != 2 || got[1].PostedName != "y.r00" {
			t.Fatalf("refused=%v: cache was overwritten to %v, want the stored one kept", refused, got)
		}
	}
	if err := u.SaveNZBPar2Match("m1", fresh, stale); err != nil {
		t.Fatalf("SaveNZBPar2Match: %v", err)
	}
	if got := get(); len(got) != 2 || got[1].PostedName != "y.r01" {
		t.Fatalf("refused cache not replaced: %v", got)
	}
}
