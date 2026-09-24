package manager

import (
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// The DFS cache is keyed by folder name, so a same-name twin's readiness row
// showed the live owner's cached bytes as its own (My.Life.With.the.Fenton.Boys
// S03, 2026-08-08). Only the grab the name serves owns that coverage.
func TestPrecacheOwnsCacheName(t *testing.T) {
	m, strg := newTestManagerForReapVerdict(t)
	p := &Precache{manager: m}
	now := time.Now()

	const name, file = "My.Show.S01E02", "My.Show.S01E02.mkv"
	for _, e := range []*storage.Entry{
		{InfoHash: "old-hash", Name: name, Files: map[string]*storage.File{file: {Name: file, InfoHash: "old-hash", Size: 100, AddedOn: now}}},
		{InfoHash: "new-hash", Name: name, Files: map[string]*storage.File{file: {Name: file, InfoHash: "new-hash", Size: 100, AddedOn: now.Add(time.Second)}}},
	} {
		if err := strg.AddOrUpdate(e); err != nil {
			t.Fatalf("AddOrUpdate(%s): %v", e.InfoHash, err)
		}
	}
	if p.ownsCacheName(name, file, "old-hash") {
		t.Fatalf("superseded twin owns the name's cache coverage")
	}
	if !p.ownsCacheName(name, file, "new-hash") {
		t.Fatalf("live owner does not own its own cache coverage")
	}

	// A season split out of a multi-season NZB: its files carry the NZB's
	// hash, not the season entry's.
	const sName, sFile = "Pack.S01", "Pack.S01E01.mkv"
	season := &storage.Entry{InfoHash: generateSeasonHash("nzb-pack", 1), Name: sName,
		Files: map[string]*storage.File{sFile: {Name: sFile, InfoHash: "nzb-pack", Size: 100, AddedOn: now}}}
	if err := strg.AddOrUpdate(season); err != nil {
		t.Fatalf("AddOrUpdate(season): %v", err)
	}
	if !p.ownsCacheName(sName, sFile, season.InfoHash) {
		t.Fatalf("a split season entry does not own its own folder's cache coverage")
	}

	if !p.ownsCacheName("Unknown", "x.mkv", "whatever") {
		t.Fatalf("an unresolvable name must not drop the row")
	}
}
