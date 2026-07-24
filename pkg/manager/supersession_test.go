package manager

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// TestFileSupersededRequiresPopulatedInfoHash documents fileSuperseded's
// contract: an empty InfoHash can only ever prove "unreferenced" for a
// still-referenced slot, never "referenced but replaced" - any ambiguity
// resolves to not-superseded by design (see fileSuperseded's doc comment).
// This is exactly why a BrokenFile's InfoHash must be populated at the
// point it's recorded broken - RepairPlaybackFileNow's playback-failure
// path used to leave it empty, so a BrokenFile it created could never be
// detected as superseded even after a healthy re-grab replaced it (fixed by
// populating it from item.Files[name].InfoHash, matching the sweep-probe
// path's brokenFiles()).
func TestFileSupersededRequiresPopulatedInfoHash(t *testing.T) {
	refs := map[string]map[string]string{
		"My.Show.S01E01": {
			"My.Show.S01E01.mkv": "new-healthy-hash",
		},
	}

	t.Run("empty InfoHash can never detect supersession", func(t *testing.T) {
		if fileSuperseded(refs, "My.Show.S01E01", "My.Show.S01E01.mkv", "") {
			t.Fatalf("fileSuperseded(...) with empty InfoHash = true, want false (ambiguous - ok per contract, but proves why callers must populate it)")
		}
	})

	t.Run("populated InfoHash differing from the current reference is superseded", func(t *testing.T) {
		if !fileSuperseded(refs, "My.Show.S01E01", "My.Show.S01E01.mkv", "old-broken-hash") {
			t.Fatalf("fileSuperseded(...) with a different, populated InfoHash = false, want true")
		}
	})

	t.Run("populated InfoHash matching the current reference is still broken", func(t *testing.T) {
		if fileSuperseded(refs, "My.Show.S01E01", "My.Show.S01E01.mkv", "new-healthy-hash") {
			t.Fatalf("fileSuperseded(...) reported superseded=true when the InfoHash still matches the current reference, want false")
		}
	})
}

// TestClassifySupersessionNeedsBrokenFileInfoHash is the whole-entry version
// of the above, exercised through classifySupersession the way the
// "Clear replaced" supersession sweep actually consults it: a BrokenFile
// left with InfoHash="" (the RepairPlaybackFileNow bug) is permanently
// stuck as stillBroken regardless of what the reference set shows, even
// once a working re-grab has taken over the exact same (entry, file) slot.
// Populating it (the fix) lets the sweep correctly clear it.
func TestClassifySupersessionNeedsBrokenFileInfoHash(t *testing.T) {
	refs := map[string]map[string]string{
		"My.Show.S01E01": {
			"My.Show.S01E01.mkv": "new-healthy-hash",
		},
	}

	h := &storage.EntryHealth{
		EntryName: "My.Show.S01E01",
		BrokenFiles: []storage.BrokenFile{
			{EntryName: "My.Show.S01E01", FileName: "My.Show.S01E01.mkv", InfoHash: ""},
		},
	}
	res := classifySupersession(h, refs)
	if len(res.superseded) != 0 || len(res.stillBroken) != 1 {
		t.Fatalf("BrokenFile with empty InfoHash: superseded=%d stillBroken=%d, want 0/1 - a healthy replacement can never be detected without it",
			len(res.superseded), len(res.stillBroken))
	}

	h.BrokenFiles[0].InfoHash = "old-broken-hash"
	res = classifySupersession(h, refs)
	if len(res.superseded) != 1 || len(res.stillBroken) != 0 {
		t.Fatalf("BrokenFile with a populated, different InfoHash: superseded=%d stillBroken=%d, want 1/0",
			len(res.superseded), len(res.stillBroken))
	}
}
