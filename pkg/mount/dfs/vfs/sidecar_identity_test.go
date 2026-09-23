package vfs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSidecarBelongsTo(t *testing.T) {
	for _, tc := range []struct {
		name string
		info ItemInfo
		hash string
		size int64
		want bool
	}{
		{"same grab", ItemInfo{InfoHash: "a", Size: 1024}, "a", 1024, true},
		{"other grab, same size", ItemInfo{InfoHash: "a", Size: 1024}, "b", 1024, false},
		{"legacy sidecar, same size", ItemInfo{Size: 1024}, "b", 1024, true},
		{"legacy sidecar, other size", ItemInfo{Size: 1024}, "b", 2048, false},
		{"size unknown", ItemInfo{InfoHash: "a", Size: 1024}, "a", 0, true},
		{"empty sidecar", ItemInfo{}, "a", 1024, true},
	} {
		if got := sidecarBelongsTo(tc.info, tc.hash, tc.size); got != tc.want {
			t.Errorf("%s: sidecarBelongsTo = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestLoadSidecarResetsOtherGrab(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "video.mkv.json")
	cachePath := filepath.Join(dir, "video.mkv")
	meta := `{"info_hash":"old","size":1024,"ranges":[{"Pos":0,"Size":1024}]}`
	if err := os.WriteFile(metaPath, []byte(meta), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cachePath, make([]byte, 1024), 0644); err != nil {
		t.Fatal(err)
	}
	c := newTestCache(dir)

	info, err := c.loadSidecar("k", "entry", "video.mkv", metaPath, cachePath, "old", 1024)
	if err != nil || info.Rs.Size() != 1024 || info.InfoHash != "old" {
		t.Fatalf("own grab: got %+v, %v; want its 1024 cached bytes kept", info, err)
	}
	if _, err := os.Stat(cachePath); err != nil {
		t.Fatalf("own grab's data file removed: %v", err)
	}

	info, err = c.loadSidecar("k", "entry", "video.mkv", metaPath, cachePath, "new", 1024)
	if err != nil {
		t.Fatal(err)
	}
	if info.Rs.Size() != 0 || info.InfoHash != "new" {
		t.Fatalf("other grab: got %+v; want empty ranges stamped with the new InfoHash", info)
	}
	if _, err := os.Stat(cachePath); !os.IsNotExist(err) {
		t.Fatalf("other grab's data file kept: stat err=%v", err)
	}
}

// A legacy sidecar (no InfoHash) is kept when the size matches, and stamped.
func TestLoadSidecarStampsLegacySidecar(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "video.mkv.json")
	if err := os.WriteFile(metaPath, []byte(`{"size":1024,"ranges":[{"Pos":0,"Size":100}]}`), 0644); err != nil {
		t.Fatal(err)
	}
	c := newTestCache(dir)
	info, err := c.loadSidecar("k", "entry", "video.mkv", metaPath, filepath.Join(dir, "video.mkv"), "h", 1024)
	if err != nil || info.Rs.Size() != 100 || info.InfoHash != "h" {
		t.Fatalf("got %+v, %v; want 100 bytes kept and InfoHash h", info, err)
	}
}

// A same-name re-grab must not see the old grab's sidecar as cached bytes:
// the pre-cache "durable cache complete" gate and the persist walk ask
// DiskCoverage/DiskHasRange before any item is opened.
func TestDiskSidecarIgnoresOtherGrab(t *testing.T) {
	cacheDir := t.TempDir()
	entryDir := filepath.Join(cacheDir, "entry")
	if err := os.MkdirAll(entryDir, 0755); err != nil {
		t.Fatal(err)
	}
	meta := `{"info_hash":"old","size":1024,"ranges":[{"Pos":0,"Size":1024}]}`
	if err := os.WriteFile(filepath.Join(entryDir, "video.mkv.json"), []byte(meta), 0644); err != nil {
		t.Fatal(err)
	}
	c := newTestCache(cacheDir)

	c.identity = func(string, string) (string, int64, bool) { return "old", 1024, true }
	if cached, _, _, ok := c.DiskCoverage("entry", "video.mkv"); !ok || cached != 1024 {
		t.Fatalf("own sidecar: DiskCoverage = %d, %v; want 1024, true", cached, ok)
	}
	if !c.DiskHasRange("entry", "video.mkv", 0, 512) {
		t.Fatal("own sidecar: DiskHasRange = false")
	}

	c.identity = func(string, string) (string, int64, bool) { return "new", 1024, true }
	if _, _, _, ok := c.DiskCoverage("entry", "video.mkv"); ok {
		t.Fatal("another grab's sidecar reported as coverage")
	}
	if c.DiskHasRange("entry", "video.mkv", 0, 512) {
		t.Fatal("another grab's sidecar reported a cached range")
	}
}
