package vfs

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs/ranges"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

const (
	padEntry = "Some.Release"
	padFile  = "episode.mkv"
	padSize  = 1 << 20
)

// newPadTestItem is a writable item whose overlay reports [padStart,
// padEnd) as dead or padded, through the Cache.padRanges seam.
func newPadTestItem(t *testing.T, padStart, padEnd int64) (*Cache, *CacheItem) {
	t.Helper()
	c := newWritableTestCache(t)
	c.padRanges = func(_ *storage.Entry, _ string, off, end int64) [][2]int64 {
		s, e := max(padStart, off), min(padEnd, end)
		if s >= e {
			return nil
		}
		return [][2]int64{{s, e}}
	}
	item := newWritableTestItem(t, c, padEntry, padFile, padSize)
	item.entry = &storage.Entry{InfoHash: "nzb-pad", Name: padEntry}
	t.Cleanup(func() { _ = item.buf.Close() })
	return c, item
}

// streamInto writes p at off the way streamChunk does: through a
// generation-checked cacheWriter, then markPads on what it wrote.
func streamInto(t *testing.T, item *CacheItem, p []byte, off int64) *cacheWriter {
	t.Helper()
	dl := &downloader{dls: &Downloaders{item: item}}
	gen := item.forgetGeneration()
	w := &cacheWriter{dl: dl, item: item, offset: off, gen: gen}
	if _, err := w.Write(p); err != nil {
		t.Fatalf("write: %v", err)
	}
	item.markPads(w.wrote)
	return w
}

// A stream that wrote a pad marks exactly the padded bytes it wrote - not
// the real bytes around them, and not bytes that were already cached - and
// the mark survives a flush and a reload from the sidecar.
func TestPadMarkSavedAndReloaded(t *testing.T) {
	c, item := newPadTestItem(t, 4096, 12288)

	// [4096, 6144) was cached earlier with real bytes: not this stream's pad.
	if _, _, err := item.WriteAtNoOverwrite(bytes.Repeat([]byte{9}, 2048), 4096); err != nil {
		t.Fatalf("pre-cache: %v", err)
	}
	p := bytes.Repeat([]byte{1}, padSize)
	for i := 4096; i < 12288; i++ {
		p[i] = 0 // the zero-fill the reader served
	}
	streamInto(t, item, p, 0)

	want := ranges.Ranges{{Pos: 6144, Size: 6144}}
	if got := item.PadRanges(); !got.Equal(want) {
		t.Fatalf("marks = %+v, want %+v (pad minus the bytes already cached)", got, want)
	}

	item.flushMetadata(true)
	dataPath := filepath.Join(c.config.CacheDir, padEntry, padFile)
	info, err := c.loadSidecar(item.key, padEntry, padFile, item.metaPath, dataPath, "nzb-pad", padSize)
	if err != nil {
		t.Fatalf("loadSidecar: %v", err)
	}
	if !info.Pads.Equal(want) {
		t.Fatalf("reloaded marks = %+v, want %+v", info.Pads, want)
	}
	if !info.Rs.Present(ranges.Range{Pos: 0, Size: padSize}) {
		t.Fatalf("reloaded ranges = %+v, want the whole file", info.Rs)
	}
}

// A stream that wrote no pad marks nothing.
func TestPadMarkNoneWithoutOverlayRecords(t *testing.T) {
	_, item := newPadTestItem(t, padSize+1, padSize+2) // outside the file
	streamInto(t, item, bytes.Repeat([]byte{1}, padSize), 0)
	if got := item.PadRanges(); len(got) != 0 {
		t.Fatalf("marks = %+v, want none", got)
	}
}

// A forget between the write and the mark only stops the mark for the
// bytes it dropped: a mark there would stand for bytes no longer cached.
// A forget elsewhere in the file (a PAR2 repair's, say) must not cost this
// stream its marks, or the zeros it wrote would stay unmarked for good.
func TestPadMarkAfterForgetSinceWrite(t *testing.T) {
	t.Run("forget of the pad itself", func(t *testing.T) {
		_, item := newPadTestItem(t, 0, 4096)
		dl := &downloader{dls: &Downloaders{item: item}}
		w := &cacheWriter{dl: dl, item: item, gen: item.forgetGeneration()}
		if _, err := w.Write(make([]byte, 8192)); err != nil {
			t.Fatalf("write: %v", err)
		}
		item.ForgetRange(0, 4096)
		if n := item.markPads(w.wrote); n != 0 {
			t.Fatalf("marked %d forgotten bytes, want 0", n)
		}
		if got := item.PadRanges(); len(got) != 0 {
			t.Fatalf("marks = %+v, want none", got)
		}
	})
	t.Run("forget of an unrelated range", func(t *testing.T) {
		_, item := newPadTestItem(t, 0, 4096)
		dl := &downloader{dls: &Downloaders{item: item}}
		w := &cacheWriter{dl: dl, item: item, gen: item.forgetGeneration()}
		if _, err := w.Write(make([]byte, 8192)); err != nil {
			t.Fatalf("write: %v", err)
		}
		item.ForgetRange(6144, 1024)
		want := ranges.Ranges{{Pos: 0, Size: 4096}}
		if n := item.markPads(w.wrote); n != 4096 {
			t.Fatalf("marked %d bytes, want 4096", n)
		}
		if got := item.PadRanges(); !got.Equal(want) {
			t.Fatalf("marks = %+v, want %+v", got, want)
		}
	})
}

// Removing the overlay records forgets the marked range of an open item: a
// read finds it missing (so it is fetched again), the rest stays cached,
// and the marks are gone.
func TestForgetPaddedRangesOpenItemRefetches(t *testing.T) {
	c, item := newPadTestItem(t, 4096, 8192)
	streamInto(t, item, bytes.Repeat([]byte{1}, padSize), 0)
	m := &Manager{cache: c}

	if n := m.ForgetPaddedRanges(padEntry, padFile); n != 4096 {
		t.Fatalf("forgot %d bytes, want 4096", n)
	}
	pad := ranges.Range{Pos: 4096, Size: 4096}
	if got := item.FindMissing(pad); got != pad {
		t.Fatalf("missing in the marked range after forget = %+v, want all of %+v", got, pad)
	}
	if !item.HasRange(ranges.Range{Pos: 0, Size: 4096}) || !item.HasRange(ranges.Range{Pos: 8192, Size: padSize - 8192}) {
		t.Fatal("bytes outside the mark were forgotten too")
	}
	if got := item.PadRanges(); len(got) != 0 {
		t.Fatalf("marks after forget = %+v, want none", got)
	}
	// A second removal finds nothing left to forget.
	if n := m.ForgetPaddedRanges(padEntry, padFile); n != 0 {
		t.Fatalf("second forget = %d bytes, want 0", n)
	}
}

// The same for an item that is not open: the sidecar loses the marked
// range and the marks, and a reopen would fetch it again.
func TestForgetPaddedRangesClosedItemRewritesSidecar(t *testing.T) {
	c, item := newPadTestItem(t, 4096, 8192)
	streamInto(t, item, bytes.Repeat([]byte{1}, padSize), 0)
	item.flushMetadata(true)
	c.items.Delete(item.key) // closed: only the sidecar is left
	m := &Manager{cache: c}

	if n := m.ForgetPaddedRanges(padEntry, padFile); n != 4096 {
		t.Fatalf("forgot %d bytes, want 4096", n)
	}
	var info ItemInfo
	if err := decodeJSONFile(item.metaPath, &info); err != nil {
		t.Fatalf("read sidecar: %v", err)
	}
	if len(info.Pads) != 0 {
		t.Fatalf("sidecar marks = %+v, want none", info.Pads)
	}
	want := ranges.Ranges{{Pos: 0, Size: 4096}, {Pos: 8192, Size: padSize - 8192}}
	if !info.Rs.Equal(want) {
		t.Fatalf("sidecar ranges = %+v, want %+v (the marked range dropped)", info.Rs, want)
	}
}

// A repaired or patched range (ForgetRange) drops the marks inside it, and
// only those.
func TestForgetRangeDropsMarksInRange(t *testing.T) {
	c, item := newPadTestItem(t, 4096, 12288)
	streamInto(t, item, bytes.Repeat([]byte{1}, padSize), 0)
	item.ForgetRange(4096, 4096)
	want := ranges.Ranges{{Pos: 8192, Size: 4096}}
	if got := item.PadRanges(); !got.Equal(want) {
		t.Fatalf("marks = %+v, want %+v", got, want)
	}

	// The closed-item path (forgetDiskRange) does the same to the sidecar.
	item.flushMetadata(true)
	c.items.Delete(item.key)
	c.forgetDiskRange(padEntry, padFile, 8192, 4096)
	var info ItemInfo
	if err := decodeJSONFile(item.metaPath, &info); err != nil {
		t.Fatalf("read sidecar: %v", err)
	}
	if len(info.Pads) != 0 {
		t.Fatalf("sidecar marks after forgetDiskRange = %+v, want none", info.Pads)
	}
}

// A sidecar written before the marker existed loads unchanged, with no
// marks, and an item with no marks writes none.
func TestOldSidecarWithoutPadsLoads(t *testing.T) {
	c := newWritableTestCache(t)
	dir := filepath.Join(c.config.CacheDir, padEntry)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	metaPath := filepath.Join(dir, padFile+".json")
	old := `{"info_hash":"nzb-pad","size":1048576,"ranges":[{"Pos":0,"Size":65536}],"mod_time":"2026-11-01T00:00:00Z","atime":"2026-11-01T00:00:00Z"}`
	if err := os.WriteFile(metaPath, []byte(old), 0o644); err != nil {
		t.Fatal(err)
	}
	info, err := c.loadSidecar("k", padEntry, padFile, metaPath, filepath.Join(dir, padFile), "nzb-pad", padSize)
	if err != nil {
		t.Fatalf("loadSidecar: %v", err)
	}
	if len(info.Pads) != 0 || !info.Rs.Equal(ranges.Ranges{{Pos: 0, Size: 65536}}) || info.Size != padSize {
		t.Fatalf("old sidecar loaded as %+v", info)
	}
	data, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"pads"`) {
		t.Fatalf("an item with no marks wrote a pads field: %s", data)
	}
	// Forgetting marks on it is a no-op that leaves the sidecar alone.
	if n := c.forgetDiskPads(padEntry, padFile); n != 0 {
		t.Fatalf("forgetDiskPads on an old sidecar = %d, want 0", n)
	}
	got, _ := os.ReadFile(metaPath)
	if string(got) != old {
		t.Fatalf("old sidecar rewritten: %s", got)
	}
}
