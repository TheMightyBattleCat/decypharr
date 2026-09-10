package vfs

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirrobot01/decypharr/internal/buffer"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs/ranges"
)

// writeSparse creates a size-byte sparse file holding data only at [off, off+n).
func writeSparse(t *testing.T, size, off, n int64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "video.mkv")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(bytes.Repeat([]byte{0xAB}, int(n)), off); err != nil {
		t.Fatal(err)
	}
	return path
}

// The shape a process that exited without flushing leaves behind: metadata
// claims the whole file, but only one MiB of it ever reached disk.
func TestDropRangesNotOnDisk_DropsClaimedHoles(t *testing.T) {
	path := writeSparse(t, 8*testMiB, 4*testMiB, testMiB)
	if extents, ok := dataExtents(path); !ok || extents.Present(ranges.Range{Pos: 0, Size: 4096}) {
		t.Skip("filesystem does not report holes")
	}

	rs := ranges.Ranges{{Pos: 0, Size: 8 * testMiB}}
	missing := dropRangesNotOnDisk(path, &rs)

	if !rs.Present(ranges.Range{Pos: 4 * testMiB, Size: testMiB}) {
		t.Fatalf("kept ranges %v lost the bytes that are on disk", rs)
	}
	if rs.Present(ranges.Range{Pos: 0, Size: 4096}) {
		t.Fatalf("kept ranges %v still claim the hole at the start of the file", rs)
	}
	// 7 MiB were never written; allow for the filesystem rounding extents to
	// its own block size.
	if missing < 6*testMiB {
		t.Fatalf("missing = %d bytes, want about 7 MiB", missing)
	}
}

func TestDropRangesNotOnDisk_KeepsWrittenRanges(t *testing.T) {
	path := writeSparse(t, 2*testMiB, 0, 2*testMiB)
	rs := ranges.Ranges{{Pos: 0, Size: 2 * testMiB}}

	if missing := dropRangesNotOnDisk(path, &rs); missing != 0 {
		t.Fatalf("missing = %d bytes, want 0 for a fully written file", missing)
	}
	if !rs.Equal(ranges.Ranges{{Pos: 0, Size: 2 * testMiB}}) {
		t.Fatalf("ranges changed to %v", rs)
	}
}

func TestDropRangesNotOnDisk_NoDataFileLeavesRangesAlone(t *testing.T) {
	rs := ranges.Ranges{{Pos: 0, Size: testMiB}}

	if missing := dropRangesNotOnDisk(filepath.Join(t.TempDir(), "absent.mkv"), &rs); missing != 0 {
		t.Fatalf("missing = %d bytes, want 0 when there is no data file to check", missing)
	}
	if !rs.Equal(ranges.Ranges{{Pos: 0, Size: testMiB}}) {
		t.Fatalf("ranges changed to %v", rs)
	}
}

// FlushAll runs at shutdown ahead of the unmount: it must put an open item's
// RAM-only bytes into its data file, so the metadata that already lists them is
// true even if the process exits before the item closes.
func TestCacheFlushAllWritesOpenItemsToDisk(t *testing.T) {
	cacheDir := t.TempDir()
	dataPath := filepath.Join(cacheDir, "entry", "video.mkv")

	pool := buffer.NewPool(buffer.PoolConfig{Name: "test"})
	t.Cleanup(func() { _ = pool.Close() })
	buf, err := pool.NewBuffer(buffer.Config{DiskPath: dataPath, TotalSize: 2 * testMiB})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = buf.Close() })

	c := newTestCache(cacheDir)
	c.pool = pool
	item := &CacheItem{cache: c, key: "entry/video.mkv", buf: buf, metaPath: dataPath + ".json"}
	c.items.Store(item.key, item)

	want := bytes.Repeat([]byte{0xC3}, int(testMiB))
	if _, _, err := item.WriteAtNoOverwrite(want, 0); err != nil {
		t.Fatal(err)
	}

	c.FlushAll()

	got, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[:testMiB], want) {
		t.Fatal("FlushAll left an open item's cached bytes out of its data file")
	}
}
