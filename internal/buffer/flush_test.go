package buffer

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// Flush must put a RAM-resident block's bytes into the disk file while the
// Buffer stays open. DFS records "this range is cached" on its own schedule;
// a process that exits before the block is evicted or the Buffer closed would
// otherwise leave the range recorded with only zeros on disk.
func TestFlushWritesRAMBlocksToDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "item")
	pool := NewPool(PoolConfig{Name: "test"})
	t.Cleanup(func() { _ = pool.Close() })

	b, err := pool.NewBuffer(Config{MemorySize: 8 << 20, DiskPath: path, TotalSize: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })

	want := bytes.Repeat([]byte{0x5A}, blockSize)
	if _, err := b.WriteAt(want, blockSize); err != nil {
		t.Fatal(err)
	}

	onDisk := func() []byte {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return data[blockSize : 2*blockSize]
	}
	if bytes.Equal(onDisk(), want) {
		t.Fatal("precondition: the write should still be RAM-only before Flush")
	}

	if err := b.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if !bytes.Equal(onDisk(), want) {
		t.Fatal("Flush left the block's bytes out of the disk file")
	}
}
