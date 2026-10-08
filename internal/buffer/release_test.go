package buffer

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
)

// segSize is deliberately not a multiple of blockSize: like a usenet
// article (~700 KB against 1 MB blocks), nearly every segment shares its
// first and last block with a neighbour.
const segSize = 700 * 1000

func segData(i int) []byte { return bytes.Repeat([]byte{byte(i%250 + 1)}, segSize) }

// allocated is how much of the file the filesystem has given blocks to.
func allocated(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Sys().(*syscall.Stat_t).Blocks * 512
}

func newTestBuffer(t *testing.T, mem, total int64) (*Buffer, *Pool, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "scratch")
	pool := NewPool(PoolConfig{Name: "test"})
	t.Cleanup(func() { _ = pool.Close() })
	b, err := pool.NewBuffer(Config{MemorySize: mem, DiskPath: path, TotalSize: total})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b, pool, path
}

// The scratch workload this package now has to carry without touching the
// disk: a long file written in segment-sized pieces, each batch read back and
// discarded before the next is written, with the data in flight always well
// inside the RAM ceiling. Nothing may be flushed or written through, the
// file must stay empty, and no hole is punched for bytes that were never in
// it.
//
// Segments are discarded one at a time on purpose. Each block shared by two
// segments is then trimmed from both sides and never lies "fully inside" a
// discard; such blocks used to stay resident, empty, until they filled the
// RAM ceiling and every later write went to disk.
func TestWriteReadDiscardWithinCeilingNeverTouchesDisk(t *testing.T) {
	const (
		mem      = 16 << 20
		segments = 600 // ~420 MB, 26 times the ceiling
		batch    = 8   // ~5.6 MB in flight
	)
	b, pool, path := newTestBuffer(t, mem, segments*segSize)

	got := make([]byte, segSize)
	for first := 0; first < segments; first += batch {
		for i := first; i < first+batch; i++ {
			if _, err := b.WriteAt(segData(i), int64(i)*segSize); err != nil {
				t.Fatal(err)
			}
		}
		for i := first; i < first+batch; i++ {
			if _, err := b.ReadAt(got, int64(i)*segSize); err != nil {
				t.Fatalf("segment %d: %v", i, err)
			}
			if !bytes.Equal(got, segData(i)) {
				t.Fatalf("segment %d read back wrong", i)
			}
			if err := b.Discard(int64(i)*segSize, segSize); err != nil {
				t.Fatal(err)
			}
		}
		if st := b.Stats(); st.BlocksInRAM != 0 {
			t.Fatalf("after releasing segments %d-%d: %d blocks still resident, want 0", first, first+batch-1, st.BlocksInRAM)
		}
	}

	ps := pool.Stats()
	if ps.FlushedBytes != 0 || ps.WriteThroughBytes != 0 {
		t.Fatalf("flushed %d and wrote through %d bytes, want none on disk", ps.FlushedBytes, ps.WriteThroughBytes)
	}
	if want := int64(segments) * segSize; ps.WrittenBytes != want || ps.ReleasedInRAMBytes != want {
		t.Fatalf("written %d, released in RAM %d; want %d for both", ps.WrittenBytes, ps.ReleasedInRAMBytes, want)
	}
	if st := b.Stats(); st.HolesPunched != 0 {
		t.Fatalf("punched %d holes for bytes that never reached the file", st.HolesPunched)
	}
	if n := allocated(t, path); n != 0 {
		t.Fatalf("scratch file has %d bytes allocated, want 0", n)
	}
	if ps.MemoryInUse != 0 || ps.DiskInUse != 0 {
		t.Fatalf("pool still accounts %d bytes of RAM and %d of disk", ps.MemoryInUse, ps.DiskInUse)
	}
}

// Discarding one segment must not disturb the neighbour it shares a block
// with: the neighbour reads back intact from RAM and still reaches the file
// when the buffer is flushed.
func TestDiscardKeepsTheNeighbourSharingABlock(t *testing.T) {
	b, _, path := newTestBuffer(t, 16<<20, 4*segSize)
	for i := range 3 {
		if _, err := b.WriteAt(segData(i), int64(i)*segSize); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Discard(segSize, segSize); err != nil { // the middle one
		t.Fatal(err)
	}

	got := make([]byte, segSize)
	for _, i := range []int{0, 2} {
		if _, err := b.ReadAt(got, int64(i)*segSize); err != nil || !bytes.Equal(got, segData(i)) {
			t.Fatalf("segment %d after its neighbour was discarded: err=%v, intact=%v", i, err, bytes.Equal(got, segData(i)))
		}
	}
	if _, err := b.ReadAt(got, segSize); err != ErrNotPresent {
		t.Fatalf("discarded segment read = %v, want ErrNotPresent", err)
	}

	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	file, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, 2} {
		if !bytes.Equal(file[i*segSize:(i+1)*segSize], segData(i)) {
			t.Fatalf("segment %d did not reach the file intact", i)
		}
	}
	if !bytes.Equal(file[segSize:2*segSize], make([]byte, segSize)) {
		t.Fatal("bytes of the discarded segment were flushed to the file")
	}
}

// Past the RAM ceiling the buffer still works exactly as before: writes go
// through to the file, read back correctly, and discarding them punches the
// file. Only the saving is lost, never the data.
func TestBeyondTheCeilingDataGoesToDiskAndIsPunchedOnDiscard(t *testing.T) {
	const segments = 12 // ~8.4 MB against a 2 MB ceiling
	b, pool, path := newTestBuffer(t, 2<<20, segments*segSize)
	for i := range segments {
		if _, err := b.WriteAt(segData(i), int64(i)*segSize); err != nil {
			t.Fatal(err)
		}
	}
	if ps := pool.Stats(); ps.WriteThroughBytes == 0 {
		t.Fatal("nothing was written through although the data is four times the ceiling")
	}
	got := make([]byte, segSize)
	for i := range segments {
		if _, err := b.ReadAt(got, int64(i)*segSize); err != nil || !bytes.Equal(got, segData(i)) {
			t.Fatalf("segment %d: err=%v, intact=%v", i, err, bytes.Equal(got, segData(i)))
		}
	}
	if allocated(t, path) == 0 {
		t.Fatal("precondition: the written-through data should occupy the file")
	}

	if err := b.Discard(0, segments*segSize); err != nil {
		t.Fatal(err)
	}
	st := b.Stats()
	if st.HolesPunched == 0 {
		t.Fatal("discarding data that was on disk punched nothing")
	}
	if st.BlocksInRAM != 0 || st.BytesPresent != 0 {
		t.Fatalf("after discarding everything: %d blocks resident, %d bytes present", st.BlocksInRAM, st.BytesPresent)
	}
	// Hole punching is Linux-only (punch_other.go is a no-op).
	if n := allocated(t, path); n != 0 && runtime.GOOS == "linux" {
		t.Fatalf("file still has %d bytes allocated after the punch", n)
	}
}

// A block that was flushed and is then discarded is on disk, however it got
// there, so the discard has to punch it.
func TestDiscardPunchesWhatAFlushPutOnDisk(t *testing.T) {
	b, _, path := newTestBuffer(t, 16<<20, 4*segSize)
	if _, err := b.WriteAt(segData(0), 0); err != nil {
		t.Fatal(err)
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if allocated(t, path) == 0 {
		t.Fatal("precondition: the flushed segment should occupy the file")
	}
	if err := b.Discard(0, segSize); err != nil {
		t.Fatal(err)
	}
	if b.Stats().HolesPunched != 1 {
		t.Fatalf("holes punched = %d, want 1", b.Stats().HolesPunched)
	}
}
