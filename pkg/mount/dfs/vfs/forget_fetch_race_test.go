package vfs

import (
	"bytes"
	"errors"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs/ranges"
)

// TestForgetBetweenFetchAndWriteDropsStaleBytes covers a downloader whose
// fetch started before a ForgetRange but whose write lands after it - e.g.
// it fetched the zero-fill pad for a range, then a PAR2 repair forgot that
// range so the corrected bytes are read next time. The stale bytes must not
// be stored, the range must stay missing, and the downloader must not move
// past it, so the next fetch (or a reader's) gets the range again.
func TestForgetBetweenFetchAndWriteDropsStaleBytes(t *testing.T) {
	c := newWritableTestCache(t)
	const size = 1 << 20
	item := newWritableTestItem(t, c, "Some.Release", "episode.mkv", size)
	t.Cleanup(func() { _ = item.buf.Close() })

	dls := &Downloaders{item: item}
	dl := &downloader{dls: dls}
	full := ranges.Range{Pos: 0, Size: size}

	// The fetch starts: the writer captures the generation, as streamChunk
	// does before calling Stream.
	stale := &cacheWriter{dl: dl, item: item, gen: item.forgetGeneration()}
	pad := make([]byte, size) // what the fetch produced: a zero-fill pad

	// The repair lands while those bytes are in flight.
	item.ForgetRange(0, size)

	if _, err := stale.Write(pad); !errors.Is(err, errForgottenSinceFetch) {
		t.Fatalf("write after forget: err = %v, want errForgottenSinceFetch", err)
	}
	if !stale.forgotten.Load() {
		t.Fatal("writer not marked forgotten, so streamChunk would count the drop as a failure")
	}
	if item.HasRange(full) || item.FindMissing(full) != full {
		t.Fatalf("stale bytes stored: missing = %+v, want all of %+v", item.FindMissing(full), full)
	}
	dl.mu.Lock()
	off := dl.offset
	dl.mu.Unlock()
	if off != 0 || stale.offset != 0 || stale.written != 0 {
		t.Fatalf("downloader moved past the dropped bytes: dl.offset=%d writer.offset=%d written=%d", off, stale.offset, stale.written)
	}

	// The re-fetch (a fresh streamChunk, so a fresh generation) stores the
	// corrected bytes and a read returns them.
	fixed := make([]byte, size)
	for i := range fixed {
		fixed[i] = byte(i%251) + 1
	}
	fresh := &cacheWriter{dl: dl, item: item, gen: item.forgetGeneration()}
	if _, err := fresh.Write(fixed); err != nil {
		t.Fatalf("re-fetch write: %v", err)
	}
	if !item.HasRange(full) {
		t.Fatal("re-fetched bytes not stored")
	}
	got := make([]byte, size)
	if _, err := item.buf.ReadAt(got, 0); err != nil {
		t.Fatalf("read after re-fetch: %v", err)
	}
	if !bytes.Equal(got, fixed) {
		t.Fatal("read after re-fetch did not return the corrected bytes")
	}
}

// TestWriteWithoutForgetKeepsBytes is the control: with no forget since the
// fetch started, the generation-checked write stores the bytes as before.
func TestWriteWithoutForgetKeepsBytes(t *testing.T) {
	c := newWritableTestCache(t)
	const size = 1 << 20
	item := newWritableTestItem(t, c, "Some.Release", "episode.mkv", size)
	t.Cleanup(func() { _ = item.buf.Close() })

	// An earlier forget, before this fetch started, must not affect it.
	item.ForgetRange(0, size)

	dl := &downloader{dls: &Downloaders{item: item}}
	w := &cacheWriter{dl: dl, item: item, gen: item.forgetGeneration()}
	p := bytes.Repeat([]byte{7}, size)
	if _, err := w.Write(p); err != nil {
		t.Fatalf("write: %v", err)
	}
	if w.forgotten.Load() || !item.HasRange(ranges.Range{Pos: 0, Size: size}) {
		t.Fatalf("bytes not stored: forgotten=%v", w.forgotten.Load())
	}
}
