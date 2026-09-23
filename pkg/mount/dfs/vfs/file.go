package vfs

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
)

// StreamingFile is the FUSE file interface for VFS
type StreamingFile struct {
	item     *CacheItem
	fileSize int64
	closed   atomic.Bool

	// entry is the Manager.files entry this handle holds a reference on,
	// so ReleaseFile releases that one and not whichever entry the name
	// maps to by then (a replacement, after the item was retired).
	entry *fileEntry

	// observedPct is the read position (percent of the file, min 1) last
	// reported to pre-cache - see observeRead. 0 until the first report.
	observedPct atomic.Int32
}

// NewStreamingFile creates a new streaming file handle. It returns nil when
// the item has been claimed for teardown by the cache janitor — the caller
// must fetch a fresh item and try again (see Manager.GetFile).
func NewStreamingFile(item *CacheItem) *StreamingFile {
	if !item.Open() { // take an open reference; fails on a claimed item
		return nil
	}

	return &StreamingFile{
		item:     item,
		fileSize: item.info.Size,
	}
}

// ReadAt implements io.ReaderAt using a background context.
// Prefer ReadAtContext when a caller context is available (e.g. from a FUSE handle).
func (f *StreamingFile) ReadAt(p []byte, off int64) (int, error) {
	return f.ReadAtContext(context.Background(), p, off)
}

// ReadAtContext reads from the file, passing ctx into the download layer so
// the operation can be interrupted by a read timeout or client disconnect.
func (f *StreamingFile) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	if f.closed.Load() {
		return 0, errors.New("file closed")
	}

	if off >= f.fileSize {
		return 0, io.EOF
	}

	// Clamp read size
	readSize := int64(len(p))
	if off+readSize > f.fileSize {
		readSize = f.fileSize - off
		p = p[:readSize]
	}

	n, err := f.item.ReadAtContext(ctx, p, off)
	if n > 0 {
		f.observeRead(off)
	}

	// Handle partial read at EOF
	if n < int(readSize) && err == nil {
		err = io.EOF
	}

	return n, err
}

// observeReadStep is how far (percent of the file) a handle's reads move
// between reports to pre-cache.
const observeReadStep = 5

// observeRead reports this handle's read position to pre-cache (see
// manager.ObserveMountRead) each time it passes another observeReadStep
// percent of the file, so a fully cached file still crosses the pre-cache
// threshold. Pre-cache's own dedup keeps it to one burst or walk step.
func (f *StreamingFile) observeRead(off int64) {
	if f.fileSize <= 0 || f.item == nil || f.item.cache == nil || f.item.cache.manager == nil {
		return
	}
	pct := int32(off * 100 / f.fileSize)
	last := f.observedPct.Load()
	if !observeDue(last, pct) || !f.observedPct.CompareAndSwap(last, max(pct, 1)) {
		return
	}
	f.item.cache.manager.ObserveMountRead(f.item.entry, f.item.filename, off, f.fileSize)
}

// observeDue reports whether a read at pct should be reported, the last
// report having been at last (0: none yet).
func observeDue(last, pct int32) bool {
	return last == 0 || pct >= last+observeReadStep
}

// Size returns the file size
func (f *StreamingFile) Size() int64 {
	return f.fileSize
}

// Close closes the file handle
func (f *StreamingFile) Close() error {
	if f.closed.Swap(true) {
		return nil
	}
	f.item.Release() // Decrement opens count

	return nil
}
