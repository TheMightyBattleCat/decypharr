package manager

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// shareSeekDiscardMax is how far forward a Seek reads and throws away from the
// open stream rather than reopening it at the new offset.
const shareSeekDiscardMax = 256 << 10

// StreamReader is a seekable byte stream over one file of an entry. It is the
// interface the NFS and SMB shares read through (see pkg/share). A failed
// stream is not kept: the next Read reopens it at the current offset.
type StreamReader interface {
	io.ReadCloser
	io.Seeker
	// Size returns the total file size in bytes.
	Size() int64
	// Prime opens the stream eagerly so an open failure surfaces before any
	// bytes are consumed.
	Prime() error
}

// OpenStreamForFile opens a tracked stream for a catalog FileInfo, resolving
// the backing storage entry by name. It lets the share layer stream without
// depending on storage.Entry directly. client identifies the consumer.
func (m *Manager) OpenStreamForFile(ctx context.Context, info *FileInfo, offset int64, client string) (StreamReader, error) {
	entry, err := m.GetEntryByName(info.Parent(), info.Name())
	if err != nil {
		return nil, fmt.Errorf("resolve entry for %s/%s: %w", info.Parent(), info.Name(), err)
	}
	return m.OpenStream(ctx, entry, info.Name(), offset, client)
}

// OpenStream opens a seekable stream over one file of an entry and registers
// it in the active-streams view. Connecting is lazy - use Prime to fail fast.
//
// Upstream serves this from its own stream session layer. This fork keeps its
// own playback path (Manager.Stream, with its padding, PAR2 patch and
// provider routing), so the stream here is that same path behind a pipe: each
// open runs Stream from the current offset to the end of the file in a
// goroutine, and a seek beyond shareSeekDiscardMax, or backwards, cancels it
// and starts another.
func (m *Manager) OpenStream(ctx context.Context, entry *storage.Entry, filename string, offset int64, client string) (StreamReader, error) {
	file, ok := entry.Files[filename]
	if !ok {
		return nil, fmt.Errorf("file %s not found in entry %s", filename, entry.Name)
	}
	if file.Size <= 0 {
		return nil, fmt.Errorf("file %s has invalid size %d", filename, file.Size)
	}
	if offset < 0 || offset > file.Size {
		return nil, fmt.Errorf("offset %d out of range for %s (size %d)", offset, filename, file.Size)
	}
	s := &pipeStream{
		ctx:      ctx,
		size:     file.Size,
		pos:      offset,
		streamID: m.TrackStream(entry, filename, client),
		untrack:  m.UntrackStream,
	}
	s.run = func(ctx context.Context, start int64, w io.Writer) error {
		return m.Stream(ctx, entry, filename, start, -1, w, nil, client)
	}
	return s, nil
}

// pipeStream presents Manager.Stream, which pushes a byte range into a writer,
// as a StreamReader.
type pipeStream struct {
	ctx  context.Context
	run  func(ctx context.Context, start int64, w io.Writer) error
	size int64

	streamID string
	untrack  func(string)

	mu     sync.Mutex
	pos    int64
	body   *bufio.Reader
	stop   func()
	closed bool
}

func (s *pipeStream) Size() int64 { return s.size }

// openLocked starts Stream at the current offset. The caller holds s.mu.
func (s *pipeStream) openLocked() {
	ctx, cancel := context.WithCancel(s.ctx)
	pr, pw := io.Pipe()
	start := s.pos
	done := make(chan struct{})
	go func() {
		defer close(done)
		// A nil error closes the pipe with io.EOF, which is right: Stream
		// returned after writing through to the end of the file.
		_ = pw.CloseWithError(s.run(ctx, start, pw))
	}()
	s.body = bufio.NewReaderSize(pr, 64<<10)
	s.stop = func() {
		cancel()
		// Unblock a Stream that is mid-Write, then wait for it so no fetch
		// outlives the reader that asked for it.
		_ = pr.CloseWithError(context.Canceled)
		<-done
	}
}

// dropLocked stops the running Stream, if any. The caller holds s.mu.
func (s *pipeStream) dropLocked() {
	if s.stop != nil {
		s.stop()
	}
	s.body, s.stop = nil, nil
}

func (s *pipeStream) Prime() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return io.ErrClosedPipe
	}
	if s.pos >= s.size {
		return nil
	}
	if s.body == nil {
		s.openLocked()
	}
	if _, err := s.body.Peek(1); err != nil {
		s.dropLocked()
		return err
	}
	return nil
}

func (s *pipeStream) Read(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}
	if s.pos >= s.size {
		return 0, io.EOF
	}
	if s.body == nil {
		s.openLocked()
	}
	n, err := s.body.Read(p)
	s.pos += int64(n)
	if err != nil {
		s.dropLocked()
		if errors.Is(err, io.EOF) && s.pos < s.size {
			// Stream stopped short without reporting why.
			err = io.ErrUnexpectedEOF
		}
		if n > 0 {
			// Hand over the bytes; the next Read reopens and reports.
			err = nil
		}
	}
	return n, err
}

func (s *pipeStream) Seek(offset int64, whence int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, io.ErrClosedPipe
	}
	var target int64
	switch whence {
	case io.SeekStart:
		target = offset
	case io.SeekCurrent:
		target = s.pos + offset
	case io.SeekEnd:
		target = s.size + offset
	default:
		return 0, fmt.Errorf("invalid whence %d", whence)
	}
	if target < 0 || target > s.size {
		return 0, fmt.Errorf("seek to %d out of range (size %d)", target, s.size)
	}
	if target == s.pos {
		return target, nil
	}
	if delta := target - s.pos; s.body != nil && delta > 0 && delta <= shareSeekDiscardMax {
		n, err := s.body.Discard(int(delta))
		s.pos += int64(n)
		if err == nil {
			return s.pos, nil
		}
	}
	s.dropLocked()
	s.pos = target
	return target, nil
}

func (s *pipeStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.dropLocked()
	if s.untrack != nil && s.streamID != "" {
		s.untrack(s.streamID)
	}
	return nil
}
