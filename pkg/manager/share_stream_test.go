package manager

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
)

// newTestPipeStream serves data through pipeStream the way Manager.Stream
// would: each open writes from the start offset to the end of the file.
func newTestPipeStream(data []byte, opens *atomic.Int32, fail func(open int32) error) *pipeStream {
	return &pipeStream{
		ctx:  context.Background(),
		size: int64(len(data)),
		run: func(ctx context.Context, start int64, w io.Writer) error {
			n := opens.Add(1)
			if fail != nil {
				if err := fail(n); err != nil {
					return err
				}
			}
			for off := start; off < int64(len(data)); {
				if err := ctx.Err(); err != nil {
					return err
				}
				end := min(off+4096, int64(len(data)))
				if _, err := w.Write(data[off:end]); err != nil {
					return err
				}
				off = end
			}
			return nil
		},
	}
}

func testPattern(n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(i * 7)
	}
	return data
}

func TestPipeStreamReadsWholeFile(t *testing.T) {
	data := testPattern(1 << 20)
	var opens atomic.Int32
	s := newTestPipeStream(data, &opens, nil)
	defer s.Close()

	got, err := io.ReadAll(s)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read %d bytes that differ from the %d-byte file", len(got), len(data))
	}
	if opens.Load() != 1 {
		t.Fatalf("a sequential read opened the stream %d times, want 1", opens.Load())
	}
}

func TestPipeStreamSeek(t *testing.T) {
	data := testPattern(2 << 20)
	var opens atomic.Int32
	s := newTestPipeStream(data, &opens, nil)
	defer s.Close()

	buf := make([]byte, 1000)
	read := func(at int64) {
		t.Helper()
		if _, err := s.Seek(at, io.SeekStart); err != nil {
			t.Fatalf("Seek(%d): %v", at, err)
		}
		if _, err := io.ReadFull(s, buf); err != nil {
			t.Fatalf("read at %d: %v", at, err)
		}
		if !bytes.Equal(buf, data[at:at+1000]) {
			t.Fatalf("bytes at %d differ from the file", at)
		}
	}

	read(0)
	// A short forward seek is read through on the open stream.
	read(1000 + 100_000)
	if opens.Load() != 1 {
		t.Fatalf("short forward seek opened the stream %d times, want 1", opens.Load())
	}
	// A long forward seek and a backward seek each reopen at the new offset.
	read(1_500_000)
	if opens.Load() != 2 {
		t.Fatalf("long forward seek: %d opens, want 2", opens.Load())
	}
	read(10)
	if opens.Load() != 3 {
		t.Fatalf("backward seek: %d opens, want 3", opens.Load())
	}
	if _, err := s.Seek(int64(len(data))+1, io.SeekStart); err == nil {
		t.Fatal("seek past the end of the file succeeded")
	}
}

func TestPipeStreamFailedOpenIsRetried(t *testing.T) {
	data := testPattern(64 << 10)
	boom := errors.New("provider unavailable")
	var opens atomic.Int32
	s := newTestPipeStream(data, &opens, func(open int32) error {
		if open == 1 {
			return boom
		}
		return nil
	})
	defer s.Close()

	if err := s.Prime(); !errors.Is(err, boom) {
		t.Fatalf("Prime error = %v, want the stream's error", err)
	}
	got, err := io.ReadAll(s)
	if err != nil {
		t.Fatalf("read after a failed open: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("bytes after a failed open differ from the file")
	}
}

func TestPipeStreamShortStreamIsAnError(t *testing.T) {
	data := testPattern(64 << 10)
	s := &pipeStream{
		ctx:  context.Background(),
		size: int64(len(data)),
		run: func(_ context.Context, start int64, w io.Writer) error {
			// Stops halfway without an error.
			if start < 32<<10 {
				_, err := w.Write(data[start : 32<<10])
				return err
			}
			return nil
		},
	}
	defer s.Close()

	got, err := io.ReadAll(s)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("error = %v, want io.ErrUnexpectedEOF", err)
	}
	if len(got) != 32<<10 {
		t.Fatalf("read %d bytes before the error, want %d", len(got), 32<<10)
	}
}

func TestPipeStreamCloseStopsStream(t *testing.T) {
	data := testPattern(4 << 20)
	var opens atomic.Int32
	stopped := make(chan struct{})
	s := newTestPipeStream(data, &opens, nil)
	inner := s.run
	s.run = func(ctx context.Context, start int64, w io.Writer) error {
		defer close(stopped)
		return inner(ctx, start, w)
	}

	if _, err := io.ReadFull(s, make([]byte, 100)); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("Close returned while the stream was still running")
	}
	if _, err := s.Read(make([]byte, 1)); err == nil {
		t.Fatal("Read after Close succeeded")
	}
}
