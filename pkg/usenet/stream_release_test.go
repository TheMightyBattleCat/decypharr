package usenet

import (
	"errors"
	"io"
	"testing"
)

// keepingWriter stands in for the mount's cache writer.
type keepingWriter struct {
	kept    int64
	failAt  int64 // refuse the write that would pass this many bytes; 0 = never
	shortAt int64 // accept only half of the write that would pass this many; 0 = never
}

func (k *keepingWriter) Write(p []byte) (int, error) {
	if k.failAt > 0 && k.kept+int64(len(p)) > k.failAt {
		return 0, errors.New("cache write failed")
	}
	if k.shortAt > 0 && k.kept+int64(len(p)) > k.shortAt {
		k.kept += int64(len(p) / 2)
		return len(p) / 2, nil
	}
	k.kept += int64(len(p))
	return len(p), nil
}

type releaseCall struct{ off, end int64 }

// A stream into a writer that keeps it releases, from the scratch cache,
// every whole segment the writer has accepted - including the segment cut by
// the end of one release step, which the next step reaches back over - and
// never a byte the writer has not taken yet.
func TestReleasingWriterReleasesWhatTheWriterAccepted(t *testing.T) {
	const (
		start   = 5_000_000
		seg     = 716_800
		total   = 40 << 20
		bufSize = 1 << 20
	)
	keep := &keepingWriter{}
	var calls []releaseCall
	rw := &releasingWriter{w: keep, from: start, next: start, release: func(off, length int64) int {
		if end := off + length; end > start+keep.kept {
			t.Fatalf("released up to %d with only %d accepted", end, start+keep.kept)
		}
		calls = append(calls, releaseCall{off, off + length})
		return 0
	}}

	buf := make([]byte, bufSize)
	for sent := 0; sent < total; sent += bufSize {
		if n, err := rw.Write(buf); n != bufSize || err != nil {
			t.Fatalf("Write = %d, %v", n, err)
		}
	}
	rw.flush()

	if len(calls) < 2 {
		t.Fatalf("%d release calls for a 40 MB stream, want it done in steps", len(calls))
	}
	if maxCalls := total/streamReleaseStep + 2; len(calls) > maxCalls {
		t.Fatalf("%d release calls for a 40 MB stream, want at most %d", len(calls), maxCalls)
	}
	// Every segment that lies wholly inside the stream is wholly inside some
	// release call.
	first := (start + seg - 1) / seg
	for i := int64(first); (i+1)*seg <= start+total; i++ {
		covered := false
		for _, c := range calls {
			if c.off <= i*seg && (i+1)*seg <= c.end {
				covered = true
				break
			}
		}
		if !covered {
			t.Fatalf("segment %d [%d, %d) was never wholly inside a release", i, i*seg, (i+1)*seg)
		}
	}
}

// A write the writer refuses, or takes only part of, releases nothing: those
// bytes are not known to be anywhere but the scratch cache.
func TestReleasingWriterReleasesNothingForAFailedWrite(t *testing.T) {
	for name, keep := range map[string]*keepingWriter{
		"refused": {failAt: 1},
		"short":   {shortAt: 1},
	} {
		released := false
		rw := &releasingWriter{w: keep, release: func(off, length int64) int {
			released = true
			return 0
		}}
		n, err := rw.Write(make([]byte, streamReleaseStep+streamReleaseOverlap))
		if name == "refused" && err == nil {
			t.Fatalf("%s: the writer's error was swallowed", name)
		}
		if name == "short" && n == streamReleaseStep+streamReleaseOverlap {
			t.Fatalf("%s: the short count was hidden", name)
		}
		rw.flush()
		if released {
			t.Fatalf("%s write: bytes were released from the scratch cache", name)
		}
	}
}

var _ io.Writer = (*releasingWriter)(nil)
