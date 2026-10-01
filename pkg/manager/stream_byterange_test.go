package manager

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// fakeLink stands in for a debrid download link: it serves archive and
// records the Range header of each request.
type fakeLink struct {
	*httptest.Server
	mu     sync.Mutex
	ranges []string
}

func newFakeLink(t *testing.T, archive []byte, honourRange bool) *fakeLink {
	t.Helper()
	l := &fakeLink{}
	l.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l.mu.Lock()
		l.ranges = append(l.ranges, r.Header.Get("Range"))
		l.mu.Unlock()
		if !honourRange {
			r.Header.Del("Range")
		}
		http.ServeContent(w, r, "archive.rar", time.Time{}, bytes.NewReader(archive))
	}))
	t.Cleanup(l.Close)
	return l
}

func (l *fakeLink) lastRange() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.ranges) == 0 {
		return ""
	}
	return l.ranges[len(l.ranges)-1]
}

// A byte-ranged file is one file inside a larger download (a stored RAR that
// the debrid provider did not unpack): its bytes sit at ByteRange of the
// link, and File.Size is its own length. Every caller of Manager.Stream
// passes offsets inside the file; streamLink alone moves them when it asks
// the link, and what the client is told stays in the file's coordinates.
//
// Each case runs the offsets through normalizeStreamRange first, as Stream
// does, then checks the range the link was asked for, the bytes written, and
// the status and headers handed to onReady.
func TestStreamLinkAppliesByteRangeOffset(t *testing.T) {
	archive := testPattern(5000)
	link := newFakeLink(t, archive, true)
	m := &Manager{streamClient: link.Client(), config: &config.Config{}}

	deep := &storage.File{Name: "deep.mkv", Size: 1000, ByteRange: &[2]int64{4000, 4999}}
	front := &storage.File{Name: "front.mkv", Size: 1000, ByteRange: &[2]int64{64, 1063}}
	zero := &storage.File{Name: "zero.mkv", Size: 1000, ByteRange: &[2]int64{0, 999}}
	whole := &storage.File{Name: "whole.mkv", Size: 5000}

	cases := []struct {
		name       string
		file       *storage.File
		start, end int64 // what the caller hands Stream
		wantRange  string
		wantOff    int64 // where the bytes written sit in the archive
		wantLen    int64
		wantStatus int
		wantCR     string
	}{
		// WebDAV and /stream with no Range header, and the share / .strm
		// pipe from offset 0.
		{"deep slice, whole file", deep, 0, -1, "bytes=4000-4999", 4000, 1000, http.StatusOK, ""},
		// The mount's first read, or "bytes=0-99".
		{"deep slice, first 100 bytes", deep, 0, 99, "bytes=4000-4099", 4000, 100, http.StatusPartialContent, "bytes 0-99/1000"},
		// A player reading an MKV index; this was refused, or read from
		// offset 950 of the archive.
		{"deep slice, last 50 bytes", deep, 950, 999, "bytes=4950-4999", 4950, 50, http.StatusPartialContent, "bytes 950-999/1000"},
		// The pipe resuming mid-file.
		{"deep slice, open-ended from 900", deep, 900, -1, "bytes=4900-4999", 4900, 100, http.StatusPartialContent, "bytes 900-999/1000"},
		// One file stored at the front of an archive: this lost its last 64
		// bytes.
		{"front slice, whole file", front, 0, -1, "bytes=64-1063", 64, 1000, http.StatusOK, ""},
		{"front slice, last 50 bytes", front, 950, 999, "bytes=1014-1063", 1014, 50, http.StatusPartialContent, "bytes 950-999/1000"},
		// A slice that starts at the link's first byte still must not
		// report the archive's size.
		{"slice at offset zero, whole file", zero, 0, -1, "bytes=0-999", 0, 1000, http.StatusOK, ""},
		{"slice at offset zero, middle", zero, 100, 199, "bytes=100-199", 100, 100, http.StatusPartialContent, "bytes 100-199/1000"},
		// A file that is the whole link is untouched.
		{"plain file, middle", whole, 100, 199, "bytes=100-199", 100, 100, http.StatusPartialContent, "bytes 100-199/5000"},
		{"plain file, tail", whole, 4900, -1, "bytes=4900-4999", 4900, 100, http.StatusPartialContent, "bytes 4900-4999/5000"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			start, end, err := normalizeStreamRange(c.file.Size, c.start, c.end)
			if err != nil {
				t.Fatalf("range %d-%d refused: %v", c.start, c.end, err)
			}

			var meta *StreamMetadata
			var body bytes.Buffer
			err = m.streamLink(context.Background(), link.URL, c.file, start, end, &body, func(sm *StreamMetadata) error {
				meta = sm
				return nil
			})
			if err != nil {
				t.Fatalf("streamLink: %v", err)
			}
			if got := link.lastRange(); got != c.wantRange {
				t.Errorf("link asked for %q, want %q", got, c.wantRange)
			}
			if want := archive[c.wantOff : c.wantOff+c.wantLen]; !bytes.Equal(body.Bytes(), want) {
				t.Errorf("wrote %d bytes that are not archive[%d:%d]", body.Len(), c.wantOff, c.wantOff+c.wantLen)
			}
			if meta == nil {
				t.Fatal("onReady not called")
			}
			if meta.StatusCode != c.wantStatus {
				t.Errorf("status %d, want %d", meta.StatusCode, c.wantStatus)
			}
			if meta.ContentLength != c.wantLen {
				t.Errorf("content length %d, want %d", meta.ContentLength, c.wantLen)
			}
			if got := meta.Header.Get("Content-Range"); got != c.wantCR {
				t.Errorf("Content-Range %q, want %q", got, c.wantCR)
			}

			// The mount passes no onReady: same request, same bytes.
			body.Reset()
			if err := m.streamLink(context.Background(), link.URL, c.file, start, end, &body, nil); err != nil {
				t.Fatalf("streamLink without onReady: %v", err)
			}
			if got := link.lastRange(); got != c.wantRange {
				t.Errorf("without onReady: link asked for %q, want %q", got, c.wantRange)
			}
			if want := archive[c.wantOff : c.wantOff+c.wantLen]; !bytes.Equal(body.Bytes(), want) {
				t.Errorf("without onReady: wrote %d bytes that are not archive[%d:%d]", body.Len(), c.wantOff, c.wantOff+c.wantLen)
			}
		})
	}
}

// A link that answers a range request with the whole archive must not be
// passed on as the file: its first bytes are the archive header.
func TestStreamLinkRefusesIgnoredByteRange(t *testing.T) {
	link := newFakeLink(t, testPattern(5000), false)
	m := &Manager{streamClient: link.Client(), config: &config.Config{}}
	file := &storage.File{Name: "deep.mkv", Size: 1000, ByteRange: &[2]int64{4000, 4999}}

	var body bytes.Buffer
	ready := false
	err := m.streamLink(context.Background(), link.URL, file, 0, 99, &body, func(*StreamMetadata) error {
		ready = true
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "ignored the byte range") {
		t.Fatalf("err = %v, want the ignored-range error", err)
	}
	if ready || body.Len() != 0 {
		t.Fatalf("headers sent = %v, %d bytes written; want nothing sent", ready, body.Len())
	}
}
