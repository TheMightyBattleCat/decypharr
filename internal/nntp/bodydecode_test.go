package nntp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"net"
	"strings"
	"testing"
	"time"

	nntpyenc "github.com/sirrobot01/decypharr/internal/nntp/yenc"
)

// bodyPayload returns bytes from 'A'..'Z' cycling; every encoded byte
// (b+42) lands outside yEnc's escape set, so the wire form needs no escapes.
func bodyPayload(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = 'A' + byte(i%26)
	}
	return p
}

// encodeBody produces the yEnc article body for a payload from bodyPayload,
// with a correct pcrc32 so decodes exercise CRC verification.
func encodeBody(payload []byte) string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "=ybegin part=1 line=128 size=%d name=test.bin\r\n", len(payload))
	fmt.Fprintf(&buf, "=ypart begin=1 end=%d\r\n", len(payload))
	for i := 0; i < len(payload); i += 128 {
		for _, b := range payload[i:min(i+128, len(payload))] {
			buf.WriteByte(b + 42)
		}
		buf.WriteString("\r\n")
	}
	fmt.Fprintf(&buf, "=yend size=%d pcrc32=%08x\r\n", len(payload), crc32.ChecksumIEEE(payload))
	return buf.String()
}

func newBodyTestConn(t *testing.T) (*Connection, net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	c := &Connection{
		conn:   client,
		reader: bufio.NewReaderSize(client, 128*1024),
		writer: bufio.NewWriterSize(client, 4*1024),
	}
	return c, server
}

// serveResponses answers each incoming command line with the next canned
// response, then keeps the pipe open.
func serveResponses(t *testing.T, server net.Conn, responses ...string) {
	t.Helper()
	go func() {
		reader := bufio.NewReader(server)
		for _, resp := range responses {
			if _, err := reader.ReadString('\n'); err != nil {
				return
			}
			if _, err := server.Write([]byte(resp)); err != nil {
				return
			}
		}
	}()
}

func TestRequestBodyDecodesAndReusesConnection(t *testing.T) {
	c, server := newBodyTestConn(t)

	first := bodyPayload(300 * 1024)
	second := bodyPayload(64 * 1024)
	serveResponses(t, server,
		"222 0 <a@b> body\r\n"+encodeBody(first)+".\r\n",
		"222 0 <c@d> body\r\n"+encodeBody(second)+".\r\n",
	)

	for i, want := range [][]byte{first, second} {
		res, err := c.requestBody("<x@y>", timeouts.StreamBodyTimeout)
		if err != nil {
			t.Fatalf("requestBody %d: %v", i+1, err)
		}
		if !bytes.Equal(res.Data, want) {
			t.Fatalf("article %d: got %d bytes, want %d", i+1, len(res.Data), len(want))
		}
		if res.Meta.FileName != "test.bin" {
			t.Errorf("article %d FileName = %q", i+1, res.Meta.FileName)
		}
		if res.Meta.PartSize != int64(len(want)) {
			t.Errorf("article %d PartSize = %d, want %d", i+1, res.Meta.PartSize, len(want))
		}
		putBodyBuf(res.Data)
	}
}

func TestRequestBodyStatusNotFound(t *testing.T) {
	c, server := newBodyTestConn(t)
	serveResponses(t, server, "430 no such article\r\n")

	_, err := c.requestBody("<gone@b>", timeouts.StreamBodyTimeout)
	var nntpErr *Error
	if !errors.As(err, &nntpErr) || nntpErr.Type != ErrorTypeArticleNotFound {
		t.Fatalf("err = %v, want ErrorTypeArticleNotFound", err)
	}
}

func TestRequestBodyCrcMismatch(t *testing.T) {
	c, server := newBodyTestConn(t)

	payload := bodyPayload(4 * 1024)
	body := encodeBody(payload)
	good := fmt.Sprintf("pcrc32=%08x", crc32.ChecksumIEEE(payload))
	serveResponses(t, server, "222 0 <a@b> body\r\n"+strings.Replace(body, good, "pcrc32=deadbeef", 1)+".\r\n")

	_, err := c.requestBody("<a@b>", timeouts.StreamBodyTimeout)
	if !errors.Is(err, nntpyenc.ErrCrcMismatch) {
		t.Fatalf("err = %v, want ErrCrcMismatch", err)
	}
	var nntpErr *Error
	if !errors.As(err, &nntpErr) || nntpErr.Type != ErrorTypeYencDecode {
		t.Fatalf("err = %v, want ErrorTypeYencDecode", err)
	}
}

// A body with no yEnc data stays a corrupt-article error, as it was with the
// old decoder, so failover asks the next provider for an intact copy.
func TestRequestBodyNonYencBody(t *testing.T) {
	c, server := newBodyTestConn(t)
	serveResponses(t, server, "222 0 <a@b> body\r\nplain text, no yEnc here\r\n.\r\n")

	_, err := c.requestBody("<a@b>", timeouts.StreamBodyTimeout)
	if !IsCorruptArticleError(err) {
		t.Fatalf("err = %v, want a corrupt-article error for a yEnc-less body", err)
	}
}

// A decode check that fails at the terminator leaves the connection at a
// clean response boundary: it stays open and serves the next article.
func TestRequestBodyCorruptArticleKeepsConnection(t *testing.T) {
	c, server := newBodyTestConn(t)

	bad := bodyPayload(4 * 1024)
	good := bodyPayload(8 * 1024)
	body := encodeBody(bad)
	crc := fmt.Sprintf("pcrc32=%08x", crc32.ChecksumIEEE(bad))
	serveResponses(t, server,
		"222 0 <a@b> body\r\n"+strings.Replace(body, crc, "pcrc32=deadbeef", 1)+".\r\n",
		"222 0 <c@d> body\r\n"+encodeBody(good)+".\r\n",
	)

	if _, err := c.requestBody("<a@b>", timeouts.StreamBodyTimeout); !IsCorruptArticleError(err) {
		t.Fatalf("first article: err = %v, want a corrupt-article error", err)
	}
	if c.IsClosed() {
		t.Fatal("connection closed after a fully read corrupt article")
	}
	res, err := c.requestBody("<c@d>", timeouts.StreamBodyTimeout)
	if err != nil {
		t.Fatalf("second article: %v", err)
	}
	if !bytes.Equal(res.Data, good) {
		t.Fatalf("second article: got %d bytes, want %d", len(res.Data), len(good))
	}
}

// A body cut off mid-transfer may leave part of the article on the wire, so
// the connection is closed rather than reused.
func TestRequestBodyMidStreamFailureClosesConnection(t *testing.T) {
	c, server := newBodyTestConn(t)
	body := encodeBody(bodyPayload(64 * 1024))
	go func() {
		reader := bufio.NewReader(server)
		if _, err := reader.ReadString('\n'); err != nil {
			return
		}
		_, _ = server.Write([]byte("222 0 <a@b> body\r\n" + body[:len(body)/2]))
		_ = server.Close()
	}()

	if _, err := c.requestBody("<a@b>", timeouts.StreamBodyTimeout); err == nil {
		t.Fatal("expected an error")
	}
	if !c.IsClosed() {
		t.Fatal("connection left open after a half-read body")
	}
}

// A short timeout (the PAR2 size probe's) is enforced with a socket read
// deadline, not the janitor's 5 s sweep, so a stalled article fails on time.
func TestShortTimeoutFailsWithoutJanitorDelay(t *testing.T) {
	c, server := newBodyTestConn(t)
	go func() {
		reader := bufio.NewReader(server)
		if _, err := reader.ReadString('\n'); err != nil {
			return
		}
		_, _ = server.Write([]byte("222 0 <a@b> body\r\n=ybegin part=1 line=128 size=4096 name=x\r\n"))
		// ...and never sends the rest.
	}()

	start := time.Now()
	_, err := c.GetHeaderPrefixWithTimeout("<a@b>", 16, 500*time.Millisecond)
	if err == nil {
		t.Fatal("expected a timeout")
	}
	if !IsTimeoutError(err) {
		t.Fatalf("err = %v, want a timeout error", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("stalled probe took %v; the short timeout waited for the janitor", elapsed)
	}
}

// Where the new decoder and the old one disagree. Both demand that the
// decoded length match the declared part size, so on success they report
// the same metadata; they differ on which header gives that size and which
// CRC they check.
func TestDecoderHeaderEdgeCases(t *testing.T) {
	payload := bodyPayload(4 * 1024)
	crc := crc32.ChecksumIEEE(payload)
	header := fmt.Sprintf("=ybegin part=1 total=2 line=128 size=%d name=x\r\n=ypart begin=1 end=%d\r\n", 2*len(payload), len(payload))
	var lines strings.Builder
	for i := 0; i < len(payload); i += 128 {
		for _, b := range payload[i:min(i+128, len(payload))] {
			lines.WriteByte(b + 42)
		}
		lines.WriteString("\r\n")
	}

	// A =ypart range claiming 1 KB more than the article carries.
	longHeader := fmt.Sprintf("=ybegin part=1 total=2 line=128 size=%d name=x\r\n=ypart begin=1 end=%d\r\n", 2*len(payload), len(payload)+1024)

	cases := []struct {
		name    string
		header  string
		trailer string
		wantErr error
	}{
		// The =ypart range decides the part size now. The old decoder took
		// =yend size= instead and rejected this article.
		{"yend size disagrees, data matches ypart", header, fmt.Sprintf("=yend size=%d pcrc32=%08x\r\n", len(payload)-1, crc), nil},
		// ...and accepted this one, as a short article.
		{"ypart claims more than data and yend size", longHeader, fmt.Sprintf("=yend size=%d pcrc32=%08x\r\n", len(payload), crc), nntpyenc.ErrDataCorruption},
		// The old decoder rejected a trailer with no size=.
		{"yend without size", header, fmt.Sprintf("=yend part=1 pcrc32=%08x\r\n", crc), nil},
		// The old decoder ignored crc32= on a multi-part article (it is the
		// whole file's CRC); the new one checks it when pcrc32= is absent.
		{"multi-part with only crc32", header, "=yend size=4096 crc32=deadbeef\r\n", nntpyenc.ErrCrcMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, server := newBodyTestConn(t)
			serveResponses(t, server, "222 0 <a@b> body\r\n"+tc.header+lines.String()+tc.trailer+".\r\n")
			res, err := c.requestBody("<a@b>", timeouts.StreamBodyTimeout)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("err = %v, want success", err)
				}
				if res.Meta.PartSize != int64(len(payload)) || res.Meta.Offset != 0 {
					t.Fatalf("meta = %+v", res.Meta)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) || !IsCorruptArticleError(err) {
				t.Fatalf("err = %v, want corrupt-article %v", err, tc.wantErr)
			}
		})
	}
}

func TestRequestBodyMidStreamDisconnect(t *testing.T) {
	c, server := newBodyTestConn(t)

	body := encodeBody(bodyPayload(64 * 1024))
	go func() {
		reader := bufio.NewReader(server)
		if _, err := reader.ReadString('\n'); err != nil {
			return
		}
		_, _ = server.Write([]byte("222 0 <a@b> body\r\n" + body[:len(body)/2]))
		_ = server.Close()
	}()

	_, err := c.requestBody("<a@b>", timeouts.StreamBodyTimeout)
	var nntpErr *Error
	if !errors.As(err, &nntpErr) || nntpErr.Type != ErrorTypeConnection {
		t.Fatalf("err = %v, want ErrorTypeConnection", err)
	}
}

type sizeRecordingWriter struct {
	buf   bytes.Buffer
	sizes []int
}

func (w *sizeRecordingWriter) Write(p []byte) (int, error) {
	w.sizes = append(w.sizes, len(p))
	return w.buf.Write(p)
}

// TestStreamBodySingleWrite pins the write granularity of the streaming
// path: the whole decoded article must reach the segment cache in one Write,
// so a segment costs one pwrite+lock cycle instead of one per decoder read.
func TestStreamBodySingleWrite(t *testing.T) {
	c, server := newBodyTestConn(t)

	payload := bodyPayload(300 * 1024)
	serveResponses(t, server, "222 0 <a@b> body\r\n"+encodeBody(payload)+".\r\n")

	dst := &sizeRecordingWriter{}
	n, err := c.StreamBody("<a@b>", dst)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(payload)) {
		t.Fatalf("streamed %d bytes, want %d", n, len(payload))
	}
	if !bytes.Equal(dst.buf.Bytes(), payload) {
		t.Fatal("payload corrupted")
	}
	if len(dst.sizes) != 1 {
		t.Fatalf("article written in %d writes, want 1 (sizes: %v)", len(dst.sizes), dst.sizes)
	}
}

func TestGetHeaderPrefixSnippet(t *testing.T) {
	c, server := newBodyTestConn(t)

	payload := bodyPayload(32 * 1024)
	serveResponses(t, server, "222 0 <a@b> body\r\n"+encodeBody(payload)+".\r\n")

	meta, err := c.GetHeaderPrefix("<a@b>", 100)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(meta.Snippet, payload[:100]) {
		t.Fatal("snippet mismatch")
	}
	if meta.Size != int64(len(payload)) || meta.Begin != 1 || meta.End != int64(len(payload)) {
		t.Fatalf("meta = %+v", meta)
	}
}
