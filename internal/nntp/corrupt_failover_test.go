package nntp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	nntpyenc "github.com/sirrobot01/decypharr/internal/nntp/yenc"
)

// yencWire encodes data as a one-part yEnc article body as it travels over
// NNTP: 128-column lines, dot-stuffed, without the ".\r\n" terminator. drop
// removes that many encoded payload bytes after encoding, the way a damaged
// copy on one backbone comes back a few bytes short.
func yencWire(data []byte, drop int) string {
	var enc []byte
	for _, b := range data {
		c := b + 42
		switch c {
		case 0, '\n', '\r', '=':
			enc = append(enc, '=', c+64)
		default:
			enc = append(enc, c)
		}
	}
	enc = enc[:len(enc)-drop]

	var sb strings.Builder
	fmt.Fprintf(&sb, "=ybegin part=1 total=1 line=128 size=%d name=test.bin\r\n", len(data))
	fmt.Fprintf(&sb, "=ypart begin=1 end=%d\r\n", len(data))
	for len(enc) > 0 {
		n := min(128, len(enc))
		// Don't split an escape pair across lines.
		if enc[n-1] == '=' && n < len(enc) {
			n++
		}
		line := enc[:n]
		enc = enc[n:]
		if line[0] == '.' {
			sb.WriteByte('.')
		}
		sb.Write(line)
		sb.WriteString("\r\n")
	}
	fmt.Fprintf(&sb, "=yend size=%d part=1 pcrc32=%08x\r\n", len(data), crc32.ChecksumIEEE(data))
	return sb.String()
}

func testPayload() []byte {
	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = byte(i*7 + i/300)
	}
	return data
}

// fetchBody runs StreamBodyMeta through ExecuteWithFailover, like the segment
// fetcher does.
func fetchBody(c *Client, id string, w *bytes.Buffer) error {
	return c.ExecuteWithFailover(context.Background(), func(conn *Connection) error {
		w.Reset()
		_, _, err := conn.StreamBodyMeta(id, w)
		return err
	})
}

func skipWithoutCRCDecoder(t *testing.T) {
	t.Helper()
	if !strings.HasPrefix(nntpyenc.Backend(), "rapidyenc") {
		t.Skip("the pure-Go decoder checks neither size nor CRC")
	}
}

// A copy that fails its size/CRC check on the first provider is fetched from
// the next one instead of failing the read.
func TestExecuteWithFailoverCorruptArticleTriesNextProvider(t *testing.T) {
	skipWithoutCRCDecoder(t)
	data := testPayload()
	bad := startFakeNNTP(t, 0)
	bad.bodies = map[string]string{"seg@test": yencWire(data, 36)}
	good := startFakeNNTP(t, 0)
	good.bodies = map[string]string{"seg@test": yencWire(data, 0)}
	pa, pb := twoLocalProviders(t, bad, good)
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)

	// More fetches than the bad provider has connection slots: a slot the
	// corrupt branch failed to give back would stall the later ones.
	const fetches = 3 * 4
	for i := range fetches {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var out bytes.Buffer
		err := c.ExecuteWithFailover(ctx, func(conn *Connection) error {
			out.Reset()
			_, _, err := conn.StreamBodyMeta("seg@test", &out)
			return err
		})
		cancel()
		if err != nil {
			t.Fatalf("fetch %d with an intact copy on the second provider: %v", i, err)
		}
		if !bytes.Equal(out.Bytes(), data) {
			t.Fatalf("fetch %d: got %d bytes, want the intact %d", i, out.Len(), len(data))
		}
	}
	if bad.bodyReqs.Load() != fetches || good.bodyReqs.Load() != fetches {
		t.Fatalf("BODY requests bad=%d good=%d, want %d each", bad.bodyReqs.Load(), good.bodyReqs.Load(), fetches)
	}
	if n := len(c.pools[pa.Host].slots); n != 0 {
		t.Fatalf("bad provider holds %d connection slots after the fetches, want 0", n)
	}
}

// Corrupt everywhere: the decode error comes back, still recognisable as a
// corrupt article, after every provider was asked once.
func TestExecuteWithFailoverCorruptEverywhere(t *testing.T) {
	skipWithoutCRCDecoder(t)
	data := testPayload()
	a := startFakeNNTP(t, 0)
	a.bodies = map[string]string{"seg@test": yencWire(data, 1)}
	b := startFakeNNTP(t, 0)
	b.bodies = map[string]string{"seg@test": yencWire(data, 23)}
	pa, pb := twoLocalProviders(t, a, b)
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)

	var out bytes.Buffer
	err := fetchBody(c, "seg@test", &out)
	if !IsCorruptArticleError(err) {
		t.Fatalf("error = %v, want a corrupt-article decode error", err)
	}
	if a.bodyReqs.Load() != 1 || b.bodyReqs.Load() != 1 {
		t.Fatalf("BODY requests a=%d b=%d, want 1 each", a.bodyReqs.Load(), b.bodyReqs.Load())
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

// A decode-classified error that is not about the article (here the
// destination failing) would fail the same way on every provider, so it still
// returns at once.
func TestExecuteWithFailoverWriterErrorDoesNotFailOver(t *testing.T) {
	data := testPayload()
	a := startFakeNNTP(t, 0)
	a.bodies = map[string]string{"seg@test": yencWire(data, 0)}
	b := startFakeNNTP(t, 0)
	b.bodies = map[string]string{"seg@test": yencWire(data, 0)}
	pa, pb := twoLocalProviders(t, a, b)
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)

	err := c.ExecuteWithFailover(context.Background(), func(conn *Connection) error {
		_, _, err := conn.StreamBodyMeta("seg@test", failingWriter{})
		return err
	})
	if err == nil || IsCorruptArticleError(err) {
		t.Fatalf("error = %v, want a non-corrupt decode error", err)
	}
	if got := a.bodyReqs.Load() + b.bodyReqs.Load(); got != 1 {
		t.Fatalf("%d BODY requests, want 1 (no failover)", got)
	}
}
