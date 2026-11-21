package nntp

// Article bodies with lines before "=ybegin". rapidyenc sniffs the format
// on each such line: a line starting with a space (not all spaces) panics
// in its uuencode length check, and a line of two spaces switches it to
// uuencode, which it checks for neither size nor CRC and stops consuming
// after its first read window. Each must come back as a corrupt article
// (failover asks the next provider), at once, and leave no half-read
// connection in the pool.

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
)

func hostilePayload(seed byte) []byte {
	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = byte(i*7+i/300) ^ seed
	}
	return data
}

func oneLocalProvider(t *testing.T, s *fakeNNTP) *Client {
	t.Helper()
	p := config.UsenetProvider{Host: "127.0.0.1", Port: s.port(), Priority: 1, MaxConnections: 1}
	return newStatTestClient(t, []config.UsenetProvider{p}, 100)
}

// uuWire uuencodes data (a multiple of 45 bytes, so every line is a full
// "M" line) with begin and end lines, the way rapidyenc recognises it.
func uuWire(data []byte) string {
	enc := func(v byte) byte {
		if v == 0 {
			return '`'
		}
		return v + 32
	}
	var sb strings.Builder
	sb.WriteString("begin 644 test.bin\r\n")
	for i := 0; i < len(data); i += 45 {
		line := data[i : i+45]
		sb.WriteByte(enc(byte(len(line))))
		for j := 0; j < len(line); j += 3 {
			a, b, c := line[j], line[j+1], line[j+2]
			sb.WriteByte(enc(a >> 2))
			sb.WriteByte(enc((a<<4 | b>>4) & 63))
			sb.WriteByte(enc((b<<2 | c>>6) & 63))
			sb.WriteByte(enc(c & 63))
		}
		sb.WriteString("\r\n")
	}
	sb.WriteString("`\r\nend\r\n")
	return sb.String()
}

// fetchBodyWithin is fetchBody that fails the test, rather than hanging,
// when the read takes longer than limit.
func fetchBodyWithin(t *testing.T, c *Client, id string, w *bytes.Buffer, limit time.Duration) (error, time.Duration) {
	t.Helper()
	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- fetchBody(c, id, w) }()
	select {
	case err := <-done:
		return err, time.Since(start)
	case <-time.After(limit):
		_ = c.Close()
		<-done
		t.Fatalf("reading %s took longer than %s", id, limit)
		return nil, 0
	}
}

// Through ExecuteOnce (the parser's PAR2 size probe), a decoder panic is a
// corrupt article, the connection is not pooled, and the next article on
// that provider is read on a fresh connection.
func TestLeadingSpaceLineIsCorruptAndNotPooled(t *testing.T) {
	skipWithoutCRCDecoder(t)
	data := hostilePayload(0x11)
	s := startFakeNNTP(t, 0)
	s.bodies = map[string]string{
		"hostile@test": " x\r\n" + yencWire(data, 0),
		"good@test":    yencWire(data, 0),
	}
	c := oneLocalProvider(t, s)

	err := c.ExecuteOnce(context.Background(), func(conn *Connection) error {
		_, e := conn.GetHeaderPrefixWithTimeout("hostile@test", 0, timeouts.StreamBodyTimeout)
		return e
	})
	if customerror.IsPanicError(err) {
		t.Fatalf("the decoder panic reached the caller: %v", err)
	}
	if !IsCorruptArticleError(err) {
		t.Fatalf("ExecuteOnce on a body with a leading \" x\" line: err = %v, want a corrupt-article error", err)
	}
	if n, _ := idleConns(c, "127.0.0.1"); n != 0 {
		t.Fatalf("%d idle connection(s) pooled after the hostile body, want 0", n)
	}

	var out bytes.Buffer
	if err := fetchBody(c, "good@test", &out); err != nil {
		t.Fatalf("a normal article after the hostile one: %v", err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Fatalf("a normal article after the hostile one: %d bytes, not the article's data", out.Len())
	}
	if got := s.accepts.Load(); got != 2 {
		t.Errorf("connections accepted = %d, want 2 (the normal article on a fresh one)", got)
	}
}

// Failover asks the next provider when the first serves a body that panics
// the decoder.
func TestLeadingSpaceLineFailsOverToNextProvider(t *testing.T) {
	skipWithoutCRCDecoder(t)
	data := hostilePayload(0x44)
	bad := startFakeNNTP(t, 0)
	bad.bodies = map[string]string{"seg@test": " x\r\n" + yencWire(data, 0)}
	good := startFakeNNTP(t, 0)
	good.bodies = map[string]string{"seg@test": yencWire(data, 0)}
	pa, pb := twoLocalProviders(t, bad, good)
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)

	var out bytes.Buffer
	if err := fetchBody(c, "seg@test", &out); err != nil {
		t.Fatalf("fetch with a good copy on the second provider: %v", err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Fatalf("got %d bytes, not the article's data", out.Len())
	}
	if bad.bodyReqs.Load() == 0 || good.bodyReqs.Load() == 0 {
		t.Fatalf("BODY requests: first provider %d, second %d; want both asked", bad.bodyReqs.Load(), good.bodyReqs.Load())
	}
	if n, _ := idleConns(c, "127.0.0.1"); n != 0 {
		t.Errorf("%d idle connection(s) pooled on the provider that served the hostile body, want 0", n)
	}
}

// A body with no "=ybegin" is refused as corrupt at once, whatever rapidyenc
// makes of it: uuencode within one read window (returned as data before),
// past it (a stall for the idle deadline before), or a proper uuencoded
// body with begin and end lines (which sets a file size).
func TestBodyWithoutYencHeaderIsRefusedAtOnce(t *testing.T) {
	skipWithoutCRCDecoder(t)
	uuData := hostilePayload(0x55)[:45*20]
	cases := []struct {
		name string
		body string
		data []byte
	}{
		{"two-space line, 4096 bytes", "  \r\n" + yencWire(hostilePayload(0x33)[:4096], 0), hostilePayload(0x33)[:4096]},
		{"two-space line, 64 KiB", "  \r\n" + yencWire(hostilePayload(0x22), 0), hostilePayload(0x22)},
		{"uuencoded with begin and end", uuWire(uuData), uuData},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			good := hostilePayload(0x66)
			s := startFakeNNTP(t, 0)
			s.bodies = map[string]string{"odd@test": tc.body, "good@test": yencWire(good, 0)}
			c := oneLocalProvider(t, s)

			var out bytes.Buffer
			err, took := fetchBodyWithin(t, c, "odd@test", &out, 10*time.Second)
			if !IsCorruptArticleError(err) {
				t.Fatalf("err = %v (%d bytes, equal to the data: %v), want a corrupt-article error", err, out.Len(), bytes.Equal(out.Bytes(), tc.data))
			}
			if took > 5*time.Second {
				t.Fatalf("refused after %s, want at once", took)
			}

			out.Reset()
			if err, _ := fetchBodyWithin(t, c, "good@test", &out, 10*time.Second); err != nil {
				t.Fatalf("a normal article afterwards: %v", err)
			}
			if !bytes.Equal(out.Bytes(), good) {
				t.Fatalf("a normal article afterwards: %d bytes, not its data", out.Len())
			}
		})
	}
}

// The guard against a decoder that stops consuming must never fire on a
// good reply, however its bytes arrive: in small pieces, with the
// terminator split across reads, and back to back on one connection.
func TestRequestBodySplitTerminatorDecodes(t *testing.T) {
	skipWithoutCRCDecoder(t)
	for _, split := range []int{1, 2, 3, 4} {
		c, server := newBodyTestConn(t)
		payloads := [][]byte{bodyPayload(200 * 1024), bodyPayload(70 * 1024)}
		go func() {
			reader := bytes.NewBuffer(nil)
			buf := make([]byte, 512)
			for i, p := range payloads {
				// Wait for the BODY command line.
				for !bytes.Contains(reader.Bytes(), []byte("\n")) {
					n, err := server.Read(buf)
					if err != nil {
						return
					}
					reader.Write(buf[:n])
				}
				line, _ := reader.ReadBytes('\n')
				_ = line
				id := []string{"<a@b>", "<c@d>"}[i]
				wire := []byte("222 0 " + id + " body\r\n" + encodeBody(p) + ".\r\n")
				head, tail := wire[:len(wire)-split], wire[len(wire)-split:]
				for len(head) > 0 {
					n := min(1000, len(head))
					if _, err := server.Write(head[:n]); err != nil {
						return
					}
					head = head[n:]
				}
				time.Sleep(10 * time.Millisecond)
				if _, err := server.Write(tail); err != nil {
					return
				}
			}
		}()
		for i, want := range payloads {
			res, err := c.requestBody([]string{"<a@b>", "<c@d>"}[i], timeouts.StreamBodyTimeout)
			if err != nil {
				t.Fatalf("split %d, reply %d: %v", split, i+1, err)
			}
			if !bytes.Equal(res.Data, want) {
				t.Fatalf("split %d, reply %d: %d bytes, want %d", split, i+1, len(res.Data), len(want))
			}
			putBodyBuf(res.Data)
		}
		if c.IsClosed() {
			t.Fatalf("split %d: connection closed after good replies", split)
		}
	}
}

// ExecuteOnce pools a connection again only after a clean single-line
// status reply; any other error leaves it closed, since the reply may be
// half read.
func TestExecuteOncePoolsOnlyAfterCleanStatusReply(t *testing.T) {
	data := hostilePayload(0x77)
	s := startFakeNNTP(t, 0)
	s.bodies = map[string]string{"good@test": yencWire(data, 0)}
	c := oneLocalProvider(t, s)

	err := c.ExecuteOnce(context.Background(), func(conn *Connection) error {
		_, e := conn.GetHeaderPrefixWithTimeout("missing@test", 0, timeouts.StreamBodyTimeout)
		return e
	})
	if !IsArticleNotFoundError(err) {
		t.Fatalf("missing article: err = %v, want a 430", err)
	}
	if n, _ := idleConns(c, "127.0.0.1"); n != 1 {
		t.Fatalf("after a 430: %d idle connection(s), want 1 (a clean status reply keeps the session)", n)
	}

	own := errors.New("caller's own failure")
	err = c.ExecuteOnce(context.Background(), func(conn *Connection) error {
		if _, e := conn.GetHeaderPrefixWithTimeout("good@test", 0, timeouts.StreamBodyTimeout); e != nil {
			return e
		}
		return own
	})
	if !errors.Is(err, own) {
		t.Fatalf("err = %v, want the caller's own error", err)
	}
	if n, _ := idleConns(c, "127.0.0.1"); n != 0 {
		t.Fatalf("after an error that is not an NNTP status reply: %d idle connection(s), want 0", n)
	}
}

// requestBody closes the connection itself when the decoder left part of a
// reply unread, whoever holds the connection, so no pool can hand it out.
func TestRequestBodyClosesAfterUnreadReply(t *testing.T) {
	skipWithoutCRCDecoder(t)
	for _, tc := range []struct{ name, body string }{
		{"decoder panic", " x\r\n" + encodeBody(bodyPayload(4096))},
		{"decoder stops consuming", "  \r\n" + encodeBody(bodyPayload(64*1024))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, server := newBodyTestConn(t)
			serveResponses(t, server, "222 0 <a@b> body\r\n"+tc.body+".\r\n")
			done := make(chan error, 1)
			go func() {
				_, err := c.requestBody("<a@b>", timeouts.StreamBodyTimeout)
				done <- err
			}()
			var err error
			select {
			case err = <-done:
			case <-time.After(10 * time.Second):
				_ = c.Close()
				<-done
				t.Fatal("requestBody did not return within 10s")
			}
			if !IsCorruptArticleError(err) {
				t.Fatalf("err = %v, want a corrupt-article error", err)
			}
			if !c.IsClosed() {
				t.Fatal("connection left open with part of the reply still buffered")
			}
		})
	}
}
