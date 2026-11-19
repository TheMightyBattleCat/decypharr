package reader

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/nntp"
)

// serveYencPart starts a fake NNTP server that answers every BODY with one
// yEnc part of a multi-part posted file: n bytes of 'a', claimed as part
// part of total, starting at begin (1-based) in a file of fileSize bytes.
func serveYencPart(t *testing.T, n, part, total, begin, fileSize int) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var enc strings.Builder
	fmt.Fprintf(&enc, "=ybegin part=%d total=%d line=128 size=%d name=foreign.bin\r\n", part, total, fileSize)
	fmt.Fprintf(&enc, "=ypart begin=%d end=%d\r\n", begin, begin+n-1)
	for i := range n {
		enc.WriteByte('a') // decodes to 'a'-42: no escaping needed
		if (i+1)%128 == 0 {
			enc.WriteString("\r\n")
		}
	}
	if n%128 != 0 {
		enc.WriteString("\r\n")
	}
	fmt.Fprintf(&enc, "=yend size=%d part=%d\r\n", n, part)
	body := enc.String()

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				r := bufio.NewReader(c)
				_, _ = c.Write([]byte("200 fake ready\r\n"))
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					cmd, arg, _ := strings.Cut(strings.TrimRight(line, "\r\n"), " ")
					switch strings.ToUpper(cmd) {
					case "BODY":
						_, _ = c.Write([]byte("222 0 " + arg + "\r\n" + body + ".\r\n"))
					case "QUIT":
						_, _ = c.Write([]byte("205 bye\r\n"))
						return
					case "DATE":
						_, _ = c.Write([]byte("111 20261118120000\r\n"))
					default:
						_, _ = c.Write([]byte("500 unknown command\r\n"))
					}
				}
			}(c)
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port
}

// bodyForeign returns each provider's body_foreign stat, by host.
func bodyForeign(t *testing.T, c *nntp.Client) map[string]int64 {
	t.Helper()
	provs, _ := c.Stats()["providers"].([]map[string]any)
	out := map[string]int64{}
	for _, p := range provs {
		n, ok := p["body_foreign"].(int64)
		if !ok {
			t.Fatalf("provider %v has no body_foreign stat: %v", p["host"], p)
		}
		out[p["host"].(string)] = n
	}
	return out
}

// A provider that answers a segment's BODY with another upload's article (a
// part far smaller than the slot, not its file's last) is counted in that
// provider's body_foreign stat; the provider that serves the right part is
// not, and the segment is fetched from it.
func TestForeignBodyCountedAgainstItsProvider(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	// The forced-missing hook reads its env var once, on the first fetch in
	// the test binary; keep this fetch from fixing it empty for later tests
	// that set it.
	oldForced := forceMissingMessageIDs
	forceMissingMessageIDs = func() map[string]struct{} { return nil }
	t.Cleanup(func() { forceMissingMessageIDs = oldForced })

	foreignPort := serveYencPart(t, 300, 1, 3, 1, 9000) // 300 bytes: not the 1000 the slot needs
	rightPort := serveYencPart(t, 1000, 1, 3, 1, 3000)
	probe, err := net.DialTimeout("tcp", net.JoinHostPort("localhost", strconv.Itoa(rightPort)), time.Second)
	if err != nil {
		t.Skipf("localhost does not reach 127.0.0.1: %v", err)
	}
	_ = probe.Close()

	cfg := config.Get()
	oldUsenet := cfg.Usenet
	cfg.Usenet = config.Usenet{
		Providers: []config.UsenetProvider{
			{Host: "127.0.0.1", Port: foreignPort, MaxConnections: 2, Priority: 1},
			{Host: "localhost", Port: rightPort, MaxConnections: 2, Priority: 2},
		},
		MaxConnections: 4,
	}
	t.Cleanup(func() { cfg.Usenet = oldUsenet })
	client, err := nntp.NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	segments := []SegmentMeta{
		{MessageID: "<seg1@test>", Number: 1, Bytes: 1000, StartOffset: 0, EndOffset: 999},
		{MessageID: "<seg2@test>", Number: 2, Bytes: 1000, StartOffset: 1000, EndOffset: 1999},
	}
	rcfg := DefaultConfig()
	rcfg.DiskPath = t.TempDir()
	rcfg.DownloadTimeout = 5 * time.Second
	ctx := context.Background()
	stats := &ReaderStats{}
	cache, err := NewSegmentCache(ctx, segments, rcfg, stats, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewSegmentCache: %v", err)
	}
	defer cache.Close()
	sf := NewSegmentFetcher(ctx, client, cache, rcfg, stats, zerolog.Nop())
	defer sf.Close()

	fetchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := sf.Fetch(fetchCtx, 0); err != nil {
		t.Fatalf("fetch with the right part on the second provider: %v", err)
	}
	got := make([]byte, 1000)
	if n, ok := cache.ReadInto(0, got); !ok || n != len(got) {
		t.Fatalf("read back the slot: %d bytes, ok=%v", n, ok)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte{'a' - 42}, 1000)) {
		t.Fatal("slot does not hold the right provider's part")
	}

	counts := bodyForeign(t, client)
	if counts["127.0.0.1"] != 1 || counts["localhost"] != 0 {
		t.Fatalf("body_foreign = %v, want 127.0.0.1:1 localhost:0", counts)
	}
}
