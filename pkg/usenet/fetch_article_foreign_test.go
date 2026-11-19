package usenet

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

// serveYencArticle starts a fake NNTP server that answers every BODY with a
// one-part yEnc article of n bytes of 'a' claiming yEnc part part.
func serveYencArticle(t *testing.T, n, part int) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var enc strings.Builder
	fmt.Fprintf(&enc, "=ybegin part=%d total=9 line=128 size=%d name=vol.par2\r\n", part, 9*n)
	fmt.Fprintf(&enc, "=ypart begin=%d end=%d\r\n", (part-1)*n+1, part*n)
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

// An article FetchArticleChecked's check refuses (here: yEnc part 5 where
// part 2 was asked for) is counted in the body_foreign stat of the provider
// that sent it, not of the one that sent the right part, and the right part is
// returned.
func TestFetchArticleCheckedCountsForeignBodyPerProvider(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	foreignPort := serveYencArticle(t, 500, 5)
	rightPort := serveYencArticle(t, 500, 2)
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
	u := &Usenet{nntp: client, logger: zerolog.Nop()}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	data, err := u.FetchArticleChecked(ctx, "<vol-part2@test>", func(meta *nntp.YencMetadata) string {
		if meta == nil || meta.Part != 2 {
			return "wrong part"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("fetch with the right part on the second provider: %v", err)
	}
	if !bytes.Equal(data, bytes.Repeat([]byte{'a' - 42}, 500)) {
		t.Fatalf("got %d bytes, not the right provider's part", len(data))
	}

	provs, _ := client.Stats()["providers"].([]map[string]any)
	counts := map[string]any{}
	for _, p := range provs {
		counts[p["host"].(string)] = p["body_foreign"]
	}
	if counts["127.0.0.1"] != int64(1) || counts["localhost"] != int64(0) {
		t.Fatalf("body_foreign = %v, want 127.0.0.1:1 localhost:0", counts)
	}
}
