package parser

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/nntp"
)

// serveStatOK is a minimal NNTP server: it greets, answers every STAT as
// present (echoing the ID) and QUIT as goodbye.
func serveStatOK(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_, _ = fmt.Fprint(conn, "200 ready\r\n")
				r := bufio.NewReader(conn)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					fields := strings.Fields(line)
					switch {
					case len(fields) == 2 && strings.EqualFold(fields[0], "STAT"):
						_, _ = fmt.Fprintf(conn, "223 0 %s\r\n", fields[1])
					case len(fields) > 0 && strings.EqualFold(fields[0], "QUIT"):
						_, _ = fmt.Fprint(conn, "205 bye\r\n")
						return
					default:
						_, _ = fmt.Fprint(conn, "500 unknown\r\n")
					}
				}
			}(conn)
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port
}

// The only primary is in its quota reserve band (Reserve = Quota, so the
// soft threshold is already crossed at zero bytes used). Bulk requests get
// "no eligible providers available"; the first STAT of a new NZB must still
// be answered, or every grab fails until the quota period resets and the
// Arr blocklists each release.
func TestParseStatIsAnsweredFromAQuotaReserve(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	port := serveStatOK(t)

	cfg := config.Get()
	oldUsenet := cfg.Usenet
	cfg.Usenet = config.Usenet{
		Providers: []config.UsenetProvider{{
			Host: "127.0.0.1", Port: port, MaxConnections: 1, Priority: 1,
			Quota: "1GB", Reserve: "1GB", QuotaPeriod: "day",
		}},
		MaxConnections: 1,
	}
	t.Cleanup(func() { cfg.Usenet = oldUsenet })
	client, err := nntp.NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	// Control: a bulk request is refused, so the provider is in its reserve.
	bulkErr := client.ExecuteWithFailover(context.Background(), func(conn *nntp.Connection) error {
		_, _, err := conn.Stat("<bulk@test>")
		return err
	})
	if bulkErr == nil || !strings.Contains(bulkErr.Error(), "no eligible providers available") {
		t.Fatalf("bulk STAT err = %v, want \"no eligible providers available\" (provider not in its reserve band)", bulkErr)
	}

	p := NewParser(client, 1, zerolog.Nop())
	if err := p.statSegment(context.Background(), "<seg@test>"); err != nil {
		t.Fatalf("parse STAT with every primary in its reserve: %v, want it answered", err)
	}
}
