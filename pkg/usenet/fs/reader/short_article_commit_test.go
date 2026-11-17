package reader

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/nntp"
)

// serveOneArticle starts a fake NNTP server that answers every BODY with the
// same single-part yEnc article holding data. data must not need escaping.
func serveOneArticle(t *testing.T, data []byte) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var enc strings.Builder
	fmt.Fprintf(&enc, "=ybegin line=128 size=%d name=short.bin\r\n", len(data))
	for i, c := range data {
		enc.WriteByte(c + 42)
		if (i+1)%128 == 0 {
			enc.WriteString("\r\n")
		}
	}
	if len(data)%128 != 0 {
		enc.WriteString("\r\n")
	}
	fmt.Fprintf(&enc, "=yend size=%d\r\n", len(data))
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
						_, _ = c.Write([]byte("111 20261117120000\r\n"))
					default:
						_, _ = c.Write([]byte("500 unknown command\r\n"))
					}
				}
			}(c)
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port
}

// A segment whose record skips a header (SegmentDataStart > 0) receives an
// article that decodes to no more than that skip. Nothing is stored, so the
// fetch must not report success and leave the slot Fetching with no owner:
// it is article-not-found from this provider, the slot leaves Fetching, and a
// later read gets an answer at once instead of waiting out its context.
func TestShortArticleDoesNotLeaveSlotFetching(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	data := make([]byte, 800) // shorter than the 1000-byte header skip
	for i := range data {
		data[i] = 'a' - 42 // encodes to 'a': no escaping needed
	}
	port := serveOneArticle(t, data)

	cfg := config.Get()
	oldUsenet := cfg.Usenet
	cfg.Usenet = config.Usenet{
		Providers:      []config.UsenetProvider{{Host: "127.0.0.1", Port: port, MaxConnections: 2, Priority: 1}},
		MaxConnections: 2,
	}
	t.Cleanup(func() { cfg.Usenet = oldUsenet })
	client, err := nntp.NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	segments := []SegmentMeta{
		{MessageID: "<short@test>", Number: 1, Bytes: 500, StartOffset: 0, EndOffset: 499, SegmentDataStart: 1000},
		{MessageID: "<next@test>", Number: 2, Bytes: 500, StartOffset: 500, EndOffset: 999},
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
	err = sf.Fetch(fetchCtx, 0)
	if err == nil {
		t.Error("fetch of an article that stored nothing reported success")
	} else if !nntp.IsArticleNotFoundError(err) {
		t.Errorf("fetch error = %v, want article-not-found", err)
	}
	if st := cache.GetState(0); st == StateFetching || st == StateOnDisk {
		t.Errorf("slot state after the fetch: %v, want it out of Fetching and not OnDisk", st)
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer waitCancel()
	if err := cache.WaitForSegment(waitCtx, 0); errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a later read waits on the slot until its own context ends: %v", err)
	}
}
