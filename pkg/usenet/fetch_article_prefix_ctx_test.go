package usenet

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/nntp"
)

// serveStallingArticles starts a fake NNTP server that serves every BODY as a
// one-part yEnc article of n bytes of 'a' named name, except the message IDs
// in stall: those get the 222 status and the =ybegin line, then nothing more
// (a server that stalls mid-body). It returns the port and a count of BODY
// commands received.
func serveStallingArticles(t *testing.T, n int, name string, stall ...string) (int, *atomic.Int64) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	stalls := map[string]bool{}
	for _, id := range stall {
		stalls["<"+id+">"] = true
	}

	ybegin := fmt.Sprintf("=ybegin part=1 total=1 line=128 size=%d name=%s\r\n", n, name)
	var enc strings.Builder
	enc.WriteString(ybegin)
	fmt.Fprintf(&enc, "=ypart begin=1 end=%d\r\n", n)
	for i := range n {
		enc.WriteByte('a') // decodes to 'a'-42: no escaping needed
		if (i+1)%128 == 0 {
			enc.WriteString("\r\n")
		}
	}
	if n%128 != 0 {
		enc.WriteString("\r\n")
	}
	fmt.Fprintf(&enc, "=yend size=%d part=1\r\n", n)
	body := enc.String()

	var bodies atomic.Int64
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
						bodies.Add(1)
						if stalls[arg] {
							// Start the body, then go silent until the
							// client hangs up.
							_, _ = c.Write([]byte("222 0 " + arg + "\r\n" + ybegin))
							continue
						}
						_, _ = c.Write([]byte("222 0 " + arg + "\r\n" + body + ".\r\n"))
					case "QUIT":
						_, _ = c.Write([]byte("205 bye\r\n"))
						return
					case "DATE":
						_, _ = c.Write([]byte("111 20261119120000\r\n"))
					default:
						_, _ = c.Write([]byte("500 unknown command\r\n"))
					}
				}
			}(c)
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port, &bodies
}

// The geometry and volume-order head reads end with the caller's ctx when a
// server stalls mid-body, instead of waiting out the connection's 60 s
// StreamBodyTimeout, and the error is the ctx's own. The connection the
// stall held is dropped, not pooled half-read: with one connection slot, the
// next read on the same pool gets the right article.
func TestFetchArticlePrefixAndHeadEndWithCtxOnStall(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	const n = 500
	want := bytes.Repeat([]byte{'a' - 42}, n)
	port, bodies := serveStallingArticles(t, n, "vol.part01.rar", "stall@test")

	cfg := config.Get()
	oldUsenet := cfg.Usenet
	cfg.Usenet = config.Usenet{
		Providers:      []config.UsenetProvider{{Host: "127.0.0.1", Port: port, MaxConnections: 1, Priority: 1}},
		MaxConnections: 1,
	}
	t.Cleanup(func() { cfg.Usenet = oldUsenet })
	client, err := nntp.NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	u := &Usenet{nntp: client, logger: zerolog.Nop()}

	const ctxLimit = 1500 * time.Millisecond
	// Generous next to the ctx, far below the 60 s StreamBodyTimeout (plus
	// the idle janitor's sweep) the read waited out before.
	const margin = 3 * time.Second

	// await runs fetch and fails the test if it is still blocked well past
	// its ctx, without waiting out the old 60 s.
	await := func(what string, fetch func(ctx context.Context) error) (time.Duration, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), ctxLimit)
		defer cancel()
		start := time.Now()
		done := make(chan error, 1)
		go func() { done <- fetch(ctx) }()
		select {
		case err := <-done:
			return time.Since(start), err
		case <-time.After(ctxLimit + margin):
			t.Fatalf("%s: still blocked %v after the call, with a %v ctx", what, ctxLimit+margin, ctxLimit)
			return 0, nil
		}
	}
	good := func(what string) {
		t.Helper()
		var head ArticleHead
		_, err := await(what, func(ctx context.Context) error {
			var err error
			head, err = u.FetchArticleHead(ctx, "<good@test>", 64)
			return err
		})
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		if !bytes.Equal(head.Prefix, want[:64]) || head.Name != "vol.part01.rar" || head.Part != 1 {
			t.Fatalf("%s: got prefix %q name %q part %d, want the served article's first 64 bytes, vol.part01.rar, part 1",
				what, head.Prefix, head.Name, head.Part)
		}
	}

	good("control read before the stall")

	took, err := await("FetchArticlePrefix on a stalled article", func(ctx context.Context) error {
		p, err := u.FetchArticlePrefix(ctx, "<stall@test>", 64)
		if err == nil {
			return fmt.Errorf("stalled article returned %d bytes and no error", len(p))
		}
		return err
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("FetchArticlePrefix on a stalled article: error = %v, want the ctx's deadline error", err)
	}
	if took < ctxLimit-100*time.Millisecond {
		t.Fatalf("FetchArticlePrefix on a stalled article returned after %v, before its %v ctx ended", took, ctxLimit)
	}
	good("read after the stalled prefix read")

	_, err = await("FetchArticleHead on a stalled article", func(ctx context.Context) error {
		_, err := u.FetchArticleHead(ctx, "<stall@test>", 64)
		return err
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("FetchArticleHead on a stalled article: error = %v, want the ctx's deadline error", err)
	}
	good("read after the stalled head read")

	// Each stalled read sent one BODY: ending on its own ctx is not a
	// provider failure, so failover neither retried it nor moved on.
	if got := bodies.Load(); got != 5 {
		t.Fatalf("server saw %d BODY commands, want 5 (3 good reads, 2 stalled reads sent once each)", got)
	}

	// An already-ended ctx sends nothing.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := u.FetchArticlePrefix(ctx, "<good@test>", 64); !errors.Is(err, context.Canceled) {
		t.Fatalf("FetchArticlePrefix with an ended ctx: error = %v, want context.Canceled", err)
	}
	if got := bodies.Load(); got != 5 {
		t.Fatalf("an ended ctx sent a BODY: server saw %d, want 5", got)
	}
}
