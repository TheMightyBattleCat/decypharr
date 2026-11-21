package nntp

// A provider that refuses a command (481/482 auth, 502 access denied, 480,
// 451, 500/501) says nothing about the article. Failover must ask the next
// provider, close the refusing session rather than pool it, and route
// around the provider until a dial to it succeeds again.

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// refusingNNTP greets 200 and answers every BODY and STAT with one fixed
// status line.
type refusingNNTP struct {
	ln       net.Listener
	status   string
	bodyReqs atomic.Int64
	statReqs atomic.Int64
	wg       sync.WaitGroup
}

func startRefusingNNTP(t *testing.T, status string) *refusingNNTP {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &refusingNNTP{ln: ln, status: status}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.wg.Add(1)
			go s.serve(conn)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

func (s *refusingNNTP) port() int { return s.ln.Addr().(*net.TCPAddr).Port }

func (s *refusingNNTP) serve(conn net.Conn) {
	defer s.wg.Done()
	defer func() { _ = conn.Close() }()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	_, _ = w.WriteString("200 fake ready\r\n")
	_ = w.Flush()
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case strings.HasPrefix(line, "BODY "):
			s.bodyReqs.Add(1)
			_, _ = w.WriteString(s.status + "\r\n")
		case strings.HasPrefix(line, "STAT "):
			s.statReqs.Add(1)
			_, _ = w.WriteString(s.status + "\r\n")
		case line == "DATE":
			_, _ = w.WriteString("111 20261120120000\r\n")
		default:
			_, _ = w.WriteString("500 unknown\r\n")
		}
		_ = w.Flush()
	}
}

func idleCount(c *Client, host string) int {
	pp := c.pools[host]
	pp.mu.Lock()
	defer pp.mu.Unlock()
	return len(pp.conns)
}

// skipUnlessLocalhostReaches skips when "localhost" does not reach a
// listener on 127.0.0.1 (the tests use the two names as two hosts).
func skipUnlessLocalhostReaches(t *testing.T, port int) {
	t.Helper()
	probe, err := net.DialTimeout("tcp", net.JoinHostPort("localhost", strconv.Itoa(port)), time.Second)
	if err != nil {
		t.Skipf("localhost does not reach 127.0.0.1: %v", err)
	}
	_ = probe.Close()
}

// A 500 to BODY or STAT is read as a reply out of step with the command (a
// connection error, the connection closed): it fails over too, but is not
// treated as a refusal, so the provider is not routed around for it.
var refusalStatuses = []string{
	"502 access denied",
	"480 authentication required",
	"481 authentication rejected",
	"451 article unavailable",
	"500 command not recognized",
}

func TestFailoverBodyMovesPastARefusal(t *testing.T) {
	payload := testPayload()
	for _, status := range refusalStatuses {
		t.Run(strings.Fields(status)[0], func(t *testing.T) {
			a := startRefusingNNTP(t, status)
			b := startFakeNNTP(t, 0)
			b.bodies = map[string]string{"x@test": yencWire(payload, 0)}
			skipUnlessLocalhostReaches(t, b.port())
			pa := config.UsenetProvider{Host: "127.0.0.1", Port: a.port(), Priority: 1, MaxConnections: 2}
			pb := config.UsenetProvider{Host: "localhost", Port: b.port(), Priority: 2, MaxConnections: 2}
			c := newBodyTestClient(t, []config.UsenetProvider{pa, pb})

			// Two idle sessions on provider 1 before the read: one refuses,
			// and the other, which would refuse too, must be closed rather
			// than handed to the next request.
			var held []*Connection
			for range 2 {
				conn, _, err := c.getConnectionFromProvider(context.Background(), pa)
				if err != nil {
					t.Fatalf("checkout from provider 1: %v", err)
				}
				held = append(held, conn)
			}
			for _, conn := range held {
				c.put(conn, pa)
			}
			if n := idleCount(c, pa.Host); n != 2 {
				t.Fatalf("provider 1 has %d idle connections before the read, want 2", n)
			}

			var buf bytes.Buffer
			if err := fetchBody(c, "x@test", &buf); err != nil {
				t.Fatalf("provider 1 answered %q, provider 2 holds the article: err = %v (provider 2 asked %d times)", status, err, b.bodyReqs.Load())
			}
			if !bytes.Equal(buf.Bytes(), payload) {
				t.Fatalf("bytes differ from provider 2's article")
			}
			if a.bodyReqs.Load() != 1 || b.bodyReqs.Load() != 1 {
				t.Errorf("BODYs: provider 1 %d, provider 2 %d; want 1 each", a.bodyReqs.Load(), b.bodyReqs.Load())
			}
			if strings.HasPrefix(status, "500") {
				// Out of step: only the connection that got it is closed.
				if n := idleCount(c, pa.Host); n != 1 {
					t.Errorf("provider 1 has %d idle connections after a 500, want 1 (the one that got it closed)", n)
				}
				return
			}
			if n := idleCount(c, pa.Host); n != 0 {
				t.Errorf("provider 1 has %d idle connections after refusing; want the refusing session and the other idle one closed", n)
			}
			if !c.pools[pa.Host].inDialCooldown() {
				t.Errorf("provider 1 is not routed around after refusing on a live session")
			}

			// The next read goes straight to provider 2: provider 1 sits in
			// its cooldown with no idle session while provider 2 is warm.
			if err := fetchBody(c, "x@test", &buf); err != nil || !bytes.Equal(buf.Bytes(), payload) {
				t.Fatalf("second read: err = %v", err)
			}
			if got := a.bodyReqs.Load(); got != 1 {
				t.Errorf("provider 1 asked again on the second read (%d BODYs); want it routed around", got)
			}
		})
	}
}

// The single STAT path (the parser's first STAT of a new NZB).
func TestFailoverStatMovesPastARefusal(t *testing.T) {
	a := startRefusingNNTP(t, "502 access denied")
	b := startFakeNNTP(t, 0)
	skipUnlessLocalhostReaches(t, b.port())
	pa := config.UsenetProvider{Host: "127.0.0.1", Port: a.port(), Priority: 1, MaxConnections: 2}
	pb := config.UsenetProvider{Host: "localhost", Port: b.port(), Priority: 2, MaxConnections: 2}
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)

	if _, _, err := c.Stat(context.Background(), "x@test"); err != nil {
		t.Fatalf("STAT with provider 1 refusing and provider 2 holding the article: err = %v (provider 2 saw %d STATs)", err, b.stats.Load())
	}
	if a.statReqs.Load() != 1 || b.stats.Load() == 0 {
		t.Errorf("STATs: provider 1 %d, provider 2 %d; want 1 and at least 1", a.statReqs.Load(), b.stats.Load())
	}
}

// With every provider refusing, the refusal is the answer: not a 430 (which
// would pad and queue repair) and not "all providers failed".
func TestFailoverReturnsTheRefusalWhenEveryProviderRefuses(t *testing.T) {
	a := startRefusingNNTP(t, "502 access denied")
	b := startRefusingNNTP(t, "481 authentication rejected")
	skipUnlessLocalhostReaches(t, b.port())
	c := newBodyTestClient(t, []config.UsenetProvider{
		{Host: "127.0.0.1", Port: a.port(), Priority: 1, MaxConnections: 2},
		{Host: "localhost", Port: b.port(), Priority: 2, MaxConnections: 2},
	})

	var buf bytes.Buffer
	err := fetchBody(c, "x@test", &buf)
	var nntpErr *Error
	if !errors.As(err, &nntpErr) {
		t.Fatalf("err = %v, want an NNTP refusal", err)
	}
	if IsArticleNotFoundError(err) {
		t.Fatalf("err = %v reads as a 430", err)
	}
	if nntpErr.Type != ErrorTypeAuthentication && nntpErr.Type != ErrorTypePermissionDenied {
		t.Errorf("err type = %v, want the refusal (auth or permission denied)", nntpErr.Type)
	}
	if a.bodyReqs.Load() != 1 || b.bodyReqs.Load() != 1 {
		t.Errorf("BODYs: provider 1 %d, provider 2 %d; want 1 each", a.bodyReqs.Load(), b.bodyReqs.Load())
	}
	kinds := FailoverOutcomes(err)
	if len(kinds) != 2 || kinds[0].Kind != "refused" || kinds[1].Kind != "refused" {
		t.Errorf("outcomes = %v, want both refused", DescribeOutcomes(kinds))
	}
}

// A 430 from one provider and a refusal from the other is a 430 (playback
// pads on it) but not one every provider gave: the refusing provider is
// unasked. A refusing provider on the backbone that gave the 430 is
// covered by it (control).
func TestFailoverNotFoundWithARefusalIsPartial(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, tc := range []struct {
		name         string
		refuserFirst bool
	}{{"refusal_then_430", true}, {"430_then_refusal", false}} {
		t.Run(tc.name, func(t *testing.T) {
			ref := startRefusingNNTP(t, "502 access denied")
			gone := startFakeNNTP(t, 0, "gone@test")
			skipUnlessLocalhostReaches(t, gone.port())
			pr := config.UsenetProvider{Host: "127.0.0.1", Port: ref.port(), MaxConnections: 2, Priority: 2}
			pg := config.UsenetProvider{Host: "localhost", Port: gone.port(), MaxConnections: 2, Priority: 1}
			if tc.refuserFirst {
				pr.Priority, pg.Priority = 1, 2
			}
			c := newStatTestClient(t, []config.UsenetProvider{pr, pg}, 0)

			_, _, err := c.Stat(ctx, "gone@test")
			if !IsArticleNotFoundError(err) {
				t.Fatalf("err = %v, want a 430", err)
			}
			if !IsPartialNotFound(err) {
				t.Fatalf("err = %v is a complete 430 although %s refused", err, pr.Host)
			}
			if got := UnaskedProviders(err); !slices.Equal(got, []string{pr.Host}) {
				t.Errorf("unasked = %v, want [%s]", got, pr.Host)
			}
		})
	}

	t.Run("refuser_on_answering_backbone", func(t *testing.T) {
		ref := startRefusingNNTP(t, "502 access denied")
		gone := startFakeNNTP(t, 0, "gone@test")
		skipUnlessLocalhostReaches(t, gone.port())
		c := newStatTestClient(t, []config.UsenetProvider{
			{Host: "127.0.0.1", Port: ref.port(), MaxConnections: 2, Priority: 1, Backbone: "bb"},
			{Host: "localhost", Port: gone.port(), MaxConnections: 2, Priority: 2, Backbone: "bb"},
		}, 0)
		_, _, err := c.Stat(ctx, "gone@test")
		if !IsArticleNotFoundError(err) || IsPartialNotFound(err) {
			t.Fatalf("err = %v (430=%v partial=%v), want a complete 430", err, IsArticleNotFoundError(err), IsPartialNotFound(err))
		}
	})
}

// ExecuteOnce has no failover, but a refusing session is still not pooled.
func TestExecuteOnceDoesNotPoolARefusingSession(t *testing.T) {
	a := startRefusingNNTP(t, "502 access denied")
	pa := config.UsenetProvider{Host: "127.0.0.1", Port: a.port(), MaxConnections: 2}
	c := newStatTestClient(t, []config.UsenetProvider{pa}, 0)

	err := c.ExecuteOnce(context.Background(), func(conn *Connection) error {
		_, _, err := conn.Stat("x@test")
		return err
	})
	var nntpErr *Error
	if !errors.As(err, &nntpErr) || nntpErr.Type != ErrorTypePermissionDenied {
		t.Fatalf("err = %v, want the 502", err)
	}
	if n := idleCount(c, pa.Host); n != 0 {
		t.Errorf("%d idle connections after a refusal; the refusing session was pooled", n)
	}
}
