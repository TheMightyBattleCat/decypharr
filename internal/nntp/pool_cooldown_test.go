package nntp

import (
	"bufio"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
)

// closedPort returns a loopback port with nothing listening, so dials to it
// are refused at once.
func closedPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

func emptyPool(p config.UsenetProvider) *ProviderPool {
	return &ProviderPool{
		conns:  make([]*connectionEntry, 0, p.MaxConnections),
		slots:  make(chan struct{}, p.MaxConnections),
		max:    p.MaxConnections,
		config: p,
	}
}

// pipeConn returns a pipe-backed connection. With answerDate the far end
// answers every DATE with 111; without it the far end reads and never
// answers, so a ping times out.
func pipeConn(t *testing.T, host string, answerDate bool) *Connection {
	t.Helper()
	clientSide, serverSide := net.Pipe()
	t.Cleanup(func() { _ = clientSide.Close(); _ = serverSide.Close() })
	go func() {
		r := bufio.NewReader(serverSide)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			if answerDate && strings.HasPrefix(line, "DATE") {
				_, _ = io.WriteString(serverSide, "111 20260930120000\r\n")
			}
		}
	}()
	return &Connection{
		conn:    clientSide,
		reader:  bufio.NewReader(clientSide),
		writer:  bufio.NewWriter(clientSide),
		address: host,
	}
}

func pushIdle(pp *ProviderPool, conn *Connection, idleFor time.Duration) {
	pp.conns = append(pp.conns, acquireConnectionEntry(conn, pp.config, utils.Now().Add(-idleFor)))
}

// After the primary's first failed dial, later acquisitions go straight to
// the warm secondary without dialing the primary again.
func TestDialCooldownReroutesAroundDeadProvider(t *testing.T) {
	dead := config.UsenetProvider{Host: "127.0.0.1", Port: closedPort(t), MaxConnections: 2, Priority: 1}
	deadPool := emptyPool(dead)
	healthy := config.UsenetProvider{Host: "healthy", Port: 119, MaxConnections: 2, Priority: 2}
	healthyPool := emptyPool(healthy)
	conn := pipeConn(t, healthy.Host, true)
	pushIdle(healthyPool, conn, 0)

	c := &Client{
		pools:     map[string]*ProviderPool{dead.Host: deadPool, healthy.Host: healthyPool},
		providers: []config.UsenetProvider{dead, healthy},
		logger:    zerolog.Nop(),
	}

	got, prov, err := c.getAnyAvailableConnection(context.Background(), providerExclusions{})
	if err != nil {
		t.Fatalf("first acquisition failed: %v", err)
	}
	if prov.Host != healthy.Host || got != conn {
		t.Fatalf("expected the healthy provider's pooled connection, got provider %s", prov.Host)
	}
	if !deadPool.inDialCooldown() {
		t.Fatal("expected the dead provider to be cooling down after its failed dial")
	}
	c.put(got, prov)

	for range 5 {
		got, prov, err = c.getAnyAvailableConnection(context.Background(), providerExclusions{})
		if err != nil {
			t.Fatalf("acquisition during cooldown failed: %v", err)
		}
		if prov.Host != healthy.Host {
			t.Fatalf("expected the healthy provider during cooldown, got %s", prov.Host)
		}
		c.put(got, prov)
	}
	if streak := deadPool.dialFailStreak.Load(); streak != 1 {
		t.Fatalf("dead provider was dialed again during cooldown: streak=%d", streak)
	}
}

// With no other provider, a cooldown must not delay or hide dial errors:
// every acquisition still dials and fails fast with the dial error.
func TestDialCooldownKeepsSingleProviderFailFast(t *testing.T) {
	dead := config.UsenetProvider{Host: "127.0.0.1", Port: closedPort(t), MaxConnections: 2}
	deadPool := emptyPool(dead)
	c := &Client{
		pools:     map[string]*ProviderPool{dead.Host: deadPool},
		providers: []config.UsenetProvider{dead},
		logger:    zerolog.Nop(),
	}

	for i := range 3 {
		start := time.Now()
		_, _, err := c.getAnyAvailableConnection(context.Background(), providerExclusions{})
		if err == nil {
			t.Fatalf("attempt %d: expected a dial error", i)
		}
		if strings.Contains(err.Error(), "no eligible providers") || err == errDialCooldown {
			t.Fatalf("attempt %d: dial error replaced by %q", i, err)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("attempt %d: cooldown delayed a fail-fast dial: %v", i, elapsed)
		}
	}
	if streak := deadPool.dialFailStreak.Load(); streak < 3 {
		t.Fatalf("expected every acquisition to dial, streak=%d", streak)
	}
	if len(deadPool.slots) != 0 {
		t.Fatalf("slots leaked: %d held", len(deadPool.slots))
	}
}

// A checkout whose verify ping times out closes the rest of the idle pool
// instead of pinging each older entry in turn.
func TestCheckoutFlushesPoolOnPingTimeout(t *testing.T) {
	saved := timeouts
	timeouts.PingTimeout = 200 * time.Millisecond
	t.Cleanup(func() { timeouts = saved })

	p := config.UsenetProvider{Host: "127.0.0.1", Port: closedPort(t), MaxConnections: 4}
	pp := emptyPool(p)
	c := &Client{
		pools:     map[string]*ProviderPool{p.Host: pp},
		providers: []config.UsenetProvider{p},
		logger:    zerolog.Nop(),
	}

	// Idle past StaleThreshold, so checkout pings, but not past IdleTimeout.
	idle := (timeouts.StaleThreshold + timeouts.IdleTimeout) / 2
	lower := []*Connection{pipeConn(t, p.Host, true), pipeConn(t, p.Host, true)}
	for _, conn := range lower {
		pushIdle(pp, conn, idle)
	}
	silent := pipeConn(t, p.Host, false)
	pushIdle(pp, silent, idle) // top of the LIFO stack: checked first

	pp.slots <- struct{}{}
	start := time.Now()
	_, err := c.getOrCreateFromPool(context.Background(), pp, p, true)
	if err == nil {
		t.Fatal("expected checkout to fail: the flush drops the idle entries and the dial is refused")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("checkout took %v; the flushed entries were pinged one by one", elapsed)
	}
	if !silent.IsClosed() {
		t.Fatal("silent connection not closed")
	}
	for i, conn := range lower {
		if !conn.IsClosed() {
			t.Fatalf("idle connection %d not closed by the flush", i)
		}
	}
}

// ping must hold to PingTimeout even when the peer stops reading, not to
// the longer HandshakeTimeout that sendCommandArg sets.
func TestPingWriteHonoursPingTimeout(t *testing.T) {
	saved := timeouts
	// Above the cached clock's 500 ms tick, so the deadline is not already past.
	timeouts.PingTimeout = time.Second
	timeouts.HandshakeTimeout = 5 * time.Second
	t.Cleanup(func() { timeouts = saved })

	clientSide, serverSide := net.Pipe() // unbuffered: a write blocks until read
	t.Cleanup(func() { _ = clientSide.Close(); _ = serverSide.Close() })
	conn := &Connection{
		conn:   clientSide,
		reader: bufio.NewReader(clientSide),
		writer: bufio.NewWriter(clientSide),
	}

	start := time.Now()
	if err := conn.ping(); err == nil {
		t.Fatal("expected ping to fail against a peer that never reads")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("ping blocked %v; its write ran on HandshakeTimeout", elapsed)
	}
}
