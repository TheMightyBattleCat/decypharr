package nntp

import (
	"context"
	"math/rand/v2"
	"net"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
)

// waitSlotsFree waits until no slot of pp is taken. It checks by filling the
// slots itself, which also orders this goroutine after every racer's release
// (so a test may then touch globals those racers read).
func waitSlotsFree(t *testing.T, pp *ProviderPool, name string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		got := 0
		for got < cap(pp.slots) {
			select {
			case pp.slots <- struct{}{}:
				got++
				continue
			default:
			}
			break
		}
		for range got {
			<-pp.slots
		}
		if got == cap(pp.slots) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("provider %s: %d of %d slot(s) still taken after %v", name, cap(pp.slots)-got, cap(pp.slots), within)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A checkout that races both pools gets B's ready connection while A's racer
// still pings a stale idle connection. Whatever happens to the caller after,
// B's slot must come back: pre-fix the loop held B's connection until A
// finished, and a caller giving up in that window left B's slot taken for
// good.
func TestRaceWinnerSlotNotLeakedWhenCallerGivesUp(t *testing.T) {
	a := startFakeNNTP(t, 0)
	a.noDate = true // a stale idle connection's ping waits out PingTimeout
	b := startFakeNNTP(t, 0)
	pa, pb := twoLocalProviders(t, a, b)
	pa.MaxConnections = 1
	pb.MaxConnections = 1
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)
	poolA, poolB := c.pools[pa.Host], c.pools[pb.Host]

	ctx := context.Background()
	staleA, err := c.createConnection(ctx, pa)
	if err != nil {
		t.Fatalf("dial A: %v", err)
	}
	freshB, err := c.createConnection(ctx, pb)
	if err != nil {
		t.Fatalf("dial B: %v", err)
	}
	// A: idle for 15 s (past StaleThreshold, inside IdleTimeout), so the
	// checkout pings it. B: idle just now, handed out with no ping.
	poolA.conns = append(poolA.conns, acquireConnectionEntry(staleA, pa, utils.Now().Add(-15*time.Second)))
	poolB.conns = append(poolB.conns, acquireConnectionEntry(freshB, pb, utils.Now()))

	// Both pools full: the checkout's scan finds no free slot and races.
	poolA.slots <- struct{}{}
	poolB.slots <- struct{}{}

	callerCtx, giveUp := context.WithCancel(ctx)
	defer giveUp()
	type res struct {
		conn     *Connection
		provider config.UsenetProvider
		err      error
	}
	done := make(chan res, 1)
	go func() {
		conn, p, err := c.getAnyAvailableConnection(callerCtx, providerExclusions{})
		done <- res{conn, p, err}
	}()

	time.Sleep(100 * time.Millisecond)
	<-poolA.slots // A's racer takes it and sits in the stale entry's ping
	time.Sleep(100 * time.Millisecond)
	<-poolB.slots // B's racer takes it and wins at once
	time.Sleep(200 * time.Millisecond)
	giveUp() // the caller's deadline ends while A's racer is still pinging

	r := <-done
	if r.conn != nil {
		if r.provider.Host != pb.Host {
			t.Errorf("checkout got provider %s; want B (%s), the only ready one", r.provider.Host, pb.Host)
		}
		c.returnOrReleaseConn(r.conn, r.provider)
	}

	// Within a second of the caller being done, B's slot is free again ...
	waitSlotsFree(t, poolB, "B", time.Second)

	// ... and B can be checked out again.
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	conn, _, perr := c.getConnectionFromProvider(probeCtx, pb)
	if perr != nil {
		t.Fatalf("a later checkout from provider B failed: %v", perr)
	}
	c.returnOrReleaseConn(conn, pb)

	// A's racer gives its slot back once its ping has timed out.
	waitSlotsFree(t, poolA, "A", timeouts.PingTimeout+3*time.Second)
	waitSlotsFree(t, poolB, "B", time.Second)
}

// A ready connection is handed over at once, not held while another racer
// dials a server that accepts TCP and never greets; and that dial is closed
// when the race is over instead of running out the handshake timeout.
func TestRaceWinnerNotHeldBySilentDial(t *testing.T) {
	saved := timeouts
	timeouts.HandshakeTimeout = 2 * time.Second
	t.Cleanup(func() { timeouts = saved })

	silent, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = silent.Close() })
	go func() {
		for {
			conn, err := silent.Accept()
			if err != nil {
				return
			}
			go func() { // accept, never greet
				buf := make([]byte, 1)
				_, _ = conn.Read(buf)
				_ = conn.Close()
			}()
		}
	}()
	b := startFakeNNTP(t, 0)
	pa := config.UsenetProvider{Host: "127.0.0.1", Port: silent.Addr().(*net.TCPAddr).Port, Priority: 1, MaxConnections: 1}
	_, pb := twoLocalProviders(t, b, b)
	pb.MaxConnections = 1
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)
	poolA, poolB := c.pools[pa.Host], c.pools[pb.Host]

	freshB, err := c.createConnection(context.Background(), pb)
	if err != nil {
		t.Fatalf("dial B: %v", err)
	}
	poolB.conns = append(poolB.conns, acquireConnectionEntry(freshB, pb, utils.Now()))
	poolA.slots <- struct{}{}
	poolB.slots <- struct{}{}

	type res struct {
		conn     *Connection
		provider config.UsenetProvider
		err      error
		at       time.Duration
	}
	start := time.Now()
	done := make(chan res, 1)
	go func() {
		conn, p, err := c.getAnyAvailableConnection(context.Background(), providerExclusions{})
		done <- res{conn, p, err, time.Since(start)}
	}()
	time.Sleep(100 * time.Millisecond)
	<-poolA.slots // A's racer: no idle connection, dials the silent server
	time.Sleep(100 * time.Millisecond)
	<-poolB.slots // B's racer: a fresh idle connection, ready at once
	freedB := time.Since(start)

	r := <-done
	if r.err != nil || r.conn == nil {
		t.Fatalf("checkout: conn=%v err=%v; want B's connection", r.conn != nil, r.err)
	}
	if r.provider.Host != pb.Host {
		t.Errorf("checkout got provider %s; want B (%s)", r.provider.Host, pb.Host)
	}
	c.returnOrReleaseConn(r.conn, r.provider)
	if wait := r.at - freedB; wait > 250*time.Millisecond {
		t.Errorf("the checkout returned %v after B's connection was ready; want it handed over at once (HandshakeTimeout %v)",
			wait.Round(time.Millisecond), timeouts.HandshakeTimeout)
	}

	// A's cancelled dial frees its slot well inside the handshake timeout.
	waitSlotsFree(t, poolA, "A", 500*time.Millisecond)
	waitSlotsFree(t, poolB, "B", time.Second)
}

// The caller's context ends just as a racer delivers its connection: either
// the caller gets the connection or an error, never both or neither, and once
// it is put back no slot of either provider stays taken. Run with -race and a
// high -count; pre-fix a winner received just before ctx.Done was dropped.
func TestRaceHandOffWhenCallerGivesUpLeavesNoSlot(t *testing.T) {
	a := startFakeNNTP(t, 0)
	b := startFakeNNTP(t, 0)
	pa, pb := twoLocalProviders(t, a, b)
	pa.MaxConnections = 1
	pb.MaxConnections = 1
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)
	poolA, poolB := c.pools[pa.Host], c.pools[pb.Host]

	ctx := context.Background()
	freshB, err := c.createConnection(ctx, pb)
	if err != nil {
		t.Fatalf("dial B: %v", err)
	}
	poolB.conns = append(poolB.conns, acquireConnectionEntry(freshB, pb, utils.Now()))
	poolA.slots <- struct{}{}
	poolB.slots <- struct{}{}

	callerCtx, giveUp := context.WithCancel(ctx)
	defer giveUp()
	type res struct {
		conn     *Connection
		provider config.UsenetProvider
		err      error
	}
	done := make(chan res, 1)
	go func() {
		conn, p, err := c.getAnyAvailableConnection(callerCtx, providerExclusions{})
		done <- res{conn, p, err}
	}()

	time.Sleep(20 * time.Millisecond) // both racers wait for a slot
	if rand.IntN(2) == 0 {
		<-poolB.slots // B's racer wins ...
		giveUp()      // ... as the caller gives up
	} else {
		giveUp()
		<-poolB.slots
	}

	r := <-done
	if (r.conn == nil) == (r.err == nil) {
		t.Fatalf("checkout returned conn=%v err=%v; want exactly one", r.conn != nil, r.err)
	}
	if r.conn != nil {
		c.returnOrReleaseConn(r.conn, r.provider)
	}
	<-poolA.slots // the test's own hold on A

	waitSlotsFree(t, poolA, "A", 2*time.Second)
	waitSlotsFree(t, poolB, "B", 2*time.Second)
}
