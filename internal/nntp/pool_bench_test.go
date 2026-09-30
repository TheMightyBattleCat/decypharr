package nntp

import (
	"bufio"
	"context"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
)

// Pool acquisition benchmarks, adapted from upstream 5b55a7d5
// (https://github.com/sirrobot01/decypharr/commit/5b55a7d5). They measure how
// long a checkout waits when demand exceeds the provider's connection slots.

// newBenchClient builds a Client around the given pools without any real
// network config.
func newBenchClient(providers []config.UsenetProvider, pools []*ProviderPool) *Client {
	byHost := make(map[string]*ProviderPool, len(pools))
	for _, pp := range pools {
		byHost[pp.config.Host] = pp
	}
	return &Client{
		pools:     byHost,
		providers: providers,
		logger:    zerolog.Nop(),
	}
}

// newBenchPool creates a pool pre-filled with `max` pipe-backed connections so
// checkout never dials. The entries are fresh, so checkout never pings.
func newBenchPool(b *testing.B, host string, max int) (*ProviderPool, config.UsenetProvider) {
	provider := config.UsenetProvider{Host: host, Port: 119, MaxConnections: max}
	pp := &ProviderPool{
		conns:  make([]*connectionEntry, 0, max),
		slots:  make(chan struct{}, max),
		max:    max,
		config: provider,
	}
	for range max {
		clientSide, serverSide := net.Pipe()
		conn := &Connection{
			conn:    clientSide,
			reader:  bufio.NewReader(clientSide),
			writer:  bufio.NewWriter(clientSide),
			address: host,
		}
		b.Cleanup(func() { _ = conn.Close(); _ = serverSide.Close() })
		pp.conns = append(pp.conns, acquireConnectionEntry(conn, provider, utils.Now()))
	}
	return pp, provider
}

// reportWaitQuantiles reports acquisition-wait quantiles in milliseconds.
func reportWaitQuantiles(b *testing.B, waits []time.Duration) {
	if len(waits) == 0 {
		return
	}
	sort.Slice(waits, func(i, j int) bool { return waits[i] < waits[j] })
	q := func(p float64) float64 {
		idx := int(p * float64(len(waits)-1))
		return float64(waits[idx]) / float64(time.Millisecond)
	}
	b.ReportMetric(q(0.50), "p50-wait-ms")
	b.ReportMetric(q(0.99), "p99-wait-ms")
	b.ReportMetric(float64(waits[len(waits)-1])/float64(time.Millisecond), "max-wait-ms")
}

// BenchmarkPoolCheckoutUncontended measures one checkout/return cycle with a
// free slot and a warm pooled connection.
func BenchmarkPoolCheckoutUncontended(b *testing.B) {
	pp, provider := newBenchPool(b, "bench-a", 8)
	c := newBenchClient([]config.UsenetProvider{provider}, []*ProviderPool{pp})
	ctx := context.Background()

	b.ResetTimer()
	for range b.N {
		conn, prov, err := c.getAnyAvailableConnection(ctx, providerExclusions{})
		if err != nil {
			b.Fatal(err)
		}
		c.put(conn, prov)
	}
}

// benchContended runs `workers` goroutines that each loop: acquire a
// connection, hold it for `hold` (a segment download), return it. Ideal
// ns/op is hold/slots; the gap to it is time freed slots sat unclaimed.
func benchContended(b *testing.B, pools []*ProviderPool, providers []config.UsenetProvider, workers int, hold time.Duration) {
	c := newBenchClient(providers, pools)
	ctx := context.Background()
	slots := 0
	for _, pp := range pools {
		slots += pp.max
	}

	waitsPerWorker := make([][]time.Duration, workers)
	var next atomic.Int64
	var wg sync.WaitGroup

	b.ResetTimer()
	start := time.Now()
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for next.Add(1) <= int64(b.N) {
				t0 := time.Now()
				conn, prov, err := c.getAnyAvailableConnection(ctx, providerExclusions{})
				if err != nil {
					b.Error(err)
					return
				}
				waitsPerWorker[w] = append(waitsPerWorker[w], time.Since(t0))
				time.Sleep(hold)
				c.put(conn, prov)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()

	var waits []time.Duration
	for _, ws := range waitsPerWorker {
		waits = append(waits, ws...)
	}
	reportWaitQuantiles(b, waits)
	ideal := float64(hold) / float64(slots) * float64(b.N)
	b.ReportMetric(ideal/float64(elapsed)*100, "slot-utilization-%")
}

func BenchmarkPoolContended3x(b *testing.B) {
	// 20 provider slots, 60 demanders, 20ms simulated segment fetch.
	pp, p := newBenchPool(b, "bench-a", 20)
	benchContended(b, []*ProviderPool{pp}, []config.UsenetProvider{p}, 60, 20*time.Millisecond)
}

func BenchmarkPoolContended8x(b *testing.B) {
	// Heavier oversubscription with a faster op: stresses wakeup delivery.
	pp, p := newBenchPool(b, "bench-a", 8)
	benchContended(b, []*ProviderPool{pp}, []config.UsenetProvider{p}, 64, 5*time.Millisecond)
}

func BenchmarkPoolContendedTwoProviders(b *testing.B) {
	// Two primaries, all busy: every waiter goes through raceForConnection.
	pa, a := newBenchPool(b, "bench-a", 20)
	pb, p2 := newBenchPool(b, "bench-b", 20)
	benchContended(b, []*ProviderPool{pa, pb}, []config.UsenetProvider{a, p2}, 120, 20*time.Millisecond)
}

// startSilentServer accepts TCP connections and never sends an NNTP greeting:
// a provider that is up at the TCP level but unresponsive.
func startSilentServer(b *testing.B) *net.TCPAddr {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
		}
	}()
	b.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		for _, conn := range conns {
			_ = conn.Close()
		}
		mu.Unlock()
	})
	return ln.Addr().(*net.TCPAddr)
}

// BenchmarkAcquireDeadPrimary measures per-acquisition latency when the
// highest-priority provider accepts TCP but never answers, while a healthy
// lower-priority provider has warm pooled connections.
func BenchmarkAcquireDeadPrimary(b *testing.B) {
	addr := startSilentServer(b)

	saved := timeouts
	timeouts.HandshakeTimeout = 1 * time.Second
	b.Cleanup(func() { timeouts = saved })

	dead := config.UsenetProvider{Host: "127.0.0.1", Port: addr.Port, MaxConnections: 4, Priority: 1}
	deadPool := &ProviderPool{
		conns:  make([]*connectionEntry, 0, dead.MaxConnections),
		slots:  make(chan struct{}, dead.MaxConnections),
		max:    dead.MaxConnections,
		config: dead,
	}
	healthyPool, healthy := newBenchPool(b, "localhost", 8)
	healthy.Priority = 2

	c := newBenchClient([]config.UsenetProvider{dead, healthy}, []*ProviderPool{deadPool, healthyPool})
	ctx := context.Background()

	b.ResetTimer()
	for range b.N {
		conn, prov, err := c.getAnyAvailableConnection(ctx, providerExclusions{})
		if err != nil {
			b.Fatal(err)
		}
		c.put(conn, prov)
	}
}
