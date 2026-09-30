package nntp

import (
	"bufio"
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
)

func TestPhaseHistQuantiles(t *testing.T) {
	var h phaseHist
	for range 90 {
		h.observe(3 * time.Millisecond) // bucket [2,5)
	}
	for range 9 {
		h.observe(400 * time.Millisecond) // bucket [300,500)
	}
	h.observe(time.Minute) // above the largest bound

	s := h.snapshot()
	if s["count"] != int64(100) {
		t.Fatalf("count = %v", s["count"])
	}
	if s["p50_ms"] != int64(5) || s["p90_ms"] != int64(500) || s["p99_ms"] != int64(-1) {
		t.Fatalf("quantiles = p50 %v, p90 %v, p99 %v", s["p50_ms"], s["p90_ms"], s["p99_ms"])
	}
}

// The pool records how long a connection is held and how long it then sits
// idle before its next use.
func TestPoolRecordsHoldAndIdle(t *testing.T) {
	p := config.UsenetProvider{Host: "timing", MaxConnections: 1}
	pp := emptyPool(p)
	conn := pipeConn(t, p.Host, true)
	pushIdle(pp, conn, 0)
	c := &Client{pools: map[string]*ProviderPool{p.Host: pp}, providers: []config.UsenetProvider{p}, logger: zerolog.Nop()}

	got, prov, err := c.getAnyAvailableConnection(context.Background(), providerExclusions{})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	c.put(got, prov)
	time.Sleep(30 * time.Millisecond)
	got, prov, err = c.getAnyAvailableConnection(context.Background(), providerExclusions{})
	if err != nil {
		t.Fatal(err)
	}
	c.put(got, prov)

	if hold := pp.timing.hold.snapshot(); hold["count"] != int64(2) || hold["p90_ms"] != int64(50) {
		t.Fatalf("hold = %v, want two samples, the first in the 20-50 ms bucket", hold)
	}
	if idle := pp.timing.idle.snapshot(); idle["count"] != int64(1) || idle["p50_ms"] != int64(50) {
		t.Fatalf("idle = %v, want one sample in the 20-50 ms bucket", idle)
	}
}

// A server that holds its reply shows up as send -> first byte latency, not
// as transfer time.
func TestRequestBodyRecordsLatencyAndTransfer(t *testing.T) {
	c, server := newBodyTestConn(t)
	var pt providerTiming
	c.timing = &pt

	body := "222 0 <a@b> body\r\n" + encodeBody(bodyPayload(64*1024)) + ".\r\n"
	go func() {
		reader := bufio.NewReader(server)
		if _, err := reader.ReadString('\n'); err != nil {
			return
		}
		time.Sleep(120 * time.Millisecond)
		_, _ = server.Write([]byte(body))
	}()

	res, err := c.requestBody("<a@b>", timeouts.StreamBodyTimeout)
	if err != nil {
		t.Fatal(err)
	}
	putBodyBuf(res.Data)

	lat := pt.latency.snapshot()
	if lat["count"] != int64(1) || lat["p50_ms"] != int64(200) {
		t.Fatalf("latency = %v, want one sample in the 100-200 ms bucket", lat)
	}
	tr := pt.transfer.snapshot()
	if tr["count"] != int64(1) || tr["mean_ms"].(float64) >= 100 {
		t.Fatalf("transfer = %v, want one sample well under the reply delay", tr)
	}
}
