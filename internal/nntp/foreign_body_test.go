package nntp

import (
	"bytes"
	"context"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"

	"github.com/sirrobot01/decypharr/internal/config"
)

// A BODY reply the caller refuses as another upload's article is counted
// against the provider that sent it, and only that one; failover still moves
// on to the next provider as for a 430. A plain 430 is not counted.
func TestExecuteWithFailoverCountsForeignBodiesPerProvider(t *testing.T) {
	data := testPayload()
	foreign := data[:len(data)/3] // a part of the wrong size: not the one asked for
	bad := startFakeNNTP(t, 0)
	bad.bodies = map[string]string{"seg@test": yencWire(foreign, 0)}
	good := startFakeNNTP(t, 0)
	good.bodies = map[string]string{"seg@test": yencWire(data, 0)}
	pa, pb := twoLocalProviders(t, bad, good)
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)

	var out bytes.Buffer
	err := c.ExecuteWithFailover(context.Background(), func(conn *Connection) error {
		out.Reset()
		_, meta, err := conn.StreamBodyMeta("seg@test", &out)
		if err != nil {
			return err
		}
		if meta == nil || meta.Size != int64(len(data)) {
			return NewForeignArticleError("wrong size")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("fetch with the right article on the second provider: %v", err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Fatalf("got %d bytes, want the second provider's %d", out.Len(), len(data))
	}
	if got := c.pools[pa.Host].foreignBodies.Load(); got != 1 {
		t.Errorf("foreign bodies on the provider that sent one = %d, want 1", got)
	}
	if got := c.pools[pb.Host].foreignBodies.Load(); got != 0 {
		t.Errorf("foreign bodies on the provider that sent the right one = %d, want 0", got)
	}

	// A 430 everywhere is a missing article, not a foreign one.
	if err := fetchBody(c, "gone@test", &out); !IsArticleNotFoundError(err) {
		t.Fatalf("missing article: error = %v, want not-found", err)
	}
	if a, b := c.pools[pa.Host].foreignBodies.Load(), c.pools[pb.Host].foreignBodies.Load(); a != 1 || b != 0 {
		t.Errorf("after a plain 430: foreign bodies %d/%d, want 1/0", a, b)
	}

	// The count is in the provider stats.
	c.speedTestResults = xsync.NewMap[string, SpeedTestResult]() // as NewClient sets it
	stats := c.Stats()
	provs, _ := stats["providers"].([]map[string]any)
	seen := 0
	for _, p := range provs {
		want := int64(0)
		if p["host"] == pa.Host {
			want = 1
		}
		if got, ok := p["body_foreign"].(int64); !ok || got != want {
			t.Errorf("stats for %v: body_foreign = %v, want %d", p["host"], p["body_foreign"], want)
		}
		seen++
	}
	if seen != 2 {
		t.Fatalf("stats list %d providers, want 2: %v", seen, stats)
	}
}
