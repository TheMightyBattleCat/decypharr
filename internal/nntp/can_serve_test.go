package nntp

import (
	"context"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

// CanServe answers "could a checkout get a connection now" the way
// getAnyAvailableConnection decides it, without taking a slot: a lead for
// the context's priority must exist and one of them must not be cooling
// down after failed dials.
func TestCanServe(t *testing.T) {
	urgent := WithPriority(context.Background(), PriorityUrgent)
	primary := config.UsenetProvider{Host: "primary", Port: 1, Priority: 1, MaxConnections: 2}
	backup := config.UsenetProvider{Host: "backup", Port: 2, Priority: 2, MaxConnections: 2, Backup: true}

	t.Run("normal primary", func(t *testing.T) {
		c := newStatTestClient(t, []config.UsenetProvider{primary}, 100)
		if ok, reason := c.CanServe(context.Background()); !ok {
			t.Fatalf("CanServe = false (%s), want true", reason)
		}
		if got := len(c.pools[primary.Host].slots); got != 0 {
			t.Fatalf("CanServe took %d slot(s)", got)
		}
	})

	t.Run("reserve band", func(t *testing.T) {
		c := newStatTestClientBW(t, []config.UsenetProvider{primary, backup}, 100, quotaTracker(primary.Host, 95, 100, 10))
		ok, reason := c.CanServe(context.Background())
		if ok || !strings.Contains(reason, "quota") {
			t.Fatalf("bulk CanServe = %v (%q), want false naming the quota (backup alone is fill-only)", ok, reason)
		}
		if _, _, err := c.getAnyAvailableConnection(context.Background(), providerExclusions{}); err == nil || !strings.Contains(err.Error(), "no eligible providers available") {
			t.Fatalf("bulk checkout err = %v; CanServe disagrees with getAnyAvailableConnection", err)
		}
		if ok, reason := c.CanServe(urgent); !ok {
			t.Fatalf("urgent CanServe = false (%s), want true: an urgent request draws a primary's reserve", reason)
		}
	})

	t.Run("hard quota", func(t *testing.T) {
		c := newStatTestClientBW(t, []config.UsenetProvider{primary}, 100, quotaTracker(primary.Host, 100, 100, 10))
		if ok, _ := c.CanServe(urgent); ok {
			t.Fatal("urgent CanServe = true with the only primary over its hard quota")
		}
	})

	t.Run("every primary cooling down", func(t *testing.T) {
		second := config.UsenetProvider{Host: "second", Port: 3, Priority: 2, MaxConnections: 2}
		c := newStatTestClient(t, []config.UsenetProvider{primary, second}, 100)
		c.pools[primary.Host].noteDialFailure()
		if ok, reason := c.CanServe(context.Background()); !ok {
			t.Fatalf("CanServe = false (%s) with one primary still warm", reason)
		}
		c.pools[second.Host].noteDialFailure()
		ok, reason := c.CanServe(context.Background())
		if ok || !strings.Contains(reason, "cooling down") {
			t.Fatalf("CanServe = %v (%q), want false naming the cooldown", ok, reason)
		}
		// A pool cooling down but with an idle connection can still serve.
		pp := c.pools[second.Host]
		pp.mu.Lock()
		pp.conns = append(pp.conns, &connectionEntry{})
		pp.mu.Unlock()
		if ok, reason := c.CanServe(context.Background()); !ok {
			t.Fatalf("CanServe = false (%s) with an idle connection in a cooling pool", reason)
		}
		pp.mu.Lock()
		pp.conns = nil
		pp.mu.Unlock()
	})

	t.Run("no providers", func(t *testing.T) {
		c := newStatTestClient(t, nil, 100)
		if ok, _ := c.CanServe(context.Background()); ok {
			t.Fatal("CanServe = true with no providers")
		}
	})
}
