package nntp

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// A 430 is an answer about the article only from the providers that gave
// it. When another configured provider could not be dialled (and then sat in
// its dial cooldown), failover still settles on the 430 - playback pads on
// it - but marks it partial and names the provider that was never asked.
func TestFailoverNotFoundIsPartialWhileAProviderIsUnreachable(t *testing.T) {
	srv := startFakeNNTP(t, 0, "gone@test")
	down := config.UsenetProvider{Host: "127.0.0.1", Port: closedPort(t), MaxConnections: 2, Priority: 1}
	up := config.UsenetProvider{Host: "localhost", Port: srv.port(), MaxConnections: 2, Priority: 2}
	c := newStatTestClient(t, []config.UsenetProvider{down, up}, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// First call: the dial to down fails. Second call: down is in its dial
	// cooldown and skipped. Neither is an answer about the article.
	for _, call := range []string{"dial_fails", "dial_cooldown"} {
		_, _, err := c.Stat(ctx, "gone@test")
		if !IsArticleNotFoundError(err) {
			t.Fatalf("%s: err = %v, want a 430 (playback pads on it)", call, err)
		}
		if !IsPartialNotFound(err) {
			t.Fatalf("%s: err = %v not marked partial although %s was never asked", call, err, down.Host)
		}
		if got := UnaskedProviders(err); !slices.Equal(got, []string{down.Host}) {
			t.Errorf("%s: unasked = %v, want [%s]", call, got, down.Host)
		}
	}
	if !c.pools[down.Host].inDialCooldown() {
		t.Error("the unreachable provider is not in its dial cooldown: the second call did not cover the cooldown skip")
	}
}

// Controls: a 430 every configured provider gave, or one a provider on the
// unreachable provider's backbone gave, is complete.
func TestFailoverNotFoundIsCompleteWhenEveryProviderAnswered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("both_430", func(t *testing.T) {
		a := startFakeNNTP(t, 0, "gone@test")
		b := startFakeNNTP(t, 0, "gone@test")
		c := newStatTestClient(t, []config.UsenetProvider{
			{Host: "127.0.0.1", Port: a.port(), MaxConnections: 2, Priority: 1},
			{Host: "localhost", Port: b.port(), MaxConnections: 2, Priority: 2},
		}, 0)
		_, _, err := c.Stat(ctx, "gone@test")
		if !IsArticleNotFoundError(err) || IsPartialNotFound(err) {
			t.Fatalf("err = %v (430=%v partial=%v), want a complete 430", err, IsArticleNotFoundError(err), IsPartialNotFound(err))
		}
	})

	t.Run("single_provider", func(t *testing.T) {
		a := startFakeNNTP(t, 0, "gone@test")
		c := newStatTestClient(t, []config.UsenetProvider{
			{Host: "127.0.0.1", Port: a.port(), MaxConnections: 2},
		}, 0)
		_, _, err := c.Stat(ctx, "gone@test")
		if !IsArticleNotFoundError(err) || IsPartialNotFound(err) {
			t.Fatalf("err = %v, want a complete 430", err)
		}
	})

	t.Run("unreachable_on_answering_backbone", func(t *testing.T) {
		srv := startFakeNNTP(t, 0, "gone@test")
		c := newStatTestClient(t, []config.UsenetProvider{
			{Host: "127.0.0.1", Port: closedPort(t), MaxConnections: 2, Priority: 1, Backbone: "bb"},
			{Host: "localhost", Port: srv.port(), MaxConnections: 2, Priority: 2, Backbone: "bb"},
		}, 0)
		_, _, err := c.Stat(ctx, "gone@test")
		if !IsArticleNotFoundError(err) || IsPartialNotFound(err) {
			t.Fatalf("err = %v (partial=%v), want a complete 430: a provider on the same backbone answered", err, IsPartialNotFound(err))
		}
	})
}
