package nntp

import (
	"context"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

// A provider that reports a dead article present settles it as available.
// The result names that provider, and with it excluded the others' answer
// stands: not found.
func TestBatchStatNamesTheProviderAndHonoursExcludedHosts(t *testing.T) {
	liar := startFakeNNTP(t, 0)                // reports everything present
	honest := startFakeNNTP(t, 0, "dead@test") // does not have the dead article
	pl, ph := twoLocalProviders(t, liar, honest)
	c := newStatTestClient(t, []config.UsenetProvider{pl, ph}, 100)
	msgIDs := []string{"dead@test", "alive@test"}

	for _, home := range []config.UsenetProvider{pl, ph} {
		res, err := c.batchStatAcrossProviders(context.Background(), msgIDs, home, nil)
		if err != nil {
			t.Fatalf("home %s: %v", home.Host, err)
		}
		if !res[0].Available || res[0].Host != pl.Host {
			t.Fatalf("home %s: dead article = %+v, want present and attributed to %s", home.Host, res[0], pl.Host)
		}
		if !res[1].Available || res[1].Host == "" {
			t.Fatalf("home %s: alive article = %+v, want present with the answering host", home.Host, res[1])
		}

		ctx := WithStatExcludedHosts(context.Background(), []string{pl.Host})
		before := liar.stats.Load()
		res, err = c.batchStatAcrossProviders(ctx, msgIDs, home, nil)
		if err != nil {
			t.Fatalf("home %s, liar excluded: %v", home.Host, err)
		}
		if res[0].Available || !IsArticleNotFoundError(res[0].Error) {
			t.Fatalf("home %s, liar excluded: dead article = %+v, want article-not-found", home.Host, res[0])
		}
		if !res[1].Available || res[1].Host != ph.Host {
			t.Fatalf("home %s, liar excluded: alive article = %+v, want present on %s", home.Host, res[1], ph.Host)
		}
		if got := liar.stats.Load() - before; got != 0 {
			t.Fatalf("home %s: the excluded provider was sent %d STATs, want 0", home.Host, got)
		}
	}
}

// With every provider excluded nobody was asked: the result is "could not
// be verified", never a not-found the caller would read as proof of damage.
func TestBatchStatWithEveryHostExcludedIsUnverified(t *testing.T) {
	a := startFakeNNTP(t, 0, "dead@test")
	b := startFakeNNTP(t, 0, "dead@test")
	pa, pb := twoLocalProviders(t, a, b)
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)

	ctx := WithStatExcludedHosts(context.Background(), []string{pa.Host, pb.Host})
	res, err := c.batchStatAcrossProviders(ctx, []string{"dead@test"}, pa, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Available || res[0].Error == nil || IsArticleNotFoundError(res[0].Error) {
		t.Fatalf("result = %+v, want an unverified error that is not article-not-found", res[0])
	}
}

// An empty exclusion list leaves the context, and so the call, unchanged.
func TestWithStatExcludedHostsEmptyIsNoOp(t *testing.T) {
	ctx := context.Background()
	if WithStatExcludedHosts(ctx, nil) != ctx || WithStatExcludedHosts(ctx, []string{""}) != ctx {
		t.Fatal("an empty exclusion list wrapped the context")
	}
}

// The same holds through the whole call - the probe before the chunks and
// the pool's workers, whose home may be the provider left out: it is sent
// nothing, and the dead article comes back not found.
func TestBatchStatCompleteNeverAsksAnExcludedHost(t *testing.T) {
	liar := startFakeNNTP(t, 0)
	honest := startFakeNNTP(t, 0, "dead@test")
	pl, ph := twoLocalProviders(t, liar, honest)
	c := newStatTestClient(t, []config.UsenetProvider{pl, ph}, 100)

	msgIDs := append(ids("x", 400), "dead@test")
	res, err := c.BatchStatComplete(context.Background(), msgIDs)
	if err != nil || res.FoundCount != len(msgIDs) {
		t.Fatalf("without exclusion: found %d/%d, err %v (the liar should settle the dead article as present)", res.FoundCount, len(msgIDs), err)
	}

	before := liar.stats.Load()
	ctx := WithStatExcludedHosts(context.Background(), []string{pl.Host})
	res, err = c.BatchStatComplete(ctx, msgIDs)
	if err != nil {
		t.Fatal(err)
	}
	if got := liar.stats.Load() - before; got != 0 {
		t.Fatalf("the excluded provider was sent %d STATs, want 0", got)
	}
	if res.FoundCount != len(msgIDs)-1 || res.ErrorCount != 0 {
		t.Fatalf("found %d, errors %d of %d; want every article but the dead one found and no errors", res.FoundCount, res.ErrorCount, len(msgIDs))
	}
	last := res.Results[len(msgIDs)-1]
	if last.MessageID != "dead@test" || last.Available || !IsArticleNotFoundError(last.Error) {
		t.Fatalf("dead article = %+v, want article-not-found", last)
	}
}
