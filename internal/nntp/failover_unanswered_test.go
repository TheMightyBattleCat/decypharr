package nntp

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

// fetchTimingOutOn is fetchBody, except every attempt on host fails with a
// timeout before reaching the server - a provider that holds the article but
// is too slow to hand it over.
func fetchTimingOutOn(c *Client, host, id string) error {
	return c.ExecuteWithFailover(context.Background(), func(conn *Connection) error {
		if conn.address == host {
			return &Error{Type: ErrorTypeTimeout, Message: "test: body idle timeout"}
		}
		var out bytes.Buffer
		_, _, err := conn.StreamBodyMeta(id, &out)
		return err
	})
}

// A provider that only timed out never answered, so a 430 from the others is
// not "missing everywhere": the fetch must come back transient, never as the
// not-found that makes the reader pad the segment and queue a PAR2 repair.
// Seen live 2026-09-22: nine Deep in Orbit S02E01 articles were padded as
// missing on every provider while newshosting, eweka and easynews held them.
func TestExecuteWithFailoverTimeoutPlusNotFoundIsNotMissing(t *testing.T) {
	for _, retries := range []int{0, 2} {
		holder := startFakeNNTP(t, 0)
		holder.bodies = map[string]string{"seg@test": yencWire(testPayload(), 0)}
		empty := startFakeNNTP(t, 0)
		pa, pb := twoLocalProviders(t, holder, empty)
		c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)
		c.retries = retries

		err := fetchTimingOutOn(c, pa.Host, "seg@test")
		if err == nil {
			t.Fatalf("retries=%d: fetch succeeded, want a timeout", retries)
		}
		if IsArticleNotFoundError(err) {
			t.Fatalf("retries=%d: error = %v, want the timeout, not a not-found", retries, err)
		}
		if !IsTimeoutError(err) {
			t.Fatalf("retries=%d: error = %v, want a timeout", retries, err)
		}
	}
}

// When every provider answers 430 the not-found still comes back as before.
func TestExecuteWithFailoverNotFoundEverywhere(t *testing.T) {
	a := startFakeNNTP(t, 0)
	b := startFakeNNTP(t, 0)
	pa, pb := twoLocalProviders(t, a, b)
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)
	c.retries = 2

	var out bytes.Buffer
	err := fetchBody(c, "seg@test", &out)
	if !IsArticleNotFoundError(err) {
		t.Fatalf("error = %v, want not-found", err)
	}
	if a.bodyReqs.Load() != 1 || b.bodyReqs.Load() != 1 {
		t.Fatalf("BODY requests a=%d b=%d, want 1 each", a.bodyReqs.Load(), b.bodyReqs.Load())
	}
}

// A timeout on one provider followed by a 430 on another sharing its
// backbone is a definitive not-found: that backbone answered.
func TestExecuteWithFailoverTimeoutOnAnsweredBackboneIsMissing(t *testing.T) {
	a := startFakeNNTP(t, 0)
	b := startFakeNNTP(t, 0)
	pa, pb := twoLocalProviders(t, a, b)
	pa.Backbone, pb.Backbone = "shared", "shared"
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)
	c.retries = 2

	err := fetchTimingOutOn(c, pa.Host, "seg@test")
	if !IsArticleNotFoundError(err) {
		t.Fatalf("error = %v, want not-found (the shared backbone answered)", err)
	}
}

// A timeout that moves the retry onto another provider must charge that
// provider's 430 to it, not to the one that timed out: the provider that
// timed out is asked again and delivers the article.
func TestExecuteWithFailoverRetryCharges430ToTheProviderThatSentIt(t *testing.T) {
	data := testPayload()
	holder := startFakeNNTP(t, 0)
	holder.bodies = map[string]string{"seg@test": yencWire(data, 0)}
	empty := startFakeNNTP(t, 0)
	pa, pb := twoLocalProviders(t, holder, empty)
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)
	c.retries = 1

	timeouts := 0
	var out bytes.Buffer
	err := c.ExecuteWithFailover(context.Background(), func(conn *Connection) error {
		if conn.address == pa.Host && timeouts == 0 {
			timeouts++
			return &Error{Type: ErrorTypeTimeout, Message: "test: one slow read"}
		}
		out.Reset()
		_, _, err := conn.StreamBodyMeta("seg@test", &out)
		return err
	})
	if err != nil {
		t.Fatalf("fetch: %v, want the article from the provider that first timed out", err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Fatalf("got %d bytes, want %d", out.Len(), len(data))
	}
}

// A connection error is not held against the 430: nothing downstream turns
// repeated connection errors into a pad, so a dead article on a provider that
// keeps resetting would fail every read, never padded or repaired.
func TestExecuteWithFailoverConnectionErrorPlusNotFoundIsMissing(t *testing.T) {
	a := startFakeNNTP(t, 0)
	b := startFakeNNTP(t, 0)
	pa, pb := twoLocalProviders(t, a, b)
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)
	c.retries = 2

	err := c.ExecuteWithFailover(context.Background(), func(conn *Connection) error {
		if conn.address == pa.Host {
			return &Error{Type: ErrorTypeConnection, Message: "test: connection reset"}
		}
		var out bytes.Buffer
		_, _, err := conn.StreamBodyMeta("seg@test", &out)
		return err
	})
	if !IsArticleNotFoundError(err) {
		t.Fatalf("error = %v, want not-found", err)
	}
}

// The error carries what each provider answered, so a caller that pads a
// segment on a not-found can log which providers were asked and what they
// said - and it stays transparent to every error check.
func TestExecuteWithFailoverRecordsProviderOutcomes(t *testing.T) {
	holder := startFakeNNTP(t, 0)
	holder.bodies = map[string]string{"seg@test": yencWire(testPayload(), 0)}
	empty := startFakeNNTP(t, 0)
	pa, pb := twoLocalProviders(t, holder, empty)
	pa.Backbone, pb.Backbone = "shared", "shared" // so the 430 is the verdict
	c := newStatTestClient(t, []config.UsenetProvider{pa, pb}, 100)

	err := fetchTimingOutOn(c, pa.Host, "seg@test")
	if !IsArticleNotFoundError(err) {
		t.Fatalf("error = %v, want not-found (still transparent through the wrapper)", err)
	}
	got := DescribeOutcomes(FailoverOutcomes(err))
	if !strings.Contains(got, pa.Host+"=timeout") || !strings.Contains(got, pb.Host+"=not_found") {
		t.Fatalf("outcomes = %q, want a timeout for %s and a not-found for %s", got, pa.Host, pb.Host)
	}
}
