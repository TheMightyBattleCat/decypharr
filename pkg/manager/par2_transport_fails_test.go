package manager

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// blockUntilDone is a PAR2 article fetch that stalls until its context ends,
// as a news server that never answers does.
func blockUntilDone(ctx context.Context, _ string) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func stallingPar2Volume() storage.Par2FileRef {
	return storage.Par2FileRef{Name: "x.vol00+04.par2", Size: 300, Segments: []storage.Par2SegmentRef{
		{MessageID: "<p1>", Bytes: 100}, {MessageID: "<p2>", Bytes: 100}, {MessageID: "<p3>", Bytes: 100},
	}}
}

// A recovery article that times out while the job is still running is a
// transport failure: the shortfall that follows it is tagged transient and
// the pass backs off instead of going terminal (P-19).
func TestPar2RecoveryArticleTimeoutCountsAsTransportFailure(t *testing.T) {
	old := par2ArticleFetchTimeout
	par2ArticleFetchTimeout = 50 * time.Millisecond
	t.Cleanup(func() { par2ArticleFetchTimeout = old })

	jobCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var fails atomic.Int32
	fetch := countPar2TransportFails(jobCtx, blockUntilDone, &fails)

	start := time.Now()
	_, err := fetchPar2FileConcurrent(jobCtx, fetch, stallingPar2Volume(), 2)
	if err == nil {
		t.Fatal("a volume whose articles all time out was returned as fetched")
	}
	if wall := time.Since(start); wall > 5*time.Second {
		t.Fatalf("fetch took %v; the per-article timeout did not apply", wall)
	}
	if fails.Load() == 0 {
		t.Fatalf("a timed-out PAR2 article was not counted as a transport failure (err %v)", err)
	}

	short := par2RecoveryShortfall(14, 11, int(fails.Load()), 0)
	if !strings.Contains(short.Error(), "(transient:") {
		t.Fatalf("shortfall after a timeout is not tagged transient: %v", short)
	}
	if c := classifyPar2Failure(short); c.terminal {
		t.Fatalf("shortfall after a timeout classed %s, want transient: %v", c.name(), short)
	}
}

// A fetch that fails because the job itself was cancelled says nothing about
// the servers and is not counted.
func TestPar2CancelledJobFetchIsNotATransportFailure(t *testing.T) {
	jobCtx, cancel := context.WithCancel(context.Background())
	cancel()
	var fails atomic.Int32
	fetch := countPar2TransportFails(jobCtx, blockUntilDone, &fails)

	if _, err := fetch(jobCtx, "<p1>"); err == nil {
		t.Fatal("fetch on a cancelled job returned no error")
	}
	if n := fails.Load(); n != 0 {
		t.Fatalf("a fetch ended by the job's cancellation was counted: %d", n)
	}
	if c := classifyPar2Failure(par2RecoveryShortfall(14, 11, int(fails.Load()), 0)); !c.terminal {
		t.Fatalf("a shortfall with no transport failures classed %s, want terminal", c.name())
	}
}
