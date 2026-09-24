package manager

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// A job whose context ends mid recovery fetch must not be classified
// terminal: fetchMoreVolumes reports the context error, and so does the
// top-up that wraps it.
func TestFetchMoreVolumesCancelledReportsCtxErr(t *testing.T) {
	vol0 := par2FixtureBytes(t, "fixture.vol0+1.par2")
	vols := []par2Volume{makeTestVol("real0.par2", 1, "real0@news")}
	fetch := mockVolumeFetch(map[string][]byte{"real0@news": vol0}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, sources, added, err := fetchMoreVolumes(ctx, parallelFetchNopLogger, fetch, vols, 0, 1, 0, nil, "entry", 4)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if added != 0 || len(sources) != 0 {
		t.Fatalf("cancelled fetch added %d sources", added)
	}
	if class := classifyPar2Failure(fmt.Errorf("fetch recovery volumes: %w", err)); class.terminal {
		t.Errorf("cancelled recovery fetch classified terminal: %q", class.reason)
	}
}

func TestTopUpCancelledIsTransient(t *testing.T) {
	indexOnly := par2FixtureBytes(t, "fixture.par2")
	vol0 := par2FixtureBytes(t, "fixture.vol0+1.par2")
	vol1 := par2FixtureBytes(t, "fixture.vol1+2.par2")
	idx, err := par2.ParseIndex([]par2.Source{{Name: "fixture.par2", Data: indexOnly}})
	if err != nil {
		t.Fatalf("seed ParseIndex: %v", err)
	}
	vols := []par2Volume{
		makeTestVol("real0.par2", 1, "real0@news"),
		makeTestVol("real1.par2", 2, "real1@news"),
	}
	fetch := mockVolumeFetch(map[string][]byte{"real0@news": vol0, "real1@news": vol1}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, err = topUpParsedRecovery(ctx, parallelFetchNopLogger, fetch, vols, 0, 2, idx,
		[]par2.Source{{Name: "fixture.par2", Data: indexOnly}}, "entry", 4, nil)
	if err == nil {
		t.Fatal("cancelled top-up returned nil; runRepair would report a terminal shortfall")
	}
	if class := classifyPar2Failure(err); class.terminal {
		t.Errorf("cancelled top-up classified terminal: %q", class.reason)
	}
}

// Enough recovery fetched before the context ended is not an error.
func TestFetchMoreVolumesSatisfiedIgnoresLaterCancel(t *testing.T) {
	vol0 := par2FixtureBytes(t, "fixture.vol0+1.par2")
	vols := []par2Volume{makeTestVol("real0.par2", 1, "real0@news")}
	fetch := mockVolumeFetch(map[string][]byte{"real0@news": vol0}, nil)
	_, fetched, _, added, err := fetchMoreVolumes(context.Background(), parallelFetchNopLogger, fetch, vols, 0, 1, 0, nil, "entry", 4)
	if err != nil || added != 1 || fetched < 1 {
		t.Fatalf("live fetch: err=%v added=%d fetched=%d", err, added, fetched)
	}
}

func TestPar2ErrWithJobCtx(t *testing.T) {
	shortfall := errors.New("2 damaged slices but only 0 recovery slices fetched/available")
	if !classifyPar2Failure(shortfall).terminal {
		t.Fatal("precondition: the bare shortfall is terminal")
	}

	live := context.Background()
	if got := par2ErrWithJobCtx(shortfall, live); got != shortfall {
		t.Errorf("live ctx changed the error: %v", got)
	}

	for name, mk := range map[string]func() context.Context{
		"cancelled": func() context.Context { c, cancel := context.WithCancel(live); cancel(); return c },
		"deadline":  func() context.Context { c, cancel := context.WithTimeout(live, 0); defer cancel(); <-c.Done(); return c },
	} {
		t.Run(name, func(t *testing.T) {
			ctx := mk()
			got := par2ErrWithJobCtx(shortfall, ctx)
			if !errors.Is(got, shortfall) || !errors.Is(got, ctx.Err()) {
				t.Fatalf("wrapped error %v lost a cause", got)
			}
			if class := classifyPar2Failure(got); class.terminal {
				t.Errorf("failure after the job ctx ended classified terminal: %q", class.reason)
			}
		})
	}
	if par2ErrWithJobCtx(nil, live) != nil {
		t.Error("nil error not preserved")
	}
}
