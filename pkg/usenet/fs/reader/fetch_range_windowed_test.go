package reader

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/nntp"
)

// newWindowedTestReader builds a StreamingReader over nSegs equal-sized segments
// with every segment pre-marked StateOnDisk, so Fetch is an instant no-op that
// never touches the (nil) NNTP client. FetchRangeWindowed then exercises purely
// its worker/cursor/horizon logic.
func newWindowedTestReader(t *testing.T, nSegs int, segBytes int64) *StreamingReader {
	t.Helper()
	config.SetConfigPath(t.TempDir())

	segments := make([]SegmentMeta, nSegs)
	for i := range segments {
		segments[i] = SegmentMeta{MessageID: fmt.Sprintf("<win-%d@test>", i), Number: i + 1, Bytes: segBytes}
	}

	sr, err := NewStreamingReader(context.Background(), &nntp.Client{}, segments,
		WithMaxConnections(6),
		WithDiskPath(t.TempDir()),
		WithLogger(zerolog.Nop()),
	)
	if err != nil {
		t.Fatalf("NewStreamingReader: %v", err)
	}
	for i := range segments {
		sr.cache.SetState(i, StateOnDisk)
	}
	return sr
}

// TestFetchRangeWindowed_HorizonMeasuredFromBase proves the horizon (and the
// window it defines) is measured from `base`, not from the absolute file
// offset. On the sweep's mid-file decode windows `base` is multi-GB; if the
// method compared SegmentOffset(seg) directly against horizon(), every segment
// would sit past the horizon forever and the prefetch would fetch nothing.
//
// Here base = 10 MiB and total = 50 MiB while the file is 60 MiB. Held at the
// ramp the call must not return; with the horizon fully open at `total` the
// correct (base-relative) code drains the whole [base, base+total) range, while
// the buggy (absolute) code leaves every segment past absolute 50 MiB parked
// and never returns.
func TestFetchRangeWindowed_HorizonMeasuredFromBase(t *testing.T) {
	const segBytes = 1 << 20
	sr := newWindowedTestReader(t, 60, segBytes)
	defer sr.Close()

	const base = int64(10 * segBytes)
	const total = int64(50 * segBytes)

	var horizon atomic.Int64
	horizon.Store(-1) // ramp

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- sr.FetchRangeWindowed(ctx, base, total, 8, func() int64 { return horizon.Load() })
	}()

	// Ramp: a negative horizon holds every worker off, so the call must not return.
	select {
	case <-done:
		t.Fatal("FetchRangeWindowed returned during the ramp (horizon -1)")
	case <-time.After(150 * time.Millisecond):
	}

	// Open the horizon fully. Base-relative: seg 59 starts 49 MiB from base
	// <= 50 MiB -> drains -> returns. Absolute: seg 59 at 59 MiB > 50 MiB ->
	// parked -> hangs.
	horizon.Store(total)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("FetchRangeWindowed returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("FetchRangeWindowed did not drain after the horizon opened - horizon likely not measured from base")
	}
}

// TestFetchRangeWindowed_ReturnsOnCancel proves a stalled window does not pin
// the caller: cancelling ctx returns promptly with a non-nil (ctx) error.
func TestFetchRangeWindowed_ReturnsOnCancel(t *testing.T) {
	const segBytes = 1 << 20
	sr := newWindowedTestReader(t, 20, segBytes)
	defer sr.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		// horizon frozen at 0: only a segment starting exactly at offset 0 is
		// eligible; the rest park until we cancel.
		done <- sr.FetchRangeWindowed(ctx, 0, int64(20*segBytes), 4, func() int64 { return 0 })
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("FetchRangeWindowed returned nil after cancel, want ctx.Err()")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("FetchRangeWindowed did not return after ctx cancel")
	}
}
