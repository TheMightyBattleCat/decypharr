package backup

import (
	"context"
	"io"
	"time"
)

// throttle keeps the average read rate under rate bytes per second.
type throttle struct {
	rate  int64
	start time.Time
	n     int64
}

// wait accounts for n more bytes and sleeps until the average is back
// under the limit.
func (t *throttle) wait(ctx context.Context, n int) error {
	if t == nil || t.rate <= 0 {
		return nil
	}
	t.n += int64(n)
	due := time.Duration(float64(t.n) / float64(t.rate) * float64(time.Second))
	d := due - time.Since(t.start)
	if d > 0 {
		return sleepCtx(ctx, d)
	}
	if d < -time.Second {
		// Long idle or paused: do not bank the slack as a later burst.
		t.start, t.n = time.Now(), 0
	}
	return nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	tm := time.NewTimer(d)
	defer tm.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-tm.C:
		return nil
	}
}

type throttledReader struct {
	ctx context.Context
	r   io.Reader
	th  *throttle
}

func (t *throttledReader) Read(p []byte) (int, error) {
	if len(p) > 256<<10 {
		p = p[:256<<10]
	}
	n, err := t.r.Read(p)
	if n > 0 {
		if werr := t.th.wait(t.ctx, n); werr != nil {
			return n, werr
		}
	}
	return n, err
}
