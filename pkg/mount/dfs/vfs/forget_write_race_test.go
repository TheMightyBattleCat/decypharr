package vfs

import (
	"errors"
	"sync"
	"testing"

	"github.com/sirrobot01/decypharr/internal/buffer"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs/ranges"
)

// TestForgetRangeDuringWriteLeavesTrackersInStep races ForgetRange against
// WriteAtNoOverwrite on the same range. Whatever order they land in, the
// item's range tracker must never claim bytes the buffer has dropped: a
// read of such a range fails with "buffer reported missing range" and,
// once the sidecar is saved, a reopened item seeds the hole as present.
func TestForgetRangeDuringWriteLeavesTrackersInStep(t *testing.T) {
	c := newWritableTestCache(t)
	const size = 1 << 20
	item := newWritableTestItem(t, c, "Some.Release", "episode.mkv", size)
	t.Cleanup(func() { _ = item.buf.Close() })

	p := make([]byte, size)
	for i := range p {
		p[i] = byte(i%251) + 1
	}
	r := ranges.Range{Pos: 0, Size: size}
	got := make([]byte, size)

	const rounds = 400
	bad := 0
	for i := 0; i < rounds; i++ {
		item.ForgetRange(0, size)

		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			if _, _, err := item.WriteAtNoOverwrite(p, 0); err != nil {
				t.Errorf("round %d: WriteAtNoOverwrite: %v", i, err)
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			item.ForgetRange(0, size)
		}()
		close(start)
		wg.Wait()

		if !item.HasRange(r) {
			continue
		}
		if _, err := item.buf.ReadAt(got, 0); err != nil {
			if !errors.Is(err, buffer.ErrNotPresent) {
				t.Fatalf("round %d: unexpected read error: %v", i, err)
			}
			bad++
		}
	}
	if bad > 0 {
		t.Fatalf("%d of %d rounds: tracker claims the range present but the buffer dropped it", bad, rounds)
	}
}
