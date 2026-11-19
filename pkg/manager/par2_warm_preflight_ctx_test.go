package manager

import (
	"context"
	"testing"
)

// The warm preflight reads every intact slice from the disk cache. A
// cancelled sweep must stop that loop at once, not after reading the whole
// release.
func TestWarmIntactSlicesCachedStopsOnCancel(t *testing.T) {
	idx, _, _ := buildTestIndex(t, 1024, 32*1024, 32*1024) // 64 slices
	if idx.NumSlices() != 64 {
		t.Fatalf("test index has %d slices, want 64", idx.NumSlices())
	}
	damaged := map[int64]struct{}{5: {}}

	t.Run("all cached", func(t *testing.T) {
		reads := 0
		ok := warmIntactSlicesCached(context.Background(), idx, damaged, func([16]byte, int64, int64) error {
			reads++
			return nil
		})
		if !ok || reads != 63 {
			t.Fatalf("ok=%v reads=%d, want true and 63 (every slice but the damaged one)", ok, reads)
		}
	})

	t.Run("already cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		reads := 0
		ok := warmIntactSlicesCached(ctx, idx, damaged, func([16]byte, int64, int64) error {
			reads++
			return nil
		})
		if ok || reads != 0 {
			t.Fatalf("ok=%v reads=%d, want false and no cache reads", ok, reads)
		}
	})

	t.Run("cancelled mid-loop", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		reads := 0
		ok := warmIntactSlicesCached(ctx, idx, damaged, func([16]byte, int64, int64) error {
			reads++
			if reads == 3 {
				cancel()
			}
			return nil
		})
		if ok || reads != 3 {
			t.Fatalf("ok=%v reads=%d, want false and the loop stopped after the 3rd read", ok, reads)
		}
	})
}
