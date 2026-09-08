package reader

import (
	"fmt"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
)

// A PAR2 overlay patch is sized in the REPAIR's segment geometry
// (Par2SegmentRef.Bytes - a real probed yEnc size once the source-size probe
// succeeds) while the cache slot it lands in is sized in the READER's
// geometry (NZBSegment.Bytes = 0.97 x the NZB's declared wire bytes). Those
// are independent computations and differ by several KB on a real article, so
// Put must refuse anything that would run past its own slot into the next
// segment's bytes rather than silently corrupting the seam.
func TestSegmentCachePutRefusesOversizedData(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	segments := make([]SegmentMeta, 4)
	for i := range segments {
		segments[i] = SegmentMeta{MessageID: fmt.Sprintf("<seg-%d@test>", i), Number: i + 1, Bytes: 1000}
	}

	cfg := DefaultConfig()
	cfg.DiskPath = t.TempDir()
	stats := &ReaderStats{}

	cache, err := NewSegmentCache(t.Context(), segments, cfg, stats, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewSegmentCache: %v", err)
	}
	defer cache.Close()

	slot := cache.segOffsets[2] - cache.segOffsets[1]
	if slot != 1000 {
		t.Fatalf("setup: slot = %d, want 1000", slot)
	}

	// Exactly the slot: fine.
	if err := cache.Put(1, make([]byte, slot)); err != nil {
		t.Fatalf("Put with an exactly-slot-sized patch: %v", err)
	}
	// Shorter than the slot: fine (a final segment, or a genuinely short one).
	if err := cache.Put(2, make([]byte, slot-100)); err != nil {
		t.Fatalf("Put with a shorter-than-slot patch: %v", err)
	}

	// One byte over: must be refused, not written.
	err = cache.Put(1, make([]byte, slot+1))
	if err == nil {
		t.Fatal("Put with a patch 1 byte past its slot returned nil - it would overrun segment 2")
	}
	t.Logf("refused as expected: %v", err)

	// A realistic overrun: the +5040 measured between a 750000-byte probed
	// posting size and the 744960-byte 0.97 estimate for the same article.
	if err := cache.Put(0, make([]byte, slot+5040)); err == nil {
		t.Fatal("Put with a realistic geometry-divergence overrun returned nil")
	}

	// The refused write must not have advanced the segment's recorded length.
	if got := cache.SegmentDataSize(1); got != slot {
		t.Errorf("SegmentDataSize(1) = %d after a refused oversized Put, want %d (the successful write)", got, slot)
	}
}
