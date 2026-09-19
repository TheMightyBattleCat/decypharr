package manager

import (
	"testing"
	"time"
)

// A burst gets time to fetch what's left at the floor rate: the flat 30
// minutes for an episode, more for a REMUX, capped.
func TestReadAheadTimeout(t *testing.T) {
	for _, tc := range []struct {
		remaining int64
		want      time.Duration
	}{
		{0, precacheReadAheadTimeout},
		{3 << 30, precacheReadAheadTimeout},                     // 3 GiB at 4 MiB/s = 12.8 min
		{26 << 30, time.Duration(26<<30/(4<<20)) * time.Second}, // the 2026-09-18 REMUX: ~1 h 51 min
		{200 << 30, precacheReadAheadTimeoutCap},
	} {
		if got := readAheadTimeout(tc.remaining); got != tc.want {
			t.Errorf("readAheadTimeout(%d) = %v, want %v", tc.remaining, got, tc.want)
		}
	}
}
