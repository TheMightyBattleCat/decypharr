package parser

import (
	"context"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/usenet/types"
)

// A central directory entry whose local header offset leaves no room for
// the 30-byte local header in the volume it is read from is refused before
// any NNTP lookup (the parser here has no client: reaching the lookup would
// fail some other way or panic).
func TestCalculateZIPDataOffsetRefusesOffsetBeyondVolume(t *testing.T) {
	p := &ZIPParser{logger: zerolog.Nop()}
	vols := []*types.Volume{{Name: "a.zip", Size: 1000}}
	for _, off := range []int64{1 << 40, 971, -1} {
		_, err := p.calculateZIPDataOffset(context.Background(), vols, &ZIPFileEntry{Name: "movie.mkv", LocalHeaderOffset: off})
		if err == nil || !strings.Contains(err.Error(), "beyond the volume") {
			t.Errorf("offset %d in a 1000-byte volume: got %v, want a beyond-the-volume error", off, err)
		}
	}
}
