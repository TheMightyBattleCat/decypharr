package parser

import (
	"context"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/usenet/types"
)

// The last article of a RAR5 volume holds only the end of the file data and
// the end-of-archive block. When it cannot be fetched, the file header read
// from the volume's first article must still count.
func TestRAR5LastArticleFetchFailureKeepsVolume(t *testing.T) {
	parse := func(failLast bool) (int64, []int) {
		posted := newPostedArticles()
		vols := []*types.Volume{
			posted.volume("rar5set.part1.rar", rarFixture(t, "rar5set.part1.rar"), 1000),
			posted.volume("rar5set.part2.rar", rarFixture(t, "rar5set.part2.rar"), 1000),
			posted.volume("rar5set.part3.rar", rarFixture(t, "rar5set.part3.rar"), 1000),
		}
		if failLast {
			last := vols[1].Segments[len(vols[1].Segments)-1].MessageID
			posted.fail[last] = true
		}
		p := &RARParser{logger: zerolog.Nop(), maxConcurrent: 2, fetchBody: posted.fetch}
		info, err := p.parseArchive(context.Background(), vols, "", true)
		if err != nil {
			t.Fatalf("failLast=%v: parseArchive: %v", failLast, err)
		}
		if len(info.Files) != 1 {
			t.Fatalf("failLast=%v: got %d files, want 1", failLast, len(info.Files))
		}
		var parts []int
		var sum int64
		for _, vp := range info.Files[0].VolumeParts {
			parts = append(parts, vp.PartNumber)
			sum += vp.UnpackedSize
		}
		return sum, parts
	}
	wantSum, wantParts := parse(false)
	gotSum, gotParts := parse(true)
	if gotSum != wantSum || len(gotParts) != len(wantParts) {
		t.Fatalf("failing the last article of part2 lost data: bytes %d parts %v, want bytes %d parts %v",
			gotSum, gotParts, wantSum, wantParts)
	}
}
