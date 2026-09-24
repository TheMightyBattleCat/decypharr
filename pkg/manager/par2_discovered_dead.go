package manager

import (
	"sort"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// discoveredDeadRefs returns the articles this pass found missing (430) or
// corrupt on every provider (postedFileFetcher.badArticles) that the overlay
// had not recorded - the pass rebuilt their slices for the solve, and used to
// discard them: playback kept failing on an article PAR2 had just
// reconstructed (North Glen S20E01), and 34 rebuilt ToC articles went to waste
// after a 12 GB pass. Each is mapped to the reader file and segment that
// serves it (by Message-ID) so it can be recorded and patched like a pending
// dead segment. Articles the reader does not serve are skipped.
func discoveredDeadRefs(nzb *storage.NZB, matches []par2.Match, fetchers map[[16]byte]*postedFileFetcher, msgIDRange map[string]postedRange, known []par2DeadRef) []par2DeadRef {
	type readerSeg struct {
		file  string
		index int
		bytes int64
	}
	served := make(map[string]readerSeg)
	for _, f := range nzb.Files {
		if f.IsDeleted {
			continue
		}
		for i, s := range f.Segments {
			served[s.MessageID] = readerSeg{file: f.Name, index: i, bytes: s.Bytes}
		}
	}
	seen := make(map[string]bool, len(known))
	for _, dr := range known {
		seen[dr.seg.MessageID] = true
	}

	var out []par2DeadRef
	for _, m := range matches {
		f := fetchers[m.FileID]
		if f == nil || m.PostedIndex < 0 || m.PostedIndex >= len(nzb.Par2Source) {
			continue
		}
		segs := nzb.Par2Source[m.PostedIndex].Segments
		var bad []int
		f.badArticles.Range(func(k, _ any) bool {
			bad = append(bad, k.(int))
			return true
		})
		sort.Ints(bad)
		for _, i := range bad {
			if i < 0 || i >= len(segs) {
				continue
			}
			id := segs[i].MessageID
			rs, ok := served[id]
			rng, mapped := msgIDRange[id]
			if !ok || !mapped || seen[id] {
				continue
			}
			seen[id] = true
			out = append(out, par2DeadRef{
				file: rs.file,
				seg:  overlay.DeadSegment{Index: rs.index, MessageID: id, Bytes: rs.bytes},
				rng:  rng,
			})
		}
	}
	return out
}
