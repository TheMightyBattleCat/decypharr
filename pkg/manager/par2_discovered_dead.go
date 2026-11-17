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
//
// One article can be served through more than one reader slot: it holds the
// end of one archive member and the start of the next, or it belongs to two
// file records. A ref is returned for every such slot, and for every slot of
// a known (recorded) article other than the known ones, so the slices the
// solve rebuilds are patched into each slot at once. Otherwise the other slot
// was found dead only when read, and needed a second full pass.
func discoveredDeadRefs(nzb *storage.NZB, matches []par2.Match, fetchers map[[16]byte]*postedFileFetcher, msgIDRange map[string]postedRange, known []par2DeadRef) []par2DeadRef {
	type readerSeg struct {
		file  string
		index int
		bytes int64
	}
	type slotKey struct {
		file  string
		index int
	}
	served := make(map[string][]readerSeg)
	for _, f := range nzb.Files {
		if f.IsDeleted {
			continue
		}
		for i, s := range f.Segments {
			served[s.MessageID] = append(served[s.MessageID], readerSeg{file: f.Name, index: i, bytes: s.Bytes})
		}
	}
	seen := make(map[slotKey]bool, len(known))
	for _, dr := range known {
		seen[slotKey{dr.file, dr.seg.Index}] = true
	}

	var out []par2DeadRef
	addSlots := func(id string) {
		rng, mapped := msgIDRange[id]
		if !mapped {
			return
		}
		for _, rs := range served[id] {
			k := slotKey{rs.file, rs.index}
			if seen[k] {
				continue
			}
			seen[k] = true
			out = append(out, par2DeadRef{
				file: rs.file,
				seg:  overlay.DeadSegment{Index: rs.index, MessageID: id, Bytes: rs.bytes},
				rng:  rng,
			})
		}
	}

	// Other slots of the recorded articles first: those articles start in
	// the damaged set, so the pass never fetches them and they never reach
	// badArticles.
	for _, dr := range known {
		addSlots(dr.seg.MessageID)
	}
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
			addSlots(segs[i].MessageID)
		}
	}
	return out
}
