package vfs

import "github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs/ranges"

// dropRangesNotOnDisk removes from rs every byte the data file at path does
// not actually hold, and returns how many bytes it removed.
//
// The metadata writer records a range as present as soon as it is written into
// the item's buffer, but the bytes only reach the file when their RAM block is
// evicted or the item is closed. If the process exits in between (a shutdown
// whose unmount times out, a crash, a kill), the persisted metadata claims a
// hole. Seeding a new buffer with that claim marks the blocks as on disk, so
// reads pread the hole and hand the player zeros with no error - an MKV whose
// header reads as zeros won't open at all. Checking the claim against the
// file's real extents turns those ranges back into missing ones, which are
// downloaded again on demand.
func dropRangesNotOnDisk(path string, rs *ranges.Ranges) int64 {
	if len(*rs) == 0 {
		return 0
	}
	extents, ok := dataExtents(path)
	if !ok {
		return 0
	}
	var kept ranges.Ranges
	for _, r := range *rs {
		for _, e := range extents {
			if in := r.Intersection(e); !in.IsEmpty() {
				kept.Insert(in)
			}
		}
	}
	missing := rs.Size() - kept.Size()
	if missing > 0 {
		*rs = kept
	}
	return missing
}
