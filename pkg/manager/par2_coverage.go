package manager

import (
	"strings"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// Par2Coverage summarises how much PAR2 recovery a release was posted with
// and how much of it one repair can use under the current limits. It is
// worked out from the retained PAR2 file names and sizes alone, with no
// network call, so it describes what was posted: recovery articles that have
// expired since are not accounted for, and SliceSize is an estimate.
type Par2Coverage struct {
	RecoverySlicesPosted int   `json:"recovery_slices_posted"`
	SliceSize            int64 `json:"slice_size"`
	// MaxRepairableSlices is how many damaged slices one repair could
	// rebuild: the posted recovery, capped by par2.MaxRepairSlicesFor.
	MaxRepairableSlices int   `json:"max_repairable_slices"`
	MaxRepairableBytes  int64 `json:"max_repairable_bytes"`
	// LimitedBy names what sets MaxRepairableSlices: "posting" (every
	// posted recovery slice is usable), "slice_cap" or "memory".
	LimitedBy string `json:"limited_by"`
	// Sets is how many named PAR2 sets the release carries (a season pack
	// often has one per episode). A repair uses one set, so the figures
	// above are for the set with the most recovery, not a total.
	Sets int `json:"sets"`
}

// par2RecoveryPacketOverhead is a recovery packet's size beyond its slice:
// the 64-byte packet header and the 4-byte exponent.
const par2RecoveryPacketOverhead = 68

// Par2CoverageFor estimates nzb's Par2Coverage from its retained PAR2 file
// list. Named volumes (.volN+M) are grouped by set name and the set with the
// most recovery is used: its volume names give the slice count, and its
// largest volume, less its index file's fixed packets, gives the slice size.
// An obfuscated posting falls back to unnamedRecoveryEstimate's size steps.
// false when there is nothing to estimate from.
func Par2CoverageFor(files []storage.Par2FileRef) (Par2Coverage, bool) {
	vols, indexFiles := censusPar2Volumes(files)
	var posted, perSlice int64
	sets := 1
	if len(vols) > 0 {
		bySet := map[string][]par2Volume{}
		for _, v := range vols {
			stem := strings.ToLower(par2VolPattern.ReplaceAllString(v.ref.Name, ""))
			bySet[stem] = append(bySet[stem], v)
		}
		sets = len(bySet)
		var best string
		var bestCount int64
		for stem, set := range bySet {
			var n int64
			for _, v := range set {
				n += int64(v.count)
			}
			if n > bestCount || (n == bestCount && stem < best) {
				best, bestCount = stem, n
			}
		}
		// Every PAR2 file of a set carries the same fixed packets; the set's
		// own index file is the best measure of them, else the smallest one.
		var base, anyBase int64
		for _, f := range indexFiles {
			if f.Size <= 0 {
				continue
			}
			if anyBase == 0 || f.Size < anyBase {
				anyBase = f.Size
			}
			if strings.TrimSuffix(strings.ToLower(f.Name), ".par2") == best && (base == 0 || f.Size < base) {
				base = f.Size
			}
		}
		if base == 0 {
			base = anyBase
		}
		largest := bySet[best][0]
		for _, v := range bySet[best] {
			if v.count > largest.count {
				largest = v
			}
		}
		posted = bestCount
		if largest.ref.Size > base {
			perSlice = (largest.ref.Size - base) / int64(largest.count)
		}
	} else {
		n, step := unnamedRecoveryEstimate(indexFiles)
		posted, perSlice = int64(n), step
	}
	sliceSize := (perSlice - par2RecoveryPacketOverhead) &^ 3 // PAR2 slices are a multiple of 4
	if posted <= 0 || sliceSize <= 0 {
		return Par2Coverage{}, false
	}

	limit := par2.MaxRepairSlicesFor(sliceSize)
	c := Par2Coverage{
		RecoverySlicesPosted: int(min(posted, int64(^uint32(0)))),
		SliceSize:            sliceSize,
		MaxRepairableSlices:  int(min(posted, int64(limit))),
		Sets:                 sets,
	}
	switch {
	case posted <= int64(limit):
		c.LimitedBy = "posting"
	case limit < par2.MaxRepairSlices:
		c.LimitedBy = "memory"
	default:
		c.LimitedBy = "slice_cap"
	}
	c.MaxRepairableBytes = int64(c.MaxRepairableSlices) * sliceSize
	return c, true
}
