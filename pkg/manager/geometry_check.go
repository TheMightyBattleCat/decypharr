package manager

import (
	"context"
	"errors"
	"path/filepath"
	"strings"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
)

// Reasons for a Usenet file assembled wrong at import. The posting itself is
// fine - re-importing the same release under the fixed parser serves it
// correctly - so these re-grab without blocklisting (see keepReleaseReason).
const (
	// reasonSplicedVolumes: a RAR part ran into the next volume and its
	// first articles were served inside the file (871aea3's commit message).
	reasonSplicedVolumes = "import_spliced_volumes"
	// reasonMissingVolume: the file is served an article or more short of
	// its own Matroska Segment - a volume was left out at import.
	reasonMissingVolume = "import_missing_volume"
	// reasonTailTruncated: the file ends less than an article before its
	// Matroska Segment, cutting its index (Cues and/or Tags): it plays but does
	// not seek. The sweep leaves it healthy and unverified; only a manual
	// Replace re-grabs it (ReplaceUnverified).
	reasonTailTruncated = "import_tail_truncated"
	// reasonVolumeOrder: the file's archive volumes are stored in another
	// order than the volumes' own numbers give (legacy imports that kept the
	// upload order), so the file serves the right bytes at the wrong places.
	// Flagged Unverified like reasonTailTruncated; Replace re-grabs it.
	reasonVolumeOrder = "import_volume_order"
)

// keepReleaseReason reports whether a broken file's reason is an import
// fault rather than damage in the posting, so its re-grab must not blocklist
// the release.
func keepReleaseReason(reason string) bool {
	return reason == reasonSplicedVolumes || reason == reasonMissingVolume || replaceableReason(reason)
}

// replaceableReason reports whether an unverified file's reason is one Replace
// acts on: an import fault in a file that still plays.
func replaceableReason(reason string) bool {
	return reason == reasonTailTruncated || reason == reasonVolumeOrder
}

// geometryVerdict is what checkImportGeometry found for one file.
type geometryVerdict struct {
	reason        string // reasonSplicedVolumes or reasonMissingVolume: broken
	tailTruncated bool   // short by less than an article: flagged, not broken
	singleSplice  bool   // one duplicate boundary: flagged, not broken
	shortVolume   bool   // a short volume before a full one: volume order worth checking
	missingStart  bool   // the first stored volume continues the file: its start is missing
	splices       int
	shortBytes    int64
}

// matroskaHeadBytes is how much of a file's start checkImportGeometry reads:
// the EBML header (~40 bytes for "matroska"/"webm") and the Segment element's
// ID and size.
const matroskaHeadBytes = 256

// spliceBoundaries counts adjacent slices with the same article number. A
// normal volume boundary restarts strictly lower (68 -> 1); an equal number
// means a part took the next volume's articles and sorting moved them in.
func spliceBoundaries(segs []storage.NZBSegment) int {
	n := 0
	for i := 1; i < len(segs); i++ {
		if segs[i].Number == segs[i-1].Number {
			n++
		}
	}
	return n
}

// isMatroskaName reports whether name is a Matroska-family file whose head
// tells its own length.
func isMatroskaName(name string) bool {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".mkv", ".mk3d", ".webm":
		return true
	}
	return false
}

var errNotMatroska = errors.New("not a Matroska head")

// ebmlVint reads an EBML variable-length integer at b[i]. keepMarker keeps
// the length bits (element IDs); unknown reports an all-ones size.
func ebmlVint(b []byte, i int, keepMarker bool) (val int64, n int, unknown bool, err error) {
	if i >= len(b) {
		return 0, 0, false, errNotMatroska
	}
	first := b[i]
	mask := byte(0x80)
	n = 1
	for n <= 8 && first&mask == 0 {
		mask >>= 1
		n++
	}
	if n > 8 || i+n > len(b) {
		return 0, 0, false, errNotMatroska
	}
	lowBits := first & (mask - 1)
	unknown = lowBits == mask-1
	if keepMarker {
		val = int64(first)
	} else {
		val = int64(lowBits)
	}
	for k := 1; k < n; k++ {
		val = val<<8 | int64(b[i+k])
		unknown = unknown && b[i+k] == 0xFF
	}
	return val, n, unknown, nil
}

// matroskaSegmentEnd returns the byte offset where head's Matroska Segment
// element ends: the file length the container itself declares. ok is false
// for anything else, including a Segment of unknown size.
func matroskaSegmentEnd(head []byte) (end int64, ok bool) {
	const ebmlID, segmentID = 0x1A45DFA3, 0x18538067
	id, idLen, _, err := ebmlVint(head, 0, true)
	if err != nil || id != ebmlID {
		return 0, false
	}
	size, sizeLen, unknown, err := ebmlVint(head, idLen, false)
	if err != nil || unknown {
		return 0, false
	}
	pos := int64(idLen+sizeLen) + size
	if pos > int64(len(head)) {
		return 0, false
	}
	id, idLen, _, err = ebmlVint(head, int(pos), true)
	if err != nil || id != segmentID {
		return 0, false
	}
	size, sizeLen, unknown, err = ebmlVint(head, int(pos)+idLen, false)
	if err != nil || unknown {
		return 0, false
	}
	return pos + int64(idLen+sizeLen) + size, true
}

// classifyGeometry judges one stored Usenet file from its slices and, when
// available, the first bytes it serves (head; nil to skip the length check).
func classifyGeometry(f *storage.NZBFile, head []byte) geometryVerdict {
	var v geometryVerdict
	if f == nil || len(f.Segments) == 0 {
		return v
	}
	v.shortVolume = f.FileType == storage.NZBFileTypeRar && shortVolumeBeforeFull(layoutVolumes(f.Segments))
	// Several boundaries is unambiguous (Dear Judge: 101). A single one is
	// often the last volume swallowing a small trailing group file (Man of
	// Sun Dao, Netfall): wrong bytes too, but near the end and the least
	// proven shape, so it is flagged rather than re-grabbed.
	switch v.splices = spliceBoundaries(f.Segments); {
	case v.splices > 1:
		v.reason = reasonSplicedVolumes
		return v
	case v.splices == 1:
		v.singleSplice = true
		return v
	}
	if head == nil {
		return v
	}
	end, ok := matroskaSegmentEnd(head)
	if !ok || end <= f.Size {
		return v
	}
	v.shortBytes = end - f.Size
	var article int64
	for _, s := range f.Segments {
		article = max(article, s.Bytes)
	}
	if v.shortBytes >= article {
		v.reason = reasonMissingVolume
	} else {
		v.tailTruncated = true
	}
	return v
}

// checkImportGeometry runs classifyGeometry on a Usenet file's stored NZB
// meta, which costs nothing. With readHead it also reads the file's first
// matroskaHeadBytes from its first article, in memory only (never the DFS
// or segment cache): one article of bandwidth, so the caller gates it to
// probes that decode the file anyway. Any lookup or fetch failure returns an
// empty verdict and leaves the file to the normal checks.
func (r *Repair) checkImportGeometry(ctx context.Context, infoHash, name string, readHead bool) geometryVerdict {
	u := r.manager.usenet
	if u == nil {
		return geometryVerdict{}
	}
	nzb, err := u.GetNZB(infoHash)
	if err != nil || nzb == nil {
		return geometryVerdict{}
	}
	f := nzb.GetFileByName(name)
	if f == nil || len(f.Segments) == 0 {
		return geometryVerdict{}
	}
	var head []byte
	if first := f.Segments[0]; readHead && first.StartOffset == 0 && isMatroskaName(name) && spliceBoundaries(f.Segments) == 0 {
		want := first.SegmentDataStart + min(first.Bytes, matroskaHeadBytes)
		prefix, err := u.FetchArticlePrefix(ctx, first.MessageID, int(want))
		if err == nil && int64(len(prefix)) == want {
			if startsMidArchive(f, prefix) {
				return geometryVerdict{reason: reasonMissingVolume, missingStart: true}
			}
			head = prefix[first.SegmentDataStart:]
		}
	}
	return classifyGeometry(f, head)
}

// startsMidArchive reports whether a stored RAR file's first slice is data
// its archive continues from an earlier volume: the file header whose data
// starts exactly where the stored file does has split-before set, so the
// volume holding the file's first bytes was left out at import. prefix is
// the first slice's article from its first byte. Tide on Sark S02 on
// a production install stored each episode's .r00-.r83 volumes as a file without the
// .rar volume, 49,999,892 B short and with no Matroska header.
func startsMidArchive(f *storage.NZBFile, prefix []byte) bool {
	if f == nil || f.FileType != storage.NZBFileTypeRar || len(f.Segments) == 0 {
		return false
	}
	first := f.Segments[0]
	if first.SegmentDataStart <= 0 {
		return false
	}
	hs, ok := parser.ReadFileHeaderStart(prefix)
	// DataAt == SegmentDataStart is what makes this safe, not SplitBefore:
	// the walk returns a volume's first file header, which is often another
	// file's (Emberly's volume 1 starts with its cover .jpg; a season-set
	// episode starts mid-volume after the previous episode's continued
	// data). Only a header whose data begins exactly where this file's
	// slice does describes this file.
	return ok && hs.SplitBefore && hs.DataAt == first.SegmentDataStart
}
