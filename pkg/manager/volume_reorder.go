package manager

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
)

// Putting a misordered file's volumes back in order, in place.
//
// A file stored out of volume order has every article it needs; only the
// order is wrong. Where the right order is known, the volumes that change
// places are laid out alike, and the file's own content agrees, its stored
// layout is rewritten and nothing is downloaded again. Anything short of that
// is left for Replace to re-grab.

// errReorderRefused wraps every reason a file is not reordered in place: the
// caller falls back to a re-grab.
var errReorderRefused = errors.New("not reordered in place")

// errReorderBusy marks the one refusal that passes by itself: something has
// the file open. A sweep tries that file again; every other refusal is about
// the file and is recorded.
var errReorderBusy = errors.New("the file is open")

func refuseReorder(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errReorderRefused, fmt.Sprintf(format, args...))
}

// clusterSearchArticles is how many articles from a volume's start are read
// looking for a Matroska Cluster. Clusters in the largest files seen run to a
// few MB; a volume that shows none in this many articles is not confirmed.
const clusterSearchArticles = 8

// volumeReorder returns the order the stored volumes belong in: order[k] is
// the stored volume that should stand k-th. nums are the volumes' numbers from
// checkFileVolumeOrder. Every volume needs a distinct number, except that the
// first stored volume may have none (a RAR5 set's first volume carries no
// number) and then stays first. Refuses when nothing would move.
func volumeReorder(nums []int, source string) ([]int, error) {
	if source == volumeSourceRAR4First || source == "" || source == volumeSourceImport {
		return nil, refuseReorder("volume numbers from %q do not give a full order", source)
	}
	seen := make(map[int]bool, len(nums))
	lowest := 0
	for k, n := range nums {
		if n < 0 {
			if k != 0 {
				return nil, refuseReorder("stored volume %d has no number", k+1)
			}
			continue
		}
		if seen[n] {
			return nil, refuseReorder("two volumes are numbered %d", n)
		}
		seen[n] = true
		if len(seen) == 1 || n < lowest {
			lowest = n
		}
	}
	key := func(k int) int {
		if nums[k] < 0 {
			return lowest - 1
		}
		return nums[k]
	}
	order := make([]int, len(nums))
	for k := range order {
		order[k] = k
	}
	slices.SortStableFunc(order, func(a, b int) int { return key(a) - key(b) })
	for k, src := range order {
		if k != src {
			return order, nil
		}
	}
	return nil, refuseReorder("the volumes are already in order")
}

// applyVolumeOrder returns segs with the stored volumes in order. A volume
// that moves takes the place of one laid out exactly like it - as many
// articles, each giving the same bytes from the same data start - so every
// offset in the file stays what it was and only the articles behind it change.
// Volumes of other shapes (a short last volume, a first one that starts
// mid-archive) would need their offsets worked out again from headers this
// does not read: refused.
func applyVolumeOrder(segs []storage.NZBSegment, vols []layoutVolume, order []int) ([]storage.NZBSegment, error) {
	if len(order) != len(vols) {
		return nil, refuseReorder("order names %d volumes, the file has %d", len(order), len(vols))
	}
	out := slices.Clone(segs)
	for k, src := range order {
		if src == k {
			continue
		}
		to, from := vols[k], vols[src]
		if to.segs != from.segs {
			return nil, refuseReorder("stored volumes %d and %d hold %d and %d articles", k+1, src+1, to.segs, from.segs)
		}
		for j := 0; j < to.segs; j++ {
			slot, art := segs[to.first+j], segs[from.first+j]
			if slot.Bytes != art.Bytes || slot.SegmentDataStart != art.SegmentDataStart ||
				slot.EndOffset-slot.StartOffset != art.EndOffset-art.StartOffset {
				return nil, refuseReorder("stored volumes %d and %d are laid out differently at article %d", k+1, src+1, j+1)
			}
			art.StartOffset, art.EndOffset = slot.StartOffset, slot.EndOffset
			out[to.first+j] = art
		}
	}
	return out, nil
}

// volumeClusterTimestamp returns the timestamp of the first Matroska Cluster
// in the file's bytes from the start of one stored volume, reading up to
// clusterSearchArticles articles.
func volumeClusterTimestamp(ctx context.Context, segs []storage.NZBSegment, vol layoutVolume, body volumeBodyFunc) (int64, bool, error) {
	var data []byte
	for j := 0; j < min(vol.segs, clusterSearchArticles); j++ {
		seg := segs[vol.first+j]
		b, err := body(ctx, seg.MessageID)
		if err != nil {
			return 0, false, err
		}
		if seg.SegmentDataStart < int64(len(b)) {
			b = b[seg.SegmentDataStart:]
			if seg.Bytes > 0 && seg.Bytes < int64(len(b)) {
				b = b[:seg.Bytes]
			}
			data = append(data, b...)
		}
		if ts, ok := clusterTimestamp(data); ok {
			return ts, true, nil
		}
	}
	return 0, false, nil
}

// confirmOrderByContent checks order against the file itself: over the
// stretch of volumes that change places, and the one either side, the first
// Cluster timestamp of each volume must rise in the new order. Every volume
// that moves has to show one. Volume numbers can come from the poster's file
// names alone; this is what says the names were right.
func confirmOrderByContent(ctx context.Context, f *storage.NZBFile, vols []layoutVolume, order []int, body volumeBodyFunc) error {
	if !f.IsStored || f.IsEncrypted || !isMatroskaName(f.Name) {
		return refuseReorder("only a Matroska file stored uncompressed can be checked against its own content")
	}
	lo, hi := -1, -1
	for k, src := range order {
		if src != k {
			if lo < 0 {
				lo = k
			}
			hi = k
		}
	}
	lo, hi = max(0, lo-1), min(len(order)-1, hi+1)
	prev, prevPos := int64(-1), -1
	for k := lo; k <= hi; k++ {
		ts, ok, err := volumeClusterTimestamp(ctx, f.Segments, vols[order[k]], body)
		if err != nil {
			return fmt.Errorf("reading stored volume %d: %w", order[k]+1, err)
		}
		if !ok {
			if order[k] != k {
				return refuseReorder("stored volume %d shows no Matroska Cluster in its first %d articles", order[k]+1, clusterSearchArticles)
			}
			continue
		}
		if prevPos >= 0 && ts <= prev {
			return refuseReorder("the content disagrees: volume %d in the new order is timed %d, after %d", k+1, ts, prev)
		}
		prev, prevPos = ts, k
	}
	return nil
}

// reorderFileVolumes puts a stored RAR file's volumes in order in place and
// returns how many moved. It reads the volumes' numbers afresh, and rewrites
// the layout only when applyVolumeOrder and confirmOrderByContent both accept
// it, the release has no overlay records (they name articles by position),
// and nothing has the file open. The record as it was is kept beside the
// meta store (usenet.ReorderFileSegments). Any other outcome wraps
// errReorderRefused or is a fetch or storage error; the file is untouched.
func (r *Repair) reorderFileVolumes(ctx context.Context, entryName, infoHash, name string) (int, error) {
	u := r.manager.usenet
	if u == nil {
		return 0, refuseReorder("usenet is not configured")
	}
	nzb, err := u.GetNZB(infoHash)
	if err != nil || nzb == nil {
		return 0, refuseReorder("no stored record for the release")
	}
	f := nzb.GetFileByName(name)
	if f == nil || f.FileType != storage.NZBFileTypeRar || len(f.Segments) == 0 {
		return 0, refuseReorder("not a stored RAR file")
	}
	if u.OverlayEntryExists(infoHash) {
		return 0, refuseReorder("the release has repair overlay records, which name articles by their place in the file")
	}
	cctx, cancel := context.WithTimeout(ctx, volumeOrderTimeout)
	defer cancel()
	head := func(ctx context.Context, id string) (usenet.ArticleHead, error) {
		return u.FetchArticleHead(ctx, id, parser.EncryptedVolumeHeadBytes)
	}
	vo, err := checkFileVolumeOrder(cctx, f, nzb.Par2Source, head, u.FetchArticle)
	if err != nil {
		return 0, fmt.Errorf("reading volume numbers: %w", err)
	}
	if !vo.misordered {
		return 0, refuseReorder("the volumes read as in order")
	}
	vols := layoutVolumes(f.Segments)
	order, err := volumeReorder(vo.numbers, vo.source)
	if err != nil {
		return 0, err
	}
	segs, err := applyVolumeOrder(f.Segments, vols, order)
	if err != nil {
		return 0, err
	}
	if err := confirmOrderByContent(cctx, f, vols, order, u.FetchArticle); err != nil {
		return 0, err
	}
	moved := 0
	for k, src := range order {
		if src != k {
			moved++
		}
	}

	if !r.dropFileCaches(entryName, infoHash, name) {
		return 0, fmt.Errorf("%w: %w; try again when nothing is playing it", errReorderRefused, errReorderBusy)
	}
	backup, err := u.ReorderFileSegments(infoHash, name, volumeOrderKey(infoHash, f), func(cur *storage.NZBFile) string {
		return volumeOrderKey(infoHash, cur)
	}, segs)
	if err != nil {
		return 0, err
	}
	// Read the stored record back and check it the way the sweep will: the
	// volumes must now read as in order. Anything else is undone.
	if err := r.verifyReordered(cctx, infoHash, name, head); err != nil {
		if rerr := u.RestoreNZBRecord(infoHash, backup); rerr != nil {
			r.logger.Error().Err(rerr).Str("entry", entryName).Str("file", name).Str("backup", backup).
				Msg("Repair: the reordered layout failed its check and the record could not be put back; restore it from the backup by hand")
			return 0, fmt.Errorf("reordered layout failed its check (%v) and was not restored: %w", err, rerr)
		}
		r.dropFileCaches(entryName, infoHash, name)
		return 0, refuseReorder("the reordered layout failed its check and was put back: %v", err)
	}
	// A reader opened between the first drop and the write holds the old
	// layout; drop again so nothing it read stays cached.
	if !r.dropFileCaches(entryName, infoHash, name) {
		r.logger.Warn().Str("entry", entryName).Str("file", name).
			Msg("Repair: the file was opened while its volumes were being reordered; what that viewer reads may be cached in the old order until the file is closed and its cache cleared")
	}
	r.logger.Warn().Str("entry", entryName).Str("file", name).Int("volumes", len(vols)).Int("moved", moved).
		Str("source", vo.source).Str("backup", backup).
		Msg("Repair: put the file's archive volumes back in order in place; no re-grab needed")
	return moved, nil
}

// verifyReordered loads the record reorderFileVolumes just wrote and reads its
// volume numbers again.
func (r *Repair) verifyReordered(ctx context.Context, infoHash, name string, head volumeHeadFunc) error {
	u := r.manager.usenet
	nzb, err := u.GetNZB(infoHash)
	if err != nil || nzb == nil {
		return fmt.Errorf("the record does not load: %v", err)
	}
	f := nzb.GetFileByName(name)
	if f == nil || !f.LayoutCoversSize() {
		return errors.New("the file's articles no longer cover its size")
	}
	vo, err := checkFileVolumeOrder(ctx, f, nzb.Par2Source, head, u.FetchArticle)
	if err != nil {
		return err
	}
	if vo.misordered {
		return fmt.Errorf("still out of order: %s", vo.detail())
	}
	return nil
}

// dropFileCaches removes what is cached of one file - the usenet reader and
// the DFS cache file - and reports false when either is in use.
func (r *Repair) dropFileCaches(entryName, infoHash, name string) bool {
	if u := r.manager.usenet; u != nil && !u.DropIdleReader(infoHash, name) {
		return false
	}
	if ev, _ := r.manager.MountManager().(dfsCacheFileEvictor); ev != nil {
		if _, ok := ev.EvictCachedFile(entryName, name); !ok {
			return false
		}
	}
	// The next-episode precache keeps its own note of what is cached.
	if p := r.manager.precache; p != nil {
		p.dropReadiness(infoHash + ":" + name)
	}
	return true
}

// reorderMisordered tries to put each of h's misordered files back in order
// in place, and takes those it fixed off the Unverified list. The entry's
// decode stamp is cleared and it is made due, so the next sweep decodes the
// reordered file like a new one. Reports how many files it fixed.
func (r *Repair) reorderMisordered(ctx context.Context, h *storage.EntryHealth) int {
	fixed := 0
	var keep []storage.UnverifiedFile
	for _, uf := range h.UnverifiedFiles {
		if uf.Reason != reasonVolumeOrder || uf.InfoHash == "" {
			keep = append(keep, uf)
			continue
		}
		if _, err := r.reorderFileVolumes(ctx, h.EntryName, uf.InfoHash, uf.FileName); err != nil {
			ev := r.logger.Info()
			if !errors.Is(err, errReorderRefused) {
				ev = r.logger.Warn()
			}
			ev.Err(err).Str("entry", h.EntryName).Str("file", uf.FileName).
				Msg("Repair: could not put the file's volumes in order in place; it will be re-grabbed")
			keep = append(keep, uf)
			continue
		}
		fixed++
		h.VolumeOrderChecks = slices.DeleteFunc(h.VolumeOrderChecks, func(vc storage.VolumeOrderCheck) bool {
			return vc.FileName == uf.FileName
		})
	}
	if fixed == 0 {
		return 0
	}
	h.UnverifiedFiles = keep
	h.DecodeVerifiedAt = time.Time{}
	h.DecodeVerifiedFingerprint = ""
	h.DecodeVerifiedCoverage = ""
	h.NextCheckDueAt = time.Now()
	h.Dirty = true
	h.DirtyReason = "volumes reordered in place"
	return fixed
}

// autoReorderResults is the sweep's side of the in-place fix: with auto-repair
// on, each file this probe found (or an earlier one recorded) out of volume
// order is put back in order where it stands, and comes off the Unverified
// list. Nothing is ever re-grabbed from here: a file that cannot be reordered
// stays listed, with the reason on its record so later sweeps do not read its
// volumes again for the same answer - Replace is then the operator's call. A
// file that was open, or a fetch that failed, is tried again next time.
// Reports whether any file was reordered; the caller then leaves the entry
// without a decode stamp and due again, so the next sweep decodes it.
func (r *Repair) autoReorderResults(ctx context.Context, c *candidate, results []fileResult) bool {
	fixed := false
	for i := range results {
		res := &results[i]
		check := res.volumeCheck
		if !res.healthy || res.unverifiedReason != reasonVolumeOrder || check == nil || !check.Misordered {
			continue
		}
		if check.ReorderRefused != "" {
			res.unverifiedDetail = check.Detail + "; not reordered in place: " + check.ReorderRefused
			continue
		}
		if _, err := r.reorderFileVolumes(ctx, c.name, res.infoHash, res.name); err != nil {
			switch {
			case errors.Is(err, errReorderBusy) || !errors.Is(err, errReorderRefused):
				r.logger.Info().Err(err).Str("entry", c.name).Str("file", res.name).
					Msg("Repair: could not put the file's volumes in order in place this time; it stays Unverified and the next sweep tries again")
			default:
				check.ReorderRefused = strings.TrimPrefix(err.Error(), errReorderRefused.Error()+": ")
				res.unverifiedDetail = check.Detail + "; not reordered in place: " + check.ReorderRefused
				r.logger.Warn().Str("entry", c.name).Str("file", res.name).Str("reason", check.ReorderRefused).
					Msg("Repair: the file's volumes cannot be put in order in place; it stays Unverified, replace it from there")
			}
			continue
		}
		fixed = true
		res.unverifiedReason, res.unverifiedCause, res.unverifiedDetail = "", "", ""
		// The record for the new layout: the volumes now read as in order.
		// With no verdict (a fetch failed) the next probe reads them.
		res.volumeCheck = nil
		delete(c.volumeChecks, res.name)
		res.volumeCheck = r.volumeOrderOnce(ctx, c, res.infoHash, res.name)
		if res.volumeCheck != nil && res.volumeCheck.Misordered {
			// verifyReordered just read it as in order; do not list it on a
			// reading that disagrees with that one.
			res.volumeCheck = nil
		}
		res.volumeReordered = true
	}
	return fixed
}
