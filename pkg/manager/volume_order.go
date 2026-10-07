package manager

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/bits"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
)

// Volume order after import.
//
// Imports before the parser ordered volumes by their own numbers (RAR5 main
// header 66a770b, RAR4 end-of-archive 8b52c3f) kept the upload order, which
// posters of obfuscated releases scramble. The stored layout then serves every
// volume's bytes whole, but at another volume's offsets: the file plays with
// jumps, and ffprobe decodes through valid Matroska from elsewhere in the
// film, so the sweep lists it Unverified (container_errors, zero_filled,
// read_budget_spent) instead of broken. On a production install 13 of the 36 Unverified
// entries on 2026-09-16 were this (Kivi PiGNUS, Long Crossing FHC, Finding Ada
// Pahe, ...), proved from the volumes' headers; a dry run of today's parser
// on Kivi, Long Crossing and Finding Ada S02 put every volume in order.

// volumeOrderFetchers bounds how many volume heads one check reads at once.
const volumeOrderFetchers = 4

// volumeOrderTimeout bounds one file's check: one article per volume.
const volumeOrderTimeout = 5 * time.Minute

// layoutVolume is one stored volume of a file: a run of segments from one
// posted archive volume.
type layoutVolume struct {
	first  int   // index of its first segment
	segs   int   // segments in the run
	bytes  int64 // data bytes the file reads from it
	header bool  // its first segment has a data start: the volume's headers precede it
}

// layoutVolumes splits a file's segments into volumes where a segment has a
// data start (the volume's headers precede its data) or the article number
// does not increase.
func layoutVolumes(segs []storage.NZBSegment) []layoutVolume {
	var vols []layoutVolume
	prev := 0
	for i, s := range segs {
		if i == 0 || s.SegmentDataStart > 0 || s.Number <= prev {
			vols = append(vols, layoutVolume{first: i, header: s.SegmentDataStart > 0})
		}
		v := &vols[len(vols)-1]
		v.segs++
		v.bytes += s.Bytes
		prev = s.Number
	}
	return vols
}

// shortVolumeBeforeFull reports whether a volume after the file's first holds
// more than a full volume's largest article less than a full volume, with a
// full volume after it. Only an archive's final volume is short, and a file
// in volume order can hold it only last, so this is a free sign of misorder
// (Kivi, Lamplight). Equal-size swaps (Long Crossing) do not show here. The
// first volume is left out: a file of a season-wide set starts mid-volume.
func shortVolumeBeforeFull(vols []layoutVolume) bool {
	if len(vols) < 3 {
		return false
	}
	counts := make(map[int64]int, len(vols))
	var full int64
	for _, v := range vols[1:] {
		counts[v.bytes]++
		if c := counts[v.bytes]; c > counts[full] || (c == counts[full] && v.bytes > full) {
			full = v.bytes
		}
	}
	if counts[full] < 2 {
		return false
	}
	article := full / int64(max(1, maxSegs(vols)))
	short := false
	for _, v := range vols[1:] {
		switch {
		case v.bytes < full-article:
			short = true
		case short:
			return true
		}
	}
	return false
}

func maxSegs(vols []layoutVolume) int {
	n := 0
	for _, v := range vols {
		n = max(n, v.segs)
	}
	return n
}

// Sources of volume numbers, in the order checkFileVolumeOrder trusts them.
const (
	volumeSourceRAR5Header = "rar5_header"
	volumeSourceYencName   = "yenc_name"
	volumeSourceRAR4End    = "rar4_end"
	volumeSourceRAR4First  = "rar4_first_volume"
	// volumeSourceMatroska: no volume gave a number, so the stored file's own
	// Matroska Cluster timestamps order the volumes that show one.
	volumeSourceMatroska = "matroska_cluster"
	// volumeSourceImport: not read by the check - the import parser read every
	// volume's number and stored the file in that order.
	volumeSourceImport = "import"
)

// volumeOrderVerdict is what checkFileVolumeOrder found for one file.
type volumeOrderVerdict struct {
	volumes    int    // stored volumes
	numbered   int    // volumes a number was read for
	source     string // where the numbers came from; "" when none could be read
	misordered bool
	position   int // 1-based stored volume where the order first breaks
	number     int // the number read there
	prev       int // the number of the numbered volume before it
	outOfPlace int // numbered volumes not above the one before them
	// numbers holds what was read for each stored volume, -1 where nothing
	// was. They only order the volumes; for matroska_cluster they are times.
	numbers []int
}

func (v volumeOrderVerdict) detail() string {
	return fmt.Sprintf("stored volume %d of %d is archive volume %d, after %d; %d of %d numbered volumes out of place (%s)",
		v.position, v.volumes, v.number, v.prev, v.outOfPlace, v.numbered, v.source)
}

type (
	volumeHeadFunc func(ctx context.Context, messageID string) (usenet.ArticleHead, error)
	volumeBodyFunc func(ctx context.Context, messageID string) ([]byte, error)
)

// volumePartName strips a RAR volume name to what every volume of its set
// shares.
var volumePartName = regexp.MustCompile(`(?i)(\.part\d+)?\.(rar|r\d{2}|\d{3})$`)

// checkFileVolumeOrder reads the first article of every stored volume that
// starts with its headers (one article each) and orders the volumes by, in
// turn: the RAR5 main header's volume number; the part number in the yEnc
// names, when every volume's name gives one and they name one set (posters
// that obfuscate the NZB often keep the real names there); the RAR4
// end-of-archive numbers, when posted names the volumes' last articles
// (Par2Source - legacy meta without it cannot reach them, since a recovery
// record after the data keeps them out of the layout); and last the RAR4
// first-volume flag. An article whose yEnc part is not the first of its file
// (a reused message ID serving another upload) gives nothing. A fetch error
// ends the check without a verdict.
func checkFileVolumeOrder(ctx context.Context, f *storage.NZBFile, posted []storage.PostedFileRef, head volumeHeadFunc, body volumeBodyFunc) (volumeOrderVerdict, error) {
	vols := layoutVolumes(f.Segments)
	v := volumeOrderVerdict{volumes: len(vols)}
	if len(vols) < 2 {
		return v, nil
	}

	heads := make([]*usenet.ArticleHead, len(vols))
	var (
		mu       sync.Mutex
		firstErr error
		wg       sync.WaitGroup
	)
	sem := make(chan struct{}, volumeOrderFetchers)
	for k, vol := range vols {
		if !vol.header {
			continue
		}
		wg.Add(1)
		go func(k int, id string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			h, err := head(ctx, id)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			if h.Part <= 1 {
				heads[k] = &h
			}
		}(k, f.Segments[vol.first].MessageID)
	}
	wg.Wait()
	if firstErr != nil {
		return v, firstErr
	}

	nums := make([]int, len(vols))
	for k := range nums {
		nums[k] = -1
	}
	parsed := make([]parser.VolumeHead, len(vols))
	// A set with encrypted headers keeps its volume numbers behind the
	// archive's password, which the file's record holds.
	reader := parser.VolumeHeadReader{Password: f.Password}
	var rar5, rar5Numbered, rar4 int
	for k, h := range heads {
		if h == nil {
			continue
		}
		parsed[k] = reader.Read(h.Prefix)
		switch parsed[k].Version {
		case 5:
			rar5++
			if parsed[k].HasNumber {
				rar5Numbered++
			}
		case 4:
			rar4++
		}
	}

	switch {
	case rar5 >= 2 && rar5Numbered == rar5:
		for k := range heads {
			if parsed[k].Version == 5 {
				nums[k] = parsed[k].Number
			}
		}
		v.source = volumeSourceRAR5Header
	case nameNumbers(heads, nums):
		v.source = volumeSourceYencName
	case rar4 >= 2 && len(posted) > 0 && body != nil:
		n, err := rar4EndNumbers(ctx, f, vols, parsed, posted, body, nums)
		if err != nil {
			return v, err
		}
		if n >= 2 {
			v.source = volumeSourceRAR4End
			break
		}
		fallthrough
	case rar4 >= 2:
		for k := range nums {
			nums[k] = -1
		}
		// Only the first volume is marked: number it 0 and every other RAR4
		// volume 1, so a marked volume after an unmarked one breaks the order.
		marked := false
		for k := range heads {
			if parsed[k].Version != 4 {
				continue
			}
			if parsed[k].First {
				nums[k], marked = 0, true
			} else {
				nums[k] = 1
			}
		}
		if marked {
			v.source = volumeSourceRAR4First
		} else {
			for k := range nums {
				nums[k] = -1
			}
		}
	}
	if v.source == "" && body != nil && f.IsStored && !f.IsEncrypted && isMatroskaName(f.Name) {
		n, err := clusterNumbers(ctx, f, vols, body, nums)
		if err != nil {
			return v, err
		}
		if n >= 2 {
			v.source = volumeSourceMatroska
		} else {
			for k := range nums {
				nums[k] = -1
			}
		}
	}
	if v.source == "" {
		return v, nil
	}
	v.numbers = nums

	last := -1
	for k, n := range nums {
		if n < 0 {
			continue
		}
		v.numbered++
		if last >= 0 && n <= nums[last] && !(v.source == volumeSourceRAR4First && n == 1 && nums[last] == 1) {
			v.outOfPlace++
			if !v.misordered {
				v.misordered, v.position, v.number, v.prev = true, k+1, n, nums[last]
			}
		}
		last = k
	}
	return v, nil
}

// clusterTimestamp returns the timestamp of the first Matroska Cluster that
// starts in data: the Cluster ID, a valid size, then the Timestamp element
// every Cluster opens with. The four ID bytes alone turn up by chance about
// once in 6,000 articles of video; the element after them does not.
func clusterTimestamp(data []byte) (int64, bool) {
	id := []byte{0x1F, 0x43, 0xB6, 0x75}
	for off := 0; ; {
		i := bytes.Index(data[off:], id)
		if i < 0 {
			return 0, false
		}
		p := off + i + len(id)
		off += i + 1
		if p >= len(data) || data[p] == 0 {
			continue
		}
		p += bits.LeadingZeros8(data[p]) + 1 // the Cluster's size
		if p+1 >= len(data) || data[p] != 0xE7 {
			continue
		}
		n := int(data[p+1]) - 0x80 // the Timestamp's size: 1 to 8 bytes
		if n < 1 || n > 8 || p+2+n > len(data) {
			continue
		}
		var ts int64
		for _, b := range data[p+2 : p+2+n] {
			ts = ts<<8 | int64(b)
		}
		if ts < 0 {
			continue
		}
		return ts, true
	}
}

// clusterNumbers reads the first article of every stored volume and fills
// nums with the timestamp of the first Matroska Cluster in the file's bytes
// there, where one starts. A stored (uncompressed) file's bytes are the
// Matroska stream itself, so the timestamps rise through volumes in order.
// Returns how many volumes gave one.
func clusterNumbers(ctx context.Context, f *storage.NZBFile, vols []layoutVolume, body volumeBodyFunc, nums []int) (int, error) {
	var (
		mu       sync.Mutex
		firstErr error
		wg       sync.WaitGroup
		n        int
	)
	sem := make(chan struct{}, volumeOrderFetchers)
	for k, vol := range vols {
		wg.Add(1)
		go func(k int, seg storage.NZBSegment) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			data, err := body(ctx, seg.MessageID)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			if seg.SegmentDataStart >= int64(len(data)) {
				return
			}
			if ts, ok := clusterTimestamp(data[seg.SegmentDataStart:]); ok {
				nums[k] = int(ts)
				n++
			}
		}(k, f.Segments[vol.first])
	}
	wg.Wait()
	return n, firstErr
}

// nameNumbers fills nums from the yEnc names when every read head's name gives
// a volume number, the numbers are distinct, and the names share one set
// name. Reports whether it did.
func nameNumbers(heads []*usenet.ArticleHead, nums []int) bool {
	set := ""
	seen := map[int]bool{}
	n := 0
	for _, h := range heads {
		if h == nil {
			continue
		}
		num, ok := parser.NameVolumeNumber(h.Name)
		if !ok || seen[num] {
			return false
		}
		base := strings.ToLower(volumePartName.ReplaceAllString(filepath.Base(strings.TrimSpace(h.Name)), ""))
		if set == "" {
			set = base
		} else if base != set {
			return false
		}
		seen[num] = true
		n++
	}
	if n < 2 {
		return false
	}
	for k, h := range heads {
		if h != nil {
			nums[k], _ = parser.NameVolumeNumber(h.Name)
		}
	}
	return true
}

// rar4EndNumbers reads the end-of-archive volume number of every RAR4 stored
// volume whose posted file posted names: the posted file's last article, and
// the one before when the end block starts there. Returns how many it read.
func rar4EndNumbers(ctx context.Context, f *storage.NZBFile, vols []layoutVolume, parsed []parser.VolumeHead, posted []storage.PostedFileRef, body volumeBodyFunc, nums []int) (int, error) {
	byFirst := make(map[string]*storage.PostedFileRef, len(posted))
	for i := range posted {
		if segs := posted[i].Segments; len(segs) > 0 {
			byFirst[segs[0].MessageID] = &posted[i]
		}
	}
	n := 0
	for k, vol := range vols {
		if parsed[k].Version != 4 {
			continue
		}
		p := byFirst[f.Segments[vol.first].MessageID]
		if p == nil {
			continue
		}
		var tail []byte
		for j := len(p.Segments) - 1; j >= 0 && j >= len(p.Segments)-2; j-- {
			data, err := body(ctx, p.Segments[j].MessageID)
			if err != nil {
				return n, err
			}
			tail = append(data, tail...)
			if num, ok := parser.TailVolumeNumber(tail); ok {
				nums[k] = num
				n++
				break
			}
		}
	}
	return n, nil
}

// volumeOrderCache keeps each file's verdict for the life of the process, so
// a file listed Unverified for another reason is not re-read every sweep. Keyed
// by the stored layout, so a re-import or a reordered file is checked again.
var volumeOrderCache sync.Map // map[string]volumeOrderVerdict

// volumeOrderKey names a file's stored layout: its release, its name and
// every article in order. The first and last articles alone would not do - two
// volumes swapped mid-file leave both where they were.
func volumeOrderKey(infoHash string, f *storage.NZBFile) string {
	h := sha256.New()
	h.Write([]byte(infoHash))
	h.Write([]byte{0})
	h.Write([]byte(f.Name))
	for i := range f.Segments {
		h.Write([]byte{0})
		h.Write([]byte(f.Segments[i].MessageID))
	}
	return hex.EncodeToString(h.Sum(nil)[:8])
}

// checkVolumeOrder runs checkFileVolumeOrder on a stored RAR file, over NNTP in
// memory only (never the DFS or segment cache). ok is false when the file is
// not a multi-volume RAR file or no verdict was reached.
func (r *Repair) checkVolumeOrder(ctx context.Context, infoHash, name string) (volumeOrderVerdict, bool) {
	u := r.manager.usenet
	if u == nil {
		return volumeOrderVerdict{}, false
	}
	nzb, err := u.GetNZB(infoHash)
	if err != nil || nzb == nil {
		return volumeOrderVerdict{}, false
	}
	f := nzb.GetFileByName(name)
	if f == nil || f.FileType != storage.NZBFileTypeRar || len(f.Segments) == 0 {
		return volumeOrderVerdict{}, false
	}
	key := volumeOrderKey(infoHash, f)
	if cached, ok := volumeOrderCache.Load(key); ok {
		return cached.(volumeOrderVerdict), true
	}
	cctx, cancel := context.WithTimeout(ctx, volumeOrderTimeout)
	defer cancel()
	started := time.Now()
	v, err := checkFileVolumeOrder(cctx, f, nzb.Par2Source,
		func(ctx context.Context, id string) (usenet.ArticleHead, error) {
			return u.FetchArticleHead(ctx, id, parser.EncryptedVolumeHeadBytes)
		},
		u.FetchArticle)
	if err != nil {
		r.logger.Debug().Err(err).Str("entry", nzb.Name).Str("file", name).Msg("Repair: volume order check reached no verdict")
		return v, false
	}
	volumeOrderCache.Store(key, v)
	r.logger.Debug().Str("entry", nzb.Name).Str("file", name).Int("volumes", v.volumes).Int("numbered", v.numbered).
		Str("source", v.source).Bool("misordered", v.misordered).Int("out_of_place", v.outOfPlace).
		Dur("took", time.Since(started)).Msg("Repair: checked the stored volume order")
	return v, true
}

// volumeOrderOnce returns a stored RAR file's volume-order verdict: the one an
// earlier probe recorded for the layout it still has, or else a fresh read of
// its volumes' headers, which the caller records. A misordered file is logged
// when it is first found. Nil when the file is not a multi-volume RAR file or
// the check reached no verdict; that file is read again on the next probe.
func (r *Repair) volumeOrderOnce(ctx context.Context, c *candidate, infoHash, name string) *storage.VolumeOrderCheck {
	u := r.manager.usenet
	if u == nil {
		return nil
	}
	nzb, err := u.GetNZB(infoHash)
	if err != nil || nzb == nil {
		return nil
	}
	f := nzb.GetFileByName(name)
	if f == nil || f.FileType != storage.NZBFileTypeRar || len(f.Segments) == 0 {
		return nil
	}
	layout := volumeOrderKey(infoHash, f)
	if prior, ok := c.volumeChecks[name]; ok && prior.Layout == layout {
		return &prior
	}
	if f.VolumeOrderVerified {
		// The import ordered the volumes by their own numbers: nothing to read.
		vols := len(layoutVolumes(f.Segments))
		if vols < 2 {
			return nil
		}
		return &storage.VolumeOrderCheck{FileName: name, Layout: layout, Volumes: vols, Numbered: vols,
			Source: volumeSourceImport, CheckedAt: time.Now()}
	}
	vo, checked := r.checkVolumeOrder(ctx, infoHash, name)
	if !checked || vo.volumes < 2 {
		return nil
	}
	check := &storage.VolumeOrderCheck{FileName: name, Layout: layout, Volumes: vo.volumes, Numbered: vo.numbered, Source: vo.source,
		Misordered: vo.misordered, CheckedAt: time.Now()}
	if vo.misordered {
		check.Detail = vo.detail()
		r.logger.Warn().Str("entry", c.name).Str("file", name).Int("volumes", vo.volumes).Int("out_of_place", vo.outOfPlace).
			Int("position", vo.position).Int("volume_number", vo.number).Str("source", vo.source).
			Msg("Repair: archive volumes are stored out of order (assembled wrong at import); replace it from Unverified")
	}
	return check
}

// mergeVolumeOrderChecks returns the checks to keep on an entry after a probe:
// each probed file's verdict, and the earlier one for a file still in the
// entry that this probe reached no verdict for.
func mergeVolumeOrderChecks(prior []storage.VolumeOrderCheck, names []string, results []fileResult) []storage.VolumeOrderCheck {
	byName := make(map[string]storage.VolumeOrderCheck, len(prior))
	for _, vc := range prior {
		if slices.Contains(names, vc.FileName) {
			byName[vc.FileName] = vc
		}
	}
	for _, res := range results {
		switch {
		case res.volumeCheck != nil:
			byName[res.name] = *res.volumeCheck
		case res.volumeReordered:
			// Its earlier verdict was for the layout it no longer has.
			delete(byName, res.name)
		}
	}
	if len(byName) == 0 {
		return nil
	}
	out := make([]storage.VolumeOrderCheck, 0, len(byName))
	for _, vc := range byName {
		out = append(out, vc)
	}
	slices.SortFunc(out, func(a, b storage.VolumeOrderCheck) int { return strings.Compare(a.FileName, b.FileName) })
	return out
}
