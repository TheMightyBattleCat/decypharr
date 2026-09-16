package manager

import (
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// entrySizeMetaSource is the part of *usenet.Usenet syncEntrySizesFromMeta
// reads: the stored meta for an NZB ID, header only and in full.
type entrySizeMetaSource interface {
	GetNZBHeader(id string) (*storage.NZB, error)
	GetNZB(id string) (*storage.NZB, error)
}

// syncEntrySizesFromMeta gives an NZB entry's files the size their stored meta
// says, where the two disagree and the meta's article layout covers exactly
// that size. WebDAV and the mount serve the entry's size while the reader
// serves the meta layout, so a stale entry size cuts the file short.
//
// Found on a production install 2026-09-16 (13 of 20,324 NZB files, all legacy imports):
// Tide on Sark S02 and Isles and Shoals entries sized at one RAR volume
// (49,999,892 of 4.21 GB, 99,999,887 of 8.11 GB) while the meta held every
// volume, and four plain media files a few hundred bytes short of the posted
// yEnc size, which drops the Matroska index at the end. A new import copies
// the entry sizes from the meta, so this only corrects drift from before.
//
// A size is only taken when the meta file's articles run contiguously from 0
// to exactly its size; any other layout (encrypted, gapped) is left alone.
// Both the entry and its folder item are rewritten, through AddOrUpdate. It
// reports whether anything was written.
func syncEntrySizesFromMeta(s *storage.Storage, src entrySizeMetaSource, item *storage.EntryItem, log zerolog.Logger) bool {
	if s == nil || src == nil || item == nil {
		return false
	}
	byHash := make(map[string][]string)
	for name, f := range item.Files {
		if f == nil || f.Deleted || f.InfoHash == "" {
			continue
		}
		byHash[f.InfoHash] = append(byHash[f.InfoHash], name)
	}

	changed := false
	for infoHash, names := range byHash {
		hdr, err := src.GetNZBHeader(infoHash)
		if err != nil || hdr == nil {
			continue
		}
		var stale []string
		for _, name := range names {
			if mf := hdr.GetFileByName(name); mf != nil && mf.Size > 0 && mf.Size != item.Files[name].Size {
				stale = append(stale, name)
			}
		}
		if len(stale) == 0 {
			continue
		}

		nzb, err := src.GetNZB(infoHash)
		if err != nil || nzb == nil {
			continue
		}
		entry, err := s.Get(infoHash)
		if err != nil || entry == nil || !entry.IsNZB() {
			continue
		}
		write := false
		for _, name := range stale {
			mf := nzb.GetFileByName(name)
			ef := entry.Files[name]
			if mf == nil || ef == nil || ef.Deleted {
				continue
			}
			if !layoutCoversExactly(mf) {
				log.Debug().Str("entry", item.Name).Str("file", name).Int64("entry_bytes", ef.Size).Int64("meta_bytes", mf.Size).
					Msg("Repair: entry size differs from its meta, but the meta's articles do not cover its size; leaving it")
				continue
			}
			log.Warn().Str("entry", item.Name).Str("file", name).
				Int64("entry_bytes", item.Files[name].Size).Int64("meta_bytes", mf.Size).
				Msg("Repair: entry file size differs from its stored articles; correcting it to the meta size")
			ef.Size = mf.Size
			write = true
		}
		if !write {
			continue
		}
		if err := s.AddOrUpdate(entry); err != nil {
			log.Warn().Err(err).Str("entry", item.Name).Msg("Repair: failed to save corrected entry file sizes")
			continue
		}
		changed = true
	}
	return changed
}

// layoutCoversExactly reports whether f's articles run without gap or overlap
// from byte 0 to exactly f.Size.
func layoutCoversExactly(f *storage.NZBFile) bool {
	segs := f.Segments
	if len(segs) == 0 || segs[0].StartOffset != 0 {
		return false
	}
	for i := 1; i < len(segs); i++ {
		if segs[i].StartOffset != segs[i-1].EndOffset+1 {
			return false
		}
	}
	return segs[len(segs)-1].EndOffset+1 == f.Size
}
