package parser

import (
	"context"
	"fmt"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/types"
)

// fallbackArticleBytes stands in for the article size when the group's
// metadata has none: a typical 750 KiB posting article.
const fallbackArticleBytes = 750 << 10

// checkPartsInsideVolumes fails when the archive headers place part of file
// beyond the end its volume has in our geometry, on any volume but the last.
//
// A RAR file part always lies inside its own volume: the header's data size
// counts only that volume's share, and the volume ends with an end block after
// it (RAR5 and RAR4 alike, checked against real multi-volume archives). So a
// part that runs past the volume's size means that size - the sum of its
// articles' estimated bytes - is too small. buildSegmentsForVolumePart walks
// the flat article list, so such a part takes the next volume's first articles
// as its own, and sorting a part's articles by number then moves them to the
// front: the file is served with another volume's bytes spliced in at every
// affected boundary (Raiders S06E15/E16, UnTRUE S04E01 on a production install). The RAR4
// parser trims a part to the volume instead, which drops the same bytes from
// the middle of the file. Either way no seek past that point is right.
//
// The last volume is left to the shortfall check: its estimated size can be a
// few KB short without anything after it to splice in.
func checkPartsInsideVolumes(file *RARFileEntry, volumeInfos []storage.ArchiveVolumeInfo) error {
	last := len(volumeInfos) - 1
	for _, part := range file.VolumeParts {
		if part == nil || part.PartNumber < 0 || part.PartNumber >= last {
			continue
		}
		end := part.DataOffset + part.UnpackedSize + part.TrimmedBytes
		if size := volumeInfos[part.PartNumber].Size; end > size {
			return fmt.Errorf("%s: part in volume %d (%s) ends at byte %d of a volume we sized at %d; the volume sizes are wrong, so the file would be assembled with another volume's bytes",
				file.Name, part.PartNumber, volumeName(part, volumeInfos), end, size)
		}
	}
	return nil
}

// measureCrossedVolumes measures, from their own yEnc headers, the inner
// volumes that a stored file's parts run past, so the caller can rebuild the
// geometry before checkPartsInsideVolumes condemns a posting that is fine.
//
// The usual cause is getNZBSegments' fallback: a volume without its own
// measurement is sized from the group's reference file, and when that size
// does not fit its article count the last article becomes 0.97 x its NZB
// bytes - 3% of an article short for posters that declare decoded sizes. On
// Dear Judge S01E06 (a production install) every 68-article volume got a 744,960-byte
// last article instead of 768,000, so each part ran 22,922 bytes into the next
// volume and every boundary served another volume's first article.
//
// RAR writes every volume but the last at the same size, so one header per
// article count sizes all crossed volumes with that count; at most
// maxVolumeMeasurements fetches. Only runs on metadata that already holds
// header measurements, like measureUnsizedVolumes: interior articles sized
// from an estimate cannot be fixed by an exact total. Returns whether it
// recorded any size; a failed fetch leaves the volumes as they were.
func measureCrossedVolumes(ctx context.Context, group *FileGroup, files []*RARFileEntry, volumeInfos []storage.ArchiveVolumeInfo, fetch yencHeaderFetchFunc) (bool, error) {
	m := group.metadata
	if m == nil || len(m.measured) == 0 || m.segmentSize <= 0 || len(volumeInfos) != len(group.Files) {
		// Without one volume per group file, part numbers do not name group files.
		return false, nil
	}
	last := len(volumeInfos) - 1
	crossed := map[int]bool{}
	for _, file := range files {
		if file == nil || file.IsDirectory || !file.IsStored {
			continue
		}
		for _, part := range file.VolumeParts {
			if part == nil || part.PartNumber < 0 || part.PartNumber >= last {
				continue
			}
			if part.DataOffset+part.UnpackedSize+part.TrimmedBytes > volumeInfos[part.PartNumber].Size {
				crossed[part.PartNumber] = true
			}
		}
	}
	if len(crossed) == 0 {
		return false, nil
	}

	byCount := map[int][]int{} // article count -> crossed volumes
	var counts []int
	for i := range group.Files {
		if !crossed[i] {
			continue
		}
		n := len(group.Files[i].Segments)
		if _, ok := byCount[n]; !ok {
			counts = append(counts, n)
		}
		byCount[n] = append(byCount[n], i)
	}
	if len(counts) > maxVolumeMeasurements {
		return false, fmt.Errorf("%d crossed volumes have %d different article counts; not measuring", len(crossed), len(counts))
	}
	recorded := false
	for _, n := range counts {
		vols := byCount[n]
		probe := group.Files[vols[0]]
		size := m.measuredSize(probe)
		if size == 0 {
			if err := measureFile(ctx, m, probe, fetch); err != nil {
				return recorded, err
			}
			size = m.measuredSize(probe)
			recorded = true
		}
		for _, i := range vols[1:] {
			if m.measuredSize(group.Files[i]) != size {
				m.measure(group.Files[i], size)
				recorded = true
			}
		}
	}
	return recorded, nil
}

// untrimParts gives back what the RAR4 parser trimmed off a part to fit a
// volume size that has since been measured larger.
func untrimParts(files []*RARFileEntry, volumeInfos []storage.ArchiveVolumeInfo) {
	for _, file := range files {
		if file == nil {
			continue
		}
		for _, part := range file.VolumeParts {
			if part == nil || part.TrimmedBytes <= 0 || part.PartNumber < 0 || part.PartNumber >= len(volumeInfos) {
				continue
			}
			if part.DataOffset+part.UnpackedSize+part.TrimmedBytes <= volumeInfos[part.PartNumber].Size {
				part.PackedSize += part.TrimmedBytes
				part.UnpackedSize += part.TrimmedBytes
				file.PackedSize += part.TrimmedBytes
				part.TrimmedBytes = 0
			}
		}
	}
}

func volumeName(part *types.RARVolumePart, volumeInfos []storage.ArchiveVolumeInfo) string {
	if part.Name != "" {
		return part.Name
	}
	return volumeInfos[part.PartNumber].Name
}

// checkStreamCoversHeader compares the bytes file's articles cover with the
// size its archive header gives. It returns how many bytes are missing, and an
// error when that is an article or more: then articles or whole volumes are
// absent from the geometry - a volume whose article numbers have holes or
// duplicates, whose header could not be read, or whose parts could not be
// placed is skipped - and the file would be served that much short, or with
// everything after the gap shifted. Less than an article is an estimated final
// article, served a few KB short at the tail; the caller clamps and warns.
func checkStreamCoversHeader(file *RARFileEntry, streamSize, articleBytes int64) (int64, error) {
	if file.UncompressedSize <= 0 || streamSize <= 0 || file.UncompressedSize <= streamSize {
		return 0, nil
	}
	missing := file.UncompressedSize - streamSize
	if articleBytes <= 0 {
		articleBytes = fallbackArticleBytes
	}
	if missing >= articleBytes {
		return missing, fmt.Errorf("%s: its articles cover %d of the %d bytes its archive header gives, %d short; articles or volumes are missing from the NZB",
			file.Name, streamSize, file.UncompressedSize, missing)
	}
	return missing, nil
}

// droppedVolumes names the group's files that did not become archive volumes:
// their article numbers have holes or duplicates, or an article has no
// message ID. The file's parts in them are missing from the geometry.
func droppedVolumes(group *FileGroup, volumes []*types.Volume) []string {
	kept := make(map[int]bool, len(volumes))
	for _, v := range volumes {
		kept[v.Index] = true
	}
	var dropped []string
	for i, f := range group.Files {
		if !kept[i] {
			dropped = append(dropped, f.Filename)
		}
	}
	return dropped
}
