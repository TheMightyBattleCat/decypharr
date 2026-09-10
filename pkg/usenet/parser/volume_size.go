package parser

import (
	"context"
	"fmt"
	"sort"

	"github.com/Tensai75/nzbparser"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/nntp"
)

// maxVolumeMeasurements bounds the extra yEnc header fetches one archive may
// cost measureUnsizedVolumes. A normal release needs one: its final volume.
const maxVolumeMeasurements = 4

// measure records file's decoded size from its own yEnc header.
func (m *fileAnalysisResult) measure(file nzbparser.NzbFile, size int64) {
	key := fileMetaKey(file)
	if key == "" || size <= 0 {
		return
	}
	if m.measured == nil {
		m.measured = make(map[string]int64)
	}
	m.measured[key] = size
}

// measuredSize returns file's decoded size from its own yEnc header, or 0 when
// that file was not measured.
func (m *fileAnalysisResult) measuredSize(file nzbparser.NzbFile) int64 {
	if m == nil || len(m.measured) == 0 {
		return 0
	}
	return m.measured[fileMetaKey(file)]
}

// measureUnsizedVolumes measures, from their own yEnc headers, the volumes
// getNZBSegments would otherwise have to estimate: the last file in the
// order the archive processor has settled on, and any file with fewer
// articles than the group's reference file. Up to maxVolumeMeasurements
// fetches, last file first, then the shortest.
//
// enrichGroupWithFileInfo measures the reference file and the last file in
// NZB order, but RAR, 7z and zip volumes are re-sorted afterwards. When the
// true final volume is not the file measured as "last", its size was
// estimated from the NZB's byte counts, which for posters that declare
// decoded sizes put its last article 3% short. That truncated the extracted
// file by up to 23 KB, cutting the Matroska Cues and Tags, so the file could
// not seek (byte-exact on 12 releases on a production install, 2026-09-10). Measuring
// before volume descriptors are built also keeps the RAR reader's
// packed-size clamp, which works from those volume sizes, from cutting the
// final part.
//
// Only runs on metadata that already holds header measurements: interior
// articles sized from an estimate cannot be combined with an exact total.
// Returns how many volumes it measured; a failed fetch leaves that volume
// estimated as before.
func measureUnsizedVolumes(ctx context.Context, group *FileGroup, fetch yencHeaderFetchFunc) (int, error) {
	m := group.metadata
	if m == nil || len(m.measured) == 0 || m.segmentSize <= 0 || len(group.Files) == 0 {
		return 0, nil
	}

	refArticles := 0
	for _, f := range group.Files {
		refArticles = max(refArticles, len(f.Segments))
	}
	last := len(group.Files) - 1
	var candidates []int
	for i, f := range group.Files {
		if len(f.Segments) == 0 || m.measuredSize(f) > 0 {
			continue
		}
		if i == last || len(f.Segments) < refArticles {
			candidates = append(candidates, i)
		}
	}
	sort.SliceStable(candidates, func(a, b int) bool {
		ia, ib := candidates[a], candidates[b]
		if (ia == last) != (ib == last) {
			return ia == last
		}
		return len(group.Files[ia].Segments) < len(group.Files[ib].Segments)
	})

	tried := candidates[:min(len(candidates), maxVolumeMeasurements)]
	measured, failed := 0, 0
	var firstErr error
	for _, i := range tried {
		if err := measureFile(ctx, m, group.Files[i], fetch); err != nil {
			failed++
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		measured++
	}
	if firstErr != nil {
		// One line rather than one per volume: the caller logs this at WARN on
		// the import path.
		return measured, fmt.Errorf("%d of %d volumes unmeasured, first: %w", failed, len(tried), firstErr)
	}
	return measured, nil
}

// measureFile fetches file's first article header and records its decoded
// size, unless that size cannot belong to a file of this many articles.
func measureFile(ctx context.Context, m *fileAnalysisResult, file nzbparser.NzbFile, fetch yencHeaderFetchFunc) error {
	first := file.Segments[0]
	for _, seg := range file.Segments[1:] {
		if seg.Number < first.Number {
			first = seg
		}
	}
	data, err := fetch(ctx, first.Id)
	if err != nil {
		return fmt.Errorf("fetch yEnc header of %s: %w", file.Filename, err)
	}
	if data == nil || data.Size <= 0 {
		return fmt.Errorf("yEnc header of %s carries no size", file.Filename)
	}
	// The bound getNZBSegments applies: a size more than one and a half
	// articles from what the article count implies is some other file's (a
	// mixed-subject group), not this one's.
	diff := data.Size - m.segmentSize*int64(len(file.Segments))
	if diff < 0 {
		diff = -diff
	}
	if diff > m.segmentSize*3/2 {
		return fmt.Errorf("yEnc size %d of %s does not fit its %d articles", data.Size, file.Filename, len(file.Segments))
	}
	m.measure(file, data.Size)
	return nil
}

// headerFetch probes one article's yEnc header with the parser's usual
// failover - the fetch enrichGroupWithFileInfo uses.
func headerFetch(manager *nntp.Client) yencHeaderFetchFunc {
	return func(ctx context.Context, messageID string) (*nntp.YencMetadata, error) {
		var data *nntp.YencMetadata
		err := manager.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
			d, e := conn.GetHeaderPrefix(messageID, metadataOnly)
			data = d
			return e
		})
		return data, err
	}
}

// measureArchiveVolumes runs measureUnsizedVolumes with headerFetch and logs
// what it could not measure. Archive processors call it once their volumes
// are sorted and before anything is built from volume sizes.
func measureArchiveVolumes(ctx context.Context, group *FileGroup, manager *nntp.Client, logger zerolog.Logger) {
	if manager == nil {
		return
	}
	n, err := measureUnsizedVolumes(ctx, group, headerFetch(manager))
	if err != nil {
		logger.Warn().Err(err).Str("group", group.BaseName).
			Msg("Could not measure an archive volume; its last article stays an estimate")
	}
	if n > 0 {
		logger.Debug().Str("group", group.BaseName).Int("volumes", n).
			Msg("Measured archive volumes whose size the NZB order left unknown")
	}
}
