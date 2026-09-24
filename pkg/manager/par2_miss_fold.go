package manager

import (
	"fmt"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// classifyPar2Miss explains why FileID fileID in the PAR2 index ended up with
// no posted-file fetcher, and whether that miss can resolve on a bare retry.
// A FileDesc that no retained posted file could possibly be (nothing in
// par2Source shares its length or name - e.g. Outpost 5's .7z.010, described
// by the index but never retained) is STRUCTURAL and terminal. A FileDesc
// whose posted file IS in par2Source but was skipped for a transient
// fetch/hash failure, or is present but unmatched for a length/tie reason, is
// retryable.
func classifyPar2Miss(idx *par2.Index, fileID [16]byte, par2Source []storage.PostedFileRef, transientUnmatch map[string]error) (reason string, terminal bool) {
	fd := idx.Files[fileID]
	if fd == nil {
		return "unknown FileDesc", true
	}
	for i := range par2Source {
		ps := par2Source[i]
		if ps.Size != fd.Length && ps.Name != fd.Name {
			// An estimated posted size (probe failed, or a pre-Real record)
			// misses the FileDesc's exact length by up to an article. Such a
			// file that matching SKIPPED for a fetch failure is a candidate
			// too (KNOWN-estimated-identity) - but only a skipped one: volumes
			// are all near one size, and a FileDesc no retained file can be
			// (a never-posted volume) must stay structural.
			if _, skipped := transientUnmatch[ps.Name]; !skipped || !withinOneArticle(ps, fd.Length) {
				continue
			}
		}
		if skipErr, ok := transientUnmatch[ps.Name]; ok {
			if nntp.IsArticleNotFoundError(skipErr) {
				return fmt.Sprintf("posted file %q: backing article confirmed missing across all providers", ps.Name), true
			}
			return fmt.Sprintf("posted file %q failed to fetch/hash during matching", ps.Name), false
		}
		return fmt.Sprintf("posted file %q retained but unmatched (length/tie)", ps.Name), false
	}
	return fmt.Sprintf("no retained posted file for FileDesc %q (len %d)", fd.Name, fd.Length), true
}

// withinOneArticle reports whether posted file ps's recorded size is within
// one of its articles of length - the most an estimated size (0.97 yEnc
// overhead on the last article) misses the real one by.
func withinOneArticle(ps storage.PostedFileRef, length int64) bool {
	if len(ps.Segments) == 0 || ps.Size <= 0 || length <= 0 {
		return false
	}
	article := ps.Segments[0].Bytes
	if article <= 0 {
		article = ps.Size / int64(len(ps.Segments))
	}
	d := ps.Size - length
	if d < 0 {
		d = -d
	}
	return d <= article
}

// foldUncoveredFiles folds each uncovered file's slices into damagedSet so the
// solver rebuilds them from parity - whether the miss is transient (its match
// needed bytes that failed to fetch) or structural (a small file the PAR2 set
// describes but the NZB never posted: an .nfo, an .sfv). Those slices are
// only reconstructed to run the solve, never patched. Cost: one recovery
// slice per file slice. A file that does not fit budget fails the job as the
// miss's own kind: a transient miss must not become the round-loop gate's
// terminal "more damage than recorded". Returns how many slices were added.
func foldUncoveredFiles(logger zerolog.Logger, entryName string, idx *par2.Index, uncov [][16]byte, damagedSet map[int64]struct{}, budget int, classifyMiss func([16]byte) (string, bool)) (int, error) {
	folded := 0
	for _, fid := range uncov {
		reason, terminal := classifyMiss(fid)
		fd := idx.Files[fid]
		if fd == nil {
			return folded, missingFetcherErr(fid, classifyMiss)
		}
		fs, ferr := idx.DamagedSlices(fid, 0, fd.Length)
		if ferr != nil {
			return folded, fmt.Errorf("fold uncovered file %x: %w", fid, ferr)
		}
		added := 0
		for _, s := range fs {
			if _, ok := damagedSet[s]; !ok {
				added++
			}
		}
		if len(damagedSet)+added > budget {
			logger.Warn().Str("entry", entryName).Str("file", fd.Name).Str("reason", reason).
				Int("slices", len(fs)).Int("damaged", len(damagedSet)).Int("budget", budget).
				Msg("par2 repair: intact slices have no posted-file fetcher and do not fit the recovery budget - aborting before solve (0 bytes read)")
			if terminal {
				return folded, missingFetcherErr(fid, classifyMiss)
			}
			return folded, fmt.Errorf("uncovered file %q (%d slices) does not fit the %d-slice recovery budget (transient: %s)", fd.Name, len(fs), budget, reason)
		}
		for _, s := range fs {
			if _, ok := damagedSet[s]; !ok {
				damagedSet[s] = struct{}{}
				folded++
			}
		}
		logger.Info().Str("entry", entryName).Str("file", fd.Name).
			Str("reason", reason).Bool("structural", terminal).Int("slices", len(fs)).
			Msg("par2 repair: folding uncovered posted file into damaged set for reconstruction")
	}
	return folded, nil
}
