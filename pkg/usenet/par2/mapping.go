package par2

import "fmt"

// PostedFile is the minimal shape needed to match a release's posted file
// (storage.PostedFileRef, in the caller's terms) against the PAR2 index's
// FileDesc set. Kept independent of pkg/storage so this package has no
// dependency on it.
type PostedFile struct {
	Name   string
	Length int64

	// MD5_16k lazily computes the MD5 of the first 16KB of the posted file's
	// actual content (or the whole file, if shorter). Computing it requires
	// fetching real bytes (one NNTP article, typically the first segment),
	// so it's a callback rather than a precomputed field - MatchFiles only
	// calls it for files whose length ties with another candidate.
	MD5_16k func() ([16]byte, error)
}

// Match pairs one posted file (by its index in the []PostedFile given to
// MatchFiles) with the FileID PAR2 protects it as.
type Match struct {
	PostedIndex int
	FileID      [16]byte

	// NameMismatch is true when the FileDesc's recorded name differs from
	// the posted file's own name, despite length (and, if it was tied,
	// MD5-16k) matching exactly. Never disqualifies the match - length+MD5
	// already prove it's the right file - but is worth a caller logging: it
	// usually means the release was renamed after the PAR2 set was created.
	NameMismatch bool
}

// MatchFiles pairs each posted file to the FileDesc PAR2 recorded for it.
// Matching is by exact length first; when more than one FileDesc (or more
// than one posted file) shares a length, the tie is broken by MD5-16k,
// computed lazily only for the tied candidates. A posted file with no
// length match, or whose MD5-16k doesn't resolve a tie, is omitted from the
// result (not an error) - the caller (repair job) simply has no PAR2
// coverage for that file and falls back to the legacy repair path for
// anything dead within it. The posted file's name never gates a match -
// length+MD5-16k alone are already enough to disambiguate any file set PAR2
// itself can distinguish, and PAR2 doesn't guarantee a name survives a
// rename - but it is checked afterward as a sanity signal; see
// Match.NameMismatch.
func MatchFiles(idx *Index, posted []PostedFile) ([]Match, error) {
	// Group FileDescs and posted files by length.
	byLength := make(map[int64][][16]byte)
	for id, fd := range idx.Files {
		byLength[fd.Length] = append(byLength[fd.Length], id)
	}
	postedByLength := make(map[int64][]int)
	for i, pf := range posted {
		postedByLength[pf.Length] = append(postedByLength[pf.Length], i)
	}

	var matches []Match
	for length, fileIDs := range byLength {
		postedIdxs, ok := postedByLength[length]
		if !ok {
			continue
		}

		if len(fileIDs) == 1 && len(postedIdxs) == 1 {
			matches = append(matches, newMatch(idx, posted, postedIdxs[0], fileIDs[0]))
			continue
		}

		// Length tie: break it with MD5-16k, computed once per tied posted
		// file (not per candidate FileID - it's a property of the posted
		// file, independent of which FileDesc it might match).
		md5ByPosted := make(map[int][16]byte, len(postedIdxs))
		for _, pi := range postedIdxs {
			if posted[pi].MD5_16k == nil {
				continue
			}
			sum, err := posted[pi].MD5_16k()
			if err != nil {
				return nil, fmt.Errorf("par2: computing MD5-16k for posted file %q: %w", posted[pi].Name, err)
			}
			md5ByPosted[pi] = sum
		}

		for _, fid := range fileIDs {
			fd := idx.Files[fid]
			for _, pi := range postedIdxs {
				sum, ok := md5ByPosted[pi]
				if !ok || sum != fd.MD5_16k {
					continue
				}
				matches = append(matches, newMatch(idx, posted, pi, fid))
			}
		}
	}
	// Residual MD5-16k match for posted files that matched no FileDesc by
	// length. A posted file's Length can be an ESTIMATE: the final segment's
	// decoded size is only known exactly after a real fetch (see
	// par2SegmentRefsFromPostingSize's last-segment branch), so a
	// unique-geometry tail volume can miss its own FileDesc's exact length by
	// a few KB and fall out of every length bucket above, even though its
	// content is intact and PAR2 fully describes it. Length is only a cheap
	// bucketing heuristic here; MD5-16k is the real identity signal - it hashes
	// the first 16KB (the first segment), wholly independent of the last-segment
	// length estimate. For each still-unmatched posted file, hash its first 16KB
	// and match it to any still-unmatched FileDesc with an equal MD5-16k, single
	// unambiguous hit only. Best-effort and purely additive: a nil hasher, a
	// fetch error, no equal FileDesc, or a >1 collision all leave the file
	// unmatched exactly as before (the caller surfaces the same terminal), so
	// this can only turn a former miss into a rigorously-verified match, never
	// break an existing one. A wrong match is still caught downstream by
	// par2.Repair's per-slice IFSC verification, which never fabricates.
	matchedPosted := make(map[int]bool, len(matches))
	matchedFID := make(map[[16]byte]bool, len(matches))
	for _, m := range matches {
		matchedPosted[m.PostedIndex] = true
		matchedFID[m.FileID] = true
	}
	for pi := range posted {
		if matchedPosted[pi] || posted[pi].MD5_16k == nil {
			continue
		}
		sum, err := posted[pi].MD5_16k()
		if err != nil {
			continue
		}
		var cand [16]byte
		found := 0
		for _, fid := range idx.FileOrder {
			if matchedFID[fid] {
				continue
			}
			fd := idx.Files[fid]
			if fd == nil || fd.MD5_16k != sum {
				continue
			}
			cand = fid
			found++
		}
		if found == 1 {
			matches = append(matches, newMatch(idx, posted, pi, cand))
			matchedFID[cand] = true
		}
	}
	return matches, nil
}

func newMatch(idx *Index, posted []PostedFile, postedIndex int, fileID [16]byte) Match {
	m := Match{PostedIndex: postedIndex, FileID: fileID}
	if fd := idx.Files[fileID]; fd != nil {
		m.NameMismatch = fd.Name != posted[postedIndex].Name
	}
	return m
}
