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

// MatchSkip records a posted file that MatchFiles could not even attempt
// to pair because doing so required real bytes (its MD5-16k) that failed
// to fetch or hash - an NNTP timeout, a connection reset, a read error
// mid-stream. It is purely informational: the file is simply absent from
// the returned matches, exactly as any other miss would be, and the caller
// falls back to the legacy path for anything dead within it. The point is
// to let the caller tell a TRANSIENT miss (this) apart from a STRUCTURAL
// one (the file genuinely isn't in the release's retained posted-file set)
// when it later classifies a downstream "no PAR2 coverage for this file"
// failure as retryable vs terminal.
type MatchSkip struct {
	PostedIndex int
	Err         error
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
//
// A final last-one-standing pass pairs any length group that has come down
// to exactly one unmatched FileDesc and one unmatched posted file: with
// every other same-length candidate already claimed, the two can only be
// each other, so no fetch is needed to confirm it.
//
// Matching is best-effort with respect to fetch failures: if breaking a
// length tie (or the residual MD5-16k pass) needs a posted file's real
// bytes and that fetch/hash errors out, that ONE file is skipped and
// recorded in the returned []MatchSkip - the rest of the set still
// matches, and the call still succeeds. A hard error is returned only for
// a genuinely malformed input.
func MatchFiles(idx *Index, posted []PostedFile) ([]Match, []MatchSkip, error) {
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
	var skipped []MatchSkip
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
				// Best-effort: a fetch/hash failure for one tied posted
				// file skips only that file's match attempt. The caller
				// gets the rest, plus this skip record so it knows the
				// miss was transient.
				skipped = append(skipped, MatchSkip{PostedIndex: pi, Err: fmt.Errorf("computing MD5-16k for posted file %q: %w", posted[pi].Name, err)})
				continue
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
			// Same best-effort treatment as the tie-break pass: a fetch
			// failure hashing this file's first 16KB is transient, not a
			// structural "no such file in the recovery set" - record it so
			// the caller can classify a later coverage failure correctly.
			skipped = append(skipped, MatchSkip{PostedIndex: pi, Err: fmt.Errorf("residual MD5-16k for posted file %q: %w", posted[pi].Name, err)})
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
			matchedPosted[pi] = true
		}
	}

	// Last-one-standing deduction. After the length and MD5-16k passes have
	// claimed every file they can prove, a length group that comes down to
	// exactly one still-unmatched FileDesc and exactly one still-unmatched
	// posted file can only be paired one way: MatchFiles has already
	// confirmed the two lengths are equal and eliminated every other
	// same-length candidate on each side by MD5, so no ambiguity is left.
	// The pairing needs no fetch. This resolves the common multi-part RAR
	// case where one dead tie-break article would otherwise leave a file
	// "retained but unmatched" and starve the repair of fetcher coverage
	// for slices it could have read intact.
	unmatchedFDByLength := make(map[int64][][16]byte)
	for _, fid := range idx.FileOrder {
		if matchedFID[fid] {
			continue
		}
		fd := idx.Files[fid]
		if fd == nil {
			continue
		}
		unmatchedFDByLength[fd.Length] = append(unmatchedFDByLength[fd.Length], fid)
	}
	unmatchedPostedByLength := make(map[int64][]int)
	for pi := range posted {
		if matchedPosted[pi] {
			continue
		}
		unmatchedPostedByLength[posted[pi].Length] = append(unmatchedPostedByLength[posted[pi].Length], pi)
	}
	for length, fids := range unmatchedFDByLength {
		pis := unmatchedPostedByLength[length]
		if len(fids) != 1 || len(pis) != 1 {
			continue
		}
		pi, fid := pis[0], fids[0]
		matches = append(matches, newMatch(idx, posted, pi, fid))
		matchedFID[fid] = true
		matchedPosted[pi] = true
	}

	// A posted file skipped in the tie-break pass is retried in the
	// residual pass, and may be resolved by the deduction above; drop skip
	// records for anything that ultimately matched, and collapse duplicates
	// to one record per posted file.
	if len(skipped) > 0 {
		deduped := skipped[:0]
		seen := make(map[int]bool, len(skipped))
		for _, s := range skipped {
			if matchedPosted[s.PostedIndex] || seen[s.PostedIndex] {
				continue
			}
			seen[s.PostedIndex] = true
			deduped = append(deduped, s)
		}
		skipped = deduped
	}
	return matches, skipped, nil
}

func newMatch(idx *Index, posted []PostedFile, postedIndex int, fileID [16]byte) Match {
	m := Match{PostedIndex: postedIndex, FileID: fileID}
	if fd := idx.Files[fileID]; fd != nil {
		m.NameMismatch = fd.Name != posted[postedIndex].Name
	}
	return m
}
