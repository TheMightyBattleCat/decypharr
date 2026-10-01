package usenet

import (
	"context"
	"fmt"
	"time"

	"github.com/sourcegraph/conc/iter"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
)

// par2RefileFetchTimeout bounds one posted file's yEnc header fetch in
// RefilePar2Refs.
const par2RefileFetchTimeout = 30 * time.Second

// par2RefileCandidates returns the indices of the posted files in
// nzb.Par2Source that may be PAR2 files stored there by mistake: the record
// has no PAR2 files, the file's name says nothing about its type (an
// obfuscated subject), and no file of the release reads its first article -
// an archive volume or media file is read, a PAR2 file never is. Reads the
// record only.
//
// Until the parser recognised a PAR2 file by its yEnc name, the PAR2 files of
// a release with obfuscated subjects were stored as posted files, and the
// release could never be repaired.
func par2RefileCandidates(nzb *storage.NZB) []int {
	if nzb == nil || len(nzb.Par2Files) > 0 || len(nzb.Par2Source) == 0 {
		return nil
	}
	read := make(map[string]struct{})
	for i := range nzb.Files {
		for _, seg := range nzb.Files[i].Segments {
			read[seg.MessageID] = struct{}{}
		}
	}
	var out []int
	for i, pf := range nzb.Par2Source {
		if len(pf.Segments) == 0 || pf.Segments[0].MessageID == "" {
			continue
		}
		if _, ok := read[pf.Segments[0].MessageID]; ok {
			continue
		}
		if parser.FileTypeByName(pf.Name) != storage.NZBFileTypeUnknown {
			continue
		}
		out = append(out, i)
	}
	return out
}

// Par2FilesMayBeAmongPosted reports whether RefilePar2Refs has anything to
// look at for nzb. Reads the record only.
func Par2FilesMayBeAmongPosted(nzb *storage.NZB) bool {
	return len(par2RefileCandidates(nzb)) > 0
}

// yencNameFunc returns the file name in the yEnc header of one article.
type yencNameFunc func(ctx context.Context, messageID string) (string, error)

// par2NamesAmongPosted fetches the yEnc name of each candidate posted file's
// first article and returns, by that article's message ID, the names that
// are PAR2 file names. An article no provider holds is passed over; any
// other failure is returned, since a set refiled without one of its files
// would not be looked at again.
func par2NamesAmongPosted(ctx context.Context, nzb *storage.NZB, candidates []int, maxConcurrent int, yencName yencNameFunc) (map[string]string, error) {
	type result struct {
		name string
		err  error
	}
	mapper := iter.Mapper[int, result]{MaxGoroutines: max(1, maxConcurrent)}
	results := mapper.Map(candidates, func(i *int) result {
		name, err := yencName(ctx, nzb.Par2Source[*i].Segments[0].MessageID)
		return result{name: name, err: err}
	})
	names := make(map[string]string)
	for n, r := range results {
		pf := nzb.Par2Source[candidates[n]]
		if r.err != nil {
			if nntp.IsArticleNotFoundError(r.err) {
				continue
			}
			return nil, fmt.Errorf("read the yEnc header of posted file %q: %w", pf.Name, r.err)
		}
		if parser.FileTypeByName(r.name) == storage.NZBFileTypePar2 {
			names[pf.Segments[0].MessageID] = r.name
		}
	}
	return names, nil
}

// refilePar2Refs moves the posted files of nzb whose first article is a key
// of names into nzb.Par2Files, renamed to the name given (the repair reads a
// recovery volume's slice range from its file name). The other posted files
// keep their order. Returns how many moved.
func refilePar2Refs(nzb *storage.NZB, names map[string]string) int {
	if len(names) == 0 {
		return 0
	}
	kept := make([]storage.PostedFileRef, 0, len(nzb.Par2Source))
	moved := 0
	for _, pf := range nzb.Par2Source {
		if len(pf.Segments) > 0 {
			if name, ok := names[pf.Segments[0].MessageID]; ok {
				nzb.Par2Files = append(nzb.Par2Files, storage.Par2FileRef{Name: name, Size: pf.Size, Segments: pf.Segments})
				moved++
				continue
			}
		}
		kept = append(kept, pf)
	}
	if moved > 0 {
		nzb.Par2Source = kept
	}
	return moved
}

// RefilePar2Refs finds the PAR2 files a stored record keeps among its posted
// files (see par2RefileCandidates) by fetching each candidate's yEnc header,
// and moves them to Par2Files under their yEnc names. Called by the PAR2
// repair job before it gives a release up as having no PAR2 files. Returns
// how many files moved; 0 with no error when there was nothing to move.
func (u *Usenet) RefilePar2Refs(ctx context.Context, nzoID string) (int, error) {
	nzb, err := u.nzbStorage.GetNZB(nzoID)
	if err != nil {
		return 0, fmt.Errorf("failed to load NZB: %w", err)
	}
	candidates := par2RefileCandidates(nzb)
	if len(candidates) == 0 {
		return 0, nil
	}
	names, err := par2NamesAmongPosted(ctx, nzb, candidates, u.processingMaxConnections, func(ctx context.Context, messageID string) (string, error) {
		fetchCtx, cancel := context.WithTimeout(ctx, par2RefileFetchTimeout)
		defer cancel()
		var name string
		err := u.nntp.ExecuteWithFailover(fetchCtx, func(conn *nntp.Connection) error {
			d, e := conn.GetHeaderPrefixWithTimeout(messageID, 0, par2RefileFetchTimeout)
			if d != nil {
				name = d.Name
			}
			return e
		})
		return name, err
	})
	if err != nil {
		return 0, err
	}
	if len(names) == 0 {
		return 0, nil
	}

	// The fetches ran without the storage lock; the write re-reads under it,
	// so it neither re-creates a record deleted meanwhile nor touches one
	// another writer gave PAR2 files.
	moved := 0
	err = u.nzbStorage.Update(nzoID, func(cur *storage.NZB) (bool, error) {
		if len(cur.Par2Files) > 0 {
			return false, nil
		}
		moved = refilePar2Refs(cur, names)
		return moved > 0, nil
	})
	if err != nil {
		return 0, fmt.Errorf("failed to save refiled PAR2 refs: %w", err)
	}
	return moved, nil
}
