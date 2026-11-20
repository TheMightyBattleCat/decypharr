package parser

import (
	"encoding/xml"
	"errors"
	"fmt"
	"html"
	"io"
	"sort"
	"strings"

	"github.com/Tensai75/nzbparser"
	"github.com/Tensai75/subjectparser"
	"golang.org/x/net/html/charset"
)

// parseNZB is nzbparser.Parse with two changes.
//
// It sorts files and segments stably, so files sharing a subject file number
// keep their NZB document order. nzbparser.Parse sorts with sort.Sort, which
// scrambles them - and those are exactly the files whose order nothing else
// records: obfuscated volumes whose posters give every file one number, or
// scramble the counter (see groupFiles, mergeObfuscatedRarGroups).
//
// And it keeps one copy of a file posted twice under one subject (see
// keepOneCopyPerSubject), where nzbparser merges the copies' articles into one
// file with every article number twice.
//
// dropped names the subjects it kept a single copy of.
func parseNZB(r io.Reader) (nzb *nzbparser.Nzb, dropped []string, err error) {
	nzb, dropped, _, _, err = parseNZBWithPasswords(r)
	return nzb, dropped, err
}

// parseNZBWithPasswords is parseNZB that also returns every password meta,
// in document order. Meta values are character data: entities are decoded,
// so a password with "&" is not handed on as "&amp;". Nzb.Meta keeps one
// value per type and would drop all but the last password.
//
// Segment message IDs are cleaned the same way (see cleanSegmentIDs); badIDs
// names each file dropped for an ID that cannot be sent, with the reason. An
// NZB left with no file that way is an error.
func parseNZBWithPasswords(r io.Reader) (nzb *nzbparser.Nzb, dropped, badIDs, passwords []string, err error) {
	var x struct {
		XMLName  xml.Name           `xml:"nzb"`
		Comment  string             `xml:",comment"`
		Metadata []nzbMeta          `xml:"head>meta"`
		Files    nzbparser.NzbFiles `xml:"file"`
	}
	decoder := xml.NewDecoder(r)
	decoder.CharsetReader = charset.NewReaderLabel
	decoder.Strict = false // as nzbparser: ignore unknown or malformed character entities
	if err := decoder.Decode(&x); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("unable to parse NZB file: %s", err.Error())
	}
	files, badIDs := cleanSegmentIDs(x.Files)
	if len(files) == 0 && len(badIDs) > 0 {
		return nil, nil, badIDs, nil, fmt.Errorf("unable to parse NZB file: no file has valid message IDs (%s)", badIDs[0])
	}

	nzb = &nzbparser.Nzb{Comment: x.Comment, Meta: make(map[string]string, len(x.Metadata))}
	for _, md := range x.Metadata {
		nzb.Meta[md.Type] = md.Value
		if md.isPassword() {
			passwords = append(passwords, md.value())
		}
	}
	nzb.Files, dropped = keepOneCopyPerSubject(files)

	nzbparser.MakeUnique(nzb)
	nzbparser.ScanNzbFile(nzb)

	sort.SliceStable(nzb.Files, func(i, j int) bool { return nzb.Files[i].Number < nzb.Files[j].Number })
	for i := range nzb.Files {
		segs := nzb.Files[i].Segments
		sort.SliceStable(segs, func(a, b int) bool { return segs[a].Number < segs[b].Number })
	}
	return nzb, dropped, badIDs, passwords, nil
}

// cleanSegmentIDs returns files with each segment ID in the form sent to the
// news server, and without the files holding an ID that cannot be sent.
//
// nzbparser reads a segment ID as the element's raw inner XML: entities are
// not decoded and line breaks inside it are kept. An escaped "&" would be
// asked for as "&amp;" (a 430 for an article the provider holds), and a line
// break would send what follows it as a second NNTP command, leaving the
// connection one reply behind for every later user of it. Each ID is
// decoded, trimmed, and stripped of one pair of enclosing angle brackets; a
// file with an ID that is then empty, or holds a byte outside printable
// ASCII or an inner angle bracket, is dropped. bad names each dropped file
// with the reason.
func cleanSegmentIDs(files nzbparser.NzbFiles) (kept nzbparser.NzbFiles, bad []string) {
	kept = files[:0:0]
	for _, f := range files {
		segs := make(nzbparser.NzbSegments, len(f.Segments))
		var why error
		for i, seg := range f.Segments {
			id, err := cleanMessageID(seg.Id)
			if err != nil {
				why = fmt.Errorf("article %d: %w", seg.Number, err)
				break
			}
			seg.Id = id
			segs[i] = seg
		}
		if why != nil {
			bad = append(bad, fmt.Sprintf("%q: %v", f.Subject, why))
			continue
		}
		f.Segments = segs
		kept = append(kept, f)
	}
	return kept, bad
}

var errEmptyMessageID = errors.New("empty message ID")

// cleanMessageID returns raw (a segment element's inner XML) as a bare
// message ID: entities decoded, surrounding white space and one pair of
// enclosing angle brackets removed. It fails when the result is empty or
// holds a byte outside 0x21-0x7e or an angle bracket.
func cleanMessageID(raw string) (string, error) {
	id := strings.TrimSpace(html.UnescapeString(raw))
	if len(id) >= 2 && id[0] == '<' && id[len(id)-1] == '>' {
		id = id[1 : len(id)-1]
	}
	if id == "" {
		return "", errEmptyMessageID
	}
	for i := 0; i < len(id); i++ {
		if b := id[i]; b < 0x21 || b > 0x7e || b == '<' || b == '>' {
			return "", fmt.Errorf("message ID %q holds byte 0x%02x", id, b)
		}
	}
	return id, nil
}

// keepOneCopyPerSubject returns files with each subject's duplicate <file>
// entries reduced to its first complete copy.
//
// nzbparser.MakeUnique merges entries that share a subject, on the assumption
// that they list one posting's articles in pieces. Some NZBs instead list two
// postings of the same file: Keeper S03E06 SiQ has every volume twice, from
// two posters, with different message IDs and different article bytes. Merged,
// each volume has every article number twice, getNZBSegments rejects it, and
// the release cannot import. Interleaving the copies' articles would not be
// safe either: nothing says the two postings split the file at the same bytes.
//
// A copy is complete when its article numbers run without holes or repeats
// from 0 or 1 to the article count its subject gives. When a subject has a
// complete copy, the first one in document order is kept and the other entries
// dropped; a subject without one is left for MakeUnique to merge as before.
func keepOneCopyPerSubject(files nzbparser.NzbFiles) (nzbparser.NzbFiles, []string) {
	bySubject := make(map[string][]int, len(files))
	for i, f := range files {
		bySubject[f.Subject] = append(bySubject[f.Subject], i)
	}
	drop := map[int]bool{}
	var dropped []string
	for i, f := range files {
		idxs := bySubject[f.Subject]
		if len(idxs) < 2 || idxs[0] != i {
			continue
		}
		total := 0
		if s, err := subjectparser.Parse(f.Subject); err == nil {
			total = s.TotalSegments
		}
		keep := -1
		for _, j := range idxs {
			if completeCopy(files[j], total) {
				keep = j
				break
			}
		}
		if keep < 0 || !distinctCopies(files, idxs) {
			continue
		}
		for _, j := range idxs {
			if j != keep {
				drop[j] = true
			}
		}
		dropped = append(dropped, f.Subject)
	}
	if len(drop) == 0 {
		return files, nil
	}
	kept := make(nzbparser.NzbFiles, 0, len(files)-len(drop))
	for i, f := range files {
		if !drop[i] {
			kept = append(kept, f)
		}
	}
	return kept, dropped
}

// completeCopy reports whether f's articles are numbered without holes or
// repeats from 0 or 1 up, and number total when total is known.
func completeCopy(f nzbparser.NzbFile, total int) bool {
	n := len(f.Segments)
	if n == 0 || (total > 0 && n != total) {
		return false
	}
	seen := make(map[int]bool, n)
	lo, hi := f.Segments[0].Number, f.Segments[0].Number
	for _, s := range f.Segments {
		if s.Id == "" || seen[s.Number] {
			return false
		}
		seen[s.Number] = true
		lo, hi = min(lo, s.Number), max(hi, s.Number)
	}
	return (lo == 0 || lo == 1) && hi-lo+1 == n
}

// distinctCopies reports whether the entries at idxs list different articles
// for one article number, so merging them would repeat numbers. Entries that
// only repeat the same message IDs, or list disjoint numbers, merge cleanly.
func distinctCopies(files nzbparser.NzbFiles, idxs []int) bool {
	byNumber := map[int]string{}
	for _, j := range idxs {
		for _, s := range files[j].Segments {
			if id, ok := byNumber[s.Number]; ok && id != s.Id {
				return true
			}
			byNumber[s.Number] = s.Id
		}
	}
	return false
}
