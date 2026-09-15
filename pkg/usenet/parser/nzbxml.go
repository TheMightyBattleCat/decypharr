package parser

import (
	"encoding/xml"
	"fmt"
	"io"
	"sort"

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
	var x struct {
		XMLName  xml.Name `xml:"nzb"`
		Comment  string   `xml:",comment"`
		Metadata []struct {
			Type  string `xml:"type,attr"`
			Value string `xml:",innerxml"`
		} `xml:"head>meta"`
		Files nzbparser.NzbFiles `xml:"file"`
	}
	decoder := xml.NewDecoder(r)
	decoder.CharsetReader = charset.NewReaderLabel
	decoder.Strict = false // as nzbparser: ignore unknown or malformed character entities
	if err := decoder.Decode(&x); err != nil {
		return nil, nil, fmt.Errorf("unable to parse NZB file: %s", err.Error())
	}

	nzb = &nzbparser.Nzb{Comment: x.Comment, Meta: make(map[string]string, len(x.Metadata))}
	for _, md := range x.Metadata {
		nzb.Meta[md.Type] = md.Value
	}
	nzb.Files, dropped = keepOneCopyPerSubject(x.Files)

	nzbparser.MakeUnique(nzb)
	nzbparser.ScanNzbFile(nzb)

	sort.SliceStable(nzb.Files, func(i, j int) bool { return nzb.Files[i].Number < nzb.Files[j].Number })
	for i := range nzb.Files {
		segs := nzb.Files[i].Segments
		sort.SliceStable(segs, func(a, b int) bool { return segs[a].Number < segs[b].Number })
	}
	return nzb, dropped, nil
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
