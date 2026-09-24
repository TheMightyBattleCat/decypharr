package manager

import (
	"strings"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// par2SetChoice is how runRepair narrows a release that carries more than
// one PAR2 recovery set - a season pack posted as one set per episode - to
// the set protecting the damaged files. Handing every set's files to one
// ParseIndex fails ("belongs to a different recovery set"), which is
// terminal, so such a pack could never be repaired and was re-grabbed whole.
type par2SetChoice struct {
	setID   [16]byte // zero when the release has one set, or none could be chosen
	sources []par2.Source
	vols    []par2Volume
	pending map[string][]overlay.DeadSegment
	// deferred counts dead segments whose posted file another set protects;
	// they are left for the next pass.
	deferred int
}

// choosePar2Set picks the recovery set for this pass. sources are the index
// files already fetched. A set claims a damaged posted file when one of its
// FileDescs has the posted file's name (case-insensitively); only when no set
// matches by name does an exact size match claim it. A file claimed by more
// than one set counts for none. The set claiming the most damaged posted files
// wins; with no clear winner, or a single set, nothing changes.
func choosePar2Set(sources []par2.Source, vols []par2Volume, posted []storage.PostedFileRef, pending map[string][]overlay.DeadSegment) par2SetChoice {
	unchanged := par2SetChoice{sources: sources, vols: vols, pending: pending}

	type setInfo struct {
		idx   *par2.Index
		names []string // index source names, for matching volume names
	}
	sets := make(map[[16]byte]*setInfo)
	sourceSet := par2.SourceSetIDs(sources)
	for i, src := range sources {
		id := sourceSet[i]
		if id == ([16]byte{}) {
			continue
		}
		si, ok := sets[id]
		if !ok {
			idx, err := par2.ParseIndexSet([]par2.Source{src}, id)
			if err != nil {
				continue
			}
			si = &setInfo{idx: idx}
			sets[id] = si
		}
		si.names = append(si.names, src.Name)
	}
	if len(sets) < 2 {
		return unchanged
	}

	// The posted file each dead segment lives in.
	postedOf := make(map[string]int)
	for pi, pf := range posted {
		for _, s := range pf.Segments {
			postedOf[s.MessageID] = pi
		}
	}
	byName := func(idx *par2.Index, pf storage.PostedFileRef) bool {
		for _, fd := range idx.Files {
			if strings.EqualFold(fd.Name, pf.Name) {
				return true
			}
		}
		return false
	}
	bySize := func(idx *par2.Index, pf storage.PostedFileRef) bool {
		for _, fd := range idx.Files {
			if fd.Length == pf.Size {
				return true
			}
		}
		return false
	}
	// owner[pi] is the set claiming damaged posted file pi, when exactly one does.
	owner := make(map[int][16]byte)
	score := make(map[[16]byte]int)
	for _, segs := range pending {
		for _, seg := range segs {
			pi, ok := postedOf[seg.MessageID]
			if !ok {
				continue
			}
			if _, done := owner[pi]; done {
				continue
			}
			// A name match in any set outranks a size match in another.
			var claimant [16]byte
			n := 0
			for _, match := range []func(*par2.Index, storage.PostedFileRef) bool{byName, bySize} {
				for id, si := range sets {
					if match(si.idx, posted[pi]) {
						claimant = id
						n++
					}
				}
				if n > 0 {
					break
				}
			}
			if n == 1 {
				owner[pi] = claimant
				score[claimant]++
			}
		}
	}
	var best [16]byte
	bestScore, tie := 0, false
	for id, sc := range score {
		switch {
		case sc > bestScore:
			best, bestScore, tie = id, sc, false
		case sc == bestScore:
			tie = true
		}
	}
	if bestScore == 0 || tie {
		return unchanged
	}

	out := par2SetChoice{setID: best, pending: make(map[string][]overlay.DeadSegment, len(pending))}
	for i, src := range sources {
		if sourceSet[i] == best {
			out.sources = append(out.sources, src)
		}
	}
	for file, segs := range pending {
		for _, seg := range segs {
			if pi, ok := postedOf[seg.MessageID]; ok {
				if id, owned := owner[pi]; owned && id != best {
					out.deferred++
					continue
				}
			}
			out.pending[file] = append(out.pending[file], seg)
		}
	}

	// Recovery volumes named after the chosen set's index file(s). With
	// obfuscated names none match; then every volume stays and ParseIndexSet
	// drops the other sets' packets after the fetch.
	bases := make(map[string]bool)
	for _, n := range sets[best].names {
		bases[par2BaseName(n)] = true
	}
	for _, v := range vols {
		if bases[par2BaseName(v.ref.Name)] {
			out.vols = append(out.vols, v)
		}
	}
	if len(out.vols) == 0 {
		out.vols = vols
	}
	return out
}

// par2BaseName strips a PAR2 file name's volume suffix and extension:
// "Show.S01E02.vol03+04.par2" and "Show.S01E02.par2" are both "show.s01e02".
func par2BaseName(name string) string {
	n := par2VolPattern.ReplaceAllString(name, "")
	n = par2NumberedVolPattern.ReplaceAllString(n, "")
	return strings.TrimSuffix(strings.ToLower(n), ".par2")
}
