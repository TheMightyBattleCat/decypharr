package manager

import (
	"bytes"
	"context"
	"strings"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// par2SetChoice is how runRepair narrows a release that carries more than
// one PAR2 recovery set - a season pack posted as one set per episode - to
// the set protecting the damaged files. Handing every set's files to one
// ParseIndex parses only the first set it meets, which need not be the one
// the damage is in; before that it failed ("belongs to a different recovery
// set"), which is terminal, so such a pack was re-grabbed whole.
type par2SetChoice struct {
	setID   [16]byte // zero when the release has one set, or none could be chosen
	sources []par2.Source
	vols    []par2Volume
	pending map[string][]overlay.DeadSegment
	// deferred counts dead segments whose posted file another set protects;
	// they are left for the next pass.
	deferred int
	// unattributed is the number of recovery sets when there are several but
	// no damaged posted file belongs to exactly one of them, so no set can be
	// chosen; zero otherwise. ParseIndex over every set at once always fails,
	// so the caller reports this instead of attempting the pass.
	unattributed int
}

// choosePar2Set picks the recovery set for this pass. sources are the index
// files already fetched. A set claims a damaged posted file when one of its
// FileDescs has the posted file's name (case-insensitively); only when no set
// matches by name does an exact size match claim it. A file claimed by more
// than one set counts for none. The set claiming the most damaged posted files
// wins. Sets that claim equally many are ordered by set ID, so every pass makes
// the same choice: a season pack with one damaged episode in each of two sets
// repairs one set, defers the other's damage, and the next pass (where the
// repaired episode no longer has pending damage) repairs the other. A single
// set leaves everything unchanged.
func choosePar2Set(ctx context.Context, sources []par2.Source, vols []par2Volume, posted []storage.PostedFileRef, pending map[string][]overlay.DeadSegment) par2SetChoice {
	unchanged := par2SetChoice{sources: sources, vols: vols, pending: pending}

	sets, sourceSet := par2VisibleSets(ctx, sources)
	if len(sets) < 2 {
		return unchanged
	}

	postedOf := par2PostedOfSegment(posted)
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
	bestScore := 0
	for id, sc := range score {
		if sc > bestScore || (sc == bestScore && bytes.Compare(id[:], best[:]) < 0) {
			best, bestScore = id, sc
		}
	}
	if bestScore == 0 {
		unchanged.unattributed = len(sets)
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

// par2VisibleSet is one recovery set seen among the fetched PAR2 files: its
// index, parsed from the first of its files, and the names of all of them.
type par2VisibleSet struct {
	idx   *par2.Index
	names []string // source names, for matching volume names
}

// par2VisibleSets groups sources by recovery set. sourceSet[i] is the set of
// sources[i], zero when it has none. A set whose first file fails to parse is
// left out.
func par2VisibleSets(ctx context.Context, sources []par2.Source) (sets map[[16]byte]*par2VisibleSet, sourceSet [][16]byte) {
	sets = make(map[[16]byte]*par2VisibleSet)
	sourceSet = par2.SourceSetIDs(ctx, sources)
	for i, src := range sources {
		id := sourceSet[i]
		if id == ([16]byte{}) {
			continue
		}
		si, ok := sets[id]
		if !ok {
			idx, err := par2.ParseIndexSet(ctx, []par2.Source{src}, id)
			if err != nil {
				continue
			}
			si = &par2VisibleSet{idx: idx}
			sets[id] = si
		}
		si.names = append(si.names, src.Name)
	}
	return sets, sourceSet
}

// par2PostedOfSegment maps each article's message ID to the index of the
// posted file it belongs to.
func par2PostedOfSegment(posted []storage.PostedFileRef) map[string]int {
	postedOf := make(map[string]int)
	for pi, pf := range posted {
		for _, s := range pf.Segments {
			postedOf[s.MessageID] = pi
		}
	}
	return postedOf
}

// par2DamageInUnseenSet reports whether a damaged posted file is named by no
// recovery set among sources, so the set protecting it may be one no fetched
// file shows yet: a set posted with no index file, or whose index failed,
// and none of whose volumes has been fetched. The check is by name only.
// Size would not do: a season pack's episodes, or their RAR volumes, are
// often the same size, so a visible set would claim the other episode's
// file. It is made only when the release's names are real (a visible set
// names some posted file); an obfuscated posting reports false, and keeps
// matching by MD5 as before.
func par2DamageInUnseenSet(ctx context.Context, sources []par2.Source, posted []storage.PostedFileRef, pending map[string][]overlay.DeadSegment) bool {
	sets, _ := par2VisibleSets(ctx, sources)
	named := make(map[string]bool)
	for _, si := range sets {
		for _, fd := range si.idx.Files {
			named[strings.ToLower(fd.Name)] = true
		}
	}
	realNames := false
	for _, pf := range posted {
		if named[strings.ToLower(pf.Name)] {
			realNames = true
			break
		}
	}
	if !realNames {
		return false
	}
	postedOf := par2PostedOfSegment(posted)
	for _, segs := range pending {
		for _, seg := range segs {
			if pi, ok := postedOf[seg.MessageID]; ok && !named[strings.ToLower(posted[pi].Name)] {
				return true
			}
		}
	}
	return false
}

// fetchVolumesForUnseenSet fetches recovery volumes, one at a time, while
// par2DamageInUnseenSet holds and an untried volume may belong to a set not
// yet seen: its base name (par2BaseName) is that of no fetched file. Every
// volume of a set shares the base name, so this takes about one volume per
// unseen set, not every volume of the sets already seen. A volume that
// 430s leaves its base unseen, and the next volume of that base is tried.
// A one-set release whose damage is in a file the set does not protect has
// no such volume, and fetches nothing.
//
// *gone counts the volumes in a row that were gone (430) across every call
// of one pass, as fetchMoreVolumes does within one call: at
// par2RecoveryMaxConsecutiveNotFound the search stops, so a set whose
// volumes are all gone is not tried one volume at a time. A volume that
// fetches or fails without a 430 starts the count again.
//
// The volume fetched is moved to vols[nextVolIdx] first, the rest keeping
// their order, so the volumes tried stay the prefix vols[:next] that
// par2SetAfterVolumes expects. vols is copied before the first move.
// unseen is par2DamageInUnseenSet on return.
func fetchVolumesForUnseenSet(
	ctx context.Context,
	logger zerolog.Logger,
	fetch articleFetchFunc,
	vols []par2Volume,
	nextVolIdx int,
	sources []par2.Source,
	posted []storage.PostedFileRef,
	pending map[string][]overlay.DeadSegment,
	entryName string,
	maxConc int,
	progress *par2JobProgressState,
	gone *int,
) (outVols []par2Volume, next int, outSources []par2.Source, unseen bool, err error) {
	copied := false
	for {
		if err := ctx.Err(); err != nil {
			return vols, nextVolIdx, sources, true, err
		}
		if !par2DamageInUnseenSet(ctx, sources, posted, pending) {
			return vols, nextVolIdx, sources, false, nil
		}
		if *gone >= par2RecoveryMaxConsecutiveNotFound {
			return vols, nextVolIdx, sources, true, nil
		}
		known := make(map[string]bool, len(sources))
		for _, s := range sources {
			known[par2BaseName(s.Name)] = true
		}
		j := -1
		for i := nextVolIdx; i < len(vols); i++ {
			if !known[par2BaseName(vols[i].ref.Name)] {
				j = i
				break
			}
		}
		if j < 0 {
			return vols, nextVolIdx, sources, true, nil
		}
		if !copied {
			vols = append([]par2Volume(nil), vols...)
			copied = true
		}
		v := vols[j]
		copy(vols[nextVolIdx+1:j+1], vols[nextVolIdx:j])
		vols[nextVolIdx] = v
		logger.Info().Str("entry", entryName).Str("volume", v.ref.Name).
			Msg("par2: a damaged file is in no recovery set fetched so far, fetching a volume of another set")
		var notFound int
		nextVolIdx, _, sources, _, notFound, err = fetchVolumeBatch(ctx, logger, fetch, vols[:nextVolIdx+1], nextVolIdx, 1, 0, sources, entryName, maxConc)
		if err != nil {
			return vols, nextVolIdx, sources, true, par2Failf(par2Transient, "fetch recovery volumes: %w", err)
		}
		if notFound > 0 {
			*gone++
		} else {
			*gone = 0
		}
		if progress != nil {
			progress.SetRecoveryVolsFetched(nextVolIdx)
			progress.Touch()
		}
	}
}

// par2SetAfterVolumes chooses the recovery set again once the first recovery
// volumes are among sources. The first choice saw only the index files: when
// one of them failed to fetch, or a set was posted with no index file, the
// volumes are the first sign of a second set. ok is false when the volumes
// show no more than one set, and nothing changes.
//
// vols[:nextVolIdx] are the volumes already fetched (or tried). The chosen
// set's volumes keep their order, so its fetched ones stay a prefix; next is
// their count, the index later fetches resume from in the narrowed list.
func par2SetAfterVolumes(ctx context.Context, sources []par2.Source, vols []par2Volume, nextVolIdx int, posted []storage.PostedFileRef, pending map[string][]overlay.DeadSegment) (c par2SetChoice, next int, ok bool) {
	seen := make(map[[16]byte]bool)
	for _, id := range par2.SourceSetIDs(ctx, sources) {
		if id != ([16]byte{}) {
			seen[id] = true
		}
	}
	if len(seen) < 2 {
		return par2SetChoice{}, nextVolIdx, false
	}
	c = choosePar2Set(ctx, sources, vols, posted, pending)
	if c.setID == ([16]byte{}) {
		return c, nextVolIdx, true
	}
	tried := make(map[string]bool, nextVolIdx)
	for _, v := range vols[:min(nextVolIdx, len(vols))] {
		tried[v.ref.Name] = true
	}
	for _, v := range c.vols {
		if !tried[v.ref.Name] {
			break
		}
		next++
	}
	return c, next, true
}

// par2BaseName strips a PAR2 file name's volume suffix and extension:
// "Show.S01E02.vol03+04.par2" and "Show.S01E02.par2" are both "show.s01e02".
func par2BaseName(name string) string {
	n := par2VolPattern.ReplaceAllString(name, "")
	n = par2NumberedVolPattern.ReplaceAllString(n, "")
	return strings.TrimSuffix(strings.ToLower(n), ".par2")
}
