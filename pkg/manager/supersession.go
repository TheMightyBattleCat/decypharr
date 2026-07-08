package manager

import (
	"context"
	"fmt"
	"sync"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// supersessionResult is the outcome of comparing one broken entry's
// BrokenFiles against the current Arr reference set.
type supersessionResult struct {
	// entryReferenced is true when at least one file of this entry (broken or
	// not) is still mapped back to it by some Arr. It gates entry deletion:
	// an entry that's still in active use for other files (a season pack
	// where only some episodes were individually re-grabbed) must never be
	// deleted, even once its own broken list empties out.
	entryReferenced bool
	// stillBroken are the BrokenFiles that remain genuinely broken: either an
	// Arr still references them, or there's no Arr context to judge them by
	// at all (managed-source files, or files no Arr ever owned), in which
	// case they're always kept rather than guessed at.
	stillBroken []storage.BrokenFile
	// superseded are the BrokenFiles an Arr no longer references - the Arr
	// has already replaced them with a working copy elsewhere.
	superseded []storage.BrokenFile
}

// classifySupersession decides, per broken file, whether it's still needed
// or was replaced. A file with no Arr context (ArrName/ArrFileID empty - a
// managed-source entry, or one no Arr ever owned) can never be judged
// superseded and is always kept.
func classifySupersession(h *storage.EntryHealth, refs map[string]map[string]struct{}) supersessionResult {
	entryRefs := refs[h.EntryName]
	res := supersessionResult{entryReferenced: len(entryRefs) > 0}
	for _, bf := range h.BrokenFiles {
		if bf.ArrName == "" || bf.ArrFileID == 0 {
			res.stillBroken = append(res.stillBroken, bf)
			continue
		}
		if _, ok := entryRefs[bf.FileName]; ok {
			res.stillBroken = append(res.stillBroken, bf)
			continue
		}
		res.superseded = append(res.superseded, bf)
	}
	return res
}

// applySupersession persists the outcome of classifySupersession for one
// entry's health record. Returns cleared=true when the broken-list record
// was removed entirely, which happens whenever every remaining broken file
// turned out to be superseded (res.stillBroken is empty) - regardless of
// whether other, non-broken files in the same entry are still referenced.
//
// Deleting the underlying entry from decypharr (cleanupEntry) is strictly
// narrower than clearing the health record: it only ever happens when
// res.entryReferenced is also false, i.e. nothing about this entry - broken
// or otherwise - is in use anymore. The season-pack case (some other file in
// the same entry is still referenced) always survives with its broken list
// merely trimmed or cleared, never deleted.
func (r *Repair) applySupersession(h *storage.EntryHealth, res supersessionResult, cleanupEntry bool) (cleared bool, err error) {
	if len(res.superseded) == 0 {
		return false, nil
	}

	if len(res.stillBroken) == 0 {
		if err := r.manager.storage.DeleteEntryHealth(h.EntryName); err != nil {
			return false, err
		}
		r.logger.Info().Str("entry", h.EntryName).Int("superseded_files", len(res.superseded)).
			Msg("Repair: superseded by replacement; removed from broken list")

		if cleanupEntry && !res.entryReferenced {
			r.deleteSupersededEntry(h.EntryName)
		}
		return true, nil
	}

	h.BrokenFiles = res.stillBroken
	h.FailureReason = topReason(h.BrokenFiles)
	if err := r.manager.storage.SaveEntryHealth(h); err != nil {
		return false, err
	}
	r.logger.Info().Str("entry", h.EntryName).
		Int("superseded_files", len(res.superseded)).
		Int("remaining_broken", len(h.BrokenFiles)).
		Msg("Repair: some broken files superseded by replacement; removed from broken list")
	return false, nil
}

// deleteSupersededEntry removes the underlying entry once nothing about it
// is referenced by any Arr anymore. Mirrors the identifier + call pattern
// finalizeEntryRepair uses to delete a fully-broken entry after a successful
// re-search: DeleteEntry is keyed by infohash, and a single entry folder can
// span more than one (a merged candidate), so every distinct infohash across
// the entry's files is deleted.
func (r *Repair) deleteSupersededEntry(entryName string) {
	item, err := r.manager.GetEntryItem(entryName)
	if err != nil || item == nil {
		return
	}
	hashes := make(map[string]struct{})
	for _, f := range item.Files {
		if f != nil && f.InfoHash != "" {
			hashes[f.InfoHash] = struct{}{}
		}
	}
	for hash := range hashes {
		if err := r.manager.DeleteEntry(hash, true); err != nil {
			r.logger.Warn().Err(err).Str("entry", entryName).Str("infohash", hash).Msg("Repair: failed to delete superseded entry")
			continue
		}
		r.logger.Info().Str("entry", entryName).Str("infohash", hash).Msg("Repair: deleted superseded entry")
	}
}

// buildArrReferencedSet maps entry-folder name -> the set of file names
// within it that some currently-eligible Arr still references, using the
// exact same GetMedia + symlink-target resolution enumerateArrCandidates
// already relies on (collectArrMediaCandidates / collectArrFiles). This
// deliberately never invents a second path-resolution implementation that
// could silently disagree with the sweep's.
//
// It queries every eligible Arr regardless of cfg.Repair.Arrs scoping: "does
// any Arr still use this file" is independent of which Arrs a scheduled
// sweep happens to be scoped to check, and narrowing to that scope could
// make a file merely excluded from the sweep's Arr filter look superseded.
//
// Any per-Arr fetch or resolution failure aborts the whole build and returns
// an error - callers must treat that as "couldn't determine anything" and
// leave the broken list untouched. Silently skipping a failed Arr (the way
// enumerateArrCandidates does for sweep enumeration) would risk treating
// real breakage as superseded during that Arr's outage.
func (r *Repair) buildArrReferencedSet(ctx context.Context) (map[string]map[string]struct{}, error) {
	arrs := r.eligibleArrs(nil)
	out := make(map[string]map[string]struct{})
	if len(arrs) == 0 {
		return out, nil
	}

	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	for _, a := range arrs {
		g.Go(func() error {
			sub, err := r.collectArrMediaCandidates(gctx, a, "")
			if err != nil {
				return fmt.Errorf("arr %q: %w", a.Name, err)
			}
			mu.Lock()
			for name, c := range sub {
				files, ok := out[name]
				if !ok {
					files = make(map[string]struct{}, len(c.contentMap))
					out[name] = files
				}
				for fileName := range c.contentMap {
					files[fileName] = struct{}{}
				}
			}
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}

// excludeFilesFromItem returns a shallow copy of item with the named files
// removed from its Files map. Used to keep an already-superseded file out of
// a recheck's probe pass without mutating the item's stored representation.
func excludeFilesFromItem(item *storage.EntryItem, exclude map[string]struct{}) *storage.EntryItem {
	if item == nil || len(exclude) == 0 {
		return item
	}
	files := make(map[string]*storage.File, len(item.Files))
	for name, file := range item.Files {
		if _, skip := exclude[name]; skip {
			continue
		}
		files[name] = file
	}
	out := *item
	out.Files = files
	return &out
}

// dropSupersededCandidates removes managed-source sweep candidates whose
// existing broken health is fully superseded (no Arr references any of
// their files anymore), clearing their records instead of spending a probe
// re-confirming "broken" on a release the library already replaced.
// Candidates that are healthy, unchecked, or only partially superseded are
// left untouched here - the sweep's normal probe pass re-derives their
// broken-file list from scratch regardless.
//
// The Arr round trip in buildArrReferencedSet is skipped entirely when
// nothing in this batch is even a broken, Arr-linked candidate. On a
// reference-set failure, candidates are returned unmodified - the sweep
// proceeds exactly as it would without this check.
func (r *Repair) dropSupersededCandidates(ctx context.Context, in map[string]*candidate, log zerolog.Logger) map[string]*candidate {
	type pending struct {
		name string
		h    *storage.EntryHealth
	}
	var pendings []pending
	for name := range in {
		h, err := r.manager.storage.GetEntryHealth(name)
		if err != nil || h == nil || h.Status != storage.HealthBroken || len(h.BrokenFiles) == 0 {
			continue
		}
		hasArrFile := false
		for _, bf := range h.BrokenFiles {
			if bf.ArrName != "" && bf.ArrFileID != 0 {
				hasArrFile = true
				break
			}
		}
		if hasArrFile {
			pendings = append(pendings, pending{name: name, h: h})
		}
	}
	if len(pendings) == 0 {
		return in
	}

	refs, err := r.buildArrReferencedSet(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("Sweep: failed to build Arr reference set for supersession check; probing all candidates")
		return in
	}

	cleanup := r.cfg().CleanupSuperseded
	dropped := 0
	for _, p := range pendings {
		res := classifySupersession(p.h, refs)
		if len(res.superseded) == 0 || len(res.stillBroken) > 0 {
			// Not superseded, or only partially - the normal probe pass
			// re-derives this entry's broken files from scratch either way.
			continue
		}
		cleared, aerr := r.applySupersession(p.h, res, cleanup)
		if aerr != nil {
			log.Warn().Err(aerr).Str("entry", p.name).Msg("Sweep: failed to apply supersession")
			continue
		}
		if cleared {
			delete(in, p.name)
			dropped++
		}
	}
	if dropped > 0 {
		log.Info().Int("dropped", dropped).Msg("Sweep: skipped fully-superseded candidates")
	}
	return in
}

// filterSupersededHealths drops or trims broken-entry candidates the Arrs no
// longer reference before a Fix pass acts on them, so "Fix" never blocklists
// or re-searches on behalf of a file the Arr already replaced. Mutates
// healths in place: fully-superseded entries are removed from the map,
// partially-superseded ones are left in with their BrokenFiles trimmed.
//
// On an Arr reference-set failure, healths is left untouched entirely - Fix
// then behaves exactly as it did before this check existed, rather than risk
// treating real breakage as replaced.
func (r *Repair) filterSupersededHealths(ctx context.Context, healths *xsync.Map[string, *storage.EntryHealth]) {
	refs, err := r.buildArrReferencedSet(ctx)
	if err != nil {
		r.logger.Warn().Err(err).Msg("Fix: failed to build Arr reference set for supersession check; fixing all candidates")
		return
	}

	cleanup := r.cfg().CleanupSuperseded
	toDelete := make([]string, 0)
	healths.Range(func(name string, h *storage.EntryHealth) bool {
		res := classifySupersession(h, refs)
		if len(res.superseded) == 0 {
			return true
		}
		cleared, aerr := r.applySupersession(h, res, cleanup)
		if aerr != nil {
			r.logger.Warn().Err(aerr).Str("entry", name).Msg("Fix: failed to apply supersession")
			return true
		}
		if cleared {
			toDelete = append(toDelete, name)
		}
		return true
	})
	for _, name := range toDelete {
		healths.Delete(name)
	}
}

// SupersededClearResult summarizes one run of ClearSuperseded for the API/UI.
type SupersededClearResult struct {
	Checked        int `json:"checked"`
	ClearedEntries int `json:"cleared_entries"`
	ClearedFiles   int `json:"cleared_files"`
	StillBroken    int `json:"still_broken"`
}

// clearSupersededRunID is a sentinel activeRunID used to hold the same
// singleton lock a sweep/fix/clear run does, for the duration of
// ClearSuperseded. It is not a real RepairRun: nothing is persisted under
// this ID, it exists only to keep a concurrent sweep from writing the same
// EntryHealth records this synchronous pass is reading and mutating.
const clearSupersededRunID = "clear-superseded"

// ClearSuperseded walks every currently-broken entry, checks whether the
// Arrs still reference its files, and clears whatever's already been
// replaced. This is the "Clear replaced" panel action: it runs supersession
// across the whole broken list in one pass, independent of any sweep.
//
// On an Arr reference-set failure, nothing is cleared and the error is
// returned as-is so the caller can surface it - a partial/failed Arr lookup
// must never be treated as "nothing is referenced".
func (r *Repair) ClearSuperseded(ctx context.Context) (SupersededClearResult, error) {
	var result SupersededClearResult
	if ctx == nil {
		ctx = r.parentCtx
	}

	// Hold the same singleton slot a sweep/fix/clear run does for the
	// duration of this pass: it reads and rewrites EntryHealth records a
	// concurrent sweep could be probing and saving at the same moment.
	r.mu.Lock()
	if r.activeRunID != "" {
		id := r.activeRunID
		r.mu.Unlock()
		return result, fmt.Errorf("repair already running (run %s)", id)
	}
	r.activeRunID = clearSupersededRunID
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		if r.activeRunID == clearSupersededRunID {
			r.activeRunID = ""
		}
		r.mu.Unlock()
	}()

	refs, err := r.buildArrReferencedSet(ctx)
	if err != nil {
		return result, fmt.Errorf("failed to build Arr reference set: %w", err)
	}

	cleanup := r.cfg().CleanupSuperseded
	_ = r.manager.storage.ForEachEntryHealth(func(h *storage.EntryHealth) error {
		if h == nil || h.Status != storage.HealthBroken || len(h.BrokenFiles) == 0 {
			return nil
		}
		result.Checked++

		res := classifySupersession(h, refs)
		if len(res.superseded) == 0 {
			result.StillBroken++
			return nil
		}

		cleared, aerr := r.applySupersession(h, res, cleanup)
		if aerr != nil {
			r.logger.Warn().Err(aerr).Str("entry", h.EntryName).Msg("ClearSuperseded: failed to apply supersession")
			result.StillBroken++
			return nil
		}
		result.ClearedFiles += len(res.superseded)
		if cleared {
			result.ClearedEntries++
		} else {
			result.StillBroken++
		}
		return nil
	})

	r.logger.Info().
		Int("checked", result.Checked).
		Int("cleared_entries", result.ClearedEntries).
		Int("cleared_files", result.ClearedFiles).
		Int("still_broken", result.StillBroken).
		Msg("Repair: supersession sweep completed")
	return result, nil
}
