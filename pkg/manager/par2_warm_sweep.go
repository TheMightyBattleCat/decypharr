// The warm sweep repair path lets the repair sweep try a PAR2 pass ahead of
// its normal delete + blocklist + re-search heal, but ONLY when that pass is
// provably free: every byte the streaming solve would otherwise fetch over
// NNTP for intact data is already sitting in the local DFS cache right now.
// Recovery data and the PAR2 index itself are still fetched fresh (STAT-ed
// alive first) - there is no cache for parity data, it doesn't correspond to
// any file content - but that's a small, bounded cost compared to the
// bandwidth (and Arr history churn) a full re-grab spends. Anything short of
// a clean, fully-warm, fully-completed pass changes nothing: the caller's
// existing regrab handling runs exactly as it did before this file existed.
package manager

import (
	"context"
	"errors"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// warmSweepRepair tries attemptWarmSweepRepair once per distinct InfoHash
// among broken, returning the InfoHash values whose pass actually completed.
// PAR2 resolves an nzbID's ENTIRE pending overlay set in one pass (see
// Par2Repair.runJob), so every broken file sharing a fixed InfoHash had its
// damage resolved together in that same pass - the caller can safely treat
// all of them as no longer broken, not just whichever one happened to
// trigger the attempt.
func (r *Repair) warmSweepRepair(ctx context.Context, broken []storage.BrokenFile) map[string]struct{} {
	fixed := make(map[string]struct{})
	if r.manager.par2Repair == nil {
		return fixed
	}
	tried := make(map[string]struct{})
	for _, bf := range broken {
		if bf.InfoHash == "" {
			continue
		}
		if _, done := tried[bf.InfoHash]; done {
			continue
		}
		tried[bf.InfoHash] = struct{}{}
		if ctx != nil && ctx.Err() != nil {
			return fixed
		}
		if r.manager.par2Repair.attemptWarmSweepRepair(ctx, bf.InfoHash) {
			fixed[bf.InfoHash] = struct{}{}
		}
	}
	return fixed
}

const (
	// par2WarmSweepPollInterval paces the StartedAt-guarded poll for RunNow's
	// job to reach a terminal phase.
	par2WarmSweepPollInterval = 1 * time.Second
)

// errWarmSweepNoFetch is returned by the stub articleFetchFunc handed to
// every postedFileFetcher this preflight builds. It should never actually
// reach the network - readCached (via cacheSlicedSource) is always tried
// first - so seeing it at all means a slice was NOT fully cache-resident,
// which is exactly what the 100%-warm gate below treats as "not warm."
var errWarmSweepNoFetch = errors.New("warm sweep preflight: article fetch disabled, slice not cache-resident")

// attemptWarmSweepRepair tries to resolve nzbID's full pending overlay
// damage in one PAR2 pass sourced entirely from local cache for intact data,
// returning true only when that pass actually reached Par2PhaseCompleted.
// Every other outcome (PAR2 disabled/manual mode, no pending damage,
// recovery data not confirmed alive, index fetch/parse failure, anything
// less than 100% of the intact slices already cache-resident, RunNow
// rejecting the request, or the poll below timing out) returns false and
// leaves everything - including a job RunNow may have started, which keeps
// running independently - exactly as if this function had never been
// called.
func (p *Par2Repair) attemptWarmSweepRepair(ctx context.Context, nzbID string) (passed bool) {
	if p == nil || nzbID == "" || p.manager.usenet == nil {
		return false
	}
	// stage names the check a declined pass stopped at: every exit below used
	// to be silent, so nothing showed the warm pass was considered at all.
	stage := "config"
	defer func() {
		if !passed {
			p.logger.Debug().Str("entry", nzbID).Str("stage", stage).Msg("par2 warm sweep pass declined; the sweep re-grabs")
		}
	}()
	if ctx == nil {
		ctx = context.Background()
	}
	cfg := config.Get().Repair
	if !cfg.Par2RepairEnabled() {
		return false
	}
	// This is triggered by the sweep, not an explicit user action - honor
	// the same automatic-trigger policy AutoEnqueue does, even though the
	// mechanism below (RunNow) is the manual-bypass one. RunNow is used only
	// to skip the BATCH lane's off-peak Schedule/StopSchedule window - a
	// cache-only pass has none of the bandwidth-heavy-hours reasoning that
	// window exists for - not to bypass the user's own auto-repair mode
	// preference.
	if cfg.Par2RepairMode == config.Par2RepairModeManual {
		return false
	}

	u := p.manager.usenet

	stage = "entry lookup"
	entry, err := p.manager.GetEntry(nzbID)
	if err != nil || entry == nil {
		return false
	}
	// Resolved independently from GetEntry, NOT taken from the caller's
	// EntryHealth.EntryName - a deleted/superseded grab and its same-named
	// replacement share an EntryName but not an nzbID, and PeekCachedRange
	// below is keyed by entryName against the CURRENT mount's cache. Using
	// any other source for this string risks peeking a stale or unrelated
	// cache item.
	entryName := entry.Name

	stage = "no PAR2 data in the NZB record"
	nzb, err := u.GetNZB(nzbID)
	if err != nil || len(nzb.Par2Source) == 0 || len(nzb.Par2Files) == 0 {
		return false
	}

	stage = "no pending damage"
	pending, err := u.OverlayPendingRepair(nzbID)
	if err != nil || len(pending) == 0 {
		return false
	}
	// No par2_repair_min_segments gate here. The threshold saves bandwidth
	// on automatic passes; this one reads intact data from the local cache,
	// and the alternative when it declines is the sweep's full re-grab and
	// blocklist - so turning away the smallest damage cost more, not less.

	preflightCtx, cancel := context.WithTimeout(ctx, par2ArticleFetchTimeout*4)
	defer cancel()

	stage = "PAR2 index fetch/parse"
	allVols, indexFiles := censusPar2Volumes(nzb.Par2Files)
	if len(indexFiles) == 0 {
		return false
	}
	var sources []par2.Source
	for _, f := range indexFiles {
		data, ferr := fetchWholePar2File(preflightCtx, u.FetchArticle, f)
		if ferr != nil {
			return false
		}
		sources = append(sources, par2.Source{Name: f.Name, Data: data})
	}
	if len(sources) == 0 {
		return false
	}
	setChoice := choosePar2Set(sources, allVols, nzb.Par2Source, pending)
	idx, err := par2.ParseIndexSet(sources, setChoice.setID)
	if err != nil {
		return false
	}

	stage = "posted-file match"
	posted := make([]par2.PostedFile, len(nzb.Par2Source))
	for i, f := range nzb.Par2Source {
		f := f
		posted[i] = par2.PostedFile{
			Name:   f.Name,
			Length: f.Size,
			MD5_16k: func() ([16]byte, error) {
				return computeMD5_16k(preflightCtx, u.FetchArticleChecked, f)
			},
		}
	}
	matches, ok := par2MatchFromCache(idx, nzb.Par2Source, nzb.Par2Match)
	if !ok {
		var err error
		matches, _, err = par2.MatchFiles(idx, posted)
		if err != nil {
			return false
		}
	}
	if len(matches) == 0 {
		return false
	}

	stage = "no mount cache"
	var cacheReader dfsCacheRangeReader
	if mgr := p.manager.MountManager(); mgr != nil {
		cacheReader, _ = mgr.(dfsCacheRangeReader)
	}
	if cacheReader == nil {
		// No DFS mount cache to peek at all - nothing here can ever be
		// "warm," so there is nothing this preflight can prove.
		return false
	}
	cacheSource := &cacheSlicedSource{
		reader:      cacheReader,
		entryName:   entryName,
		byMessageID: buildCacheSegmentMap(nzb),
		deadRanges:  buildDeadOutputRanges(nzb, pending),
		// cacheBytes/progress deliberately nil: this is a read-only peek,
		// not real job progress - see cacheSlicedSource.readCached's nil
		// handling of both fields.
	}

	// noFetch never actually fetches; ReadRange below only reaches it on a
	// cache miss (see postedFileFetcher.ReadRange), at which point the
	// slice is - by definition - not fully cache-resident, and this whole
	// preflight must fail. Using the exact same ReadRange the real repair
	// pass uses means the padding/clamp behavior at a file's tail (see its
	// doc comment) is mirrored exactly, not reimplemented.
	noFetch := func(_ context.Context, _ string) ([]byte, error) {
		return nil, errWarmSweepNoFetch
	}
	fetchers := make(map[[16]byte]*postedFileFetcher, len(matches))
	msgIDRange := make(map[string]postedRange)
	for _, m := range matches {
		file := nzb.Par2Source[m.PostedIndex]
		f := newPostedFileFetcher(preflightCtx, uncheckedPosted(noFetch), file, cacheSource, idx.Files[m.FileID].Length, p.logger)
		fetchers[m.FileID] = f
		for i, seg := range file.Segments {
			msgIDRange[seg.MessageID] = postedRange{fileID: m.FileID, start: f.base[i], end: f.base[i] + f.segSizes[i]}
		}
	}

	// Map every pending dead segment to the damaged slice set, exactly as
	// runRepair does - anything not mappable means this preflight can't
	// reason about the entry's real damage, so it must not proceed.
	stage = "damage not mappable to slices, or over the repair cap"
	damagedSet := make(map[int64]struct{})
	for _, segs := range pending {
		for _, seg := range segs {
			rng, ok := msgIDRange[seg.MessageID]
			if !ok {
				return false
			}
			slices, derr := idx.DamagedSlices(rng.fileID, rng.start, rng.end)
			if derr != nil {
				return false
			}
			for _, s := range slices {
				damagedSet[s] = struct{}{}
			}
		}
	}
	if len(damagedSet) == 0 || len(damagedSet) > par2.MaxRepairSlicesFor(idx.SliceSize) {
		return false
	}

	// Enough live recovery data for this damage: the recovery slices of
	// volumes whose every article STATs present must cover the damaged
	// slices. This used to require every article of every retained PAR2
	// file alive, so one expired article in a volume the pass would never
	// read disabled the pass and sent the file to a re-grab.
	stage = "not enough live recovery data"
	alive, aerr := aliveRecoverySlices(preflightCtx, u.StatSegments, setChoice.vols)
	if aerr != nil || alive < uint32(len(damagedSet)) {
		return false
	}

	// The 100% warm gate: every slice par2.Repair's streaming pass would
	// read as "intact" (i.e. every slice NOT in damagedSet) must be fully
	// servable from the local cache alone, clamped to the posted file's
	// real length exactly like ReadRange itself clamps (see its doc
	// comment) - a single miss anywhere fails the whole gate.
	stage = "intact data not fully cached"
	for s := int64(0); s < idx.NumSlices(); s++ {
		if _, damaged := damagedSet[s]; damaged {
			continue
		}
		fileID, local, lerr := idx.SliceLocation(s)
		if lerr != nil {
			return false
		}
		f, ok := fetchers[fileID]
		if !ok {
			return false
		}
		if _, rerr := f.ReadRange(local*idx.SliceSize, idx.SliceSize); rerr != nil {
			return false
		}
	}

	// Every intact slice this pass needs is already on disk, and recovery
	// data was just confirmed alive - safe to actually solve it now.
	//
	// Waits for the pass to end, however long it takes. This used to give
	// up after 5 minutes and return false, and the caller then re-grabbed -
	// deleting the entry while the still-running pass wrote its patches.
	//
	// RunNow claims the entry for the pass, overwriting the sweep's re-grab
	// claim, and the pass releases it. When the pass does not fix the file
	// the sweep re-grabs next, so put that claim back (as the cold path
	// does) rather than re-grab unclaimed.
	var priorKind repairHandlerKind
	var priorLive bool
	if p.repair != nil && p.repair.handlers != nil {
		kind, terminal, exists := p.repair.handlers.State(nzbID)
		priorKind, priorLive = kind, exists && !terminal
	}
	stage = "pass ran but did not complete"
	completed := p.runNowAndWait(ctx, nzbID)
	if !completed && priorLive && priorKind == handlerRegrab && ctx.Err() == nil {
		p.repair.handlers.Set(nzbID, handlerRegrab)
	}
	return completed
}

// aliveRecoverySlices STATs every article of vols and returns the recovery
// slices of the volumes whose articles are all confirmed present. A volume
// with any article gone or unanswered counts for nothing: the gate this
// feeds decides whether to try a pass instead of re-grabbing, and must not
// promise recovery data it has not seen.
func aliveRecoverySlices(ctx context.Context, stat func(context.Context, []string) ([]nntp.StatResult, error), vols []par2Volume) (uint32, error) {
	var ids []string
	for _, v := range vols {
		for _, seg := range v.ref.Segments {
			ids = append(ids, seg.MessageID)
		}
	}
	if len(ids) == 0 {
		return 0, nil
	}
	results, err := stat(ctx, ids)
	if err != nil {
		return 0, err
	}
	present := make(map[string]bool, len(results))
	for _, r := range results {
		if r.Available {
			present[r.MessageID] = true
		}
	}
	var alive uint32
	for _, v := range vols {
		whole := len(v.ref.Segments) > 0
		for _, seg := range v.ref.Segments {
			if !present[seg.MessageID] {
				whole = false
				break
			}
		}
		if whole {
			alive += v.count
		}
	}
	return alive, nil
}
