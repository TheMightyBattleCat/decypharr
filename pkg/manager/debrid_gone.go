package manager

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Reasons for a debrid torrent entry that can no longer be served. Unlike the
// import faults (keepReleaseReason), the release itself is unusable here, so
// the re-grab blocklists it and the Arr picks another release - often Usenet.
const (
	// reasonDebridNotConfigured: none of the entry's debrids is configured
	// any more (a production install: 744 TorBox, 5 DebridLink, 1 AllDebrid entries).
	// Every read fails with client_not_found and nothing re-inserts it.
	reasonDebridNotConfigured = "debrid_not_configured"
	// reasonDebridUnavailable: the link service gave up on the entry after
	// its re-insertions failed (entry.Bad) - not cached, or removed.
	reasonDebridUnavailable = "debrid_unavailable"
)

const (
	// defaultDebridGoneLimit caps how many entries one bulk fix re-grabs:
	// each is an Arr search, and indexers rate-limit.
	defaultDebridGoneLimit = 50
	// defaultDebridGoneDeleteLimit caps how many orphans one bulk fix
	// deletes. A delete is a store write, not an Arr search.
	defaultDebridGoneDeleteLimit = 200
)

// debridGoneRunID holds the repair run slot while a bulk fix deletes and
// marks entries, so a sweep does not probe them meanwhile. Not a real run.
const debridGoneRunID = "debrid-gone"

// noReinsertReason reports whether re-inserting a broken torrent cannot help:
// there is no configured debrid to insert into, or re-insertion already
// failed.
func noReinsertReason(reason string) bool {
	return reason == reasonDebridNotConfigured || reason == reasonDebridUnavailable
}

// debridGoneReason says why torrent entry e can no longer be served, or ""
// when it can. configured reports whether a debrid has a client. An entry
// with any placement on a configured debrid is left to re-insertion, which
// moves it there.
func debridGoneReason(e *storage.Entry, configured func(string) bool) string {
	if e == nil || e.IsNZB() {
		return ""
	}
	if e.Bad {
		return reasonDebridUnavailable
	}
	if e.ActiveProvider != "" && configured(e.ActiveProvider) {
		return ""
	}
	for name := range e.Providers {
		if configured(name) {
			return ""
		}
	}
	return reasonDebridNotConfigured
}

// debridGoneRefs is what the Arrs point at (entry folder -> file -> InfoHash
// serving it, as buildArrReferencedSet), split by whether repair may act
// through the Arr.
type debridGoneRefs struct {
	// repair: Arrs repair re-grabs through (eligibleArrs).
	repair map[string]map[string]string
	// skipRepair: Arrs with skip_repair. Their files are in use, but Fix
	// broken can neither see nor re-grab them.
	skipRepair map[string]map[string]string
}

// What a bulk fix does with an unservable entry.
const (
	debridGoneRegrab = "regrab" // a repair Arr points at it: re-grab
	debridGoneOrphan = "orphan" // no Arr points at it: delete
	debridGoneKeep   = "keep"   // in use, but not re-grabbable from here
)

// debridGoneAction decides what to do with unservable entry e, filed under
// folder name. Only this entry's own InfoHash counts as use: a slot served by
// a same-named replacement belongs to the replacement. A slot whose InfoHash
// is unknown proves nothing either way, so the entry is kept.
func debridGoneAction(e *storage.Entry, name string, refs debridGoneRefs) string {
	// uses reports whether set points at fname through this entry (mine) or
	// through a slot of unknown InfoHash.
	uses := func(set map[string]map[string]string, fname string) (mine, unknown bool) {
		hash, ok := set[name][fname]
		return ok && hash == e.InfoHash, ok && hash == ""
	}
	usedByRepair, usedElsewhere := false, false
	for fname, f := range e.Files {
		if f == nil || f.Deleted {
			continue
		}
		mine, unknown := uses(refs.repair, fname)
		otherMine, otherUnknown := uses(refs.skipRepair, fname)
		usedByRepair = usedByRepair || mine
		usedElsewhere = usedElsewhere || unknown || otherMine || otherUnknown
	}
	switch {
	case usedByRepair:
		return debridGoneRegrab
	case usedElsewhere:
		return debridGoneKeep
	}
	return debridGoneOrphan
}

// DebridGoneEntry is one torrent entry the bulk pass found unservable.
type DebridGoneEntry struct {
	Name     string `json:"name"`
	InfoHash string `json:"info_hash"`
	Provider string `json:"provider"`
	Reason   string `json:"reason"`
	Files    int    `json:"files"`
}

// DebridGoneResult is what FindDebridGone and FixDebridGone report. Total is
// Orphaned + Kept + AlreadyBroken + Regrab.
type DebridGoneResult struct {
	Total    int            `json:"total"`
	ByReason map[string]int `json:"by_reason"`
	// Orphaned entries are pointed at by no Arr: the Arr moved to another
	// release or dropped the title. Deleting them loses nothing playable.
	Orphaned int `json:"orphaned"`
	// Kept entries are pointed at only by a skip_repair Arr, or through a
	// file slot with no InfoHash. Neither deleted nor re-grabbed.
	Kept int `json:"kept"`
	// AlreadyBroken entries were marked by an earlier batch and wait for Fix
	// broken; Regrab counts those still to mark.
	AlreadyBroken int `json:"already_broken"`
	Regrab        int `json:"regrab"`
	// Invalid rows have no InfoHash, so they can be neither deleted nor
	// re-grabbed. Not in Total.
	Invalid int `json:"invalid"`
	// Entries are the re-grab candidates, Orphans the delete candidates; the
	// API caps both.
	Entries []DebridGoneEntry `json:"entries"`
	Orphans []DebridGoneEntry `json:"orphans"`
	// Set by FixDebridGone.
	Deleted      int                `json:"deleted"`
	DeleteFailed int                `json:"delete_failed"`
	Marked       int                `json:"marked"`
	Run          *storage.RepairRun `json:"run,omitempty"`
	// Error is why the re-grab did not start after deletes went through.
	Error string `json:"error,omitempty"`
}

// DebridGoneFixOptions bounds one FixDebridGone batch.
type DebridGoneFixOptions struct {
	// Limit caps re-grabs (default defaultDebridGoneLimit).
	Limit int
	// Delete must be set for orphans to be deleted at all.
	Delete bool
	// DeleteLimit caps deletes (default defaultDebridGoneDeleteLimit).
	DeleteLimit int
}

func (r *Repair) debridConfigured(name string) bool {
	return name != "" && r.manager.clients != nil && r.manager.ProviderClient(name) != nil
}

// buildDebridGoneRefs lists every Arr's library once. Arrs with skip_repair
// are listed too: skip_repair keeps repair from acting through an Arr, it
// does not make the Arr's files unused. Any Arr that cannot be listed fails
// the whole build, so a missing library never reads as "nothing uses this".
func (r *Repair) buildDebridGoneRefs(ctx context.Context) (debridGoneRefs, error) {
	repairArrs := r.eligibleArrs(nil)
	var skipArrs []*arr.Arr
	for _, a := range r.manager.arr.GetAll() {
		if a != nil && a.Host != "" && a.Token != "" && a.SkipRepair {
			skipArrs = append(skipArrs, a)
		}
	}
	refs := debridGoneRefs{skipRepair: map[string]map[string]string{}}
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() (err error) {
		refs.repair, err = r.buildArrReferencedSetFor(gctx, repairArrs)
		return err
	})
	if len(skipArrs) > 0 {
		g.Go(func() (err error) {
			refs.skipRepair, err = r.buildArrReferencedSetFor(gctx, skipArrs)
			return err
		})
	}
	return refs, g.Wait()
}

// FindDebridGone lists torrent entries that can no longer be served and what
// a bulk fix would do with each. No debrid API call and no file read, but
// every Arr's library is listed (about a minute on a production install).
func (r *Repair) FindDebridGone(ctx context.Context) (DebridGoneResult, error) {
	refs, err := r.buildDebridGoneRefs(ctx)
	if err != nil {
		return DebridGoneResult{ByReason: map[string]int{}}, fmt.Errorf("could not list the Arrs' libraries: %w", err)
	}
	return r.findDebridGone(refs)
}

// findDebridGone classifies stored entries against refs. Sorted by name.
func (r *Repair) findDebridGone(refs debridGoneRefs) (DebridGoneResult, error) {
	res := DebridGoneResult{ByReason: map[string]int{}}
	seen := map[string]bool{}
	err := r.manager.storage.ForEachBatch(500, func(batch []*storage.Entry) error {
		for _, e := range batch {
			reason := debridGoneReason(e, r.debridConfigured)
			if reason == "" || seen[e.InfoHash] {
				continue
			}
			if e.InfoHash == "" {
				res.Invalid++
				continue
			}
			seen[e.InfoHash] = true
			name := e.GetFolder()
			files := 0
			for _, f := range e.Files {
				if f != nil && !f.Deleted {
					files++
				}
			}
			res.ByReason[reason]++
			g := DebridGoneEntry{Name: name, InfoHash: e.InfoHash, Provider: e.ActiveProvider, Reason: reason, Files: files}
			switch debridGoneAction(e, name, refs) {
			case debridGoneOrphan:
				res.Orphaned++
				res.Orphans = append(res.Orphans, g)
			case debridGoneKeep:
				res.Kept++
			default:
				if h, _ := r.manager.storage.GetEntryHealth(name); h != nil && h.Status == storage.HealthBroken && h.FailureReason == reason {
					res.AlreadyBroken++
					continue
				}
				res.Regrab++
				res.Entries = append(res.Entries, g)
			}
		}
		return nil
	})
	byName := func(a, b DebridGoneEntry) int { return strings.Compare(a.Name, b.Name) }
	slices.SortFunc(res.Entries, byName)
	slices.SortFunc(res.Orphans, byName)
	res.Total = res.Orphaned + res.Kept + res.AlreadyBroken + res.Regrab
	return res, err
}

// FixDebridGone runs one bulk batch: with opts.Delete it deletes up to
// DeleteLimit orphans (no Arr points at them; their debrid placements are
// left alone), then marks up to Limit entries a repair Arr still points at
// broken and runs Fix broken on them: delete, blocklist and re-search.
// Entries it cannot re-grab stay marked broken for a later Fix.
func (r *Repair) FixDebridGone(ctx context.Context, opts DebridGoneFixOptions) (DebridGoneResult, error) {
	if ctx == nil {
		ctx = r.parentCtx
	}
	// Fail before the Arr listing rather than a minute into it.
	r.mu.Lock()
	id := r.activeRunID
	r.mu.Unlock()
	if id != "" {
		return DebridGoneResult{ByReason: map[string]int{}}, fmt.Errorf("repair already running (run %s)", id)
	}
	refs, err := r.buildDebridGoneRefs(ctx)
	if err != nil {
		return DebridGoneResult{ByReason: map[string]int{}}, fmt.Errorf("could not list the Arrs' libraries: %w", err)
	}
	return r.fixDebridGone(ctx, refs, opts)
}

func (r *Repair) fixDebridGone(ctx context.Context, refs debridGoneRefs, opts DebridGoneFixOptions) (DebridGoneResult, error) {
	if opts.Limit <= 0 {
		opts.Limit = defaultDebridGoneLimit
	}
	if opts.DeleteLimit <= 0 {
		opts.DeleteLimit = defaultDebridGoneDeleteLimit
	}

	r.mu.Lock()
	if r.activeRunID != "" {
		id := r.activeRunID
		r.mu.Unlock()
		return DebridGoneResult{ByReason: map[string]int{}}, fmt.Errorf("repair already running (run %s)", id)
	}
	r.activeRunID = debridGoneRunID
	r.mu.Unlock()
	names, found, err := r.deleteAndMarkDebridGone(refs, opts)
	r.mu.Lock()
	if r.activeRunID == debridGoneRunID {
		r.activeRunID = ""
	}
	r.mu.Unlock()
	if err != nil {
		return found, err
	}

	if len(names) == 0 {
		if found.Deleted == 0 {
			return found, errors.New("no unservable debrid entries to delete or re-grab")
		}
		return found, nil
	}
	run, err := r.FixBroken(ctx, names)
	found.Run = run
	return found, err
}

// deleteAndMarkDebridGone does the store writes of one batch under the run
// slot and returns the names marked broken.
func (r *Repair) deleteAndMarkDebridGone(refs debridGoneRefs, opts DebridGoneFixOptions) ([]string, DebridGoneResult, error) {
	found, err := r.findDebridGone(refs)
	if err != nil {
		return nil, found, err
	}
	found.Entries = found.Entries[:min(opts.Limit, len(found.Entries))]
	if !opts.Delete {
		found.Orphans = nil
	}
	found.Orphans = found.Orphans[:min(opts.DeleteLimit, len(found.Orphans))]

	for _, g := range found.Orphans {
		if err := r.manager.deleteEntry(g.InfoHash, false); err != nil {
			found.DeleteFailed++
			r.logger.Warn().Err(err).Str("entry", g.Name).Str("infohash", g.InfoHash).Msg("Debrid-gone: failed to delete orphaned entry")
			continue
		}
		found.Deleted++
		r.logger.Info().Str("entry", g.Name).Str("infohash", g.InfoHash).Str("reason", g.Reason).
			Msg("Debrid-gone: deleted entry no Arr points at")
	}
	if found.Deleted > 0 {
		r.manager.InvalidateEntryCache()
		go func() {
			if err := r.manager.RefreshMount(); err != nil {
				r.logger.Error().Err(err).Msg("Mount refresh after entry deletion failed")
			}
		}()
	}

	names := make([]string, 0, len(found.Entries))
	for _, g := range found.Entries {
		entry, err := r.manager.GetEntry(g.InfoHash)
		if err != nil || entry == nil {
			continue
		}
		if h := debridGoneHealth(r.manager.storage, entry, g); h != nil {
			r.saveHealth(h)
			names = append(names, g.Name)
		}
	}
	found.Marked = len(names)
	r.logger.Info().Int("deleted", found.Deleted).Int("delete_failed", found.DeleteFailed).
		Int("marked", found.Marked).Int("orphaned", found.Orphaned).Int("regrab", found.Regrab).
		Int("kept", found.Kept).Msg("Debrid-gone: bulk batch")
	return names, found, nil
}

// debridGoneHealth builds the broken health record for an unservable torrent
// entry, keeping what the store already knows about it.
func debridGoneHealth(s *storage.Storage, entry *storage.Entry, g DebridGoneEntry) *storage.EntryHealth {
	h, _ := s.GetEntryHealth(g.Name)
	if h == nil {
		h = &storage.EntryHealth{EntryName: g.Name}
	}
	now := time.Now()
	h.BrokenFiles = h.BrokenFiles[:0]
	for name, f := range entry.Files {
		if f == nil || f.Deleted {
			continue
		}
		h.BrokenFiles = append(h.BrokenFiles, storage.BrokenFile{
			EntryName: g.Name,
			FileName:  name,
			InfoHash:  entry.InfoHash,
			Protocol:  config.ProtocolTorrent,
			Reason:    g.Reason,
			Size:      f.Size,
		})
	}
	if len(h.BrokenFiles) == 0 {
		return nil
	}
	slices.SortFunc(h.BrokenFiles, func(a, b storage.BrokenFile) int { return strings.Compare(a.FileName, b.FileName) })
	h.Status = storage.HealthBroken
	h.Protocol = config.ProtocolTorrent
	h.FailureReason = g.Reason
	h.FileCount = len(h.BrokenFiles)
	h.BrokenCount = len(h.BrokenFiles)
	h.LastFailedAt = now
	h.LastCheckedAt = now
	return h
}
