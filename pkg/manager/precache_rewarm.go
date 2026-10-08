package manager

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

const (
	// precacheRewarmInterval is how often rewarmLoop asks Sonarr whether a
	// re-grabbed episode's replacement has been imported yet.
	precacheRewarmInterval = 2 * time.Minute

	// precacheRewarmWindow is how long a re-grabbed episode waits for its
	// replacement before it is forgotten.
	precacheRewarmWindow = 24 * time.Hour
)

// rewarmTarget is a pre-cached episode that was re-grabbed: once Sonarr
// serves it from a different entry than oldInfoHash, that entry is burst
// into the cache like any walked episode. Before, the walk had moved on and
// the replacement stayed cold until played.
type rewarmTarget struct {
	ref         walkIdentity // episodeNumber is the re-grabbed episode
	oldInfoHash string
	oldKey      string // its readiness row, dropped once the replacement is warmed
	until       time.Time
}

func rewarmKey(ref walkIdentity) string {
	return fmt.Sprintf("%s|%d|%d|%d", ref.arr.Name, ref.seriesId, ref.seasonNumber, ref.episodeNumber)
}

// addRewarm records ref, re-grabbed away from oldInfoHash, for rewarmLoop.
func (p *Precache) addRewarm(ref walkIdentity, oldInfoHash, oldKey string) {
	if ref.arr == nil {
		return
	}
	p.rewarmMu.Lock()
	p.rewarm[rewarmKey(ref)] = &rewarmTarget{ref: ref, oldInfoHash: oldInfoHash, oldKey: oldKey, until: time.Now().Add(precacheRewarmWindow)}
	p.rewarmMu.Unlock()
}

// rewarmLoop runs rewarmDue, and retries queued watched-episode evictions
// (evictWatchedDue), every precacheRewarmInterval until Stop.
func (p *Precache) rewarmLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(precacheRewarmInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.rewarmDue()
			p.evictWatchedDue()
		}
	}
}

// rewarmDue checks each re-grabbed episode once: expired ones are
// forgotten; one Sonarr now serves from a new entry is burst like a walked
// episode, and its old readiness row dropped.
func (p *Precache) rewarmDue() {
	now := time.Now()
	p.rewarmMu.Lock()
	targets := make([]*rewarmTarget, 0, len(p.rewarm))
	for k, t := range p.rewarm {
		if now.After(t.until) {
			delete(p.rewarm, k)
			continue
		}
		targets = append(targets, t)
	}
	p.rewarmMu.Unlock()
	if len(targets) == 0 || !config.Get().Repair.PrecacheReadAheadEnabled() || p.Paused() {
		return
	}

	base := p.baseCtx()
	for _, t := range targets {
		if base.Err() != nil {
			return
		}
		lookupCtx, cancel := context.WithTimeout(base, precacheWalkResolveTimeout)
		next, found, err := p.lookupEpisode(lookupCtx, t.ref)
		cancel()
		if err != nil || !found || !next.HasFile {
			continue // not imported yet (or Sonarr unreachable); ask again next tick
		}
		entry, _, ok := p.resolveEpisodeEntry(next)
		if !ok || entry.InfoHash == t.oldInfoHash {
			continue // Sonarr still serves the old grab
		}

		p.logger.Info().Str("series", t.ref.seriesName).Int("season", t.ref.seasonNumber).
			Int("episode", t.ref.episodeNumber).Str("entry", entry.Name).
			Msg("next-episode pre-cache: re-grabbed episode imported; warming its replacement")
		step := p.burstEpisode(base, t.ref, next) // limits the burst itself
		if step == stepDeferred {
			continue // budget or bandwidth said not now; try again next tick
		}
		// Burst, or nothing left to do (already warmed by a walk, paused).
		p.rewarmMu.Lock()
		delete(p.rewarm, rewarmKey(t.ref))
		p.rewarmMu.Unlock()
		p.dropReadiness(t.oldKey)
	}
}

// lookupEpisode fetches ref's episode from Sonarr. NextEpisode returns the
// episode after the one it is given.
func (p *Precache) lookupEpisode(ctx context.Context, ref walkIdentity) (arr.NextEpisodeInfo, bool, error) {
	if p.rewarmLookup != nil {
		return p.rewarmLookup(ctx, ref)
	}
	return ref.arr.NextEpisode(ctx, ref.seriesId, ref.seasonNumber, ref.episodeNumber-1)
}

func (p *Precache) resolveEpisodeEntry(next arr.NextEpisodeInfo) (*storage.Entry, string, bool) {
	if p.rewarmResolve != nil {
		return p.rewarmResolve(next)
	}
	return p.episodeEntry(next)
}

func (p *Precache) burstEpisode(ctx context.Context, ref walkIdentity, next arr.NextEpisodeInfo) episodeStep {
	if p.rewarmBurst != nil {
		return p.rewarmBurst(ctx, ref, next)
	}
	return p.precacheEpisodeFile(ctx, ref, next)
}

// episodeEntry resolves next's library path back to the usenet decypharr
// entry it links to (the same symlink-target trick resolveSonarrEpisode
// uses). ok=false when it is not a decypharr symlink or not usenet-backed
// (overlay/repair is usenet-only).
func (p *Precache) episodeEntry(next arr.NextEpisodeInfo) (*storage.Entry, string, bool) {
	target := readSymlinkTarget(next.Path)
	if target == "" {
		p.logger.Debug().Str("file", next.Path).Msg("next-episode precache: next episode is not a local symlink")
		return nil, "", false
	}
	dir, filename := filepath.Split(target)
	entryName := filepath.Clean(filepath.Base(filepath.Clean(dir)))

	entry, err := p.manager.GetEntryByName(entryName, filename)
	if err != nil || entry == nil || entry.Protocol != config.ProtocolNZB {
		return nil, "", false
	}
	return entry, filename, true
}

// OnPar2Repaired settles the readiness rows of nzbID's still-damaged files
// after a PAR2 pass completes. Before, a pass that landed after
// awaitReadiness stopped waiting left the row "still damaged" and the
// repaired segments out of the durable cache until restart. Rows whose file
// is now clean are marked repaired and their segments persisted; the rest
// get the current count.
func (p *Precache) OnPar2Repaired(nzbID string) {
	if p == nil || nzbID == "" || p.manager.usenet == nil {
		return
	}
	p.readinessMu.Lock()
	var rows []EpisodeReadiness
	for _, r := range p.readiness {
		if r.InfoHash == nzbID && !r.Clean && r.SegmentsPending > 0 {
			rows = append(rows, r)
		}
	}
	p.readinessMu.Unlock()
	if len(rows) == 0 {
		return
	}
	entry, err := p.manager.GetEntry(nzbID)
	if err != nil || entry == nil {
		return
	}

	for _, r := range rows {
		c, ok := p.overlayPendingCount(entry, r.Filename)
		if !ok {
			continue
		}
		if c > 0 {
			r.SegmentsPending = c
			p.storeReadiness(r)
			continue
		}
		r.SegmentsRepaired += r.SegmentsPending
		r.SegmentsPending = 0
		r.ReadyAt = time.Now()
		p.storeReadiness(r)
		p.notifyReadiness(entry, r)

		var size int64
		if f, ok := entry.Files[r.Filename]; ok && f != nil {
			size = f.Size
		}
		if size <= 0 {
			continue
		}
		key := r.InfoHash + ":" + r.Filename
		p.markInflight(key)
		go func(filename string) {
			defer p.unmarkInflight(key)
			p.persistRepaired(usenet.ContextForBurstDownload(p.baseCtx()), entry, filename, size)
		}(r.Filename)
	}
}

// refreshReadinessDamage brings a still-damaged row's pending count up to
// date from the overlay; the row used to keep whatever the wait last saw
// until restart. Returns false when the row's entry no longer exists (a
// re-grab replaced it), so the caller drops the row.
func (p *Precache) refreshReadinessDamage(r *EpisodeReadiness) bool {
	if !p.entryExists(r.InfoHash) {
		return false
	}
	if r.Clean || r.SegmentsPending <= 0 || p.manager.usenet == nil {
		return true
	}
	pending, err := p.manager.usenet.OverlayPendingRepair(r.InfoHash)
	if err != nil {
		return true
	}
	if c := len(pending[r.Filename]); c < r.SegmentsPending {
		r.SegmentsRepaired += r.SegmentsPending - c
		r.SegmentsPending = c
	}
	return true
}

// entryExists reports whether infoHash is still a stored entry. True when
// it can't tell (no storage, lookup error): only a definite "gone" counts.
func (p *Precache) entryExists(infoHash string) bool {
	if p.manager.storage == nil {
		return true
	}
	exists, err := p.manager.EntryExists(infoHash)
	return err != nil || exists
}

// persistRepaired persists filename's segments once a repair has patched
// them, but only after invalidateRepaired's second forget of those ranges,
// which would otherwise drop what this writes. ctx should not zero-fill.
func (p *Precache) persistRepaired(ctx context.Context, entry *storage.Entry, filename string, fileSize int64) {
	select {
	case <-time.After(repairedRangeReforgetDelay + 5*time.Second):
	case <-ctx.Done():
		return
	}
	p.persistCleanRanges(ctx, entry, filename, fileSize)
}
