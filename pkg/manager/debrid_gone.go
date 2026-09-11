package manager

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
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

// defaultDebridGoneLimit caps how many entries one bulk fix re-grabs: each
// is an Arr search, and indexers rate-limit.
const defaultDebridGoneLimit = 50

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

// DebridGoneEntry is one torrent entry the bulk pass found unservable.
type DebridGoneEntry struct {
	Name     string `json:"name"`
	InfoHash string `json:"info_hash"`
	Provider string `json:"provider"`
	Reason   string `json:"reason"`
	Files    int    `json:"files"`
}

// DebridGoneResult is what FindDebridGone and FixDebridGone report.
type DebridGoneResult struct {
	Total    int            `json:"total"`
	ByReason map[string]int `json:"by_reason"`
	// Superseded entries have every file served by another copy already (the
	// Arr re-grabbed it); AlreadyBroken ones were marked by an earlier batch
	// and wait for Fix broken. Neither is in Entries, so batches progress.
	Superseded    int                `json:"superseded"`
	AlreadyBroken int                `json:"already_broken"`
	Entries       []DebridGoneEntry  `json:"entries"`
	Run           *storage.RepairRun `json:"run,omitempty"`
}

func (r *Repair) debridConfigured(name string) bool {
	return name != "" && r.manager.clients != nil && r.manager.ProviderClient(name) != nil
}

// FindDebridGone lists torrent entries that can no longer be served, from
// stored entries alone: no debrid API call, no file read. Sorted by name.
func (r *Repair) FindDebridGone() (DebridGoneResult, error) {
	res := DebridGoneResult{ByReason: map[string]int{}}
	seen := map[string]bool{}
	err := r.manager.storage.ForEachBatch(500, func(batch []*storage.Entry) error {
		for _, e := range batch {
			reason := debridGoneReason(e, r.debridConfigured)
			if reason == "" || seen[e.InfoHash] {
				continue
			}
			seen[e.InfoHash] = true
			name := e.GetFolder()
			files, served := 0, 0
			item, _ := r.manager.GetEntryItem(name)
			for fname, f := range e.Files {
				if f == nil || f.Deleted {
					continue
				}
				files++
				if item != nil {
					if cur := item.Files[fname]; cur != nil && cur.InfoHash == e.InfoHash {
						served++
					}
				}
			}
			res.ByReason[reason]++
			if item != nil && files > 0 && served == 0 {
				res.Superseded++
				continue
			}
			if h, _ := r.manager.storage.GetEntryHealth(name); h != nil && h.Status == storage.HealthBroken && h.FailureReason == reason {
				res.AlreadyBroken++
				continue
			}
			res.Entries = append(res.Entries, DebridGoneEntry{
				Name: name, InfoHash: e.InfoHash, Provider: e.ActiveProvider, Reason: reason, Files: files,
			})
		}
		return nil
	})
	slices.SortFunc(res.Entries, func(a, b DebridGoneEntry) int { return strings.Compare(a.Name, b.Name) })
	res.Total = res.Superseded + res.AlreadyBroken + len(res.Entries)
	return res, err
}

// FixDebridGone marks up to limit unservable torrent entries broken and runs
// Fix broken on them: each eligible Arr's library is listed once, files the
// Arr already replaced are cleared, and the rest are deleted, blocklisted and
// re-searched. Entries it cannot re-grab stay marked broken for a later Fix.
func (r *Repair) FixDebridGone(ctx context.Context, limit int) (DebridGoneResult, error) {
	found, err := r.FindDebridGone()
	if err != nil {
		return found, err
	}
	if limit <= 0 {
		limit = defaultDebridGoneLimit
	}
	found.Entries = found.Entries[:min(limit, len(found.Entries))]
	if len(found.Entries) == 0 {
		return found, errors.New("no unservable debrid entries")
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
	if len(names) == 0 {
		return found, errors.New("no unservable debrid entries could be marked broken")
	}
	run, err := r.FixBroken(ctx, names)
	found.Run = run
	return found, err
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
	h.FileCount = len(h.BrokenFiles)
	h.BrokenCount = len(h.BrokenFiles)
	h.FailureReason = g.Reason
	h.LastFailedAt = now
	h.LastCheckedAt = now
	return h
}
