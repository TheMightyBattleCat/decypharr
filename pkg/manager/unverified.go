package manager

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Why a probe left a healthy file unverified (storage.UnverifiedFile.Reason),
// besides reasonTailTruncated.
const (
	// unverifiedNoSeekIndex: the file has no usable seek index and the bounded
	// head scan reached no verdict. On a production install every such file checked was
	// served short of its Matroska Segment, but this does not prove it.
	unverifiedNoSeekIndex = "no_seek_index"
	// unverifiedTimeout: a probe ran out of time.
	unverifiedTimeout = "decode_timeout"
	// unverifiedReadBudget: the verification's read budget was spent.
	unverifiedReadBudget = "read_budget_spent"
	// unverifiedDecodeErrors: ffprobe printed errors but decoded to the end of
	// its window (ffprobeReasonDecodedThrough).
	unverifiedDecodeErrors = "decoded_with_errors"
	// unverifiedCancelled: the run was stopped mid-check. Says nothing about
	// the file, so it is never recorded.
	unverifiedCancelled = "check_cancelled"
	// unverifiedInconclusive: no verdict, cause not recorded.
	unverifiedInconclusive = "decode_inconclusive"
)

// defaultReplaceUnverifiedLimit caps how many entries one Replace re-grabs.
// Lower than defaultDebridGoneLimit: these files still play, and each is gone
// from the library until its re-grab lands.
const defaultReplaceUnverifiedLimit = 25

// unverifiedCause is where a decode check records why it is about to end
// without a verdict. probeFile puts one on the context it hands the checker.
type unverifiedCause struct{ reason string }

type unverifiedCauseCtxKey struct{}

func contextWithUnverifiedCause(ctx context.Context, c *unverifiedCause) context.Context {
	return context.WithValue(ctx, unverifiedCauseCtxKey{}, c)
}

// noteUnverified records reason on ctx's unverifiedCause, if any. The last note
// wins, so a caller that knows more than the probe it ran (the head scan)
// overrides the probe's note.
func noteUnverified(ctx context.Context, reason string) {
	if c, _ := ctx.Value(unverifiedCauseCtxKey{}).(*unverifiedCause); c != nil {
		c.reason = reason
	}
}

// timeoutCause names a probe context that ended: cancelled when parent did,
// otherwise the probe's own timeout.
func timeoutCause(parent context.Context) string {
	if parent.Err() != nil {
		return unverifiedCancelled
	}
	return unverifiedTimeout
}

// get returns the recorded reason, or unverifiedInconclusive when no site
// recorded one.
func (c *unverifiedCause) get() string {
	if c == nil || c.reason == "" {
		return unverifiedInconclusive
	}
	return c.reason
}

// unverifiedFiles lists the healthy files in results that the probe could not
// verify. A cancelled check is left out: it says nothing about the file.
func unverifiedFiles(c *candidate, results []fileResult) []storage.UnverifiedFile {
	var out []storage.UnverifiedFile
	for _, res := range results {
		if !res.healthy || res.unverifiedReason == "" || res.unverifiedReason == unverifiedCancelled {
			continue
		}
		uf := storage.UnverifiedFile{
			FileName:   res.name,
			InfoHash:   res.infoHash,
			Reason:     res.unverifiedReason,
			ShortBytes: res.shortBytes,
		}
		if c != nil && c.item != nil {
			if f := c.item.Files[res.name]; f != nil {
				uf.Size = f.Size
			}
		}
		out = append(out, uf)
	}
	slices.SortFunc(out, func(a, b storage.UnverifiedFile) int { return strings.Compare(a.FileName, b.FileName) })
	return out
}

// ListUnverified returns the entries on the Unverified list whose entry still
// exists, sorted by name.
func (r *Repair) ListUnverified() ([]*storage.EntryHealth, error) {
	var out []*storage.EntryHealth
	err := r.manager.storage.ForEachEntryHealth(func(h *storage.EntryHealth) error {
		if h.IsUnverified() && r.manager.EntryNameHasBackingEntry(h.EntryName) {
			out = append(out, h)
		}
		return nil
	})
	slices.SortFunc(out, func(a, b *storage.EntryHealth) int { return strings.Compare(a.EntryName, b.EntryName) })
	return out, err
}

func isTailTruncated(uf storage.UnverifiedFile) bool { return uf.Reason == reasonTailTruncated }

// ReplaceUnverifiedResult is what ReplaceUnverified reports.
type ReplaceUnverifiedResult struct {
	// Eligible counts the unverified entries with a tail-truncated file among
	// those asked for; Queued names the ones this call re-grabs and Remaining
	// how many are left for another call.
	Eligible  int                `json:"eligible"`
	Queued    []string           `json:"queued"`
	Remaining int                `json:"remaining"`
	Run       *storage.RepairRun `json:"run,omitempty"`
}

// ReplaceUnverified re-grabs up to limit unverified entries' tail-truncated
// files (names, or every such entry when empty): each is marked broken with
// reasonTailTruncated and handed to FixBroken, which deletes and re-searches it
// through its Arr without blocklisting the release. Files left unverified for
// any other reason are not touched - a timed-out check says nothing about the
// file. When FixBroken refuses (a run is active), the records are restored.
func (r *Repair) ReplaceUnverified(ctx context.Context, names []string, limit int) (ReplaceUnverifiedResult, error) {
	var res ReplaceUnverifiedResult
	if limit <= 0 {
		limit = defaultReplaceUnverifiedLimit
	}
	r.mu.Lock()
	active := r.activeRunID
	r.mu.Unlock()
	if active != "" {
		return res, fmt.Errorf("repair already running (run %s)", active)
	}

	all, err := r.ListUnverified()
	if err != nil {
		return res, err
	}
	wanted := make(map[string]bool, len(names))
	for _, n := range names {
		if n = strings.TrimSpace(n); n != "" {
			wanted[n] = true
		}
	}
	var eligible []*storage.EntryHealth
	for _, h := range all {
		if (len(wanted) == 0 || wanted[h.EntryName]) && slices.ContainsFunc(h.UnverifiedFiles, isTailTruncated) {
			eligible = append(eligible, h)
		}
	}
	res.Eligible = len(eligible)
	if len(eligible) == 0 {
		return res, errors.New("no tail-truncated entries to replace")
	}
	chosen := eligible[:min(limit, len(eligible))]
	res.Remaining = len(eligible) - len(chosen)

	originals := make([]storage.EntryHealth, 0, len(chosen))
	for _, h := range chosen {
		orig := *h
		orig.UnverifiedFiles = slices.Clone(h.UnverifiedFiles)
		orig.BrokenFiles = slices.Clone(h.BrokenFiles)
		originals = append(originals, orig)
		markTailTruncatedBroken(h, time.Now())
		r.saveHealth(h)
		res.Queued = append(res.Queued, h.EntryName)
	}

	run, err := r.FixBroken(ctx, res.Queued)
	if err != nil {
		for i := range originals {
			r.saveHealth(&originals[i])
		}
		res.Queued = nil
		res.Remaining = res.Eligible
		return res, err
	}
	res.Run = run
	r.logger.Info().Int("entries", len(res.Queued)).Int("remaining", res.Remaining).Str("run_id", run.ID).
		Msg("Repair: replacing tail-truncated files, re-grabbing each release without blocklisting it")
	return res, nil
}

// markTailTruncatedBroken turns h's tail-truncated files into broken files
// with reasonTailTruncated, leaving its other unverified files listed.
func markTailTruncatedBroken(h *storage.EntryHealth, now time.Time) {
	var keep []storage.UnverifiedFile
	h.BrokenFiles = nil
	for _, uf := range h.UnverifiedFiles {
		if !isTailTruncated(uf) {
			keep = append(keep, uf)
			continue
		}
		h.BrokenFiles = append(h.BrokenFiles, storage.BrokenFile{
			EntryName: h.EntryName,
			FileName:  uf.FileName,
			InfoHash:  uf.InfoHash,
			Protocol:  h.Protocol,
			Reason:    reasonTailTruncated,
			Size:      uf.Size,
		})
	}
	h.UnverifiedFiles = keep
	h.Status = storage.HealthBroken
	h.BrokenCount = len(h.BrokenFiles)
	h.FailureReason = reasonTailTruncated
	h.LastFailedAt = now
	h.DecodeVerifiedAt = time.Time{}
	h.DecodeVerifiedFingerprint = ""
	h.DecodeVerifiedCoverage = ""
}
