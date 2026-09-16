package manager

import (
	"context"
	"errors"
	"fmt"
	"regexp"
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

// What a decoded_with_errors file's ffprobe errors point at
// (storage.UnverifiedFile.Cause), from decodeErrorCause.
const (
	// decodeCauseStreamEnded: the WebDAV body ended before the file did
	// ("[http] Stream ends prematurely", "Read error"): the reader could not
	// fetch part of the file. Playback stops at the same byte.
	decodeCauseStreamEnded = "stream_ended"
	// decodeCauseZeroFilled: Matroska found 0x00 where an element starts, a
	// run of zeros where data should be.
	decodeCauseZeroFilled = "zero_filled"
	// decodeCauseFileEnded: "File ended prematurely", the served file is
	// shorter than its own element sizes say.
	decodeCauseFileEnded = "file_ended"
	// decodeCauseContainer: an element or packet length that cannot be right,
	// garbage where Matroska structure should be.
	decodeCauseContainer = "container_errors"
	// decodeCauseSeekWarnings: every line is a decoder message ffmpeg prints
	// when decoding starts at a seek point without the frames before it
	// (All Still on the Eastern Ridge: the same two lines once per seek,
	// whether the window was 46 or 286 frames). Says nothing against the file.
	decodeCauseSeekWarnings = "seek_warnings"
	// decodeCauseCodec: other codec errors in the video stream.
	decodeCauseCodec = "codec_errors"
)

var (
	decodeStreamEndedLine = regexp.MustCompile(`Stream ends prematurely|Read error`)
	decodeZeroFilledLine  = regexp.MustCompile(`0x00 at pos \d+ .*invalid as first byte of an EBML number`)
	decodeFileEndedLine   = regexp.MustCompile(`File ended prematurely`)
	decodeContainerLine   = regexp.MustCompile(`EBML number|exceeds containing master element|unknown-length element|Truncating packet|Invalid NAL unit size|Error splitting the input into NAL units`)
	decodeSeekWarningLine = regexp.MustCompile(`mmco: unref short failure|number of reference frames \(\S+\) exceeds max|ignoring pic cod ext|first frame is no keyframe|^Last message repeated \d+ times`)
)

// decodeErrorCause classifies the stderr of a decode probe that printed errors
// but decoded through, most severe first. A line naming the file's structure
// or the reader outranks codec errors, which are often its consequence.
func decodeErrorCause(stderr string) string {
	var lines []string
	for line := range strings.SplitSeq(stderr, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	anyLine := func(re *regexp.Regexp) bool {
		return slices.ContainsFunc(lines, re.MatchString)
	}
	switch {
	case anyLine(decodeStreamEndedLine):
		return decodeCauseStreamEnded
	case anyLine(decodeZeroFilledLine):
		return decodeCauseZeroFilled
	case anyLine(decodeFileEndedLine):
		return decodeCauseFileEnded
	case anyLine(decodeContainerLine):
		return decodeCauseContainer
	case len(lines) > 0 && !slices.ContainsFunc(lines, func(l string) bool {
		// ffmpeg prefixes decoder lines "[h264 @ 0x...] "; the repeat line has none.
		if i := strings.Index(l, "] "); i >= 0 && strings.HasPrefix(l, "[") {
			l = l[i+2:]
		}
		return !decodeSeekWarningLine.MatchString(l)
	}):
		return decodeCauseSeekWarnings
	default:
		return decodeCauseCodec
	}
}

// defaultReplaceUnverifiedLimit caps how many entries one Replace re-grabs.
// Lower than defaultDebridGoneLimit: these files still play, and each is gone
// from the library until its re-grab lands.
const defaultReplaceUnverifiedLimit = 25

// unverifiedCause is where a decode check records why it is about to end
// without a verdict. probeFile puts one on the context it hands the checker.
type unverifiedCause struct {
	reason string
	// decodeCause and detail describe a decoded_with_errors reason; see
	// noteDecodeErrors.
	decodeCause string
	detail      string
}

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
		if reason != unverifiedDecodeErrors {
			c.decodeCause, c.detail = "", ""
		}
	}
}

// noteDecodeErrors records a decoded_with_errors reason with what ffprobe
// printed: its cause from the full stderr, and summary, the lines logged.
func noteDecodeErrors(ctx context.Context, stderr, summary string) {
	if c, _ := ctx.Value(unverifiedCauseCtxKey{}).(*unverifiedCause); c != nil {
		c.reason = unverifiedDecodeErrors
		c.decodeCause = decodeErrorCause(stderr)
		c.detail = summary
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
			Cause:      res.unverifiedCause,
			Detail:     res.unverifiedDetail,
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

// isReplaceable reports whether Replace acts on uf: a file that plays but was
// assembled wrong at import (see replaceableReason).
func isReplaceable(uf storage.UnverifiedFile) bool { return replaceableReason(uf.Reason) }

// ReplaceUnverifiedResult is what ReplaceUnverified reports.
type ReplaceUnverifiedResult struct {
	// Eligible counts the unverified entries with a replaceable file among
	// those asked for; Queued names the ones this call re-grabs and Remaining
	// how many are left for another call.
	Eligible  int                `json:"eligible"`
	Queued    []string           `json:"queued"`
	Remaining int                `json:"remaining"`
	Run       *storage.RepairRun `json:"run,omitempty"`
}

// ReplaceUnverified re-grabs up to limit unverified entries' replaceable files,
// tail truncated or volumes out of order (names, or every such entry when
// empty): each is marked broken with its reason and handed to FixBroken, which
// deletes and re-searches it
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
		if (len(wanted) == 0 || wanted[h.EntryName]) && slices.ContainsFunc(h.UnverifiedFiles, isReplaceable) {
			eligible = append(eligible, h)
		}
	}
	res.Eligible = len(eligible)
	if len(eligible) == 0 {
		return res, errors.New("no tail-truncated or misordered entries to replace")
	}
	chosen := eligible[:min(limit, len(eligible))]
	res.Remaining = len(eligible) - len(chosen)

	originals := make([]storage.EntryHealth, 0, len(chosen))
	for _, h := range chosen {
		orig := *h
		orig.UnverifiedFiles = slices.Clone(h.UnverifiedFiles)
		orig.BrokenFiles = slices.Clone(h.BrokenFiles)
		originals = append(originals, orig)
		markReplaceableBroken(h, time.Now())
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
		Msg("Repair: replacing files assembled wrong at import, re-grabbing each release without blocklisting it")
	return res, nil
}

// markReplaceableBroken turns h's replaceable files into broken files with
// their reason, leaving its other unverified files listed.
func markReplaceableBroken(h *storage.EntryHealth, now time.Time) {
	var keep []storage.UnverifiedFile
	h.BrokenFiles = nil
	for _, uf := range h.UnverifiedFiles {
		if !isReplaceable(uf) {
			keep = append(keep, uf)
			continue
		}
		h.BrokenFiles = append(h.BrokenFiles, storage.BrokenFile{
			EntryName: h.EntryName,
			FileName:  uf.FileName,
			InfoHash:  uf.InfoHash,
			Protocol:  h.Protocol,
			Reason:    uf.Reason,
			Size:      uf.Size,
		})
	}
	h.UnverifiedFiles = keep
	h.Status = storage.HealthBroken
	h.BrokenCount = len(h.BrokenFiles)
	h.FailureReason = h.BrokenFiles[0].Reason
	h.LastFailedAt = now
	h.DecodeVerifiedAt = time.Time{}
	h.DecodeVerifiedFingerprint = ""
	h.DecodeVerifiedCoverage = ""
}
