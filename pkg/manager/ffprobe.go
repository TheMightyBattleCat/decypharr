package manager

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"time"

	json "github.com/bytedance/sonic"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Reasons are prefixed "ffprobe_" so they're greppable in health records
// alongside the STAT-probe reasons (usenet_segment_missing, etc).
const (
	ffprobeReasonUnreadable      = "ffprobe_unreadable"
	ffprobeReasonNoStreams       = "ffprobe_no_streams"
	ffprobeReasonNoVideoStream   = "ffprobe_no_video_stream"
	ffprobeReasonNoDuration      = "ffprobe_no_duration"
	ffprobeReasonRuntimeMismatch = "ffprobe_runtime_mismatch"
	ffprobeReasonAbsurdDuration  = "ffprobe_absurd_duration"
	ffprobeReasonDecodeError     = "ffprobe_decode_error"
)

const (
	ffprobeDefaultTimeout = 90 * time.Second
	ffprobeRetryDelay     = 2 * time.Second

	// TOO LONG: broken when the file runs at least this many times its
	// expected runtime AND is at least ffprobeTooLongMinOverage over. The
	// unconfirmed ratio is wider so an unrecognized double-episode file that
	// happens to land at exactly 2x isn't deleted for being correct; real
	// extended cuts run ~1.3-1.4x, well under either threshold.
	ffprobeTooLongRatioConfirmed   = 2.0
	ffprobeTooLongRatioUnconfirmed = 2.5
	ffprobeTooLongMinOverage       = 60 * time.Minute

	// TOO SHORT: candidate for broken when under half the expected runtime and
	// at least 15 minutes short. On its own this is only metadata-level
	// evidence: ffprobe reads duration from the container header, which
	// survives a truncated assembly intact and still reports the encoded
	// length rather than however much of the stream is actually readable. So
	// a too-short ratio here is corroborated with a tail read (tailIntact)
	// before anything is marked broken - it's only trusted as "truncated
	// assembly" when that tail read also fails; otherwise the file plays to
	// its own recorded end and the mismatch is Arr metadata being wrong
	// (as-aired combined runtime, extended-cut listing) rather than the file.
	ffprobeTooShortRatio    = 0.5
	ffprobeTooShortMinUnder = 15 * time.Minute

	// ffprobeDecodeWindowCount is the number of evenly-spaced sample windows
	// the decode check opens across the file's duration.
	ffprobeDecodeWindowCount = 15

	// Large files (REMUXes) get fewer decode windows to cut cold-seek I/O
	// over WebDAV: each 2s window on a REMUX pulls ~15-25 MB, so 15 scattered
	// seeks on a 40 GB file is ~300-450 MB of cold random reads that
	// routinely blow the timeout budget. 6 windows still samples the full
	// duration end-to-end.
	ffprobeDecodeWindowCountLarge    = 6
	ffprobeDecodeWindowLargeFileSize = 10 * 1024 * 1024 * 1024 // 10 GB

	// The decode timeout scales with file size so a REMUX gets budget
	// proportional to the bytes it has to pull: f.timeout + sizeGB*perGB,
	// clamped to the cap. A 40 GB file lands around base+200s.
	ffprobeDecodeTimeoutPerGB = 5 * time.Second
	ffprobeDecodeTimeoutCap   = 10 * time.Minute

	// ffprobeDecodeWindowSpan is how long each decode window reads.
	ffprobeDecodeWindowSpan = 2 * time.Second

	// ffprobeTailWindow is the span of stream tail demuxed by tailIntact to
	// corroborate a too-short verdict: packets are read starting this long
	// before the probed duration, so a real end-of-stream packet lands in the
	// window rather than being missed by seeking in too late.
	ffprobeTailWindow = 30 * time.Second

	// Ceiling-only thresholds used when no expected runtime is available.
	ffprobeCeilingSonarr = 6 * time.Hour
	ffprobeCeilingOther  = 12 * time.Hour
)

// expectedRuntime is what the Arr believes a file's playback duration should
// be. Seconds == 0 means unknown - the checker falls back to a ceiling-only
// sanity check rather than a ratio comparison.
type expectedRuntime struct {
	Seconds               int
	EpisodeCountConfirmed bool
	ArrKind               storage.ArrKind

	// Bytes is the file's size, used to scale the decode timeout and pick
	// the decode-window count. 0 when unknown - callers fall back to the
	// flat base timeout and the full window count.
	Bytes int64
}

// ffprobeChecker validates one file's assembled stream by running ffprobe
// against decypharr's own local WebDAV endpoint. WebDAV supports HTTP Range,
// giving ffprobe the seekable access it needs (MP4s with a tail moov atom
// require a seek to EOF; a pipe would false-fail them), and since the read is
// served in-process it never touches the DFS FUSE downloaders - no risk of
// tripping the DFS circuit breaker or racing playback-escalation repair.
type ffprobeChecker struct {
	binPath string
	timeout time.Duration
	baseURL string // e.g. "http://127.0.0.1:8282/webdav/"

	// authToken is the manager's ephemeral, in-memory, per-process bearer
	// token (see webdav.Handler.isInternalBearer), sent via ffprobe's
	// -headers flag. It replaces the user's WebDAV password, which is only
	// ever stored as a bcrypt hash and so cannot be recovered and handed to
	// an external process.
	//
	// Always set (the token is generated unconditionally at Manager init).
	// When WebDAV auth is on it authenticates the read; when auth is off it
	// carries no access it wouldn't already have, and its only effect is to
	// make webdav.Handler.isInternalBearer recognise the request so that
	// handleDownload switches into ContextForVerificationRead - which is
	// exactly what a sweep/import probe wants, so a confirmed-dead segment
	// surfaces as a real NNTP error instead of being papered over by padding
	// or a PAR2 patch.
	//
	// Security note: this token appears in the ffprobe process's argument
	// list, visible to anything that can read /proc or run `ps` on this
	// host for the life of that (sub-second to low-second) process. It is a
	// process-local value, regenerated every restart and never persisted, so
	// even with auth on the blast radius of a leak is bounded to "until the
	// next restart" rather than forever.
	authToken string

	logger zerolog.Logger

	// tailIntactFn, when set, replaces the tailIntact method. Production
	// code leaves this nil (check calls f.tailIntact directly); tests set it
	// to corroborate a too-short verdict without shelling out to a real
	// ffprobe process.
	tailIntactFn func(ctx context.Context, entryFolder, fileName string, probed time.Duration) bool
}

// newFFProbeChecker builds a checker for one repair sweep run, or returns nil (with
// exactly one WARN) when ffprobe_check is enabled but can't actually be used:
// binary missing, or WebDAV disabled. Callers must treat nil as "proceed
// STAT-only" rather than failing the repair sweep.
func newFFProbeChecker(cfg *config.Config, m *Manager, log zerolog.Logger) *ffprobeChecker {
	if !cfg.Repair.FFProbeCheck {
		return nil
	}
	// The sweep reads over Usenet under whatever I/O contention the rest of
	// the repair run and live playback are creating, so it gets double the
	// import gate's default budget before a slow cold read is treated as
	// merely inconclusive.
	return buildFFProbeChecker(cfg, m, log, 2*ffprobeDefaultTimeout)
}

// newImportFFProbeChecker builds a checker for the import-time gate
// (Repair.FFProbeOnImport), independent of the repair sweep's own FFProbeCheck
// toggle - either can be on without the other. Shares every bit of binary
// resolution, WebDAV/auth wiring, and timeout parsing with the repair-sweep-side
// checker via buildFFProbeChecker, and the resulting *ffprobeChecker's
// check/checkConfirmed methods are exactly the ones the repair sweep uses - the
// import gate is a second caller of the same core, not a parallel
// implementation.
func newImportFFProbeChecker(cfg *config.Config, m *Manager, log zerolog.Logger) *ffprobeChecker {
	if !cfg.Repair.FFProbeOnImport {
		return nil
	}
	return buildFFProbeChecker(cfg, m, log, ffprobeDefaultTimeout)
}

// buildFFProbeChecker does the binary/WebDAV/auth/timeout resolution shared
// by both newFFProbeChecker and newImportFFProbeChecker. Returns nil (with
// exactly one WARN) when ffprobe can't actually be used: binary missing, or
// WebDAV disabled. Callers must treat nil as "proceed without validation"
// rather than failing whatever they're doing.
//
// defaultTimeout is the per-caller floor used when repair.ffprobe_timeout
// isn't set - the sweep and import gate pass different values (see their
// call sites) so the sweep can tolerate slower cold reads under repair/
// playback I/O contention without also loosening the import gate. An
// explicit repair.ffprobe_timeout in config always overrides either default.
func buildFFProbeChecker(cfg *config.Config, m *Manager, log zerolog.Logger, defaultTimeout time.Duration) *ffprobeChecker {
	binPath := strings.TrimSpace(cfg.Repair.FFProbePath)
	if binPath == "" {
		binPath = "ffprobe"
	}
	resolved, err := exec.LookPath(binPath)
	if err != nil {
		log.Warn().Err(err).Str("path", binPath).Msg("Repair: ffprobe validation is enabled but the ffprobe binary was not found on PATH; proceeding without it")
		return nil
	}
	if cfg.DisableWebDav {
		log.Warn().Msg("Repair: ffprobe validation is enabled but WebDAV is disabled (disable_webdav); proceeding without it")
		return nil
	}

	timeout := defaultTimeout
	if raw := strings.TrimSpace(cfg.Repair.FFProbeTimeout); raw != "" {
		if d, err := utils.ParseDuration(raw); err == nil && d > 0 {
			timeout = d
		} else {
			log.Warn().Str("value", raw).Msg("Repair: invalid repair.ffprobe_timeout; using default of 90s")
		}
	}

	// Always send the internal bearer token, regardless of the auth toggles.
	// It authenticates the probe when WebDAV auth is on, and when auth is off
	// its only effect is to trip webdav.Handler.isInternalBearer so the probe
	// read runs under ContextForVerificationRead (no padding, no PAR2 patch).
	// The token is process-local, regenerated every restart, never persisted.
	authToken := m.InternalToken()

	return &ffprobeChecker{
		binPath:   resolved,
		timeout:   timeout,
		baseURL:   fmt.Sprintf("http://127.0.0.1:%s%swebdav/", cfg.Port, cfg.URLBase),
		authToken: authToken,
		logger:    log,
	}
}

// probeTarget builds the WebDAV URL for entryFolder/fileName that both check
// and tailIntact probe against.
func (f *ffprobeChecker) probeTarget(entryFolder, fileName string) string {
	return f.baseURL + EntryAllFolder + "/" + url.PathEscape(entryFolder) + "/" + url.PathEscape(fileName)
}

// probeArgs appends the shared -headers auth flag and the target URL to
// args, so check and tailIntact only need to supply their own
// probe-specific flags. authToken is normally set (see the struct field);
// the guard only covers the theoretical case of an empty token from a
// failed generation at init.
func (f *ffprobeChecker) probeArgs(entryFolder, fileName string, args []string) []string {
	if f.authToken != "" {
		args = append(args, "-headers", "Authorization: Bearer "+f.authToken+"\r\n")
	}
	return append(args, f.probeTarget(entryFolder, fileName))
}

type ffprobeOutput struct {
	Format struct {
		Duration string `json:"duration"`
	} `json:"format"`
	Streams []struct {
		CodecType string `json:"codec_type"`
	} `json:"streams"`
}

// check runs ffprobe once against entryFolder/fileName and returns whether
// the file looks healthy. A context timeout or cancellation is treated as
// inconclusive (ok=true) rather than broken - a slow cold read over Usenet
// must never cause an auto-delete.
//
// conclusive reports whether the verdict is a real signal about the file's
// integrity worth caching for DecodeVerifyTTL: false means the probe (or its
// decode pass) was cut short by a timeout or context cancellation and proved
// nothing, so callers must not treat an ok=true here as a passed decode
// verification. A definite verdict - clean, or broken for a concrete reason -
// is conclusive=true; a skipDecode call that never ran the decode windows is
// conclusive=false.
func (f *ffprobeChecker) check(ctx context.Context, entryFolder, fileName string, expected expectedRuntime, skipDecode bool) (ok bool, reason string, conclusive bool) {
	args := f.probeArgs(entryFolder, fileName, []string{"-v", "error", "-print_format", "json", "-show_format", "-show_streams"})

	cctx, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()

	cmd := exec.CommandContext(cctx, f.binPath, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()

	if cctx.Err() != nil {
		if errors.Is(cctx.Err(), context.DeadlineExceeded) {
			f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).Msg("Repair: ffprobe timed out; treating as inconclusive")
		}
		return true, "", false
	}
	if runErr != nil {
		return false, ffprobeReasonUnreadable + ": " + firstLine(stderr.String()), true
	}

	var probe ffprobeOutput
	if err := json.Unmarshal(stdout.Bytes(), &probe); err != nil {
		return false, ffprobeReasonUnreadable + ": " + firstLine(err.Error()), true
	}

	hasVideo, hasAudio := false, false
	for _, s := range probe.Streams {
		switch s.CodecType {
		case "video":
			hasVideo = true
		case "audio":
			hasAudio = true
		}
	}
	if !hasVideo && !hasAudio {
		return false, ffprobeReasonNoStreams, true
	}
	if !hasVideo {
		return false, ffprobeReasonNoVideoStream, true
	}

	durationSec, err := strconv.ParseFloat(strings.TrimSpace(probe.Format.Duration), 64)
	if err != nil || durationSec <= 0 {
		return false, ffprobeReasonNoDuration, true
	}
	duration := time.Duration(durationSec * float64(time.Second))

	if expected.Seconds <= 0 {
		ceiling := ffprobeCeilingOther
		if expected.ArrKind == storage.ArrKindSonarr {
			ceiling = ffprobeCeilingSonarr
		}
		if duration > ceiling {
			return false, ffprobeReasonAbsurdDuration, true
		}
		if !skipDecode {
			return f.decodeWindows(ctx, entryFolder, fileName, duration, expected.Bytes)
		}
		// Decode verification was skipped for this call, so nothing was
		// deep-verified - not conclusive.
		return true, "", false
	}

	expectedDur := time.Duration(expected.Seconds) * time.Second
	ratio := duration.Seconds() / expectedDur.Seconds()

	tooLongRatio := ffprobeTooLongRatioConfirmed
	if !expected.EpisodeCountConfirmed {
		tooLongRatio = ffprobeTooLongRatioUnconfirmed
	}
	if ratio >= tooLongRatio && duration-expectedDur >= ffprobeTooLongMinOverage {
		return false, fmt.Sprintf("%s: probe=%dm expected=%dm (%.1fx)", ffprobeReasonRuntimeMismatch, int(duration.Minutes()), int(expectedDur.Minutes()), ratio), true
	}
	if ratio <= ffprobeTooShortRatio && expectedDur-duration >= ffprobeTooShortMinUnder {
		tailFn := f.tailIntactFn
		if tailFn == nil {
			tailFn = f.tailIntact
		}
		if tailFn(ctx, entryFolder, fileName, duration) {
			f.logger.Info().
				Str("entry", entryFolder).
				Str("file", fileName).
				Int("probe_minutes", int(duration.Minutes())).
				Int("expected_minutes", int(expectedDur.Minutes())).
				Msg("Repair: runtime shorter than Arr metadata but stream is complete to its own header duration; treating as metadata mismatch (split release or as-aired special), not marking broken")
			if !skipDecode {
				return f.decodeWindows(ctx, entryFolder, fileName, duration, expected.Bytes)
			}
			return true, "", false
		}
		return false, fmt.Sprintf("%s: probe=%dm expected=%dm (%.1fx); tail_unreadable", ffprobeReasonRuntimeMismatch, int(duration.Minutes()), int(expectedDur.Minutes()), ratio), true
	}

	// Grey zone: meaningfully different from expected but inside the safety
	// bands (extended cuts, specials, padded finales). Never auto-delete on
	// ambiguity - log it and let the file stand.
	if ratio < 0.9 || ratio > 1.1 {
		f.logger.Info().Str("entry", entryFolder).Str("file", fileName).Float64("ratio", ratio).Msg("Repair: ffprobe duration differs from expected but within tolerance; not marking broken")
	}
	if !skipDecode {
		return f.decodeWindows(ctx, entryFolder, fileName, duration, expected.Bytes)
	}
	return true, "", false
}

type ffprobeTailOutput struct {
	Packets []struct {
		PtsTime string `json:"pts_time"`
	} `json:"packets"`
}

// tailIntact corroborates a too-short verdict by demuxing a window of stream
// near the probed duration's end and checking that a video packet actually
// exists there. ffprobe's format duration comes from the container header,
// which survives a truncated assembly and keeps reporting the encoded
// length - so a short header duration alone doesn't prove the file is
// broken. If the stream genuinely reads through to (near) that duration, the
// short probe is a metadata mismatch (Arr's expected runtime is wrong), not
// corruption.
//
// Like check, a context timeout or cancellation is treated as inconclusive
// (true) rather than broken - never condemn a file because the corroborating
// read itself ran out of time.
func (f *ffprobeChecker) tailIntact(ctx context.Context, entryFolder, fileName string, probed time.Duration) bool {
	start := probed - ffprobeTailWindow
	if start < 0 {
		start = 0
	}
	readSpan := ffprobeTailWindow + 15*time.Second
	interval := fmt.Sprintf("%.0f%%+%.0f", start.Seconds(), readSpan.Seconds())

	args := f.probeArgs(entryFolder, fileName, []string{
		"-v", "error",
		"-read_intervals", interval,
		"-select_streams", "v:0",
		"-show_entries", "packet=pts_time",
		"-of", "json",
	})

	cctx, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()

	cmd := exec.CommandContext(cctx, f.binPath, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()

	if cctx.Err() != nil {
		if errors.Is(cctx.Err(), context.DeadlineExceeded) {
			f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).Msg("Repair: ffprobe tail check timed out; treating as inconclusive")
		}
		return true
	}
	if runErr != nil {
		return false
	}

	var out ffprobeTailOutput
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		return false
	}

	const nearEndTolerance = 45 * time.Second
	for _, p := range out.Packets {
		sec, err := strconv.ParseFloat(strings.TrimSpace(p.PtsTime), 64)
		if err != nil {
			continue
		}
		pts := time.Duration(sec * float64(time.Second))
		diff := probed - pts
		if diff < 0 {
			diff = -diff
		}
		if diff <= nearEndTolerance {
			return true
		}
	}
	return false
}

// decodeWindows probes evenly-spaced windows of the video stream, forcing
// actual frame decode (not just demux). The demux-only check above reads
// container metadata and packet headers but never asks the codec to
// reconstruct a frame, so a file with a valid container but corrupt
// compressed data passes undetected. This method closes that gap.
//
// Like every other probe in this file, timeout and cancellation are
// inconclusive (ok=true, fail-open): a slow WebDAV read must never
// auto-delete a file that might be fine. conclusive distinguishes a real
// decode verdict (clean, or a concrete decode error) from a timed-out or
// cancelled pass that proved nothing - the caller uses it to decide
// whether an ok=true here counts as a passed decode verification.
func (f *ffprobeChecker) decodeWindows(ctx context.Context, entryFolder, fileName string, duration time.Duration, fileBytes int64) (ok bool, reason string, conclusive bool) {
	if duration <= 0 {
		return true, "", true
	}

	// Adaptive window count: a large file (REMUX) gets fewer windows so the
	// probe pulls ~120-150 MB of cold random I/O over WebDAV instead of
	// ~300-450 MB. Still spans the whole duration.
	windowCount := ffprobeDecodeWindowCount
	if fileBytes > ffprobeDecodeWindowLargeFileSize {
		windowCount = ffprobeDecodeWindowCountLarge
	}

	n := windowCount
	durSec := int(duration.Seconds())
	if durSec < n {
		n = durSec
	}
	if n <= 0 {
		return true, "", true
	}

	intervals := make([]string, n)
	for i := 0; i < n; i++ {
		startSec := duration * time.Duration(i) / time.Duration(n)
		intervals[i] = fmt.Sprintf("%.0f%%+%.0f", startSec.Seconds(), ffprobeDecodeWindowSpan.Seconds())
	}

	args := f.probeArgs(entryFolder, fileName, []string{
		"-v", "error",
		"-read_intervals", strings.Join(intervals, ","),
		"-select_streams", "v:0",
		"-show_entries", "frame=pts_time",
		"-of", "csv=p=0",
	})

	// Scale the timeout with file size: base + sizeGB*perGB, clamped. A
	// REMUX has far more compressed data to pull through the decode windows
	// than a WEB episode, so a flat budget times out on the big ones and
	// wastes time on the small ones.
	timeout := f.timeout
	if fileBytes > 0 {
		sizeGB := float64(fileBytes) / (1024 * 1024 * 1024)
		scaled := f.timeout + time.Duration(sizeGB*float64(ffprobeDecodeTimeoutPerGB))
		if scaled > ffprobeDecodeTimeoutCap {
			scaled = ffprobeDecodeTimeoutCap
		}
		timeout = scaled
	}

	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(cctx, f.binPath, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()

	if cctx.Err() != nil {
		if errors.Is(cctx.Err(), context.DeadlineExceeded) {
			f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).
				Int("windows", n).Dur("timeout", timeout).Int64("file_bytes", fileBytes).
				Msg("Repair: ffprobe decode check timed out; treating as inconclusive")
		}
		return true, "", false
	}

	// Decode errors surface on stderr even when the exit code is 0 (ffprobe
	// reports per-frame codec errors but still exits cleanly when it can
	// continue demuxing). Check stderr first.
	stderrStr := strings.TrimSpace(stderr.String())
	if stderrStr != "" {
		return false, ffprobeReasonDecodeError + ": " + firstLine(stderrStr), true
	}

	if runErr != nil {
		return false, ffprobeReasonDecodeError, true
	}

	return true, "", true
}

// checkConfirmed retries once before declaring a file broken: a transient
// cold-seek can fail one read and pass the next, and since a broken verdict
// can lead to an automatic delete + re-search, a single bad read is never
// enough. Only a second consecutive broken verdict is returned as broken.
//
// The one exception is an "ffprobe_unreadable" failure: the container could
// not be opened at all (dead header segments, permanently broken assembly).
// That is structural and permanent within the same NZB - the retry 2s later
// reads the exact same dead bytes - so it is returned immediately without
// waiting out ffprobeRetryDelay. Decode-error and timeout/inconclusive
// failures still get the retry (a single decode window can glitch
// transiently).
//
// deadSignal, when non-nil, is the caller's dead-segment latch for this read
// (see deadSignalRegistry): if the fetcher observed a confirmed-dead (NNTP
// 430) segment while serving the probe - even one ffprobe's own sampling
// windows missed - the verdict is forced to broken with no retry. A retry
// would only pull the same dead article again.
//
// conclusive is threaded straight through from check: an ok=true result
// whose underlying decode pass timed out or was cancelled comes back
// conclusive=false so the sweep does not stamp it as decode-verified. A
// forced-broken dead-segment verdict is conclusive=true (a confirmed 430
// is a definite signal). A retry abandoned because ctx was cancelled
// mid-wait is conclusive=false.
func (f *ffprobeChecker) checkConfirmed(ctx context.Context, entryFolder, fileName string, expected expectedRuntime, skipDecode bool, deadSignal *DeadSegmentSignal) (ok bool, reason string, conclusive bool) {
	const deadSegmentReason = "dead_segment_detected: NNTP 430 during verification read"

	ok, reason, conclusive = f.check(ctx, entryFolder, fileName, expected, skipDecode)
	if deadSignal.Detected() {
		f.logger.Warn().Str("entry", entryFolder).Str("file", fileName).Bool("ffprobe_ok", ok).
			Msg("Repair: dead segment (NNTP 430) observed during ffprobe verification read; forcing broken verdict")
		return false, deadSegmentReason, true
	}
	if ok {
		return true, "", conclusive
	}
	if strings.HasPrefix(reason, ffprobeReasonUnreadable) {
		f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).Str("reason", reason).Msg("[repair] Repair: skipping ffprobe retry — unreadable error is permanent")
		return false, reason, conclusive
	}
	f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).Str("reason", reason).Msg("Repair: ffprobe check failed; retrying once before declaring broken")

	select {
	case <-ctx.Done():
		return true, "", false
	case <-time.After(ffprobeRetryDelay):
	}

	ok, reason, conclusive = f.check(ctx, entryFolder, fileName, expected, skipDecode)
	f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).Bool("ok", ok).Str("reason", reason).Msg("Repair: ffprobe retry result")
	if deadSignal.Detected() {
		f.logger.Warn().Str("entry", entryFolder).Str("file", fileName).Bool("ffprobe_ok", ok).
			Msg("Repair: dead segment (NNTP 430) observed during ffprobe verification retry; forcing broken verdict")
		return false, deadSegmentReason, true
	}
	if ok {
		return true, "", conclusive
	}
	return false, reason, conclusive
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	return s
}

// ffprobeCheckerCtxKey carries an optional *ffprobeChecker down through the
// probe call chain (probeAndHealCandidates -> probeEntry -> probeFiles ->
// probeFile), which is shared by the timed repair sweep, the on-demand repair sweep, and
// ad-hoc series/movie rechecks alike - a context value avoids threading a new
// parameter through every layer for what is, for two of those three
// call-sites, almost always nil.
type ffprobeCheckerCtxKey struct{}

func contextWithFFProbeChecker(ctx context.Context, checker *ffprobeChecker) context.Context {
	if checker == nil {
		return ctx
	}
	return context.WithValue(ctx, ffprobeCheckerCtxKey{}, checker)
}

func ffprobeCheckerFromContext(ctx context.Context) *ffprobeChecker {
	checker, _ := ctx.Value(ffprobeCheckerCtxKey{}).(*ffprobeChecker)
	return checker
}

// attachFFProbeChecker builds an ffprobe checker for this run when enabled
// and stores it on ctx for probeFile to pick up. newFFProbeChecker itself
// logs the one WARN when the flag is on but unusable (missing binary,
// WebDAV disabled); either way the repair sweep proceeds STAT-only.
func (r *Repair) attachFFProbeChecker(ctx context.Context, log zerolog.Logger) context.Context {
	cfg := config.Get()
	checker := newFFProbeChecker(cfg, r.manager, log)
	return contextWithFFProbeChecker(ctx, checker)
}

// expectedRuntimeFor resolves the expected playback duration for one file in
// a candidate entry from its Arr content mapping, when available.
func expectedRuntimeFor(c *candidate, name string) expectedRuntime {
	// File size comes from the entry item (the actual assembled/declared
	// size); fall back to the Arr's reported size when the item has no
	// record for this file.
	var bytes int64
	if c.item != nil {
		if f := c.item.Files[name]; f != nil {
			bytes = f.Size
		}
	}
	cf, ok := c.contentMap[name]
	if !ok {
		return expectedRuntime{ArrKind: c.arrKind, Bytes: bytes}
	}
	if bytes == 0 {
		bytes = cf.Size
	}
	return expectedRuntime{
		Seconds:               cf.RuntimeSec,
		EpisodeCountConfirmed: cf.EpisodeCountConfirmed,
		ArrKind:               c.arrKind,
		Bytes:                 bytes,
	}
}
