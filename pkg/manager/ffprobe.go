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

	// ffprobeReasonDeadSegment is the verdict checkConfirmed forces when the
	// fetcher observed a confirmed-dead (NNTP 430) segment while serving the
	// probe read. Callers match on this exact string to distinguish a
	// genuinely-dead posting from any other broken verdict (see the import
	// gate's dead-posting handling).
	ffprobeReasonDeadSegment = "dead_segment_detected: NNTP 430 during verification read"
)

const (
	ffprobeDefaultTimeout = 90 * time.Second
	ffprobeRetryDelay     = 2 * time.Second

	// ffprobeVerifyBaseTimeout is the per-probe floor for both the repair
	// sweep and the import gate (an explicit repair.ffprobe_timeout in config
	// still overrides it). Both read cold over Usenet/WebDAV under whatever I/O
	// contention playback and the rest of the run are creating, and both now
	// have large REMUXes in scope, so a single cold read gets 3 minutes before
	// it's treated as merely inconclusive rather than the old flat 90s.
	ffprobeVerifyBaseTimeout = 2 * ffprobeDefaultTimeout

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

	// Bounds for the decode check on a file with no usable container index,
	// where ffmpeg can only reach byte offset X by reading X (see
	// decodeWindows). These are the fallbacks when repair.decode_detect_bytes
	// or repair.decode_head_bytes is unset or unparseable.
	defaultDecodeDetectBytes = 2 << 30 // 2 GiB
	defaultDecodeHeadBytes   = 3 << 30 // 3 GiB

	// decodeDetectCapDivisor cuts the seek-detect probe at detectBytes/2. The
	// cut has to sit BELOW the read an unseekable file needs to reach the
	// detect window (detectBytes): at or above it, a forward scanner would
	// reach the window uncut, complete, and be classified as seekable.
	// Measured on the 2026-09-09/10 sweep, the largest single range GET per
	// file split cleanly in two: files that seek stayed at or under 24 MB
	// (p90 16 MB for the 3-10 GB ones), files that forward-scan issued GETs of
	// 128 MB and up and read 84-100% of the file, with nothing in between. Half
	// the implied read sits well over a seekable file's whole detect probe and
	// 2x under the forward scan, which also absorbs an opening that runs below
	// the file's average bitrate.
	decodeDetectCapDivisor = 2

	// decodeHeadCapNum/decodeHeadCapDen cut the head scan at 1.5x headBytes.
	// The scan's interval is sized in seconds from the file's AVERAGE bitrate,
	// so an opening that runs above average needs more than headBytes to reach
	// the interval's end; the slack keeps a healthy scan from being cut short.
	decodeHeadCapNum = 3
	decodeHeadCapDen = 2

	// The metadata probe (-show_format/-show_streams, no -read_intervals) only
	// reads the container header plus a tail moov atom, so it doesn't scale
	// with total bytes the way the decode windows do - but on a 20 GB+ REMUX
	// even that is a real cold seek over WebDAV that has blown the flat 90s
	// import budget (CiNEPHiLES 2026-09-08). Scale it gently, with a lower cap
	// than the decode path.
	ffprobeMetadataTimeoutPerGB = 2 * time.Second
	ffprobeMetadataTimeoutCap   = 5 * time.Minute

	// A healthy metadata probe returns in well under a second; anything past
	// this is logged so the size-scaling above can be judged against real
	// numbers (it runs on every file, so only the slow ones are worth a line).
	ffprobeMetadataSlowThreshold = 5 * time.Second

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

	// detectBytes and headBytes bound the decode check on a file with no
	// usable container index. Resolved once from repair.decode_detect_bytes /
	// repair.decode_head_bytes at construction; zero means the built-in
	// default. See decodeWindows.
	detectBytes int64
	headBytes   int64

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

	// runProbeFn, when set, replaces runDecodeProbe. Production code leaves
	// this nil; tests set it to drive decodeWindows' detect/spread/head tree
	// without shelling out to a real ffprobe - a fake spends the budget it is
	// handed to stand in for the read path cutting the probe. Same convention
	// as tailIntactFn.
	runProbeFn func(ctx context.Context, entryFolder, fileName, phase string, intervals []string, timeout time.Duration, fileBytes int64, budget *VerifyBudget) (ok bool, reason string, conclusive bool)

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
	return buildFFProbeChecker(cfg, m, log, ffprobeVerifyBaseTimeout)
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
	return buildFFProbeChecker(cfg, m, log, ffprobeVerifyBaseTimeout)
}

// buildFFProbeChecker does the binary/WebDAV/auth/timeout resolution shared
// by both newFFProbeChecker and newImportFFProbeChecker. Returns nil (with
// exactly one WARN) when ffprobe can't actually be used: binary missing, or
// WebDAV disabled. Callers must treat nil as "proceed without validation"
// rather than failing whatever they're doing.
//
// defaultTimeout is the per-probe floor used when repair.ffprobe_timeout
// isn't set. Both callers currently pass ffprobeVerifyBaseTimeout (the sweep
// for cold-read tolerance under I/O contention, the import gate because large
// REMUXes are now in its scope), but the parameter stays so the two can
// diverge again without touching this function. An explicit
// repair.ffprobe_timeout in config always overrides the default.
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
			log.Warn().Str("value", raw).Dur("default", defaultTimeout).Msg("Repair: invalid repair.ffprobe_timeout; using the built-in default")
		}
	}

	// Always send the internal bearer token, regardless of the auth toggles.
	// It authenticates the probe when WebDAV auth is on, and when auth is off
	// its only effect is to trip webdav.Handler.isInternalBearer so the probe
	// read runs under ContextForVerificationRead (no padding, no PAR2 patch).
	// The token is process-local, regenerated every restart, never persisted.
	authToken := m.InternalToken()

	return &ffprobeChecker{
		binPath:     resolved,
		timeout:     timeout,
		baseURL:     fmt.Sprintf("http://127.0.0.1:%s%swebdav/", cfg.Port, cfg.URLBase),
		detectBytes: parseSizeOr(log, "decode_detect_bytes", cfg.Repair.DecodeDetectBytes, defaultDecodeDetectBytes),
		headBytes:   parseSizeOr(log, "decode_head_bytes", cfg.Repair.DecodeHeadBytes, defaultDecodeHeadBytes),
		authToken:   authToken,
		logger:      log,
	}
}

// parseSizeOr resolves a repair.<key> size string ("3GB"), falling back to def
// on an empty value, or on an unparseable one with a WARN naming the key.
func parseSizeOr(log zerolog.Logger, key, raw string, def int64) int64 {
	s := strings.TrimSpace(raw)
	if s == "" {
		return def
	}
	if n, err := config.ParseSize(s); err == nil && n > 0 {
		return n
	}
	log.Warn().Str("value", raw).Int64("default_bytes", def).
		Msgf("Repair: invalid repair.%s; using the built-in default", key)
	return def
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

// scaledTimeout returns f.timeout grown by perGB for every GB of fileBytes,
// clamped to cap. fileBytes <= 0 (size unknown) falls back to the flat
// f.timeout. The decode-windows probe and the metadata probe call this with
// different perGB/cap pairs - a decode pass pulls bytes roughly proportional
// to file size, a metadata probe only seeks the header and tail.
func (f *ffprobeChecker) scaledTimeout(fileBytes int64, perGB, capAt time.Duration) time.Duration {
	if fileBytes <= 0 {
		return f.timeout
	}
	sizeGB := float64(fileBytes) / (1024 * 1024 * 1024)
	scaled := f.timeout + time.Duration(sizeGB*float64(perGB))
	if scaled > capAt {
		scaled = capAt
	}
	return scaled
}

type ffprobeOutput struct {
	Format struct {
		Duration string `json:"duration"`
	} `json:"format"`
	Streams []struct {
		CodecType string `json:"codec_type"`
	} `json:"streams"`
}

// budgetStats annotates a verdict log event with the verification read's
// byte accounting from its VerifyBudget: how much ffprobe actually pulled
// (served_mb), against the cap (budget_mb / budget_cut), the effective
// throughput (mib_s), and how much of the wall time was spent blocked on the
// fetch (read_wait_ms). This replaces the former per-range-request
// "verification range served" line, which fired dozens to hundreds of times
// per probe. Returns ev unchanged when there is no budget (unknown file
// size). Safe to chain: the returned event is still the same event.
func budgetStats(ev *zerolog.Event, b *VerifyBudget) *zerolog.Event {
	if b == nil {
		return ev
	}
	ev = ev.Int64("served_mb", b.Used()>>20).
		Int64("range_gets", b.Reads()).
		Int64("read_wait_ms", b.Wait().Milliseconds())
	if b.Limit() > 0 {
		ev = ev.Int64("budget_mb", b.Limit()>>20).Bool("budget_cut", b.Cut())
	}
	// An Exhaust is a stop, not a byte cut. Give it its own key so a line with
	// budget_cut=false and served_mb far below budget_mb still says why the
	// verification ended.
	if b.Exceeded() && !b.Cut() {
		ev = ev.Bool("budget_exhausted", true)
	}
	if s := b.MiBPerSec(); s > 0 {
		ev = ev.Float64("mib_s", s)
	}
	return ev
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
//
// coverage is how much of the file a decode verdict covered -
// decodeCoverageFull, or decodeCoveragePartial for a bounded head scan of a
// file with no usable container index (see decodeWindows). Empty whenever the
// result did not come from a decode pass.
func (f *ffprobeChecker) check(ctx context.Context, entryFolder, fileName string, expected expectedRuntime, skipDecode bool, budget *VerifyBudget) (ok bool, reason string, conclusive bool, coverage string) {
	args := f.probeArgs(entryFolder, fileName, []string{"-v", "error", "-print_format", "json", "-show_format", "-show_streams"})

	metaTimeout := f.scaledTimeout(expected.Bytes, ffprobeMetadataTimeoutPerGB, ffprobeMetadataTimeoutCap)
	cctx, cancel := context.WithTimeout(ctx, metaTimeout)
	defer cancel()

	cmd := exec.CommandContext(cctx, f.binPath, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	start := time.Now()
	runErr := cmd.Run()
	metaElapsed := time.Since(start)

	if cctx.Err() != nil {
		if errors.Is(cctx.Err(), context.DeadlineExceeded) {
			budgetStats(f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).
				Dur("timeout", metaTimeout).Dur("elapsed", metaElapsed).Int64("file_bytes", expected.Bytes), budget).
				Msg("Repair: ffprobe timed out; treating as inconclusive")
		}
		return true, "", false, ""
	}

	if metaElapsed >= ffprobeMetadataSlowThreshold {
		budgetStats(f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).
			Dur("timeout", metaTimeout).Dur("elapsed", metaElapsed).Int64("file_bytes", expected.Bytes), budget).
			Msg("Repair: ffprobe metadata probe was slow")
	}
	// The metadata probe reads through the same budget as the decode pass, so
	// it can be the read that spends it (or find it already spent by an
	// earlier pass). Either way the body it saw was cut short by us, and both
	// failure modes below - a non-zero exit and unparseable JSON - are what a
	// truncated container looks like. Never let that become a broken verdict.
	if budget.Exceeded() {
		budgetStats(f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).
			Dur("elapsed", metaElapsed), budget).
			Msg("Repair: ffprobe metadata probe exceeded its read budget; treating as inconclusive")
		return true, "", false, ""
	}

	if runErr != nil {
		return false, ffprobeReasonUnreadable + ": " + firstLine(stderr.String()), true, ""
	}

	var probe ffprobeOutput
	if err := json.Unmarshal(stdout.Bytes(), &probe); err != nil {
		return false, ffprobeReasonUnreadable + ": " + firstLine(err.Error()), true, ""
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
		return false, ffprobeReasonNoStreams, true, ""
	}
	if !hasVideo {
		return false, ffprobeReasonNoVideoStream, true, ""
	}

	durationSec, err := strconv.ParseFloat(strings.TrimSpace(probe.Format.Duration), 64)
	if err != nil || durationSec <= 0 {
		return false, ffprobeReasonNoDuration, true, ""
	}
	duration := time.Duration(durationSec * float64(time.Second))

	if expected.Seconds <= 0 {
		ceiling := ffprobeCeilingOther
		if expected.ArrKind == storage.ArrKindSonarr {
			ceiling = ffprobeCeilingSonarr
		}
		if duration > ceiling {
			return false, ffprobeReasonAbsurdDuration, true, ""
		}
		if !skipDecode {
			return f.decodeWindows(ctx, entryFolder, fileName, duration, expected.Bytes, budget)
		}
		// Decode verification was skipped for this call, so nothing was
		// deep-verified - not conclusive.
		return true, "", false, ""
	}

	expectedDur := time.Duration(expected.Seconds) * time.Second
	ratio := duration.Seconds() / expectedDur.Seconds()

	tooLongRatio := ffprobeTooLongRatioConfirmed
	if !expected.EpisodeCountConfirmed {
		tooLongRatio = ffprobeTooLongRatioUnconfirmed
	}
	if ratio >= tooLongRatio && duration-expectedDur >= ffprobeTooLongMinOverage {
		return false, fmt.Sprintf("%s: probe=%dm expected=%dm (%.1fx)", ffprobeReasonRuntimeMismatch, int(duration.Minutes()), int(expectedDur.Minutes()), ratio), true, ""
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
				return f.decodeWindows(ctx, entryFolder, fileName, duration, expected.Bytes, budget)
			}
			return true, "", false, ""
		}
		return false, fmt.Sprintf("%s: probe=%dm expected=%dm (%.1fx); tail_unreadable", ffprobeReasonRuntimeMismatch, int(duration.Minutes()), int(expectedDur.Minutes()), ratio), true, ""
	}

	// Grey zone: meaningfully different from expected but inside the safety
	// bands (extended cuts, specials, padded finales). Never auto-delete on
	// ambiguity - log it and let the file stand.
	if ratio < 0.9 || ratio > 1.1 {
		f.logger.Info().Str("entry", entryFolder).Str("file", fileName).Float64("ratio", ratio).Msg("Repair: ffprobe duration differs from expected but within tolerance; not marking broken")
	}
	if !skipDecode {
		return f.decodeWindows(ctx, entryFolder, fileName, duration, expected.Bytes, budget)
	}
	return true, "", false, ""
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

// decodeCoverageFull / decodeCoveragePartial label how much of a file a decode
// verdict covers. "partial" means the file has no usable container index and
// only a bounded prefix was scanned - see decodeWindows.
const (
	decodeCoverageFull    = "full"
	decodeCoveragePartial = "partial"
)

// Decode probe phases, as named in logs and handed to runDecodeProbe.
const (
	decodePhaseSpread = "spread"
	decodePhaseDetect = "detect"
	decodePhaseHead   = "head"
)

// spreadIntervals builds the evenly-spaced -read_intervals list that samples
// the whole duration - the normal decode check.
func (f *ffprobeChecker) spreadIntervals(duration time.Duration, fileBytes int64) []string {
	// Adaptive window count: a large file (REMUX) gets fewer windows so the
	// probe pulls ~120-150 MB of cold random I/O over WebDAV instead of
	// ~300-450 MB. Still spans the whole duration.
	n := ffprobeDecodeWindowCount
	if fileBytes > ffprobeDecodeWindowLargeFileSize {
		n = ffprobeDecodeWindowCountLarge
	}
	if durSec := int(duration.Seconds()); durSec < n {
		n = durSec
	}
	if n <= 0 {
		return nil
	}
	intervals := make([]string, n)
	for i := range n {
		startSec := duration * time.Duration(i) / time.Duration(n)
		intervals[i] = fmt.Sprintf("%.0f%%+%.0f", startSec.Seconds(), ffprobeDecodeWindowSpan.Seconds())
	}
	return intervals
}

// runDecodeProbe executes one frame-decode ffprobe over intervals and
// classifies the outcome. phase names the probe in logs; budget is the one the
// read path meters this probe's range requests against (the file budget for
// the spread, a phase budget for detect and head) and is reported on the log
// line.
//
// A spent budget is checked before stderr or the exit code: once we cut the
// body, whatever ffprobe reports describes our truncation, not the file, so
// the result can only be inconclusive. decodeWindows re-checks the budget
// itself before acting on the result, because for seek detection the cut is
// the signal.
func (f *ffprobeChecker) runDecodeProbe(ctx context.Context, entryFolder, fileName, phase string, intervals []string, timeout time.Duration, fileBytes int64, budget *VerifyBudget) (ok bool, reason string, conclusive bool) {
	args := f.probeArgs(entryFolder, fileName, []string{
		"-v", "error",
		"-read_intervals", strings.Join(intervals, ","),
		"-select_streams", "v:0",
		"-show_entries", "frame=pts_time",
		"-of", "csv=p=0",
	})

	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(cctx, f.binPath, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	start := time.Now()
	runErr := cmd.Run()
	elapsed := time.Since(start)

	// One stdout line per decoded frame. When ffprobe is stopped mid-scan
	// (timeout, or the body cut) stdout holds whatever it managed to print, so
	// this is the only per-probe measure of how far the decode actually got.
	frames := bytes.Count(stdout.Bytes(), []byte("\n"))
	evt := func() *zerolog.Event {
		return budgetStats(f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).
			Str("phase", phase).Int("windows", len(intervals)).Dur("timeout", timeout).
			Int64("file_bytes", fileBytes).Dur("elapsed", elapsed).Int("frames", frames), budget)
	}

	if cctx.Err() != nil {
		if errors.Is(cctx.Err(), context.DeadlineExceeded) {
			evt().Msg("Repair: ffprobe decode check timed out; treating as inconclusive")
		}
		return true, "", false
	}

	// A blown byte budget means *we* cut the stream short mid-probe, so
	// whatever ffprobe reports from here on describes our truncation, not the
	// file: a container error is exactly what a truncated read produces. This
	// must be checked before stderr/exit-code interpretation, and can only
	// ever be inconclusive - never a broken verdict, which would blocklist a
	// grab on the strength of our own cap.
	if budget.Exceeded() {
		evt().Msg("Repair: ffprobe decode check hit its read budget")
		return true, "", false
	}

	// Decode errors surface on stderr even when the exit code is 0 (ffprobe
	// reports per-frame codec errors but still exits cleanly when it can
	// continue demuxing). Check stderr first.
	if stderrStr := strings.TrimSpace(stderr.String()); stderrStr != "" {
		evt().Msg("Repair: ffprobe decode check found a decode error")
		return false, ffprobeReasonDecodeError + ": " + firstLine(stderrStr), true
	}

	if runErr != nil {
		evt().Err(runErr).Msg("Repair: ffprobe decode check exited non-zero")
		return false, ffprobeReasonDecodeError, true
	}

	evt().Msg("Repair: ffprobe decode check passed")
	return true, "", true
}

// decodeWindows verifies the video stream actually decodes to frames, not just
// that the container demuxes. The demux-only check above reads container
// metadata and packet headers but never asks the codec to reconstruct a frame,
// so a file with a valid container but corrupt compressed data passes
// undetected. This method closes that gap.
//
// It samples evenly-spaced windows across the duration - EXCEPT on a file
// ffmpeg cannot seek in. With no usable container index (Matroska without
// Cues: most VC-1/MPEG-2 REMUXes, ~3% of AVC ones; and web episodes that
// behave the same way on this read path, cause unknown) ffmpeg reaches each
// window by reading forward from the last, so the sample degenerates into a
// read of 84-100% of the file. Reaching byte offset X in such a file costs X,
// so no sampling scheme can verify more than a prefix; the only real choice is
// how large a prefix to pay for. Whether a file seeks is measured in bytes,
// not time: each probe below runs under its own byte budget (a phase, see
// VerifyBudget.BeginPhase), which the read path cuts it against.
//
//   - No budget, or fileBytes <= headBytes: the normal spread on the file
//     budget, exactly as before. Detection needs a budget to cut against, and
//     a file this small costs no more to sample in full than a head scan.
//   - Otherwise a detect probe: two windows, offset 0 and one placed so an
//     unseekable file must read detectBytes to reach it, cut at half that.
//     Completing inside the cut means the file seeks: the normal spread.
//     Being cut means it does not: the head scan. A decode error on an uncut
//     body is a real verdict; a timeout or cancellation short of the cut
//     proves nothing.
//   - Head scan: one interval from offset 0 over the seconds headBytes covers
//     at the file's average bitrate, cut at 1.5x headBytes, reported as
//     partial coverage. If it is cut or times out it reaches no verdict, and
//     the verification is exhausted so no retry pays for the same scan again.
//
// Phases are independent of the file budget: their bytes are not charged to
// it and its limit does not cap them, so the spread keeps exactly the cap it
// had and no sizing of one phase can starve another. The price is that one
// verification no longer has a single hard byte ceiling. A healthy unseekable
// file costs a detect cut plus a head scan (~1 + ~3 GiB at the defaults); a
// broken one can cost that twice, because checkConfirmed retries a broken
// verdict once, before it is marked broken (at most 2 x (1 + 4.5) GiB).
//
// Every phase checks its own budget before anything ffprobe reported: a cut
// body yields container errors that describe our truncation, so a spent phase
// budget can route to the next phase or end inconclusive, never return broken.
//
// Like every other probe in this file, timeout and cancellation are
// inconclusive (ok=true, fail-open): a slow WebDAV read must never
// auto-delete a file that might be fine. conclusive distinguishes a real
// decode verdict from a pass that proved nothing. coverage names the pass a
// verdict came from - decodeCoverageFull for the spread, decodeCoveragePartial
// for the head scan - and is empty when the check ended before either ran.
func (f *ffprobeChecker) decodeWindows(ctx context.Context, entryFolder, fileName string, duration time.Duration, fileBytes int64, budget *VerifyBudget) (ok bool, reason string, conclusive bool, coverage string) {
	if duration <= 0 {
		return true, "", true, decodeCoverageFull
	}
	// Phases do not draw on the file budget, so a verification that is already
	// spent - cut, or exhausted by an earlier head scan - must stop here rather
	// than open a phase and read again.
	if budget.Exceeded() {
		budgetStats(f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).
			Int64("file_bytes", fileBytes), budget).
			Msg("Repair: ffprobe decode check skipped; verification read budget already spent")
		return true, "", false, ""
	}

	run := f.runProbeFn
	if run == nil {
		run = f.runDecodeProbe
	}

	spread := func() (bool, string, bool, string) {
		intervals := f.spreadIntervals(duration, fileBytes)
		if len(intervals) == 0 {
			return true, "", true, decodeCoverageFull
		}
		// Scale the timeout with file size: base + sizeGB*perGB, clamped. A
		// REMUX has far more compressed data to pull through the decode windows
		// than a WEB episode, so a flat budget times out on the big ones and
		// wastes time on the small ones.
		timeout := f.scaledTimeout(fileBytes, ffprobeDecodeTimeoutPerGB, ffprobeDecodeTimeoutCap)
		ok, reason, conclusive := run(ctx, entryFolder, fileName, decodePhaseSpread, intervals, timeout, fileBytes, budget)
		return ok, reason, conclusive, decodeCoverageFull
	}

	headBytes := f.headBytes
	if headBytes <= 0 {
		headBytes = defaultDecodeHeadBytes
	}
	detectBytes := f.detectBytes
	if detectBytes <= 0 {
		detectBytes = defaultDecodeDetectBytes
	}

	if budget == nil || fileBytes <= headBytes {
		return spread()
	}
	detectOffset := time.Duration(float64(duration) * float64(detectBytes) / float64(fileBytes))
	detectCap := detectBytes / decodeDetectCapDivisor
	if detectOffset <= 0 || detectOffset >= duration || detectCap <= 0 {
		return spread()
	}

	// runPhase runs one probe under its own phase budget, which the read path
	// meters every range request of that probe against. The timeout is only a
	// backstop for a stuck read - the cut is what bounds the phase - so it is
	// the ceiling every decode probe already has, generous enough that a slow
	// but progressing forward scan still reaches its cut or its verdict
	// (sweep forward scans have run below 17 MiB/s).
	runPhase := func(phase string, intervals []string, limit int64) (p *VerifyBudget, ok bool, reason string, conclusive bool) {
		p = budget.BeginPhase(limit)
		defer budget.EndPhase(p)
		ok, reason, conclusive = run(ctx, entryFolder, fileName, phase, intervals, ffprobeDecodeTimeoutCap, fileBytes, p)
		return p, ok, reason, conclusive
	}

	detect, ok, reason, conclusive := runPhase(decodePhaseDetect, []string{
		fmt.Sprintf("%.0f%%+%.0f", 0.0, ffprobeDecodeWindowSpan.Seconds()),
		fmt.Sprintf("%.0f%%+%.0f", detectOffset.Seconds(), ffprobeDecodeWindowSpan.Seconds()),
	}, detectCap)
	switch {
	case detect.Exceeded():
		// Cut before reaching the window: no usable index. Go to the head scan
		// whatever ffprobe said - an error on a body we cut describes the cut -
		// and since the head scan re-reads this prefix from offset 0 under a
		// fresh budget, a real error in it is found again there.
	case !ok:
		// A decode error at offset 0 or at the detect window, on an intact
		// body. Whether the file seeks no longer matters.
		return false, reason, conclusive, ""
	case !conclusive:
		// Timed out or cancelled short of the cut: says nothing about the
		// file, and in particular not that it cannot seek.
		return true, "", false, ""
	default:
		// Reached the window inside the cut: the file seeks, and everything
		// from here is the normal check.
		return spread()
	}

	headSec := duration.Seconds() * float64(headBytes) / float64(fileBytes)
	if minSec := ffprobeDecodeWindowSpan.Seconds(); headSec < minSec {
		headSec = minSec
	}
	headCap := headBytes / decodeHeadCapDen * decodeHeadCapNum
	budgetStats(f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).
		Int64("file_bytes", fileBytes).Float64("head_seconds", headSec).
		Int64("head_cap_mb", headCap>>20), detect).
		Msg("Repair: file has no usable container index; scanning a bounded head instead of the full file")

	head, ok, reason, conclusive := runPhase(decodePhaseHead, []string{
		fmt.Sprintf("%.0f%%+%.0f", 0.0, headSec),
	}, headCap)
	if head.Exceeded() || (ok && !conclusive) {
		// Cut, timed out, or cancelled before the end of its interval: no
		// verdict, and a retry would only pay for the same prefix again.
		// Exhaust the verification so no retry layer repeats the scan.
		budget.Exhaust()
		budgetStats(f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).
			Int64("file_bytes", fileBytes), head).
			Msg("Repair: bounded head scan reached no verdict; treating as inconclusive and not retrying")
		return true, "", false, decodeCoveragePartial
	}
	return ok, reason, conclusive, decodeCoveragePartial
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
//
// coverage is threaded through from check alongside conclusive; the
// budget-spent and cancelled returns carry none.
func (f *ffprobeChecker) checkConfirmed(ctx context.Context, entryFolder, fileName string, expected expectedRuntime, skipDecode bool, deadSignal *DeadSegmentSignal, budget *VerifyBudget) (ok bool, reason string, conclusive bool, coverage string) {
	ok, reason, conclusive, coverage = f.check(ctx, entryFolder, fileName, expected, skipDecode, budget)
	if deadSignal.Detected() {
		f.logger.Warn().Str("entry", entryFolder).Str("file", fileName).Bool("ffprobe_ok", ok).
			Msg("Repair: dead segment (NNTP 430) observed during ffprobe verification read; forcing broken verdict")
		return false, ffprobeReasonDeadSegment, true, coverage
	}
	if ok {
		return true, "", conclusive, coverage
	}
	// This must come BEFORE the unreadable short-circuit below. A spent
	// budget means we truncated the body ourselves, and the single most
	// likely thing a truncated container produces is exactly an
	// "ffprobe_unreadable" failure - letting that jump the queue would
	// blocklist a grab on the strength of our own cap. The budget is also
	// shared across passes, so a retry could only re-truncate: stop here
	// rather than burn a second full pass to re-learn that (this is the
	// multiplication that turned one 4.25 GB grab into 11.16 GB of reads).
	if budget.Exceeded() {
		budgetStats(f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).Str("reason", reason), budget).
			Msg("Repair: skipping ffprobe retry — verification read budget already spent")
		return true, "", false, ""
	}
	if strings.HasPrefix(reason, ffprobeReasonUnreadable) {
		f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).Str("reason", reason).Msg("[repair] Repair: skipping ffprobe retry — unreadable error is permanent")
		return false, reason, conclusive, coverage
	}
	f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).Str("reason", reason).Msg("Repair: ffprobe check failed; retrying once before declaring broken")

	select {
	case <-ctx.Done():
		return true, "", false, ""
	case <-time.After(ffprobeRetryDelay):
	}

	ok, reason, conclusive, coverage = f.check(ctx, entryFolder, fileName, expected, skipDecode, budget)
	f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).Bool("ok", ok).Str("reason", reason).Msg("Repair: ffprobe retry result")
	// A budget spent during the retry taints that pass the same way - the
	// metadata probe can fail on a body we cut short - so it can only be
	// inconclusive, never broken.
	if budget.Exceeded() {
		budgetStats(f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).Str("reason", reason), budget).
			Msg("Repair: ffprobe retry exceeded its read budget; treating as inconclusive")
		return true, "", false, ""
	}
	if deadSignal.Detected() {
		f.logger.Warn().Str("entry", entryFolder).Str("file", fileName).Bool("ffprobe_ok", ok).
			Msg("Repair: dead segment (NNTP 430) observed during ffprobe verification retry; forcing broken verdict")
		return false, ffprobeReasonDeadSegment, true, coverage
	}
	if ok {
		return true, "", conclusive, coverage
	}
	return false, reason, conclusive, coverage
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
