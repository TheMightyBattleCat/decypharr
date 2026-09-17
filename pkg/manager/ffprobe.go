package manager

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os/exec"
	"regexp"
	"runtime"
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

	// ffprobeReasonDecodedThrough is not a broken verdict. runDecodeProbe
	// returns it with ok=true and conclusive=false when ffprobe printed errors
	// but still decoded to the end of its interval, so decodeWindows can end
	// the verification instead of letting a retry pay for the same result.
	ffprobeReasonDecodedThrough = "ffprobe_errors_decoded_through"
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

	// maxDecodeThreads caps decodeThreadsFor.
	maxDecodeThreads = 4

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
	// The scan's interval is sized from the file's AVERAGE bitrate,
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

	// decodeThreads is the -threads value for frame-decode probes (1 leaves
	// ffprobe's single-threaded default). See decodeThreadsFor.
	decodeThreads int

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
		// cfg.Repair.Workers is defaulted to 5 when unset (config.go), so
		// workers is never 0 here in production.
		decodeThreads: decodeThreadsFor(runtime.NumCPU(), cfg.Repair.Workers),
		authToken:     authToken,
		logger:        log,
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
	Streams []ffprobeStream `json:"streams"`
}

type ffprobeStream struct {
	CodecType    string `json:"codec_type"`
	CodecName    string `json:"codec_name"`
	AvgFrameRate string `json:"avg_frame_rate"`
	RFrameRate   string `json:"r_frame_rate"`
	Disposition  struct {
		AttachedPic int `json:"attached_pic"`
	} `json:"disposition"`
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
	// served_mb is what the budget charged; written_mb what the range
	// requests handed to ffprobe. See reader.VerifyBudget.ObserveRequest.
	ev = ev.Int64("served_mb", b.Used()>>20).
		Int64("written_mb", b.Written()>>20).
		Int64("requests", b.Requests()).
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
	scope := decodeScopeFromContext(ctx)
	if scope == nil {
		scope = &decodeScope{}
		ctx = contextWithDecodeScope(ctx, scope)
	}
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
		noteUnverified(ctx, timeoutCause(ctx))
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
		noteUnverified(ctx, unverifiedReadBudget)
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
	scope.coverArt = coverArtDecoders(probe.Streams)
	scope.fps = videoFrameRate(probe.Streams)

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
		f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).Float64("ratio", ratio).Msg("Repair: ffprobe duration differs from expected but within tolerance; not marking broken")
	}
	if !skipDecode {
		return f.decodeWindows(ctx, entryFolder, fileName, duration, expected.Bytes, budget)
	}
	return true, "", false, ""
}

type ffprobeTailOutput struct {
	Packets []ffprobeTailPacket `json:"packets"`
}

type ffprobeTailPacket struct {
	PtsTime string `json:"pts_time"`
	DtsTime string `json:"dts_time"`
}

// ffprobeTailNearEnd is how close to the probed duration a packet must sit for
// tailIntact to call the stream complete.
const ffprobeTailNearEnd = 45 * time.Second

// packetNearEnd reports whether any packet's timestamp lies within
// ffprobeTailNearEnd of probed. A packet without a PTS is placed by its DTS:
// Matroska stores VC-1 (and any other V_MS/VFW/FOURCC track) in VfW mode, and
// ffmpeg gives those packets a DTS only, so reading PTS alone found no packet
// near the end of any VC-1 file and turned a too-short runtime into a broken
// verdict.
func packetNearEnd(packets []ffprobeTailPacket, probed time.Duration) bool {
	for _, p := range packets {
		sec, err := strconv.ParseFloat(strings.TrimSpace(p.PtsTime), 64)
		if err != nil {
			if sec, err = strconv.ParseFloat(strings.TrimSpace(p.DtsTime), 64); err != nil {
				continue
			}
		}
		diff := probed - time.Duration(sec*float64(time.Second))
		if diff < 0 {
			diff = -diff
		}
		if diff <= ffprobeTailNearEnd {
			return true
		}
	}
	return false
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
	interval := decodeInterval(start, readSpan, decodeScopeFromContext(ctx).frameRate())

	args := f.probeArgs(entryFolder, fileName, []string{
		"-v", "error",
		"-read_intervals", interval,
		"-select_streams", "V:0",
		"-show_entries", "packet=pts_time,dts_time",
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
	return packetNearEnd(out.Packets, probed)
}

// decodeScope is what one verification learns and reuses across its probes.
// checkConfirmed opens one for both of its passes; check opens its own when
// called without one.
type decodeScope struct {
	// seek is the detect probe's classification of the file, kept so a retry
	// of the check does not pay for detection again. seekUnknown until a
	// detect probe classifies the file.
	seek seekState
	// coverArt names the decoders of attached-picture streams (cover art)
	// whose codec no other stream shares. ffprobe decodes attached pictures
	// while it opens the file, whatever -select_streams asks for, so a corrupt
	// cover prints a decoder error on every probe of a file whose video is
	// fine. Set by check from the metadata probe.
	coverArt map[string]bool
	// fps is the selected video stream's frame rate, or 0 when the metadata
	// probe gave none usable. Set by check; decodeInterval ends every read
	// interval by a frame count derived from it.
	fps float64
}

func (s *decodeScope) frameRate() float64 {
	if s == nil {
		return 0
	}
	return s.fps
}

// Frame rates outside this range are treated as unknown: a container's guess
// for a variable-rate or field-coded stream can be far off, and a wrong rate
// sizes every window by the same factor.
const (
	decodeMinFrameRate = 5
	decodeMaxFrameRate = 120
)

// videoFrameRate returns the frame rate of the stream -select_streams V:0
// picks - the first video stream that is not an attached picture - from its
// avg_frame_rate, else its r_frame_rate, or 0 when neither is a usable
// "num/den" inside [decodeMinFrameRate, decodeMaxFrameRate].
func videoFrameRate(streams []ffprobeStream) float64 {
	for _, s := range streams {
		if s.CodecType != "video" || s.Disposition.AttachedPic != 0 {
			continue
		}
		for _, r := range []string{s.AvgFrameRate, s.RFrameRate} {
			if fps := parseFrameRate(r); fps >= decodeMinFrameRate && fps <= decodeMaxFrameRate {
				return fps
			}
		}
		return 0
	}
	return 0
}

// parseFrameRate reads ffprobe's "num/den" rational, 0 when it is not one.
func parseFrameRate(r string) float64 {
	num, den, ok := strings.Cut(strings.TrimSpace(r), "/")
	if !ok {
		return 0
	}
	n, err1 := strconv.ParseFloat(num, 64)
	d, err2 := strconv.ParseFloat(den, 64)
	if err1 != nil || err2 != nil || d <= 0 || n <= 0 {
		return 0
	}
	return n / d
}

// decodeInterval builds one -read_intervals entry: from start, for span. With
// a known frame rate the span is a frame count ("START%+#N"), which ffprobe
// ends by counting the selected stream's packets. A span in seconds
// ("START%+S") ends at the first packet whose PTS reaches start+span, and
// Matroska VfW-mode tracks (VC-1 in every REMUX checked) carry no PTS, so such
// an interval never ended: the detect probe read to its cut and called a file
// with intact Cues unseekable, and the head scan read to its cut and never
// reached a verdict. Without a frame rate the seconds form is the only option.
func decodeInterval(start, span time.Duration, fps float64) string {
	if fps > 0 {
		frames := max(1, int(math.Ceil(span.Seconds()*fps)))
		return fmt.Sprintf("%.0f%%+#%d", start.Seconds(), frames)
	}
	return fmt.Sprintf("%.0f%%+%.0f", start.Seconds(), span.Seconds())
}

type seekState int

const (
	seekUnknown seekState = iota
	canSeek
	cannotSeek
)

func (s *decodeScope) seekResult() seekState {
	if s == nil {
		return seekUnknown
	}
	return s.seek
}

func (s *decodeScope) rememberSeek(v seekState) {
	if s != nil {
		s.seek = v
	}
}

type decodeScopeCtxKey struct{}

func contextWithDecodeScope(ctx context.Context, s *decodeScope) context.Context {
	return context.WithValue(ctx, decodeScopeCtxKey{}, s)
}

func decodeScopeFromContext(ctx context.Context) *decodeScope {
	s, _ := ctx.Value(decodeScopeCtxKey{}).(*decodeScope)
	return s
}

// coverArtDecoders returns the codec names of the file's attached-picture
// streams that no other stream shares, or nil. Only those can be attributed:
// ffmpeg prefixes a decoder's log lines with the decoder's name, not the
// stream's index.
func coverArtDecoders(streams []ffprobeStream) map[string]bool {
	var covers map[string]bool
	for _, s := range streams {
		if s.Disposition.AttachedPic == 1 && s.CodecName != "" {
			if covers == nil {
				covers = make(map[string]bool)
			}
			covers[s.CodecName] = true
		}
	}
	for _, s := range streams {
		if s.Disposition.AttachedPic == 0 {
			delete(covers, s.CodecName)
		}
	}
	if len(covers) == 0 {
		return nil
	}
	return covers
}

// decoderLogLine matches the "[<decoder> @ 0x<address>] " prefix ffmpeg puts
// on a decoder's log lines.
var decoderLogLine = regexp.MustCompile(`^\[([A-Za-z0-9_]+) @ 0x[0-9A-Fa-f]+\] `)

// dropDecoderLines removes from stderr the lines printed by any of decoders
// and returns what is left, trimmed, plus the first line removed. A line with
// no decoder prefix is always kept: only output attributable to one of
// decoders may be dropped.
func dropDecoderLines(stderr string, decoders map[string]bool) (kept, dropped string) {
	stderr = strings.TrimSpace(stderr)
	if len(decoders) == 0 || stderr == "" {
		return stderr, ""
	}
	lines := strings.Split(stderr, "\n")
	out := lines[:0]
	for _, line := range lines {
		if m := decoderLogLine.FindStringSubmatch(line); m != nil && decoders[m[1]] {
			if dropped == "" {
				dropped = line
			}
			continue
		}
		out = append(out, line)
	}
	return strings.TrimSpace(strings.Join(out, "\n")), dropped
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
	// decodePhaseSpreadLead re-decodes a spread's windows from
	// decodeSpreadLead earlier, after codec errors - see decodeWindows.
	decodePhaseSpreadLead = "spread_lead"
	// decodePhaseDetectLead re-decodes the detect window from
	// decodeSpreadLead earlier, after codec errors - see detectLead.
	decodePhaseDetectLead = "detect_lead"
)

// decodeSpreadLead is how far before each window a codec-errors spread is
// decoded again. An H.264 Blu-ray window that starts on an I-frame whose
// slices need the frames before it prints "top block unavailable" / "error
// while decoding MB 0 0" for its first frames, though the same frames decode
// cleanly when the decoder arrives from earlier. Measured on a production install
// 2026-09-17 on the five codec-error windows that did this (Halvard, Bold Q,
// Red Fox 2, Evening Walk, Barnaby & Quill; 12-123 error lines each): a 1 s
// lead-in cleared four and left Halvard's 74 lines, 3 s and 6 s cleared all
// five. Errors that stay (La Belle Odette's "PPS changed between slices", an
// SEI truncated at byte 0) are left unverified as before.
const decodeSpreadLead = 5 * time.Second

// spreadIntervals builds the evenly-spaced -read_intervals list that samples
// the whole duration - the normal decode check.
func (f *ffprobeChecker) spreadIntervals(duration time.Duration, fileBytes int64, fps float64) []string {
	return f.spreadIntervalsLead(duration, fileBytes, fps, 0)
}

// spreadIntervalsLead is spreadIntervals with each window starting lead
// earlier (not before 0) and running through the same frames.
func (f *ffprobeChecker) spreadIntervalsLead(duration time.Duration, fileBytes int64, fps float64, lead time.Duration) []string {
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
		from := max(0, startSec-lead)
		intervals[i] = decodeInterval(from, ffprobeDecodeWindowSpan+(startSec-from), fps)
	}
	return intervals
}

// recordedDecodeCause returns the decode-error cause a probe noted on ctx
// (noteDecodeErrors), or "" when none was.
func recordedDecodeCause(ctx context.Context) string {
	if c, _ := ctx.Value(unverifiedCauseCtxKey{}).(*unverifiedCause); c != nil {
		return c.decodeCause
	}
	return ""
}

// decodeProbeFlags is the ffprobe flag list for one frame-decode probe over
// intervals, without the auth header and target URL (see probeArgs).
func (f *ffprobeChecker) decodeProbeFlags(intervals []string) []string {
	flags := []string{"-v", "error"}
	if f.decodeThreads > 1 {
		// ffprobe leaves the decoder single-threaded unless told otherwise.
		// Measured on the production install (8 cores, 3 probes in parallel, HEVC): 159 fps
		// in total single-threaded, 256 fps with 3 threads each; frames
		// printed and stderr identical.
		flags = append(flags, "-threads", strconv.Itoa(f.decodeThreads))
	}
	return append(flags,
		"-read_intervals", strings.Join(intervals, ","),
		// V, not v: never select an attached picture (cover art), which an
		// MP4 can list ahead of its video track.
		"-select_streams", "V:0",
		// best_effort_timestamp_time exists from ffprobe 4.x through 8.x, and
		// is filled from the DTS when a packet carries none. pts_time did not
		// exist before 5.0, so on the production install's 4.2 every frame printed an empty
		// line. See parseDecodeProgress.
		"-show_entries", "frame=best_effort_timestamp_time",
		"-of", "csv=p=0",
	)
}

// decodeThreadsFor shares the machine's cores between the repair workers
// that run decode probes at the same time: ceil(cpus / workers), between 1
// and maxDecodeThreads. Beyond ~4 threads per probe the production install measured no gain.
func decodeThreadsFor(cpus, workers int) int {
	if workers < 1 {
		workers = 1
	}
	n := (cpus + workers - 1) / workers
	return max(1, min(n, maxDecodeThreads))
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
	args := f.probeArgs(entryFolder, fileName, f.decodeProbeFlags(intervals))

	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(cctx, f.binPath, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	start := time.Now()
	runErr := cmd.Run()
	elapsed := time.Since(start)

	// When ffprobe is stopped mid-scan (timeout, or the body cut) stdout holds
	// whatever it managed to print, so this is the only per-probe measure of
	// how far the decode actually got.
	progress := parseDecodeProgress(stdout.Bytes(), len(intervals))
	evt := func() *zerolog.Event {
		ev := f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).
			Str("phase", phase).Int("windows", len(intervals)).Int("threads", max(1, f.decodeThreads)).Dur("timeout", timeout).
			Int64("file_bytes", fileBytes).Dur("elapsed", elapsed).Int("frames", progress.frames)
		if progress.timestamps > 0 {
			ev = ev.Float64("decoded_to_s", progress.lastTS)
		}
		return budgetStats(ev, budget)
	}

	if cctx.Err() != nil {
		if errors.Is(cctx.Err(), context.DeadlineExceeded) {
			evt().Msg("Repair: ffprobe decode check timed out; treating as inconclusive")
		}
		noteUnverified(ctx, timeoutCause(ctx))
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
		noteUnverified(ctx, unverifiedReadBudget)
		return true, "", false
	}

	// Decode errors surface on stderr even when the exit code is 0 (ffprobe
	// reports per-frame codec errors but still exits cleanly when it can
	// continue demuxing). Check stderr first, less what a cover-art decoder
	// printed (see decodeScope.coverArt).
	var coverArt map[string]bool
	if scope := decodeScopeFromContext(ctx); scope != nil {
		coverArt = scope.coverArt
	}
	stderrStr, ignored := dropDecoderLines(stderr.String(), coverArt)
	if ignored != "" {
		evt().Str("ignored", ignored).Msg("Repair: ignoring a decoder error from the file's cover art")
	}
	if stderrStr != "" {
		lines, n := stderrSummary(stderrStr)
		// Errors that did not stop the decode are not evidence the file is
		// broken. A broken verdict deletes and re-searches the grab, and the
		// retry cannot tell a deterministic false error from real damage
		// (Brom and Lionmarsh, 2026-09-10: "File ended prematurely" on 3 GB
		// head scans that decoded to their end, byte-identical on retry, no
		// reader-side fault). A decode that stopped short of its interval is
		// still broken.
		reached, known := reachedFirstIntervalEnd(intervals, progress)
		if known && reached && runErr == nil && decodeErrorCause(stderrStr) == decodeCauseSeekWarnings {
			// Only the messages ffmpeg prints when decoding starts at a seek
			// point, on a decode that reached its window and exited cleanly:
			// a pass. Measured 2026-09-15 on 6 files (H.264 mmco and
			// reference frames, MPEG-2 pic cod ext, VC-1 no keyframe): every
			// window that printed them printed the same count when decoding
			// 5x the frames from the same seek, and decoded every frame.
			evt().Int("stderr_lines", n).Str("ignored", lines).
				Msg("Repair: ffprobe printed only seek warnings and decoded to the end of its window; counting the check as passed")
			return true, "", true
		}
		if known && reached {
			budgetStats(f.logger.Warn().Str("entry", entryFolder).Str("file", fileName).Str("phase", phase).
				Int("windows", len(intervals)).Dur("elapsed", elapsed).
				Int("frames", progress.frames).Float64("decoded_to_s", progress.lastTS).
				Str("cause", decodeErrorCause(stderrStr)).
				Int("stderr_lines", n).Str("stderr", lines), budget).
				Msg("Repair: ffprobe printed errors but decoded to the end of its window; not treating the file as broken")
			noteDecodeErrors(ctx, stderrStr, lines)
			return true, ffprobeReasonDecodedThrough, false
		}
		evt().Int("stderr_lines", n).Str("stderr", lines).Msg("Repair: ffprobe decode check found a decode error")
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
// Cues, some AVC REMUXes, and web episodes that behave the same way on this
// read path, cause unknown) ffmpeg reaches each window by reading forward from
// the last, so the sample degenerates into a read of 84-100% of the file.
// VC-1 REMUXes were once counted here too; they have intact Cues, and only
// looked unseekable because intervals sized in seconds never end on their
// PTS-less packets (see decodeInterval). Reaching byte offset X in such a file costs X,
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
//     body that stopped the decode is a real verdict; a timeout or
//     cancellation short of the cut proves nothing.
//   - Head scan: one interval from offset 0 over the frames headBytes covers
//     at the file's average bitrate, cut at 1.5x headBytes, reported as
//     partial coverage. If it is cut or times out it reaches no verdict, and
//     the verification is exhausted so no retry pays for the same scan again.
//
// Phases are independent of the file budget: their bytes are not charged to
// it and its limit does not cap them, so the spread keeps exactly the cap it
// had and no sizing of one phase can starve another. The price is that one
// verification no longer has a single hard byte ceiling. A healthy unseekable
// file costs a detect cut plus a head scan (~1 + ~3 GiB at the defaults); a
// broken one pays for the head scan twice, because checkConfirmed retries a
// broken verdict once before it is marked broken, though the retry reuses the
// first pass's detect result (at most 1 + 2 x 4.5 GiB; see decodeScope). That
// retry is kept on purpose: a false broken verdict deletes and re-searches a
// 14-34 GB grab, and a multi-GiB scan touches thousands of segments, so a
// transient fetch error surfacing as a decode error is likelier here than on a
// sparse sample, not less.
//
// Every phase checks its own budget before anything ffprobe reported: a cut
// body yields container errors that describe our truncation, so a spent phase
// budget can route to the next phase or end inconclusive, never return broken.
//
// Errors ffprobe printed while still decoding to the end of its first interval
// are not a verdict in any phase (see runDecodeProbe): the file is left
// unverified, logged at WARN, and the verification is exhausted so neither
// checkConfirmed nor the import gate reads it again for the same answer.
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
		noteUnverified(ctx, unverifiedReadBudget)
		return true, "", false, ""
	}

	run := f.runProbeFn
	if run == nil {
		run = f.runDecodeProbe
	}
	fps := decodeScopeFromContext(ctx).frameRate()

	spread := func() (bool, string, bool, string) {
		intervals := f.spreadIntervals(duration, fileBytes, fps)
		if len(intervals) == 0 {
			return true, "", true, decodeCoverageFull
		}
		// Scale the timeout with file size: base + sizeGB*perGB, clamped. A
		// REMUX has far more compressed data to pull through the decode windows
		// than a WEB episode, so a flat budget times out on the big ones and
		// wastes time on the small ones.
		timeout := f.scaledTimeout(fileBytes, ffprobeDecodeTimeoutPerGB, ffprobeDecodeTimeoutCap)
		ok, reason, conclusive := run(ctx, entryFolder, fileName, decodePhaseSpread, intervals, timeout, fileBytes, budget)
		if ok && reason == ffprobeReasonDecodedThrough && recordedDecodeCause(ctx) == decodeCauseCodec {
			// Codec errors at a window's start can be the decoder starting on
			// a frame that needs the ones before it (decodeSpreadLead). Decode
			// the same frames from earlier, on the same budget: clean means the
			// file decodes and the check passes.
			lead := f.spreadIntervalsLead(duration, fileBytes, fps, decodeSpreadLead)
			ok2, reason2, conclusive2 := run(ctx, entryFolder, fileName, decodePhaseSpreadLead, lead, timeout, fileBytes, budget)
			if ok2 && reason2 == "" && conclusive2 {
				budgetStats(f.logger.Info().Str("entry", entryFolder).Str("file", fileName).Int("windows", len(lead)), budget).
					Msg("Repair: codec errors at the decode windows' starts were not there when decoding from earlier; counting the check as passed")
				return true, "", true, decodeCoverageFull
			}
			ok, reason, conclusive = ok2, reason2, conclusive2
			if !ok {
				// A lead-in pass stopped by a decode error is still judged
				// by checkConfirmed's retry, like any spread.
				return ok, reason, conclusive, decodeCoverageFull
			}
			if reason != ffprobeReasonDecodedThrough {
				// Cut or timed out: its own noteUnverified stands.
				return ok, reason, conclusive, decodeCoverageFull
			}
		}
		if ok && reason == ffprobeReasonDecodedThrough {
			// A retry would print the same errors over the same frames.
			budget.Exhaust()
			noteUnverified(ctx, unverifiedDecodeErrors)
			return true, "", false, decodeCoverageFull
		}
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

	// checkConfirmed's retry runs this whole check again. Once a detect probe
	// has classified the file, the retry reuses that rather than paying for
	// detection twice - up to a 1 GiB cut on a file that cannot seek.
	scope := decodeScopeFromContext(ctx)
	var detect *VerifyBudget
	switch scope.seekResult() {
	case canSeek:
		return spread()
	case cannotSeek:
		// Straight to the head scan below.
	default:
		detect, ok, reason, conclusive = runPhase(decodePhaseDetect, []string{
			decodeInterval(0, ffprobeDecodeWindowSpan, fps),
			decodeInterval(detectOffset, ffprobeDecodeWindowSpan, fps),
		}, detectCap)
		switch {
		case detect.Exceeded():
			// Cut before reaching the window: no usable index. Go to the head
			// scan whatever ffprobe said - an error on a body we cut describes
			// the cut - and since the head scan re-reads this prefix from
			// offset 0 under a fresh budget, a real error in it is found again
			// there.
			scope.rememberSeek(cannotSeek)
		case !ok:
			// A decode error at offset 0 or at the detect window, on an intact
			// body. Whether the file seeks no longer matters, so nothing is
			// remembered and a retry detects again.
			return false, reason, conclusive, ""
		case !conclusive:
			// Timed out or cancelled short of the cut: says nothing about the
			// file, and in particular not that it cannot seek. Errors that
			// decoded through end the verification instead, since a retry
			// would see them again.
			if reason == ffprobeReasonDecodedThrough && recordedDecodeCause(ctx) == decodeCauseCodec {
				// Codec errors at the detect window's start can be the decoder
				// starting on a frame that needs earlier ones, as on the spread
				// (decodeSpreadLead; Emberly and Constitution Day Reckonings
				// on a production install listed codec errors from detect). Decode that
				// window again from earlier; the window at offset 0 cannot
				// start earlier.
				switch r := f.detectLead(ctx, entryFolder, fileName, detectOffset, fps, runPhase, detectCap); {
				case r.passed:
					scope.rememberSeek(canSeek)
					return spread()
				case r.failed:
					return false, r.reason, r.conclusive, ""
				}
			}
			if reason == ffprobeReasonDecodedThrough {
				budget.Exhaust()
				noteUnverified(ctx, unverifiedDecodeErrors)
			} else {
				noteUnverified(ctx, timeoutCause(ctx))
			}
			return true, "", false, ""
		default:
			// Reached the window inside the cut: the file seeks, and everything
			// from here is the normal check.
			scope.rememberSeek(canSeek)
			return spread()
		}
	}

	headSec := duration.Seconds() * float64(headBytes) / float64(fileBytes)
	if minSec := ffprobeDecodeWindowSpan.Seconds(); headSec < minSec {
		headSec = minSec
	}
	headCap := headBytes / decodeHeadCapDen * decodeHeadCapNum
	headInterval := decodeInterval(0, time.Duration(headSec*float64(time.Second)), fps)
	budgetStats(f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).
		Int64("file_bytes", fileBytes).Float64("head_seconds", headSec).Str("head_interval", headInterval).
		Int64("head_cap_mb", headCap>>20).Bool("detect_reused", detect == nil), detect).
		Msg("Repair: file has no usable container index; scanning a bounded head instead of the full file")

	head, ok, reason, conclusive := runPhase(decodePhaseHead, []string{headInterval}, headCap)
	if head.Exceeded() || (ok && !conclusive) {
		// Cut, timed out, cancelled before the end of its interval, or errors
		// that decoded through to it: no verdict, and a retry would only pay
		// for the same prefix again. Exhaust the verification so no retry
		// layer repeats the scan.
		budget.Exhaust()
		budgetStats(f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).
			Int64("file_bytes", fileBytes), head).
			Msg("Repair: bounded head scan reached no verdict; treating as inconclusive and not retrying")
		switch {
		case reason == ffprobeReasonDecodedThrough:
			noteUnverified(ctx, unverifiedDecodeErrors)
		case ctx.Err() != nil:
			noteUnverified(ctx, unverifiedCancelled)
		default:
			noteUnverified(ctx, unverifiedNoSeekIndex)
		}
		return true, "", false, decodeCoveragePartial
	}
	return ok, reason, conclusive, decodeCoveragePartial
}

// detectLeadResult is how a detect lead-in pass ended: passed (clean, so the
// file seeks and the check goes on to the spread), failed (a decode error: a
// failed check, as for any detect probe), or neither (errors again, cut or
// timed out: the first pass's finding stands).
type detectLeadResult struct {
	passed, failed bool
	reason         string
	conclusive     bool
}

// detectLead re-runs the detect phase with its second window starting
// decodeSpreadLead earlier and running through the same frames, under a fresh
// detect cap. When it neither passes nor fails, the decode cause the first pass
// recorded is put back, so the file is listed exactly as it was before.
func (f *ffprobeChecker) detectLead(ctx context.Context, entryFolder, fileName string, detectOffset time.Duration, fps float64,
	runPhase func(phase string, intervals []string, limit int64) (*VerifyBudget, bool, string, bool), detectCap int64) detectLeadResult {
	var saved unverifiedCause
	c, _ := ctx.Value(unverifiedCauseCtxKey{}).(*unverifiedCause)
	if c != nil {
		saved = *c
	}
	from := max(0, detectOffset-decodeSpreadLead)
	lead, ok, reason, conclusive := runPhase(decodePhaseDetectLead, []string{
		decodeInterval(0, ffprobeDecodeWindowSpan, fps),
		decodeInterval(from, ffprobeDecodeWindowSpan+(detectOffset-from), fps),
	}, detectCap)
	switch {
	case !lead.Exceeded() && ok && reason == "" && conclusive:
		budgetStats(f.logger.Info().Str("entry", entryFolder).Str("file", fileName), lead).
			Msg("Repair: codec errors at the detect window's start were not there when decoding from earlier; continuing the check")
		noteUnverified(ctx, "")
		return detectLeadResult{passed: true}
	case !lead.Exceeded() && !ok:
		return detectLeadResult{failed: true, reason: reason, conclusive: conclusive}
	}
	if c != nil {
		*c = saved
	}
	return detectLeadResult{}
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
	// One scope for both passes, so the retry reuses what the first learned.
	ctx = contextWithDecodeScope(ctx, &decodeScope{})
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
		noteUnverified(ctx, unverifiedReadBudget)
		return true, "", false, ""
	}
	if strings.HasPrefix(reason, ffprobeReasonUnreadable) {
		f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).Str("reason", reason).Msg("[repair] Repair: skipping ffprobe retry — unreadable error is permanent")
		return false, reason, conclusive, coverage
	}
	f.logger.Debug().Str("entry", entryFolder).Str("file", fileName).Str("reason", reason).Msg("Repair: ffprobe check failed; retrying once before declaring broken")

	select {
	case <-ctx.Done():
		noteUnverified(ctx, unverifiedCancelled)
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
		noteUnverified(ctx, unverifiedReadBudget)
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

// decodeProgress is what a decode probe's stdout says about how far it got.
type decodeProgress struct {
	frames     int     // decoded frames printed
	timestamps int     // frames that carried a timestamp
	lastTS     float64 // the last timestamp printed, in seconds

	// The same for the first interval alone. ffprobe 4.2 decodes only the
	// first interval of a -read_intervals list: it drains the decoder at the
	// end of each interval and never resets it, so later intervals are
	// demuxed but print no frames. Newer versions decode them all. Progress
	// against the first interval means the same thing on both.
	firstFrames     int
	firstTimestamps int
	firstStartTS    float64
	firstEndTS      float64
}

// decodeIntervalGap is the timestamp jump that separates one interval's frames
// from the next interval's. Frames within an interval are a frame duration
// apart; the spread's windows are minutes apart.
const decodeIntervalGap = 10 * time.Second

// parseDecodeProgress reads the csv that -show_entries
// frame=best_effort_timestamp_time prints: one line per decoded frame, holding
// the timestamp or N/A. A frame's side data (captions, HDR metadata) adds
// empty lines on ffprobe 4.2 - two per frame on many Blu-ray AVC REMUXes - and
// a trailing comma on newer versions, so empty lines are not frames and only
// the first field is read.
//
// intervals is how many intervals the probe asked for. With one, every frame
// belongs to it, whatever its timestamps do; with more, the first interval
// ends at the first jump of decodeIntervalGap forward or any jump back.
func parseDecodeProgress(stdout []byte, intervals int) decodeProgress {
	var p decodeProgress
	inFirst := true
	for line := range strings.SplitSeq(string(stdout), "\n") {
		field, _, _ := strings.Cut(strings.TrimSpace(line), ",")
		if field == "" {
			continue
		}
		p.frames++
		ts, err := strconv.ParseFloat(field, 64)
		hasTS := err == nil
		if hasTS {
			if inFirst && intervals > 1 && p.firstTimestamps > 0 &&
				(ts-p.firstEndTS > decodeIntervalGap.Seconds() || ts < p.firstEndTS-1) {
				inFirst = false
			}
			p.timestamps++
			p.lastTS = ts
		}
		if !inFirst {
			continue
		}
		p.firstFrames++
		if hasTS {
			if p.firstTimestamps == 0 {
				p.firstStartTS = ts
			}
			p.firstTimestamps++
			p.firstEndTS = ts
		}
	}
	return p
}

// intervalLength returns the length a -read_intervals entry asks for: frames
// for "START%+#N", seconds for "START%+S". ok is false for anything else.
func intervalLength(interval string) (frames int, seconds float64, ok bool) {
	_, length, found := strings.Cut(interval, "%+")
	if !found {
		return 0, 0, false
	}
	if n, isFrames := strings.CutPrefix(length, "#"); isFrames {
		if v, err := strconv.Atoi(n); err == nil && v > 0 {
			return v, 0, true
		}
		return 0, 0, false
	}
	if v, err := strconv.ParseFloat(length, 64); err == nil && v > 0 {
		return 0, v, true
	}
	return 0, 0, false
}

// Slack for reachedFirstIntervalEnd: a decode counts as having reached its
// interval's end at 90% of the frames asked for (a decoder can drop frames it
// cannot reference right after a seek) or 90% of the seconds less a quarter
// second (the last frame's timestamp sits one frame duration before the end).
const (
	decodeReachedFraction = 0.9
	decodeReachedSlackSec = 0.25
)

// reachedFirstIntervalEnd reports whether a probe decoded to the end of the
// first of its intervals. known is false when that cannot be told: an interval
// format intervalLength does not read, or a seconds interval with fewer than
// two timestamps to measure.
//
// Only the first interval is judged because it is the only one ffprobe 4.2
// decodes (see decodeProgress). Comparing against every interval would call a
// healthy multi-window spread stopped short on 4.2.
func reachedFirstIntervalEnd(intervals []string, p decodeProgress) (reached, known bool) {
	if len(intervals) == 0 {
		return false, false
	}
	frames, seconds, ok := intervalLength(intervals[0])
	if !ok {
		return false, false
	}
	if frames > 0 {
		need := int(math.Ceil(decodeReachedFraction * float64(frames)))
		return p.firstFrames >= need, true
	}
	if p.firstTimestamps < 2 {
		return false, false
	}
	span := p.firstEndTS - p.firstStartTS
	return span >= decodeReachedFraction*seconds-decodeReachedSlackSec, true
}

// stderrSummaryLines and stderrSummaryLineLen bound what a decode-error log
// line carries of ffprobe's stderr.
const (
	stderrSummaryLines   = 8
	stderrSummaryLineLen = 200
)

// stderrSummary joins the first stderrSummaryLines non-empty lines of stderr,
// each cut to stderrSummaryLineLen bytes, with " | ", and counts every
// non-empty line. The verdict keeps only the first line; this is for the log,
// where the lines after it say whether a container error came alone or with
// codec errors.
func stderrSummary(stderr string) (string, int) {
	var kept []string
	n := 0
	for line := range strings.SplitSeq(stderr, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		n++
		if len(kept) < stderrSummaryLines {
			if len(line) > stderrSummaryLineLen {
				line = line[:stderrSummaryLineLen]
			}
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, " | "), n
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
