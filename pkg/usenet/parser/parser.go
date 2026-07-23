package parser

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Tensai75/nzbparser"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sourcegraph/conc/iter"
)

var ErrMoreRarDataNeeded = fmt.Errorf("rar: need more data")

// ErrReleaseUnavailable marks a Parse failure as a confirmed-damaged release
// - missing segments (the connectivity STAT check) or too many failed PAR2
// source-size probes (buildPar2RefsWithFetch's early abort) - rather than a
// malformed-input error (bad XML, empty content). Callers use errors.Is
// against this to distinguish "this release is genuinely dead, blocklist +
// re-search it" from an ordinary parse error, and to key a short-lived
// negative cache so an identical re-grab of the same dead posting doesn't
// pay for the same STAT/probe round trips again.
var ErrReleaseUnavailable = fmt.Errorf("release unavailable")

var (
	// defaultMaxSnippetSize is used for content-type detection via magic bytes.
	// TS sync-byte check at offset 188 is the deepest we go, so 512 bytes is ample.
	defaultMaxSnippetSize = 512
	// metadataOnly requests the yEnc header (name/size/offsets) without any
	// decoded payload — the connection is drained and returned to the pool.
	metadataOnly = 0
)

const (
	// yencOverheadEstimate approximates a segment's real decoded byte length
	// from its NZB XML-declared (yEnc-ENCODED, wire) byte count, when no real
	// header fetch is available or trustworthy - see realPar2SegmentRefs's
	// doc comment.
	yencOverheadEstimate = 0.97
	// par2ProbeTimeout bounds a single PAR2 source-size probe. Short and
	// non-negotiable: this is an optimization with a documented XML-bytes
	// fallback, not a correctness requirement, so a slow/dead article should
	// fail fast into that fallback rather than eating the connection's full
	// StreamBodyTimeout.
	par2ProbeTimeout = 5 * time.Second
	// par2ProbeMaxFailedFetches is how many failed real yEnc fetches
	// buildPar2RefsWithFetch tolerates before giving up on PAR2 probing for
	// the rest of the release. Past this many dead articles in one release,
	// the post-parse availability check is going to reject it anyway, so
	// further probing only spends more round trips confirming what's already
	// known; the release is marked aborted and every remaining file falls
	// back to the XML-bytes estimate with no further fetches.
	par2ProbeMaxFailedFetches = 3
	// postingSizeToleranceFrac bounds how far a file's own non-final segment
	// byte count (as declared in the NZB XML) may deviate, as a fraction of
	// the expected value, from the release's shared posting article size
	// before that file is deemed inconsistent and gets its own real probe
	// instead of reusing the shared size.
	postingSizeToleranceFrac = 0.10
)

// NZBParser provides a simplified, robust NZB parser
type NZBParser struct {
	logger        zerolog.Logger
	manager       *nntp.Client // Connection manager for parsing operations
	maxConcurrent int          // Max concurrent connections
}

type fileAnalysisResult struct {
	fileSize     int64 // Total decoded size of the NZB file entry.
	lastFileSize int64 // Total decoded size of the last NZB file entry in the group.
	segmentSize  int64 // Decoded size of a single yEnc part/segment.
}

type contentResult struct {
	file           nzbparser.NzbFile
	fileType       storage.NZBFileType
	actualFilename string
	fileSize       int64 // decoded size of the part (from yEnc), if available
	segmentSize    int64 // decoded size of a segment (from yEnc), if available
	partNumber     int64 // yEnc part number, if available
	partBegin      int64 // yEnc begin offset, if available
}

type filePartMeta struct {
	fileSize    int64
	segmentSize int64
	partNumber  int64
	partBegin   int64
}

type FileGroup struct {
	BaseName       string
	ActualFilename string
	Type           storage.NZBFileType
	Files          []nzbparser.NzbFile
	metadata       *fileAnalysisResult
	fileMeta       map[string]filePartMeta
	Groups         map[string]struct{}
}

func (f *FileGroup) getMetadata() *fileAnalysisResult {
	if f.metadata != nil {
		return f.metadata
	}
	// Heuristic: assume segment is ~97% of reported bytes (yEnc overhead)
	if len(f.Files) == 0 || len(f.Files[0].Segments) == 0 {
		return &fileAnalysisResult{}
	}

	metadata := &fileAnalysisResult{}
	// Estimate actual segment size from reported bytes (account for ~3% yEnc overhead)
	reportedBytes := int64(f.Files[0].Segments[0].Bytes)
	if reportedBytes <= 0 {
		reportedBytes = 750000 // Default 750KB segment
	}
	metadata.segmentSize = int64(float64(reportedBytes) * 0.97)
	if metadata.segmentSize <= 0 {
		metadata.segmentSize = reportedBytes
	}
	metadata.fileSize = metadata.segmentSize * int64(len(f.Files[0].Segments))
	metadata.lastFileSize = metadata.segmentSize * int64(len(f.Files[len(f.Files)-1].Segments))
	f.metadata = metadata
	return f.metadata
}

// NewParser creates a new simplified NZB parser with a connection manager
func NewParser(manager *nntp.Client, maxConcurrent int, logger zerolog.Logger) *NZBParser {
	return &NZBParser{
		logger:        logger,
		manager:       manager,
		maxConcurrent: maxConcurrent,
	}
}

var (
	// RAR file patterns - simplified and more accurate
	rarMainPattern       = regexp.MustCompile(`\.rar$`)
	rarPartPattern       = regexp.MustCompile(`\.r\d{2}$`) // .r00, .r01, etc.
	rarVolumePattern     = regexp.MustCompile(`\.part\d+\.rar$`)
	ignoreExtensions     = []string{".sfv", ".nfo", ".jpg", ".png", ".txt", ".srt", ".idx", ".sub"}
	sevenZMainPattern    = regexp.MustCompile(`\.7z$`)
	sevenZPartPattern    = regexp.MustCompile(`\.7z\.\d{3}$`)
	extWithNumberPattern = regexp.MustCompile(`\.[^ "\.]*\.\d+$`)
	volPar2Pattern       = regexp.MustCompile(`(?i)\.vol\d+\+\d+\.par2?$`)
	partPattern          = regexp.MustCompile(`(?i)\.part\d+\.[^ "\.]*$`)
	regularExtPattern    = regexp.MustCompile(`\.[^ "\.]*$`)
)

func (p *NZBParser) Parse(ctx context.Context, filename string, content []byte) (nzb *storage.NZB, groups map[string]*FileGroup, err error) {
	// Recover from panics to prevent crashes
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error().Interface("panic", r).Str("filename", filename).Msg("Panic recovered in Parse")
			err = fmt.Errorf("parse panic: %v", r)
		}
	}()

	// Parse raw XML
	raw, err := nzbparser.Parse(bytes.NewReader(content))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse NZB content: %w", err)
	}

	// Create base NZB structure
	nzb = &storage.NZB{
		Files:    []storage.NZBFile{},
		Status:   "parsed",
		Name:     determineNZBName(filename, raw.Meta),
		Title:    raw.Meta["title"],
		Password: raw.Meta["password"],
	}
	// Group files by base Name and type
	fileGroups := p.groupFiles(ctx, raw.Files)

	if len(fileGroups) == 0 {
		return nil, nil, fmt.Errorf("no valid file groups found in NZB")
	}

	// Confirm availability BEFORE any PAR2 metadata work: a release with
	// missing segments gets rejected (and, per our re-grab-on-any-import-damage
	// policy, re-searched) regardless of what buildPar2Refs would have found,
	// so there is no reason to spend a yEnc body-probe per posted file only to
	// discard the result. Checked first, not just cheaper first: a dead
	// posting fails in one round trip instead of after N probe round trips.
	nzb.Par2Files, nzb.Par2Source, err = availabilityThenPar2Refs(ctx, p.logger, p.maxConcurrent, fileGroups, raw.Files, p.detectFileType, p.statSegment, p.fetchYencHeaderFast)
	if err != nil {
		return nil, nil, err
	}

	nzb.ID = uuid.New().String()
	return nzb, fileGroups, nil
}

// segmentStatFunc confirms a single segment exists on the server. Narrowed
// from *nntp.Client to just this one operation so availabilityThenPar2Refs
// can be exercised with a fake in tests, without a real, provider-backed
// client.
type segmentStatFunc func(ctx context.Context, messageID string) error

func (p *NZBParser) statSegment(ctx context.Context, messageID string) error {
	return p.manager.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
		_, _, statErr := conn.Stat(messageID)
		return statErr
	})
}

// availabilityThenPar2Refs is Parse's availability-check-then-PAR2-probe
// core, with the STAT and yEnc-fetch operations injected so the ordering
// (and buildPar2RefsWithFetch's per-posting reuse / early-abort behavior)
// can be exercised with fakes in tests, without a real, provider-backed NNTP
// client. Only the first segment of the first non-empty group is stat'd -
// this is a connectivity check, not a full availability scan (that's
// Usenet.checkNZBAvailability, later in the pipeline); its only job here is
// to catch a wholesale-dead posting before any PAR2 probing runs.
func availabilityThenPar2Refs(
	ctx context.Context,
	logger zerolog.Logger,
	maxConcurrent int,
	fileGroups map[string]*FileGroup,
	rawFiles nzbparser.NzbFiles,
	detectFileType func(string) storage.NZBFileType,
	statSegment segmentStatFunc,
	fetch yencHeaderFetchFunc,
) (par2Files []storage.Par2FileRef, source []storage.PostedFileRef, err error) {
	checked := false
	for _, group := range fileGroups {
		if len(group.Files) == 0 || len(group.Files[0].Segments) == 0 {
			continue
		}
		segment := group.Files[0].Segments[0]
		if statErr := statSegment(ctx, segment.Id); statErr != nil {
			return nil, nil, fmt.Errorf("failed to stat segment %s <%s>: %w: %w", group.ActualFilename, segment.Id, statErr, ErrReleaseUnavailable)
		}
		checked = true
		break
	}
	if !checked {
		return nil, nil, fmt.Errorf("no segments available to stat in NZB")
	}

	// Retain PAR2 files (parsed but otherwise discarded above) and the exact
	// as-posted segment layout of every other posted file, purely for PAR2
	// repair - see storage.NZB.Par2Files/Par2Source. Computed directly from
	// the flat, pre-grouping raw.Files list so it's independent of whatever
	// RAR/7z/zip grouping and extraction decides to do with these files
	// afterwards. Only reached once the release has passed the availability
	// check above.
	var aborted bool
	par2Files, source, aborted = buildPar2RefsWithFetch(ctx, logger, maxConcurrent, rawFiles, detectFileType, fetch)
	if aborted {
		return nil, nil, fmt.Errorf("PAR2 source-file probing aborted after %d failed article fetches; release likely damaged: %w", par2ProbeMaxFailedFetches, ErrReleaseUnavailable)
	}
	return par2Files, source, nil
}

func (p *NZBParser) Process(ctx context.Context, nzb *storage.NZB, groups map[string]*FileGroup) (result *storage.NZB, err error) {
	// Recover from panics to prevent crashes
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error().Interface("panic", r).Str("nzb", nzb.Name).Msg("Panic recovered in Process")
			err = fmt.Errorf("process panic: %v", r)
		}
	}()

	// Parse each group (with deferred archive option)
	files := p.processFileGroups(ctx, groups, nzb.Password)

	if len(files) == 0 {
		return nil, fmt.Errorf("no valid files found in NZB")
	}

	cfg := config.Get()

	// Change file name if there's only one file
	hasOneFile := len(files) == 1
	skippedFiles := 0
	var skippedErr error
	// Calculate total Size
	for _, file := range files {
		if hasOneFile {
			// Only append extension if NZB name doesn't already have the same extension
			fileExt := filepath.Ext(file.Name)
			nzbExt := filepath.Ext(nzb.Name)
			if fileExt != "" && !strings.EqualFold(nzbExt, fileExt) {
				file.Name = nzb.Name + fileExt
			} else {
				file.Name = nzb.Name
			}
		}
		if err := cfg.IsFileAllowed(file.Name, file.Size); err != nil {
			skippedFiles++
			skippedErr = err
			continue
		}
		nzb.TotalSize += file.Size
		file.NzbID = nzb.ID
		nzb.Files = append(nzb.Files, file)
	}
	if skippedFiles > 0 {
		p.logger.Info().Err(skippedErr).Int("skipped_files", skippedFiles).Str("nzb", nzb.Name).Msg("Some files were skipped due to size or extension restrictions")
	}
	if len(nzb.Files) == 0 {
		if skippedFiles > 0 {
			return nil, fmt.Errorf("all files were skipped due to size or extension restrictions(error %v)", skippedErr)
		}
		return nil, fmt.Errorf("no valid files found in NZB after processing")
	}
	return nzb, nil
}

// par2ProbeCandidate pairs a raw NZB file with its presorted segments and
// its original index in the eligible-files list, so buildPar2RefsWithFetch
// can dispatch every non-seed file through a concurrent iter.Mapper while
// still reassembling results in original order.
type par2ProbeCandidate struct {
	idx  int
	file nzbparser.NzbFile
	segs nzbparser.NzbSegments
}

type builtPar2File struct {
	name     string
	size     int64
	segments []storage.Par2SegmentRef
	isPar2   bool
}

// buildPar2RefsWithFetch is buildPar2Refs with its yEnc header fetch and file
// type classification injected, so it can be exercised with fakes in tests.
//
// A real header fetch (see realPar2SegmentRefs) is only ever attempted for
// one file per release: upload tooling always encodes same-posting volumes
// to a uniform segment byte budget, so once that shared article size is
// known from a single probe, every other file's geometry is checked against
// it (segmentsConsistentWithPostingSize) and, if consistent, its refs are
// built for free (par2SegmentRefsFromPostingSize) - no network round trip.
// Only a file whose geometry doesn't match gets its own real probe. Past
// par2ProbeMaxFailedFetches failed fetches in one release, probing stops
// entirely (aborted=true) and every remaining file falls back to the plain
// XML-bytes estimate: this many dead articles means the release is going to
// be rejected by the availability check regardless, so further probing only
// spends more round trips confirming what's already known.
func buildPar2RefsWithFetch(
	ctx context.Context,
	logger zerolog.Logger,
	maxConcurrent int,
	files nzbparser.NzbFiles,
	detectFileType func(string) storage.NZBFileType,
	fetch yencHeaderFetchFunc,
) (par2Files []storage.Par2FileRef, source []storage.PostedFileRef, aborted bool) {
	var eligible []nzbparser.NzbFile
	for _, file := range files {
		if len(file.Segments) > 0 {
			eligible = append(eligible, file)
		}
	}
	if len(eligible) == 0 {
		return nil, nil, false
	}

	sortedSegs := make([]nzbparser.NzbSegments, len(eligible))
	for i, file := range eligible {
		segs := make(nzbparser.NzbSegments, len(file.Segments))
		copy(segs, file.Segments)
		sort.Sort(segs)
		sortedSegs[i] = segs
	}

	// Prefer a seed file with more than one segment, so its non-final
	// segment length is a meaningful sample of the posting's shared article
	// size. If every eligible file has only one segment, there's no shared
	// size to derive and seedIdx stays -1 - every file then falls through to
	// its own probe below, same as before this optimization.
	seedIdx := -1
	for i, segs := range sortedSegs {
		if len(segs) >= 2 {
			seedIdx = i
			break
		}
	}

	results := make([]*builtPar2File, len(eligible))
	var failedProbes int32
	var abortedFlag int32
	var postingSegmentSize int64
	// Per-release counters for the single summary log below, replacing what
	// used to be one WARN per probed file (observed: 4297 in one live
	// import window). filesProbed/filesFailed count real fetch attempts
	// only - the posting-size reuse path is deliberately excluded, since it
	// costs no network round trip and isn't a fallback. fellBack counts
	// every file whose final result came from the plain XML-bytes estimate,
	// whichever path reached it (a failed/inconsistent probe, or a
	// post-abort skip).
	var filesProbed, filesFailed, fellBack int32

	if seedIdx >= 0 {
		file, segs := eligible[seedIdx], sortedSegs[seedIdx]
		refs, total, fetchFailed, real := realPar2SegmentRefs(ctx, logger, file.Filename, segs, fetch)
		atomic.AddInt32(&filesProbed, 1)
		if fetchFailed {
			atomic.AddInt32(&filesFailed, 1)
			if atomic.AddInt32(&failedProbes, 1) >= par2ProbeMaxFailedFetches {
				atomic.StoreInt32(&abortedFlag, 1)
			}
		}
		if real {
			// refs[i] for i < len(refs)-1 is the real derived per-article
			// size (see realPar2SegmentRefs) - a meaningful sample only when
			// there's more than one segment, guaranteed by seedIdx's choice.
			postingSegmentSize = refs[0].Bytes
		} else {
			atomic.AddInt32(&fellBack, 1)
		}
		results[seedIdx] = &builtPar2File{
			name: file.Filename, size: total, segments: refs,
			isPar2: detectFileType(file.Filename) == storage.NZBFileTypePar2,
		}
	}

	candidates := make([]par2ProbeCandidate, 0, len(eligible)-1)
	for i, file := range eligible {
		if i == seedIdx {
			continue
		}
		candidates = append(candidates, par2ProbeCandidate{idx: i, file: file, segs: sortedSegs[i]})
	}

	mapper := iter.Mapper[par2ProbeCandidate, *builtPar2File]{MaxGoroutines: maxConcurrent}
	mapped := mapper.Map(candidates, func(c *par2ProbeCandidate) *builtPar2File {
		isPar2 := detectFileType(c.file.Filename) == storage.NZBFileTypePar2

		if segmentsConsistentWithPostingSize(c.segs, postingSegmentSize) {
			refs, total := par2SegmentRefsFromPostingSize(c.segs, postingSegmentSize)
			return &builtPar2File{name: c.file.Filename, size: total, segments: refs, isPar2: isPar2}
		}

		if atomic.LoadInt32(&abortedFlag) != 0 {
			atomic.AddInt32(&fellBack, 1)
			refs, total := par2SegmentRefsFallback(c.segs)
			return &builtPar2File{name: c.file.Filename, size: total, segments: refs, isPar2: isPar2}
		}

		refs, total, fetchFailed, real := realPar2SegmentRefs(ctx, logger, c.file.Filename, c.segs, fetch)
		atomic.AddInt32(&filesProbed, 1)
		if fetchFailed {
			atomic.AddInt32(&filesFailed, 1)
			if atomic.AddInt32(&failedProbes, 1) >= par2ProbeMaxFailedFetches {
				atomic.StoreInt32(&abortedFlag, 1)
			}
		}
		if !real {
			atomic.AddInt32(&fellBack, 1)
		}
		return &builtPar2File{name: c.file.Filename, size: total, segments: refs, isPar2: isPar2}
	})
	for i, c := range candidates {
		results[c.idx] = mapped[i]
	}

	for _, b := range results {
		if b.isPar2 {
			par2Files = append(par2Files, storage.Par2FileRef{Name: b.name, Size: b.size, Segments: b.segments})
			continue
		}
		source = append(source, storage.PostedFileRef{Name: b.name, Size: b.size, Segments: b.segments})
	}

	aborted = atomic.LoadInt32(&abortedFlag) != 0
	logEvt := logger.Debug()
	if fellBack > 0 {
		logEvt = logger.Warn()
	}
	logEvt.
		Int("files_total", len(eligible)).
		Int32("files_probed", filesProbed).
		Int32("files_failed", filesFailed).
		Int32("fell_back_to_estimate", fellBack).
		Bool("aborted", aborted).
		Msg("PAR2 source-size probing complete")

	return par2Files, source, aborted
}

// segmentsConsistentWithPostingSize reports whether segs' own non-final
// segment byte sizes (as declared in the NZB XML) look consistent with a
// shared posting article size, within postingSizeToleranceFrac. A
// single-segment file has no non-final segment to compare and is always
// probed directly instead; likewise when postingSegmentSize is unknown (0).
func segmentsConsistentWithPostingSize(segs nzbparser.NzbSegments, postingSegmentSize int64) bool {
	if postingSegmentSize <= 0 || len(segs) < 2 {
		return false
	}
	expectedXMLBytes := float64(postingSegmentSize) / yencOverheadEstimate
	tolerance := expectedXMLBytes * postingSizeToleranceFrac
	for _, seg := range segs[:len(segs)-1] {
		if seg.Bytes <= 0 {
			return false
		}
		diff := float64(seg.Bytes) - expectedXMLBytes
		if diff < 0 {
			diff = -diff
		}
		if diff > tolerance {
			return false
		}
	}
	return true
}

// par2SegmentRefsFromPostingSize builds a file's per-segment refs from a
// known-good, already-probed posting article size, with no network round
// trip: every non-final segment gets the shared real decoded size, and the
// final (possibly partial) segment uses the same XML-bytes estimate
// par2SegmentRefsFallback does, since a real per-file fetch is the only way
// to learn that exactly.
func par2SegmentRefsFromPostingSize(segs nzbparser.NzbSegments, postingSegmentSize int64) ([]storage.Par2SegmentRef, int64) {
	n := len(segs)
	refs := make([]storage.Par2SegmentRef, n)
	var total int64
	for i, seg := range segs {
		b := postingSegmentSize
		if i == n-1 {
			b = int64(float64(seg.Bytes) * yencOverheadEstimate)
		}
		refs[i] = storage.Par2SegmentRef{MessageID: seg.Id, Bytes: b}
		total += b
	}
	return refs, total
}

// par2SegmentRefsFallback estimates every segment's decoded size from its
// XML-declared (yEnc-ENCODED, wire) byte count, for when no real per-file or
// per-posting size is available or trustworthy.
func par2SegmentRefsFallback(segs nzbparser.NzbSegments) ([]storage.Par2SegmentRef, int64) {
	refs := make([]storage.Par2SegmentRef, len(segs))
	var total int64
	for i, seg := range segs {
		b := int64(float64(seg.Bytes) * yencOverheadEstimate)
		refs[i] = storage.Par2SegmentRef{MessageID: seg.Id, Bytes: b}
		total += b
	}
	return refs, total
}

// yencHeaderFetchFunc fetches yEnc header metadata for one article. Narrowed
// from *nntp.Client to just this one operation so realPar2SegmentRefs can be
// exercised with a fake in tests, without a real, provider-backed client.
type yencHeaderFetchFunc func(ctx context.Context, messageID string) (*nntp.YencMetadata, error)

// fetchYencHeaderFast probes one article's yEnc header with a single
// connection attempt and a short, non-negotiable timeout - no cross-provider
// failover and no retry ladder (see nntp.Client.ExecuteOnce). This is the
// PAR2 source-size probe's own fetch: an optimization with a documented
// XML-bytes fallback, so a dead or slow article should fail fast into that
// fallback rather than eating a multi-provider retry sequence.
func (p *NZBParser) fetchYencHeaderFast(ctx context.Context, messageID string) (*nntp.YencMetadata, error) {
	probeCtx, cancel := context.WithTimeout(ctx, par2ProbeTimeout)
	defer cancel()
	var data *nntp.YencMetadata
	err := p.manager.ExecuteOnce(probeCtx, func(conn *nntp.Connection) error {
		d, e := conn.GetHeaderPrefixWithTimeout(messageID, metadataOnly, par2ProbeTimeout)
		data = d
		return e
	})
	return data, err
}

// realPar2SegmentRefs computes a posted file's real decoded per-segment byte
// lengths and total size, the same way processMediaFile/getNZBSegments do for
// files that actually get mounted - NOT from the NZB XML's bytes attribute,
// which is the yEnc-ENCODED wire size (posted-article size, inflated by yEnc
// escaping overhead) and essentially never equals the real decoded length
// PAR2's FileDesc.Length records. Found live: using the XML bytes attribute
// directly made every Par2Source length mismatch its true PAR2 FileDesc
// counterpart, so MatchFiles never found a single match on real data.
//
// One real network round trip (a header-only yEnc fetch of the first
// segment) is enough: a multipart yEnc header always carries the whole
// file's true decoded size ("size="), and this segment's own decoded length
// ("end"-"begin"+1) stands in for every other non-final segment's length -
// upload tooling always encodes same-file segments to a uniform byte budget.
// On fetch failure (or any inconsistency the same threshold check
// processFileGroup's last-segment fallback uses), falls back to the
// XML-declared bytes scaled by the same yEnc-overhead estimate used there -
// approximate, but PAR2 support for this one file is best-effort
// bookkeeping, not something worth failing the whole NZB parse over.
//
// Returns fetchFailed=true only when the fetch itself errored (a genuine
// network/article-not-found failure - the signal buildPar2RefsWithFetch
// counts toward its early-abort threshold), and real=true only when refs
// came from actual per-segment yEnc data rather than any fallback estimate -
// the signal buildPar2RefsWithFetch uses to seed the shared posting size.
func realPar2SegmentRefs(ctx context.Context, logger zerolog.Logger, filename string, segs nzbparser.NzbSegments, fetch yencHeaderFetchFunc) (refs []storage.Par2SegmentRef, total int64, fetchFailed, real bool) {
	yencData, err := fetch(ctx, segs[0].Id)
	if err != nil || yencData == nil || yencData.Size <= 0 {
		logger.Debug().Err(err).Str("file", filename).Msg("Failed to fetch real yEnc size for PAR2 source file; falling back to an XML-bytes estimate")
		refs, total = par2SegmentRefsFallback(segs)
		return refs, total, true, false
	}

	fileSize := yencData.Size
	segmentSize := yencData.End - yencData.Begin + 1
	if segmentSize <= 0 {
		refs, total = par2SegmentRefsFallback(segs)
		return refs, total, false, false
	}

	n := len(segs)
	fullSegsSize := segmentSize * int64(n-1)
	expectedTotal := fullSegsSize + segmentSize
	diff := fileSize - expectedTotal
	if diff < 0 {
		diff = -diff
	}
	if diff > (segmentSize*3)/2 {
		// The header's declared total is inconsistent with this segment
		// count/size (e.g. a mixed-subject group false match) - don't trust
		// derived per-segment math against it.
		refs, total = par2SegmentRefsFallback(segs)
		return refs, total, false, false
	}

	refs = make([]storage.Par2SegmentRef, n)
	total = 0
	for i, seg := range segs {
		b := segmentSize
		if i == n-1 {
			b = fileSize - fullSegsSize
		}
		refs[i] = storage.Par2SegmentRef{MessageID: seg.Id, Bytes: b}
		total += b
	}
	return refs, total, false, true
}

func (p *NZBParser) groupFiles(ctx context.Context, files nzbparser.NzbFiles) map[string]*FileGroup {
	// Assign XML document order as Number for files with uniform Number values.
	// This preserves upload order for obfuscated archives where the subject
	// line doesn't contain file number patterns like [X/Y].
	if len(files) > 1 {
		allSameNumber := true
		firstNum := files[0].Number
		for _, f := range files[1:] {
			if f.Number != firstNum {
				allSameNumber = false
				break
			}
		}
		if allSameNumber {
			for i := range files {
				files[i].Number = i + 1
			}
		}
	}

	var unknownFiles []nzbparser.NzbFile
	var allFiles []contentResult

	for _, file := range files {
		if len(file.Segments) == 0 {
			continue
		}

		fileType := p.detectFileType(file.Filename)
		if fileType == storage.NZBFileTypePar2 {
			// ignore PAR2 files for now
			continue
		}

		if fileType == storage.NZBFileTypeUnknown {
			unknownFiles = append(unknownFiles, file)
		} else {
			allFiles = append(allFiles, contentResult{
				file:           file,
				fileType:       fileType,
				actualFilename: file.Filename,
			})
		}
	}

	unknownResults := p.batchDetectContentTypes(ctx, unknownFiles)

	// Add unknown results
	allFiles = append(allFiles, unknownResults...)

	groups := p.groupProcessedFiles(allFiles)

	// Merge obfuscated RAR groups - when subjects are random strings,
	// each RAR volume gets its own group. This merges them back together.
	groups = p.mergeObfuscatedRarGroups(groups)

	return groups
}

// mergeObfuscatedRarGroups detects and merges RAR FileGroups that likely belong
// to the same multi-volume archive but couldn't be grouped due to obfuscated
// subjects/filenames.
//
// Obfuscation detection: When an NZB has random subjects (e.g., "yXIBWWn7qKVUVpS6")
// instead of descriptive filenames (e.g., "movie.part01.rar"), each RAR volume
// ends up in its own single-file group. This function merges those back together.
func (p *NZBParser) mergeObfuscatedRarGroups(groups map[string]*FileGroup) map[string]*FileGroup {
	// Collect all single-file RAR groups (potential obfuscation victims)
	var singleFileRarGroups []*FileGroup
	var otherGroups []*FileGroup

	for _, group := range groups {
		if group.Type == storage.NZBFileTypeRar && len(group.Files) == 1 {
			singleFileRarGroups = append(singleFileRarGroups, group)
		} else {
			otherGroups = append(otherGroups, group)
		}
	}

	// If we have multiple single-file RAR groups, this is likely obfuscation
	// Merge them into a single group
	if len(singleFileRarGroups) > 1 {
		p.logger.Debug().
			Int("single_file_rar_groups", len(singleFileRarGroups)).
			Msg("Detected potential obfuscated RAR archive, merging groups")

		// Create a merged group using the first group as base
		mergedGroup := &FileGroup{
			BaseName:       singleFileRarGroups[0].BaseName,
			ActualFilename: singleFileRarGroups[0].ActualFilename,
			Type:           storage.NZBFileTypeRar,
			Files:          make([]nzbparser.NzbFile, 0, len(singleFileRarGroups)),
			Groups:         make(map[string]struct{}),
		}

		// Merge all files from single-file RAR groups
		for _, group := range singleFileRarGroups {
			mergedGroup.Files = append(mergedGroup.Files, group.Files...)
			for g := range group.Groups {
				mergedGroup.Groups[g] = struct{}{}
			}
		}

		// Sort merged files by their NZB file Number (index in original NZB)
		// This preserves upload order which typically matches volume order
		// for multi-volume RAR archives uploaded sequentially
		sort.Slice(mergedGroup.Files, func(i, j int) bool {
			// Use the NZB file Number field which represents order in NZB
			return mergedGroup.Files[i].Number < mergedGroup.Files[j].Number
		})

		// Rebuild the groups map with the merged group
		result := make(map[string]*FileGroup)
		result[mergedGroup.BaseName] = mergedGroup
		for _, group := range otherGroups {
			result[group.BaseName] = group
		}

		p.logger.Info().
			Int("merged_files", len(mergedGroup.Files)).
			Str("group_name", mergedGroup.BaseName).
			Msg("Merged obfuscated RAR groups into single group")

		return result
	}

	// No merging needed
	return groups
}

// Batch process unknown files in parallel
func (p *NZBParser) batchDetectContentTypes(ctx context.Context, unknownFiles []nzbparser.NzbFile) []contentResult {
	if len(unknownFiles) == 0 {
		return nil
	}

	// Use up to maxConcurrent workers — same budget as the rest of the parser.
	workers := min(len(unknownFiles), p.maxConcurrent)

	mapper := iter.Mapper[nzbparser.NzbFile, contentResult]{
		MaxGoroutines: workers, // limit concurrency
	}

	mapped := mapper.Map(unknownFiles, func(f *nzbparser.NzbFile) contentResult {
		// You can still pass ctx through to your inner function.
		detectedType, actualFilename, err := p.detectFileTypeByContent(ctx, *f)
		if err != nil {
			p.logger.Trace().
				Err(err).
				Str("file", f.Filename).
				Msg("Failed to detect file type by content")
		}

		return contentResult{
			file:           *f,
			fileType:       detectedType,
			actualFilename: actualFilename,
		}
	})

	processed := make([]contentResult, 0, len(mapped))
	for _, r := range mapped {
		if r.fileType != storage.NZBFileTypeUnknown {
			processed = append(processed, r)
		}
	}
	return processed
}

// Group already processed files (fast)
func (p *NZBParser) groupProcessedFiles(allFiles []contentResult) map[string]*FileGroup {
	groups := make(map[string]*FileGroup)

	for _, item := range allFiles {
		// Skip unwanted files
		if item.fileType == storage.NZBFileTypeIgnore {
			continue
		}

		// If we only got the name from yEnc, try to infer type from it.
		if item.fileType == storage.NZBFileTypeUnknown && item.actualFilename != "" {
			if detected := p.detectFileType(item.actualFilename); detected != storage.NZBFileTypeUnknown {
				item.fileType = detected
			}
		}

		var groupKey string
		if item.actualFilename != "" && item.actualFilename != item.file.Filename {
			groupKey = p.getBaseFilename(item.actualFilename)
		} else {
			groupKey = item.file.Basefilename
		}

		group, exists := groups[groupKey]
		if !exists {
			group = &FileGroup{
				ActualFilename: item.actualFilename,
				BaseName:       groupKey,
				Type:           item.fileType,
				Files:          []nzbparser.NzbFile{},
				fileMeta:       make(map[string]filePartMeta),
				Groups:         make(map[string]struct{}),
			}
			groups[groupKey] = group
		} else if group.Type == storage.NZBFileTypeUnknown && item.fileType != storage.NZBFileTypeUnknown {
			group.Type = item.fileType
		}
		if group.ActualFilename == "" && item.actualFilename != "" {
			group.ActualFilename = item.actualFilename
		}

		// Update the filename only when content detection produced one; a
		// content-detected file whose yEnc header carried no name must keep
		// its subject-derived filename (it may be the only extension source).
		if item.actualFilename != "" {
			item.file.Filename = item.actualFilename
		}

		group.Files = append(group.Files, item.file)
		for _, g := range item.file.Groups {
			group.Groups[g] = struct{}{}
		}

		if item.fileSize > 0 || item.segmentSize > 0 || item.partNumber > 0 || item.partBegin > 0 {
			if group.fileMeta == nil {
				group.fileMeta = make(map[string]filePartMeta)
			}
			metaKey := fileMetaKey(item.file)
			if metaKey != "" {
				meta := group.fileMeta[metaKey]
				if meta.fileSize == 0 && item.fileSize > 0 {
					meta.fileSize = item.fileSize
				}
				if meta.segmentSize == 0 && item.segmentSize > 0 {
					meta.segmentSize = item.segmentSize
				}
				if meta.partNumber == 0 && item.partNumber > 0 {
					meta.partNumber = item.partNumber
				}
				if meta.partBegin == 0 && item.partBegin > 0 {
					meta.partBegin = item.partBegin
				}
				group.fileMeta[metaKey] = meta
			}
		}
	}

	return groups
}

func (p *NZBParser) getBaseFilename(filename string) string {
	if filename == "" {
		return ""
	}

	// First remove any quotes and trim spaces
	cleaned := strings.Trim(filename, `" -`)

	// Check for vol\d+\+\d+\.par2? (PAR2 Volume files)
	if volPar2Pattern.MatchString(cleaned) {
		return volPar2Pattern.ReplaceAllString(cleaned, "")
	}

	// Check for part\d+\.[^ "\.]* (part files like .part01.rar)

	if partPattern.MatchString(cleaned) {
		return partPattern.ReplaceAllString(cleaned, "")
	}

	// Check for [^ "\.]*\.\d+ (extensions with numbers like .7z.001, .r01, etc.)
	if extWithNumberPattern.MatchString(cleaned) {
		return extWithNumberPattern.ReplaceAllString(cleaned, "")
	}

	// Check for regular extensions [^ "\.]*

	if regularExtPattern.MatchString(cleaned) {
		return regularExtPattern.ReplaceAllString(cleaned, "")
	}

	return cleaned
}

// Simplified file type detection
func (p *NZBParser) detectFileType(filename string) storage.NZBFileType {
	lower := strings.ToLower(filename)

	// Check for media first
	if utils.IsMediaFile(lower) {
		return storage.NZBFileTypeMedia
	}

	// Check rar next
	if p.isRarFile(lower) {
		return storage.NZBFileTypeRar
	}

	if strings.HasSuffix(lower, ".par2") {
		return storage.NZBFileTypePar2
	}

	// Check for 7z files
	if sevenZMainPattern.MatchString(lower) || sevenZPartPattern.MatchString(lower) {
		return storage.NZBFileTypeSevenZip
	}

	if strings.HasSuffix(lower, ".zip") || strings.HasSuffix(lower, ".tar") ||
		strings.HasSuffix(lower, ".gz") || strings.HasSuffix(lower, ".bz2") {
		if strings.HasSuffix(lower, ".zip") {
			return storage.NZBFileTypeZip
		}
		return storage.NZBFileTypeUnknown
	}

	// Check for ignored file types
	for _, ext := range ignoreExtensions {
		if strings.HasSuffix(lower, ext) {
			return storage.NZBFileTypeIgnore
		}
	}
	// Default to unknown type
	return storage.NZBFileTypeUnknown
}

// Simplified RAR detection
func (p *NZBParser) isRarFile(filename string) bool {
	return rarMainPattern.MatchString(filename) ||
		rarPartPattern.MatchString(filename) ||
		rarVolumePattern.MatchString(filename)
}

func (p *NZBParser) processFileGroups(ctx context.Context, groups map[string]*FileGroup, password string) []storage.NZBFile {
	if len(groups) == 0 {
		return nil
	}
	rarCounts, sevenZCounts, zipCounts, mediaCounts, deferredCounts := 0, 0, 0, 0, 0

	// Convert map into slice of *values*, not pointers
	fileGroups := make([]FileGroup, 0, len(groups))
	for _, g := range groups {
		if len(g.Files) == 0 {
			continue
		}
		fileGroups = append(fileGroups, *g)
	}

	// Use a Mapper with limited concurrency to prevent goroutine explosion
	// when nested with RAR/archive parsers that also use parallel processing
	mapper := iter.Mapper[FileGroup, []*storage.NZBFile]{
		MaxGoroutines: p.maxConcurrent,
	}

	results := mapper.Map(fileGroups, func(g *FileGroup) []*storage.NZBFile {
		files, err := p.processFileGroup(ctx, g, password)
		if err != nil {
			p.logger.Warn().Err(err).Str("group", g.BaseName).Msg("Failed to process file group")
			return nil
		}
		return files
	})

	// Filter nils
	var files []storage.NZBFile
	for _, groupFiles := range results {
		for _, f := range groupFiles {
			if f != nil {
				files = append(files, *f)
				// Count types
				switch f.FileType {
				case storage.NZBFileTypeRar:
					rarCounts++
				case storage.NZBFileTypeSevenZip:
					sevenZCounts++
				case storage.NZBFileTypeZip:
					zipCounts++
				case storage.NZBFileTypeMedia:
					mediaCounts++
				}
			}
		}
	}

	// Count deferred archives
	for _, g := range fileGroups {
		switch g.Type {
		case storage.NZBFileTypeRar, storage.NZBFileTypeSevenZip, storage.NZBFileTypeZip:
			deferredCounts++
		}
	}

	return files
}

// Simplified individual group processing
func (p *NZBParser) processFileGroup(ctx context.Context, group *FileGroup, password string) ([]*storage.NZBFile, error) {
	if err := p.enrichGroupWithFileInfo(ctx, group); err != nil {
		return nil, err
	}

	switch group.Type {
	case storage.NZBFileTypeMedia:
		return wrapNZBFile(p.processMediaFile(group, password))
	case storage.NZBFileTypeRar:
		rarParser := NewRARParser(p.manager, p.maxConcurrent, p.logger)
		return rarParser.Process(ctx, group, password)
	case storage.NZBFileTypeSevenZip:
		zipParser := NewSevenZParser(p.manager, p.maxConcurrent, p.logger)
		return zipParser.Process(ctx, group, password)
	case storage.NZBFileTypeZip:
		zipParser := NewZIPParser(p.manager, p.maxConcurrent, p.logger)
		return zipParser.Process(ctx, group, password)
	default:
		return nil, fmt.Errorf("unsupported file type: %v", group.Type)
	}
}

func (p *NZBParser) enrichGroupWithFileInfo(ctx context.Context, group *FileGroup) error {
	sort.Slice(group.Files, func(i, j int) bool {
		if group.Files[i].Number != group.Files[j].Number {
			return group.Files[i].Number < group.Files[j].Number
		}
		return group.Files[i].Filename < group.Files[j].Filename
	})

	firstFile := group.Files[0]
	// Find the file with the most segments to use as the reference for segment size
	// This avoids issues where the first file is a small NFO/NZB with different characteristics
	maxSegments := 0
	for _, f := range group.Files {
		if len(f.Segments) > maxSegments {
			maxSegments = len(f.Segments)
			firstFile = f
		}
	}

	if len(firstFile.Segments) == 0 {
		return fmt.Errorf("no Segments in reference file of group %s", group.BaseName)
	}
	firstSegment := firstFile.Segments[0]

	lastFile := group.Files[len(group.Files)-1]
	lastSegment := lastFile.Segments[0]

	// If first and last are the same file, only need one fetch
	sameFile := len(group.Files) == 1

	type headerResult struct {
		data *nntp.YencMetadata
		err  error
	}

	// Fetch both headers in parallel
	firstCh := make(chan headerResult, 1)
	lastCh := make(chan headerResult, 1)

	go func() {
		var data *nntp.YencMetadata
		// GetHeaderPrefix drains the body and returns the connection to the pool;
		// we only need yEnc metadata (name/size/offsets), not a decoded snippet.
		err := p.manager.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
			d, e := conn.GetHeaderPrefix(firstSegment.Id, metadataOnly)
			data = d
			return e
		})
		firstCh <- headerResult{data, err}
	}()

	if !sameFile {
		go func() {
			var data *nntp.YencMetadata
			err := p.manager.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
				d, e := conn.GetHeaderPrefix(lastSegment.Id, metadataOnly)
				data = d
				return e
			})
			lastCh <- headerResult{data, err}
		}()
	}

	// Wait for first result
	var firstResult headerResult
	select {
	case firstResult = <-firstCh:
	case <-ctx.Done():
		return ctx.Err()
	}

	if firstResult.err != nil {
		return fmt.Errorf("failed to fetch first segment header: %w", firstResult.err)
	}
	yencData := firstResult.data

	// Update the group's filename if the header provides a better one
	// This fixes issues where the group name is based on a small .nzb file or similar
	if yencData.Name != "" && group.Type == storage.NZBFileTypeMedia {
		// Only update if it looks like a valid filename
		cleanName := utils.RemoveInvalidChars(yencData.Name)
		if cleanName != "" {
			group.ActualFilename = cleanName
		}
	}

	segmentSize := yencData.End - yencData.Begin + 1
	fileSize := yencData.Size

	// get last file size
	var lastFileSize int64
	if sameFile {
		lastFileSize = fileSize
	} else {
		var lastResult headerResult
		select {
		case lastResult = <-lastCh:
		case <-ctx.Done():
			return ctx.Err()
		}

		if lastResult.err != nil {
			return fmt.Errorf("failed to fetch last segment header: %w", lastResult.err)
		}
		lastFileSize = lastResult.data.Size
	}

	group.metadata = &fileAnalysisResult{
		fileSize:     fileSize,
		lastFileSize: lastFileSize,
		segmentSize:  segmentSize,
	}
	return nil
}

// Process regular media files
func (p *NZBParser) processMediaFile(group *FileGroup, password string) *storage.NZBFile {
	if len(group.Files) == 0 {
		return nil
	}

	// Sort files for consistent ordering
	sort.Slice(group.Files, func(i, j int) bool {
		return group.Files[i].Number < group.Files[j].Number
	})

	// Determine extension
	ext := determineExtension(group)
	if ext == "" {
		ext = filepath.Ext(group.ActualFilename)
	}
	if ext == "" {
		return nil
	}

	name := group.BaseName + ext

	file := &storage.NZBFile{
		Name:     name,
		Groups:   getGroupsList(group.Groups),
		Segments: []storage.NZBSegment{},
		Password: password,
		FileType: group.Type,
	}

	currentOffset := int64(0)
	for index, nzbFile := range group.Files {
		totalSize, segments := getNZBSegments(index, nzbFile, group)
		if len(segments) == 0 && len(nzbFile.Segments) > 0 {
			// getNZBSegments rejected the sub-file (missing/duplicate segment
			// numbers). Silently dropping it would splice a hole into the
			// middle of the merged stream; fail the whole file instead.
			p.logger.Warn().
				Str("group", group.BaseName).
				Str("file", nzbFile.Filename).
				Msg("Incomplete or inconsistent segment numbering; rejecting media file")
			return nil
		}
		file.Segments = append(file.Segments, segments...)
		currentOffset += totalSize
	}
	file.Size = currentOffset
	return file
}

func (p *NZBParser) detectFileTypeByContent(ctx context.Context, file nzbparser.NzbFile) (storage.NZBFileType, string, error) {
	if len(file.Segments) == 0 {
		return storage.NZBFileTypeUnknown, "", fmt.Errorf("no segments in file %s", file.Filename)
	}

	// Download first segment to check file signature
	firstSegment := file.Segments[0]
	var data *nntp.YencMetadata
	// GetHeaderPrefix returns the connection to the pool after draining;
	// only a small snippet is needed for magic-byte / filename detection.
	err := p.manager.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
		d, e := conn.GetHeaderPrefix(firstSegment.Id, defaultMaxSnippetSize)
		data = d
		return e
	})
	if err != nil {
		return storage.NZBFileTypeUnknown, "", fmt.Errorf("failed to fetch segment header for file %s: %w", file.Filename, err)
	}

	if data.Name != "" {
		fileType := p.detectFileType(data.Name)
		if fileType != storage.NZBFileTypeUnknown {
			return fileType, data.Name, nil
		}
	}

	return p.detectFileTypeFromContent(data.Snippet), data.Name, nil
}

func (p *NZBParser) detectFileTypeFromContent(data []byte) storage.NZBFileType {
	if len(data) == 0 {
		return storage.NZBFileTypeUnknown
	}

	// Check for RAR signatures (both RAR 4.x and 5.x)
	if len(data) >= 7 {
		// RAR 4.x signature
		if bytes.Equal(data[:7], []byte("Rar!\x1A\x07\x00")) {
			return storage.NZBFileTypeRar
		}
	}
	if len(data) >= 8 {
		// RAR 5.x signature
		if bytes.Equal(data[:8], []byte("Rar!\x1A\x07\x01\x00")) {
			return storage.NZBFileTypeRar
		}
	}

	// Check for ZIP signature
	if len(data) >= 4 && bytes.Equal(data[:4], []byte{0x50, 0x4B, 0x03, 0x04}) {
		return storage.NZBFileTypeZip
	}

	// Check for 7z signature
	if len(data) >= 6 && bytes.Equal(data[:6], []byte{0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C}) {
		return storage.NZBFileTypeSevenZip
	}

	// Check for common media file signatures
	if len(data) >= 4 {
		// Matroska (MKV/WebM)
		if bytes.Equal(data[:4], []byte{0x1A, 0x45, 0xDF, 0xA3}) {
			return storage.NZBFileTypeMedia
		}

		// MP4/MOV (check for 'ftyp' at offset 4)
		if len(data) >= 8 && bytes.Equal(data[4:8], []byte("ftyp")) {
			return storage.NZBFileTypeMedia
		}

		// AVI
		if len(data) >= 12 && bytes.Equal(data[:4], []byte("RIFF")) &&
			bytes.Equal(data[8:12], []byte("AVI ")) {
			return storage.NZBFileTypeMedia
		}
	}

	// MPEG checks need more specific patterns
	if len(data) >= 4 {
		// MPEG-1/2 Program Stream
		if bytes.Equal(data[:4], []byte{0x00, 0x00, 0x01, 0xBA}) {
			return storage.NZBFileTypeMedia
		}

		// MPEG-1/2 Video Stream
		if bytes.Equal(data[:4], []byte{0x00, 0x00, 0x01, 0xB3}) {
			return storage.NZBFileTypeMedia
		}
	}

	// Check for Transport Stream (TS files)
	if len(data) >= 1 && data[0] == 0x47 {
		// Additional validation: TS packets are 188 bytes, so the next
		// sync byte sits at index 188 (requires at least 189 bytes).
		if len(data) > 188 && data[188] == 0x47 {
			return storage.NZBFileTypeMedia
		}
	}

	return storage.NZBFileTypeUnknown
}
