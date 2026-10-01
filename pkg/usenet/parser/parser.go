package parser

import (
	"bytes"
	"context"
	"crypto/md5"
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
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
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
	// par2MD5PrefixSize is the number of decoded bytes fetched per file for
	// PAR2 MD5-16k name recovery. PAR2 FileDesc packets store MD5 of the
	// first 16KB (or the whole file if shorter).
	par2MD5PrefixSize = 16384
	// par2ProbeMaxFailedFetches is how many confirmed-missing articles
	// (genuine 430/423) buildPar2RefsWithFetch tolerates before giving up
	// on PAR2 probing for the rest of the release. At 20, partial source
	// decay (a handful of dead files in a large release) is fully probed
	// so recovery volumes get accurate geometry for repair. A fully-DMCA'd
	// release still short-circuits rather than wasting 70+ probe round
	// trips. The abort is informational (remaining files fall to estimate);
	// it no longer hard-rejects the release at parse time.
	par2ProbeMaxFailedFetches = 20
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
	// volumeProbe overrides probeRarVolume in tests.
	volumeProbe rarVolumeProbeFunc
}

type fileAnalysisResult struct {
	fileSize     int64 // Total decoded size of the NZB file entry.
	lastFileSize int64 // Total decoded size of the last NZB file entry in the group.
	segmentSize  int64 // Decoded size of a single yEnc part/segment.

	// measured holds the decoded size of each file whose own yEnc header was
	// fetched, keyed by fileMetaKey. fileSize and lastFileSize describe the
	// files enrichGroupWithFileInfo probed, which need not be the files at the
	// first and last index once an archive processor reorders its volumes -
	// see measureUnsizedVolumes.
	measured map[string]int64
}

type contentResult struct {
	file           nzbparser.NzbFile
	fileType       storage.NZBFileType
	actualFilename string
}

type FileGroup struct {
	BaseName       string
	ActualFilename string
	Type           storage.NZBFileType
	Files          []nzbparser.NzbFile
	metadata       *fileAnalysisResult
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
	raw, oneCopy, err := parseNZB(bytes.NewReader(content))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse NZB content: %w", err)
	}
	if len(oneCopy) > 0 {
		p.logger.Warn().Str("filename", filename).Int("files", len(oneCopy)).Strs("subjects", oneCopy[:min(len(oneCopy), 5)]).
			Msg("NZB lists files posted more than once; kept the first complete copy of each")
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
	fileGroups, par2Names := p.groupFiles(ctx, raw.Files)

	if len(fileGroups) == 0 {
		// A bare error makes the Arr retry the identical grab forever. When
		// every file's type was known from its name (no network content
		// detection that a timeout or cancellation could have cut short)
		// the NZB simply holds no media: report it unavailable so it is
		// queued failed and the Arr blocklists it.
		if ctx.Err() == nil && holdsOnlyKnownNonMedia(p, raw.Files) {
			return nil, nil, fmt.Errorf("no valid file groups found in NZB: %w", ErrReleaseUnavailable)
		}
		return nil, nil, fmt.Errorf("no valid file groups found in NZB")
	}

	// Confirm availability BEFORE any PAR2 metadata work: a release with
	// missing segments gets rejected (and, per our re-grab-on-any-import-damage
	// policy, re-searched) regardless of what buildPar2Refs would have found,
	// so there is no reason to spend a yEnc body-probe per posted file only to
	// discard the result. Checked first, not just cheaper first: a dead
	// posting fails in one round trip instead of after N probe round trips.
	nzb.Par2Files, nzb.Par2Source, err = availabilityThenPar2Refs(ctx, p.logger, p.maxConcurrent, fileGroups, raw.Files, p.detectFileType, p.statSegment, p.fetchYencHeaderFast, par2Names)
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
	par2Names map[string]string,
) (par2Files []storage.Par2FileRef, source []storage.PostedFileRef, err error) {
	checked := false
	for _, group := range fileGroups {
		if len(group.Files) == 0 || len(group.Files[0].Segments) == 0 {
			continue
		}
		segment := group.Files[0].Segments[0]
		if statErr := statSegment(ctx, segment.Id); statErr != nil {
			// Only a 430 says the release is gone. A cancelled request (the
			// Arr's add times out at 100s), a timeout, a refused connection
			// or every provider at its quota says nothing about the posting,
			// and tagging it would mark the content dead and have the Arr
			// blocklist a release that may be fine.
			if nntp.IsArticleNotFoundError(statErr) {
				return nil, nil, fmt.Errorf("failed to stat segment %s <%s>: %w: %w", group.ActualFilename, segment.Id, statErr, ErrReleaseUnavailable)
			}
			return nil, nil, fmt.Errorf("could not check segment %s <%s>: %w", group.ActualFilename, segment.Id, statErr)
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
	par2Files, source, _ = buildPar2RefsWithFetch(ctx, logger, maxConcurrent, rawFiles, detectFileType, fetch, par2Names)
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
		if err := cfg.ValidateFileAllowed(file.Name, file.Size); err != nil {
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
// Every posted (non-PAR2) file gets its own real header fetch (see
// realPar2SegmentRefs): par2.MatchFiles pairs a posted file with its FileDesc
// by exact length first, and only the file's own yEnc header gives its final
// article's size. An estimated length misses its FileDesc, which sends the
// file to an MD5-16k fetch of its first article - and a file whose first
// article is dead can then never be matched or repaired. A PAR2 file is never
// matched by length, so the seed's shared article size (from the one seed
// probe) builds its refs for free when its geometry is consistent with it
// (segmentsConsistentWithPostingSize, par2SegmentRefsFromPostingSize); the
// same refs stand in for a posted file whose own probe fails. Past
// par2ProbeMaxFailedFetches genuinely not-found fetches (nntp.
// IsArticleNotFoundError - see realPar2SegmentRefs) in one release, probing
// stops entirely (aborted=true) and every remaining file falls back to the
// plain XML-bytes estimate: this many confirmed-missing articles means the
// release is going to be rejected by the availability check regardless, so
// further probing only spends more round trips confirming what's already
// known. A probe that fails for a transient reason (timeout, connection
// drop) never counts toward this threshold - see realPar2SegmentRefs.
//
// A PAR2 file is recognised by its subject name or, for an obfuscated
// subject, by its yEnc name: the one content detection read (par2Names, keyed
// by fileMetaKey) or the one this function's own probe returns. Its ref takes
// that name, since the repair reads a volume's slice range and its set from
// the file name. Recognised by subject alone, the PAR2 files of an obfuscated
// release were stored as posted files and the release had no PAR2 files to
// repair with.
func buildPar2RefsWithFetch(
	ctx context.Context,
	logger zerolog.Logger,
	maxConcurrent int,
	files nzbparser.NzbFiles,
	detectFileType func(string) storage.NZBFileType,
	fetch yencHeaderFetchFunc,
	par2Names map[string]string,
) (par2Files []storage.Par2FileRef, source []storage.PostedFileRef, aborted bool) {
	// par2Name is file's name when that names a PAR2 file, else its yEnc name
	// when that does; "" for any other file.
	par2Name := func(file nzbparser.NzbFile, yencName string) string {
		switch detectFileType(file.Filename) {
		case storage.NZBFileTypePar2:
			return file.Filename
		case storage.NZBFileTypeUnknown:
		default:
			// Grouped by its subject as a file of the release.
			return ""
		}
		for _, n := range []string{yencName, par2Names[fileMetaKey(file)]} {
			if n != "" && detectFileType(n) == storage.NZBFileTypePar2 {
				return n
			}
		}
		return ""
	}
	built := func(file nzbparser.NzbFile, yencName string, total int64, refs []storage.Par2SegmentRef) *builtPar2File {
		if n := par2Name(file, yencName); n != "" {
			return &builtPar2File{name: n, size: total, segments: refs, isPar2: true}
		}
		return &builtPar2File{name: file.Filename, size: total, segments: refs}
	}

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
	var seedLastSegBytes int64
	var seedSegCount int
	// Per-release counters for the single summary log below, replacing what
	// used to be one WARN per probed file (observed: 4297 in one live
	// import window). filesProbed/filesNotFound count real fetch attempts
	// only - the posting-size reuse path is deliberately excluded, since it
	// costs no network round trip and isn't a fallback. fellBack counts
	// every file whose final result came from the plain XML-bytes estimate,
	// whichever path reached it (a failed/inconsistent probe, or a
	// post-abort skip); filesTransient is the subset of fellBack that used
	// the estimate for a reason OTHER than a confirmed-missing article
	// (timeout, connection drop, transport failure) - these never move the
	// abort counter below.
	var filesProbed, filesNotFound, filesTransient, fellBack int32

	if seedIdx >= 0 {
		file, segs := eligible[seedIdx], sortedSegs[seedIdx]
		var yencName string
		refs, total, notFound, real := realPar2SegmentRefs(ctx, logger, file.Filename, segs, fetchKeepingName(fetch, &yencName))
		atomic.AddInt32(&filesProbed, 1)
		if notFound {
			atomic.AddInt32(&filesNotFound, 1)
			if atomic.AddInt32(&failedProbes, 1) >= par2ProbeMaxFailedFetches {
				atomic.StoreInt32(&abortedFlag, 1)
			}
		}
		if real {
			// refs[i] for i < len(refs)-1 is the real derived per-article
			// size (see realPar2SegmentRefs) - a meaningful sample only when
			// there's more than one segment, guaranteed by seedIdx's choice.
			postingSegmentSize = refs[0].Bytes
			seedLastSegBytes = refs[len(refs)-1].Bytes
			seedSegCount = len(refs)
		} else {
			atomic.AddInt32(&fellBack, 1)
			if !notFound {
				atomic.AddInt32(&filesTransient, 1)
			}
		}
		results[seedIdx] = built(file, yencName, total, refs)
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
		// Before the probe only the subject and content detection can say so.
		isPar2 := par2Name(c.file, "") != ""
		consistent := segmentsConsistentWithPostingSize(c.segs, postingSegmentSize)

		// A PAR2 file's own size is never matched against a FileDesc, so the
		// shared article size is enough for it. A posted file is matched to its
		// FileDesc by exact length, and only its own header gives its final
		// article's size.
		if consistent && isPar2 {
			refs, total := par2SegmentRefsFromPostingSize(c.segs, postingSegmentSize, seedLastSegBytes, seedSegCount)
			return built(c.file, "", total, refs)
		}

		if atomic.LoadInt32(&abortedFlag) != 0 {
			if consistent {
				refs, total := par2SegmentRefsFromPostingSize(c.segs, postingSegmentSize, seedLastSegBytes, seedSegCount)
				return built(c.file, "", total, refs)
			}
			atomic.AddInt32(&fellBack, 1)
			refs, total := par2SegmentRefsFallback(c.segs)
			return built(c.file, "", total, refs)
		}

		var yencName string
		refs, total, notFound, real := realPar2SegmentRefs(ctx, logger, c.file.Filename, c.segs, fetchKeepingName(fetch, &yencName))
		atomic.AddInt32(&filesProbed, 1)
		if notFound {
			atomic.AddInt32(&filesNotFound, 1)
			if atomic.AddInt32(&failedProbes, 1) >= par2ProbeMaxFailedFetches {
				atomic.StoreInt32(&abortedFlag, 1)
			}
		}
		if !real {
			if consistent {
				// Interior articles still take the shared size; only the final
				// article stays an estimate.
				refs, total = par2SegmentRefsFromPostingSize(c.segs, postingSegmentSize, seedLastSegBytes, seedSegCount)
				return built(c.file, yencName, total, refs)
			}
			atomic.AddInt32(&fellBack, 1)
			if !notFound {
				atomic.AddInt32(&filesTransient, 1)
			}
		}
		return built(c.file, yencName, total, refs)
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
	// Falling back to an estimate for some files is routine (a slow or
	// transient probe, a missing PAR2 volume); only an abort - enough
	// confirmed-missing articles to stop probing - is worth a warning.
	logEvt := logger.Debug()
	if aborted {
		logEvt = logger.Warn()
	}
	logEvt.
		Int("files_total", len(eligible)).
		Int32("files_probed", filesProbed).
		Int32("files_not_found", filesNotFound).
		Int32("files_transient", filesTransient).
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
//
// The final segment is never marked Real. Neither branch measures it: the
// seed's final article only matches when the two files happen to be the same
// size, and the residual branch scales this file's NZB bytes. Both were marked
// Real, so a posted file's Size read as exact while its final article was off
// by up to a few KB: Nora S01E06 ETHEL .r16 read 13,164,355 B though its RAR
// header and data alone take 13,172,207 B, and its .rar-.r15 volumes, which
// all hold the same 199,999,980 B of header and data, read as 17 different
// sizes. 135 of 396 imports on a production install (2026-09-15) carried such a size. buildPar2RefsWithFetch probes every posted file for its own total, so
// this is only used for PAR2 files and for posted files whose probe failed.
func par2SegmentRefsFromPostingSize(segs nzbparser.NzbSegments, postingSegmentSize int64, seedLastSegBytes int64, seedSegCount int) ([]storage.Par2SegmentRef, int64) {
	n := len(segs)
	// residualOverhead is the decoded/wire ratio implied by this file's own
	// already-known-good posting size against segs[0]'s XML-declared (wire)
	// byte count. yencOverheadEstimate is a fixed global constant (0.97) that
	// doesn't track how any one release was actually yEnc-encoded - poster
	// tooling and line-length choices shift the real ratio per file, so a
	// per-file ratio derived from a segment we've already confirmed decodes
	// to postingSegmentSize is a strictly better estimate for this file's own
	// residual (final, possibly partial) segment than the global constant.
	// Guarded to (0, 1]: a ratio outside that range means segs[0].Bytes is
	// missing/bogus or the posting size doesn't correspond to a normal yEnc
	// encoding, so fall back to the global estimate rather than trust it.
	residualOverhead := yencOverheadEstimate
	if segs[0].Bytes > 0 {
		if r := float64(postingSegmentSize) / float64(segs[0].Bytes); r > 0 && r <= 1 {
			residualOverhead = r
		}
	}
	refs := make([]storage.Par2SegmentRef, n)
	var total int64
	for i, seg := range segs {
		// Never Real: the size is borrowed from another posted file (the
		// seed), accepted after only a loose XML-size check, and a posting
		// can mix article sizes (a fill, a second poster). Marked Real, the
		// repair's fetcher trusted it as measured geometry and never measured
		// it, so every boundary after the first could be off - the path to a
		// patch cut at the wrong offset. As an estimate, resolveGeometry
		// measures the file's own first article instead.
		b := postingSegmentSize
		if i == n-1 {
			if n == seedSegCount && seedLastSegBytes > 0 {
				b = seedLastSegBytes // the seed's final article: right only for a file the seed's size
			} else {
				b = int64(float64(seg.Bytes) * residualOverhead) // residual: consistent-but-different-count geometry
			}
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

// fetchKeepingName is fetch, also storing the article's yEnc file name in
// *name when the fetch returns one.
func fetchKeepingName(fetch yencHeaderFetchFunc, name *string) yencHeaderFetchFunc {
	return func(ctx context.Context, messageID string) (*nntp.YencMetadata, error) {
		data, err := fetch(ctx, messageID)
		if data != nil && data.Name != "" {
			*name = data.Name
		}
		return data, err
	}
}

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
// On fetch failure (or a declared total that leaves the final article empty
// or larger than a full one), falls back to the
// XML-declared bytes scaled by the same yEnc-overhead estimate used there -
// approximate, but PAR2 support for this one file is best-effort
// bookkeeping, not something worth failing the whole NZB parse over.
//
// Returns notFound=true only when the fetch failed because the article is
// genuinely absent from the provider (nntp.IsArticleNotFoundError) - the
// signal buildPar2RefsWithFetch counts toward its early-abort threshold. A
// fetch that fails for any other reason (timeout, connection drop, transport
// parse failure) also falls back to the estimate below, but leaves notFound
// false: par2ProbeTimeout is short and non-negotiable by design (see
// fetchYencHeaderFast), so a transient hiccup on this one probe says nothing
// about whether the article actually exists, and must not count toward
// declaring the whole release unavailable. real=true only when refs came
// from actual per-segment yEnc data rather than any fallback estimate - the
// signal buildPar2RefsWithFetch uses to seed the shared posting size.
func realPar2SegmentRefs(ctx context.Context, logger zerolog.Logger, filename string, segs nzbparser.NzbSegments, fetch yencHeaderFetchFunc) (refs []storage.Par2SegmentRef, total int64, notFound, real bool) {
	yencData, err := fetch(ctx, segs[0].Id)
	if err != nil || yencData == nil || yencData.Size <= 0 {
		// Trace, not Debug: one line per source file on every parse, and the
		// aggregate is already reported by the "PAR2 source-size probing
		// complete" WARN (files_probed / fell_back_to_estimate / not_found).
		logger.Trace().Err(err).Str("file", filename).Msg("Failed to fetch real yEnc size for PAR2 source file; falling back to an XML-bytes estimate")
		refs, total = par2SegmentRefsFallback(segs)
		return refs, total, err != nil && nntp.IsArticleNotFoundError(err), false
	}

	fileSize := yencData.Size
	segmentSize := yencData.End - yencData.Begin + 1
	if segmentSize <= 0 {
		refs, total = par2SegmentRefsFallback(segs)
		return refs, total, false, false
	}

	n := len(segs)
	fullSegsSize := segmentSize * int64(n-1)
	if last := fileSize - fullSegsSize; last <= 0 || last > segmentSize {
		// The header's declared total does not fit this many articles of this
		// size: the final article would be empty, negative or larger than a
		// full one (e.g. a mixed-subject group false match, or articles that
		// are not uniform). Don't trust derived per-segment math against it.
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
	for i := range refs {
		refs[i].Real = true
	}
	return refs, total, false, true
}

// groupFiles groups the NZB's files into releases' file groups. par2Names
// holds, by fileMetaKey, the yEnc name of every file content detection found
// to be a PAR2 file: such a file joins no group, and its subject does not say
// what it is.
func (p *NZBParser) groupFiles(ctx context.Context, files nzbparser.NzbFiles) (groups map[string]*FileGroup, par2Names map[string]string) {
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

	par2Names = sniffedPar2Names(unknownResults, p.detectFileType)

	groups = p.groupProcessedFiles(allFiles)

	// Merge obfuscated RAR groups - when subjects are random strings,
	// each RAR volume gets its own group. This merges them back together.
	// Pass the raw file list so PAR2 name recovery can find PAR2 files.
	groups = p.mergeObfuscatedRarGroups(ctx, groups, files, par2Names)

	return groups, par2Names
}

// sniffedPar2Names returns, by fileMetaKey, the yEnc name of each content
// detection result that names a PAR2 file.
func sniffedPar2Names(results []contentResult, detectFileType func(string) storage.NZBFileType) map[string]string {
	var names map[string]string
	for _, r := range results {
		if r.actualFilename == "" || detectFileType(r.actualFilename) != storage.NZBFileTypePar2 {
			continue
		}
		key := fileMetaKey(r.file)
		if key == "" {
			continue
		}
		if names == nil {
			names = make(map[string]string)
		}
		names[key] = r.actualFilename
	}
	return names
}

// mergeObfuscatedRarGroups detects and merges RAR FileGroups that likely belong
// to the same multi-volume archive but couldn't be grouped due to obfuscated
// subjects/filenames.
//
// Obfuscation detection: When an NZB has random subjects (e.g., "yXIBWWn7qKVUVpS6")
// instead of descriptive filenames (e.g., "movie.part01.rar"), each RAR volume
// ends up in its own single-file group. This function merges those back together.
//
// When PAR2 files are available, it attempts to recover real filenames from the
// PAR2 FileDesc table via MD5-16k matching. Recovered names (e.g. ".part01.rar")
// provide proper volume ordering; without them, NZB upload order is the fallback.
func (p *NZBParser) mergeObfuscatedRarGroups(ctx context.Context, groups map[string]*FileGroup, rawFiles nzbparser.NzbFiles, par2Names map[string]string) map[string]*FileGroup {
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

	if len(singleFileRarGroups) <= 1 {
		return groups
	}
	// groups is a map: put its volumes back in the order the NZB lists them, so
	// the merge below breaks subject-number ties by upload order rather than
	// differently on every run.
	docIndex := make(map[string]int, len(rawFiles))
	for i, f := range rawFiles {
		if k := fileMetaKey(f); k != "" {
			if _, ok := docIndex[k]; !ok {
				docIndex[k] = i
			}
		}
	}
	sort.SliceStable(singleFileRarGroups, func(i, j int) bool {
		return docIndex[fileMetaKey(singleFileRarGroups[i].Files[0])] < docIndex[fileMetaKey(singleFileRarGroups[j].Files[0])]
	})

	// A release's own small archives - name.proof.rar, name.subs.rar - are
	// single-file RAR groups too, and merging them made one bogus two-volume
	// archive of unrelated files (Der Schwaebische Nachmittagstee UNTAVC,
	// X.Y.Z. 99 GMA on a production install). Keep an archive whose main header says it is
	// not a volume out of the merge.
	var standalone []string
	singleFileRarGroups, otherGroups, standalone = p.separateStandaloneArchives(ctx, singleFileRarGroups, otherGroups)
	if len(standalone) > 0 {
		p.logger.Debug().Strs("archives", standalone).
			Msg("Kept single-volume RAR archives out of the obfuscated volume merge")
	}
	if len(singleFileRarGroups) <= 1 {
		return groups
	}

	p.logger.Debug().
		Int("single_file_rar_groups", len(singleFileRarGroups)).
		Msg("Detected potential obfuscated RAR archive, merging groups")

	// Try PAR2 name recovery before falling back to blind merge.
	if recovered := p.tryPar2NameRecovery(ctx, singleFileRarGroups, otherGroups, rawFiles, par2Names); recovered != nil {
		return recovered
	}

	// Fallback: merge into a single group sorted by NZB upload order.
	mergedGroup := &FileGroup{
		BaseName:       singleFileRarGroups[0].BaseName,
		ActualFilename: singleFileRarGroups[0].ActualFilename,
		Type:           storage.NZBFileTypeRar,
		Files:          make([]nzbparser.NzbFile, 0, len(singleFileRarGroups)),
		Groups:         make(map[string]struct{}),
	}

	for _, group := range singleFileRarGroups {
		mergedGroup.Files = append(mergedGroup.Files, group.Files...)
		for g := range group.Groups {
			mergedGroup.Groups[g] = struct{}{}
		}
	}

	sort.SliceStable(mergedGroup.Files, func(i, j int) bool {
		return mergedGroup.Files[i].Number < mergedGroup.Files[j].Number
	})

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

// rarVolumeProbeFunc reports whether file's archive is one volume of a
// multi-volume set, from its main header; known is false when that could not
// be read.
type rarVolumeProbeFunc func(ctx context.Context, file nzbparser.NzbFile) (isVolume, known bool)

// probeRarVolume reads the first rarVolumeProbeBytes of file's first article.
func (p *NZBParser) probeRarVolume(ctx context.Context, file nzbparser.NzbFile) (bool, bool) {
	if p.manager == nil || len(file.Segments) == 0 {
		return false, false
	}
	first := file.Segments[0]
	for _, s := range file.Segments[1:] {
		if s.Number < first.Number {
			first = s
		}
	}
	var prefix []byte
	err := p.manager.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
		d, e := conn.GetHeaderPrefix(first.Id, rarVolumeProbeBytes)
		if d != nil {
			prefix = d.Snippet
		}
		return e
	})
	if err != nil {
		return false, false
	}
	return rarArchiveIsVolume(prefix)
}

// separateStandaloneArchives moves the groups whose archive is not a volume
// from candidates to others, returning their names. A file named as a volume
// (.partNN.rar, .rNN) is taken at its name, and so is a file with the article
// count most candidates share - a set's inner volumes all have one size, so
// only its last volume differs. The rest have their main header read,
// concurrently (the fetch drains the whole first article). A header that
// cannot be read leaves the group a candidate, as before.
func (p *NZBParser) separateStandaloneArchives(ctx context.Context, candidates, others []*FileGroup) ([]*FileGroup, []*FileGroup, []string) {
	probe := p.volumeProbe
	if probe == nil {
		probe = p.probeRarVolume
	}
	counts := map[int]int{}
	for _, g := range candidates {
		counts[len(g.Files[0].Segments)]++
	}
	innerCount, innerN := 0, 1
	for n, c := range counts {
		if c > innerN || (c == innerN && c > 1 && n > innerCount) {
			innerCount, innerN = n, c
		}
	}
	mapper := iter.Mapper[*FileGroup, bool]{MaxGoroutines: max(1, p.maxConcurrent)}
	standalone := mapper.Map(candidates, func(g **FileGroup) bool {
		f := (*g).Files[0]
		lower := strings.ToLower(f.Filename)
		if s := rarVolumeScheme(lower); s == rarSchemePart || (s == rarSchemeOld && !strings.HasSuffix(lower, ".rar")) {
			return false
		}
		if innerN > 1 && len(f.Segments) == innerCount {
			return false
		}
		isVolume, known := probe(ctx, f)
		return known && !isVolume
	})
	kept := candidates[:0:0]
	var names []string
	for i, g := range candidates {
		if standalone[i] {
			others = append(others, g)
			names = append(names, g.Files[0].Filename)
			continue
		}
		kept = append(kept, g)
	}
	return kept, others, names
}

// tryPar2NameRecovery attempts to recover real filenames for obfuscated
// single-file RAR groups using PAR2 FileDesc MD5-16k matching. Returns the
// rebuilt groups map only when ALL volumes are recovered; partial recovery
// is worse than upload-order because unrecovered files have garbage names
// that interleave arbitrarily with recovered ones. Returns nil to fall back
// to blind merging on any partial failure.
//
// par2Names gives the yEnc name of a PAR2 file whose subject is obfuscated
// (see groupFiles), so the index of a fully obfuscated release is found too.
func (p *NZBParser) tryPar2NameRecovery(ctx context.Context, singleFileRarGroups []*FileGroup, otherGroups []*FileGroup, rawFiles nzbparser.NzbFiles, par2Names map[string]string) map[string]*FileGroup {
	// Find the base .par2 index file from the raw NZB file list.
	// Only try the smallest base file (no .vol recovery volumes) and
	// bail if it needs too many segments — this is the import path.
	var par2IndexFile *nzbparser.NzbFile
	for i := range rawFiles {
		f := &rawFiles[i]
		if len(f.Segments) == 0 {
			continue
		}
		name := f.Filename
		if p.detectFileType(name) != storage.NZBFileTypePar2 {
			name = par2Names[fileMetaKey(*f)]
			if name == "" {
				continue
			}
		}
		lower := strings.ToLower(name)
		if strings.Contains(lower, ".vol") {
			continue
		}
		if par2IndexFile == nil || len(f.Segments) < len(par2IndexFile.Segments) {
			par2IndexFile = f
		}
	}
	if par2IndexFile == nil {
		p.logger.Debug().Msg("PAR2 name recovery: no base PAR2 file found in NZB")
		return nil
	}
	const maxPar2Segments = 10
	if len(par2IndexFile.Segments) > maxPar2Segments {
		p.logger.Debug().
			Int("segments", len(par2IndexFile.Segments)).
			Msg("PAR2 name recovery: base PAR2 file too large, skipping")
		return nil
	}

	data, err := p.fetchPar2FileData(ctx, *par2IndexFile)
	if err != nil {
		p.logger.Debug().Err(err).Str("file", par2IndexFile.Filename).
			Msg("PAR2 name recovery: failed to fetch PAR2 file")
		return nil
	}
	idx, err := par2.ParseIndex([]par2.Source{{Name: par2IndexFile.Filename, Data: data}})
	if err != nil || len(idx.Files) == 0 {
		p.logger.Debug().Err(err).
			Msg("PAR2 name recovery: failed to parse PAR2 index or no FileDesc entries")
		return nil
	}

	// Build MD5-16k → real filename map from the PAR2 FileDesc table.
	md5ToName := make(map[[16]byte]string, len(idx.Files))
	for _, fd := range idx.Files {
		if fd.Name != "" {
			md5ToName[fd.MD5_16k] = fd.Name
		}
	}
	if len(md5ToName) == 0 {
		return nil
	}

	// For each single-file RAR group, fetch first 16KB, compute MD5,
	// and look up the real filename.
	recovered := make([]recoveredFile, 0, len(singleFileRarGroups))

	for _, group := range singleFileRarGroups {
		if len(group.Files) == 0 || len(group.Files[0].Segments) == 0 {
			p.logger.Debug().Msg("PAR2 name recovery: group with no files/segments, aborting")
			return nil
		}
		f := group.Files[0]
		seg := f.Segments[0]

		var meta *nntp.YencMetadata
		fetchErr := p.manager.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
			d, e := conn.GetHeaderPrefix(seg.Id, par2MD5PrefixSize)
			meta = d
			return e
		})
		if fetchErr != nil || meta == nil || len(meta.Snippet) < par2MD5PrefixSize {
			p.logger.Debug().
				Err(fetchErr).
				Msg("PAR2 name recovery: short or failed 16KB fetch, aborting")
			return nil
		}

		hash := md5.Sum(meta.Snippet)
		realName, ok := md5ToName[hash]
		if !ok {
			p.logger.Debug().
				Str("obfuscated", f.Filename).
				Msg("PAR2 name recovery: MD5-16k mismatch, aborting")
			return nil
		}

		recovered = append(recovered, recoveredFile{
			file: f, realName: realName, groups: group.Groups,
		})
	}

	p.logger.Info().
		Int("recovered", len(recovered)).
		Int("par2_files", len(idx.Files)).
		Msg("PAR2 name recovery: recovered real filenames for all obfuscated RAR volumes")

	result, collision := regroupRecoveredVolumes(recovered, otherGroups, p.getBaseFilename)
	if result == nil {
		p.logger.Debug().
			Str("key", collision).
			Msg("PAR2 name recovery: group key collision with existing group, aborting")
		return nil
	}
	return result
}

// recoveredFile is one obfuscated RAR volume whose real name
// tryPar2NameRecovery found in the release's PAR2 index.
type recoveredFile struct {
	file     nzbparser.NzbFile
	realName string
	groups   map[string]struct{}
}

// regroupRecoveredVolumes builds the RAR groups for volumes whose real names
// were recovered: every volume of one archive (same base name) goes into one
// group, sorted by name. It returns nil and the colliding key when a
// recovered base name is already an existing group's, since merging into a
// group of another type would mix unrelated files.
//
// Volumes of one archive share their base name, so collisions are checked
// against otherGroups only - checking the groups built here aborted every
// recovery at its second volume.
func regroupRecoveredVolumes(recovered []recoveredFile, otherGroups []*FileGroup, baseName func(string) string) (map[string]*FileGroup, string) {
	result := make(map[string]*FileGroup, len(otherGroups)+1)
	for _, group := range otherGroups {
		result[group.BaseName] = group
	}
	existing := make(map[string]bool, len(otherGroups))
	for key := range result {
		existing[key] = true
	}

	for _, rf := range recovered {
		rf.file.Filename = rf.realName
		groupKey := baseName(rf.realName)
		if existing[groupKey] {
			return nil, groupKey
		}
		group, ok := result[groupKey]
		if !ok {
			group = &FileGroup{
				BaseName:       groupKey,
				ActualFilename: rf.realName,
				Type:           storage.NZBFileTypeRar,
				Groups:         make(map[string]struct{}),
			}
			result[groupKey] = group
		}
		group.Files = append(group.Files, rf.file)
		for g := range rf.groups {
			group.Groups[g] = struct{}{}
		}
	}

	// Sort each RAR group's files by recovered filename; RARParser.Process
	// orders them by volume number afterwards.
	for _, group := range result {
		if group.Type != storage.NZBFileTypeRar || len(group.Files) <= 1 {
			continue
		}
		sort.Slice(group.Files, func(i, j int) bool {
			return group.Files[i].Filename < group.Files[j].Filename
		})
	}
	return result, ""
}

// fetchPar2FileData fetches all segments of a PAR2 file over NNTP and returns
// the concatenated decoded data.
func (p *NZBParser) fetchPar2FileData(ctx context.Context, f nzbparser.NzbFile) ([]byte, error) {
	segs := make(nzbparser.NzbSegments, len(f.Segments))
	copy(segs, f.Segments)
	sort.Sort(segs)

	var out []byte
	for _, seg := range segs {
		var data []byte
		fetchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := p.manager.ExecuteWithFailover(fetchCtx, func(conn *nntp.Connection) error {
			d, _, e := conn.GetDecodedBodyWithMetadata(seg.Id)
			data = d
			return e
		})
		cancel()
		if err != nil {
			return nil, fmt.Errorf("fetch segment %s: %w", seg.Id, err)
		}
		out = append(out, data...)
	}
	return out, nil
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

		// groupFiles drops a PAR2 file named so in its subject. One with an
		// obfuscated subject is only recognised here, from its yEnc name, and
		// "X.par2" has the same base name as X's volumes or media file: grouped,
		// it would sit among them, or - listed first - make the group a PAR2
		// one that no processor takes, and the release's files go with it.
		if item.fileType == storage.NZBFileTypePar2 {
			continue
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
// FileTypeByName is the type a file's name alone gives it; Unknown for a name
// that says nothing, such as an obfuscated subject.
func FileTypeByName(filename string) storage.NZBFileType {
	return (&NZBParser{}).detectFileType(filename)
}

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
	// Stable: files sharing a subject number stay in NZB order. A filename
	// tie-break sorted an obfuscated merge's random names over the upload
	// order mergeObfuscatedRarGroups had put them in; named volumes are ordered
	// by name by their archive processor anyway.
	sort.SliceStable(group.Files, func(i, j int) bool {
		return group.Files[i].Number < group.Files[j].Number
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
	group.metadata.measure(firstFile, fileSize)
	group.metadata.measure(lastFile, lastFileSize)
	return nil
}

// Process regular media files
func (p *NZBParser) processMediaFile(group *FileGroup, password string) *storage.NZBFile {
	if len(group.Files) == 0 {
		return nil
	}

	// Sort files for consistent ordering
	sort.SliceStable(group.Files, func(i, j int) bool {
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

	fileType, name := p.typeAndNameFromContent(data, file.Filename)
	return fileType, name, nil
}

// typeAndNameFromContent classifies a post from its first article's leading
// bytes. An obfuscated post whose name carries no known extension but whose
// bytes are recognisable media gets the matching extension added to its yEnc
// name (or, without one, its posted name), so the file is listed and streamed
// as media rather than as an extensionless file.
func (p *NZBParser) typeAndNameFromContent(data *nntp.YencMetadata, postedName string) (storage.NZBFileType, string) {
	fileType, inferredExtension := p.detectFileTypeAndExtensionFromContent(data.Snippet)
	name := data.Name
	if fileType == storage.NZBFileTypeMedia && inferredExtension != "" &&
		p.detectFileType(name) == storage.NZBFileTypeUnknown {
		if name == "" {
			name = postedName
		}
		name += inferredExtension
	}
	return fileType, name
}

// detectFileTypeAndExtensionFromContent classifies a post from its leading
// bytes and, for media, returns a canonical extension when the signature is
// specific enough to name a streamable file.
func (p *NZBParser) detectFileTypeAndExtensionFromContent(data []byte) (storage.NZBFileType, string) {
	if len(data) == 0 {
		return storage.NZBFileTypeUnknown, ""
	}

	// Check for RAR signatures (both RAR 4.x and 5.x)
	if len(data) >= 7 {
		// RAR 4.x signature
		if bytes.Equal(data[:7], []byte("Rar!\x1A\x07\x00")) {
			return storage.NZBFileTypeRar, ""
		}
	}
	if len(data) >= 8 {
		// RAR 5.x signature
		if bytes.Equal(data[:8], []byte("Rar!\x1A\x07\x01\x00")) {
			return storage.NZBFileTypeRar, ""
		}
	}

	// Check for ZIP signature
	if len(data) >= 4 && bytes.Equal(data[:4], []byte{0x50, 0x4B, 0x03, 0x04}) {
		return storage.NZBFileTypeZip, ""
	}

	// Check for 7z signature
	if len(data) >= 6 && bytes.Equal(data[:6], []byte{0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C}) {
		return storage.NZBFileTypeSevenZip, ""
	}

	// Check for common media file signatures
	if len(data) >= 4 {
		// Matroska (MKV/WebM)
		if bytes.Equal(data[:4], []byte{0x1A, 0x45, 0xDF, 0xA3}) {
			return storage.NZBFileTypeMedia, ".mkv"
		}

		// MP4/MOV (check for 'ftyp' at offset 4)
		if len(data) >= 8 && bytes.Equal(data[4:8], []byte("ftyp")) {
			return storage.NZBFileTypeMedia, ".mp4"
		}

		// AVI
		if len(data) >= 12 && bytes.Equal(data[:4], []byte("RIFF")) &&
			bytes.Equal(data[8:12], []byte("AVI ")) {
			return storage.NZBFileTypeMedia, ".avi"
		}
	}

	// MPEG checks need more specific patterns
	if len(data) >= 4 {
		// MPEG-1/2 Program Stream
		if bytes.Equal(data[:4], []byte{0x00, 0x00, 0x01, 0xBA}) {
			return storage.NZBFileTypeMedia, ".mpg"
		}

		// MPEG-1/2 Video Stream
		if bytes.Equal(data[:4], []byte{0x00, 0x00, 0x01, 0xB3}) {
			return storage.NZBFileTypeMedia, ".mpg"
		}
	}

	// Check for Transport Stream (TS files)
	if len(data) >= 1 && data[0] == 0x47 {
		// Additional validation: TS packets are 188 bytes, so the next
		// sync byte sits at index 188 (requires at least 189 bytes).
		if len(data) > 188 && data[188] == 0x47 {
			return storage.NZBFileTypeMedia, ".ts"
		}
	}

	return storage.NZBFileTypeUnknown, ""
}

// needsContentDetection reports whether grouping had to probe any file's
// content over the network (a file with segments whose type its name does
// not reveal) - a step a timeout or cancellation can cut short.
func needsContentDetection(p *NZBParser, files nzbparser.NzbFiles) bool {
	for _, f := range files {
		if len(f.Segments) > 0 && p.detectFileType(f.Filename) == storage.NZBFileTypeUnknown {
			return true
		}
	}
	return false
}

// holdsOnlyKnownNonMedia reports whether an NZB that yielded no file groups
// did so because it genuinely holds no media: at least one file has
// articles, and every such file's type is known from its name. An NZB with
// no articles at all is malformed input, not an unavailable release, and one
// needing network detection may only have been cut short.
func holdsOnlyKnownNonMedia(p *NZBParser, files nzbparser.NzbFiles) bool {
	withArticles := false
	for _, f := range files {
		if len(f.Segments) > 0 {
			withArticles = true
			break
		}
	}
	return withArticles && !needsContentDetection(p, files)
}
