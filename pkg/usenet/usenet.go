package usenet

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/fs"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
	"github.com/sirrobot01/decypharr/pkg/usenet/types"
	"golang.org/x/sync/errgroup"
)

const (
	bufferSize = 256 * 1024 // 256KB buffer for streaming
)

var streamBufferPool = sync.Pool{
	New: func() any {
		return make([]byte, bufferSize)
	},
}

func acquireStreamBuffer() []byte {
	buf := streamBufferPool.Get().([]byte)
	if cap(buf) < bufferSize {
		buf = make([]byte, bufferSize)
	}
	return buf[:bufferSize]
}

func releaseStreamBuffer(buf []byte) {
	if buf == nil {
		return
	}
	if cap(buf) < bufferSize {
		return
	}
	streamBufferPool.Put(buf[:bufferSize])
}

type fsEntry struct {
	fs            *fs.FS
	volumes       []*types.Volume
	reader        fs.PrefetchableReaderAt // Shared reader with prefetch capability
	readerSize    int64                   // Size of the volume
	readerCleanup func()                  // Cleanup function for reader
	readerOnce    sync.Once               // Ensures reader is created exactly once
	readerErr     error                   // Error from reader creation (if any)
	refCount      atomic.Int32
	lastAccessed  atomic.Int64 // Unix timestamp
}

func (fe *fsEntry) cleanup() {
	if fe.readerCleanup != nil {
		fe.readerCleanup()
		fe.readerCleanup = nil
		fe.reader = nil
	}
}

// getOrCreateReader returns the shared reader, creating it lazily on first use.
// Uses sync.Once to ensure the reader is created exactly once even under concurrent access.
func (fe *fsEntry) getOrCreateReader() (fs.PrefetchableReaderAt, int64, error) {
	fe.readerOnce.Do(func() {
		var readerAt fs.PrefetchableReaderAt
		var size int64
		var cleanup func()
		var err error

		// Single volume optimization - skip multi-volume overhead
		if len(fe.volumes) == 1 {
			readerAt, size, cleanup, err = fe.fs.CreateReaderAtForVolume(fe.volumes[0])
		} else {
			// Multi-volume case - need to create reader differently
			// For now, fall back to io.ReaderAt (no prefetch for multi-volume)
			var plainReaderAt io.ReaderAt
			plainReaderAt, size, cleanup, err = fe.fs.CreateReaderAt()
			if err != nil {
				fe.readerErr = err
				return
			}
			// Wrap in a no-op prefetchable reader
			readerAt = &noPrefetchReader{ReaderAt: plainReaderAt}
		}

		if err != nil {
			fe.readerErr = err
			return
		}

		fe.reader = readerAt
		fe.readerSize = size
		fe.readerCleanup = cleanup
	})

	if fe.readerErr != nil {
		return nil, 0, fe.readerErr
	}
	return fe.reader, fe.readerSize, nil
}

// noPrefetchReader wraps io.ReaderAt for cases where prefetch isn't available
type noPrefetchReader struct {
	io.ReaderAt
}

func (n *noPrefetchReader) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	nr, err := n.ReaderAt.ReadAt(p, off)
	if ctxErr := ctx.Err(); ctxErr != nil && nr == 0 {
		return 0, ctxErr
	}
	return nr, err
}

func (n *noPrefetchReader) Prefetch(ctx context.Context, off, length int64) {
	// No-op for multi-volume readers
}

type contextSectionReader struct {
	ctx   context.Context
	r     fs.PrefetchableReaderAt
	base  int64
	limit int64
	off   int64
}

func newContextSectionReader(ctx context.Context, r fs.PrefetchableReaderAt, off, length int64) *contextSectionReader {
	if ctx == nil {
		ctx = context.Background()
	}
	return &contextSectionReader{
		ctx:   ctx,
		r:     r,
		base:  off,
		limit: length,
	}
}

func (r *contextSectionReader) Read(p []byte) (int, error) {
	if r.off >= r.limit {
		return 0, io.EOF
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	remaining := r.limit - r.off
	if int64(len(p)) > remaining {
		p = p[:int(remaining)]
	}
	n, err := r.r.ReadAtContext(r.ctx, p, r.base+r.off)
	r.off += int64(n)
	if err == io.EOF && r.off < r.limit {
		return n, io.ErrUnexpectedEOF
	}
	if err == nil && r.off >= r.limit {
		return n, io.EOF
	}
	return n, err
}

type Usenet struct {
	nntp                     *nntp.Client
	logger                   zerolog.Logger
	metadataDir              string
	nzbStorage               *NZBStorage // File-based NZB metadata storage
	maxConnections           int         // Connections allocated per streaming file
	processingMaxConnections int         // Connections allocated per file for parsing and NZB downloads
	prefetchSize             int64       // Streaming prefetch size in bytes
	failedFiles              *xsync.Map[string, error]

	fs *xsync.Map[string, *fsEntry]
}

// fsKey builds a cache key for fs map entries efficiently.
// Uses direct byte slice manipulation to avoid strings.Builder overhead.
func fsKey(nzoID, filename string) string {
	// Single allocation: nzoID + "::" + filename
	buf := make([]byte, len(nzoID)+2+len(filename))
	n := copy(buf, nzoID)
	buf[n] = ':'
	buf[n+1] = ':'
	copy(buf[n+2:], filename)
	return string(buf)
}

// New creates a new usenet instance
func New() (*Usenet, error) {
	cfg := config.Get()
	usenetConfig := cfg.Usenet
	if len(usenetConfig.Providers) == 0 {
		return nil, fmt.Errorf("no usenet providers configured")
	}
	_logger := logger.New("usenet")

	metadataDir := filepath.Join(config.GetMainPath(), "usenet", "nzbs")
	if err := os.MkdirAll(metadataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create metadata dir: %w", err)
	}

	// Create file-based NZB storage
	nzbStorage, err := NewNZBStorage()
	if err != nil {
		return nil, fmt.Errorf("failed to create NZB storage: %w", err)
	}

	// Create NNTP client with retry configuration
	client, err := nntp.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	maxConns := usenetConfig.MaxConnections
	if maxConns <= 0 {
		maxConns = 10
	}
	processingMaxConns := usenetConfig.ProcessingMaxConnections
	if processingMaxConns <= 0 {
		processingMaxConns = maxConns
	}

	prefetchSize, err := config.ParseSize(usenetConfig.ReadAhead)
	if err != nil {
		prefetchSize = 16 * 1024 * 1024 // Default to 16MB
	}

	u := &Usenet{
		nzbStorage:               nzbStorage,
		nntp:                     client,
		logger:                   _logger,
		metadataDir:              metadataDir,
		maxConnections:           maxConns,
		processingMaxConnections: processingMaxConns,
		prefetchSize:             prefetchSize,
		fs:                       xsync.NewMap[string, *fsEntry](),
		failedFiles:              xsync.NewMap[string, error](),
	}

	// clean streams dir
	u.initStreamsDir(cfg.Usenet.DiskBufferPath)

	// Start background cleanup for idle sessions
	go u.cleanupIdleFS()

	return u, nil
}

func (u *Usenet) initStreamsDir(streamsDir string) {
	if err := os.RemoveAll(streamsDir); err != nil && !os.IsNotExist(err) {
		return
	}
	if err := os.MkdirAll(streamsDir, 0755); err != nil {
		return
	}
}

func (u *Usenet) createEntry(file *storage.NZBFile) (*fsEntry, error) {
	volumes := GetFileVolumes(file)
	if len(volumes) == 0 {
		return nil, fmt.Errorf("no volumes available for file %s", file.Name)
	}

	fsCtx := context.Background()

	usenetFS, err := fs.NewFS(fsCtx, u.nntp, u.maxConnections, u.prefetchSize, volumes, u.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create usenet FS: %w", err)
	}

	return &fsEntry{
		fs:      usenetFS,
		volumes: volumes,
	}, nil
}

// getOrCreateEntry returns the fsEntry and its cache key to avoid redundant key computation.
func (u *Usenet) getOrCreateEntry(ctx context.Context, nzoID, filename string) (*fsEntry, string, error) {
	key := fsKey(nzoID, filename)

	// Fast path: entry already exists
	if entry, ok := u.fs.Load(key); ok {
		entry.refCount.Add(1)
		entry.lastAccessed.Store(utils.NowUnix())
		return entry, key, nil
	}

	// Slow path: need to create entry
	file, err := u.getFile(nzoID, filename)
	if err != nil {
		return nil, key, err
	}

	// Pre-checks
	if err := u.preStreamChecks(file); err != nil {
		return nil, key, err
	}

	newEntry, err := u.createEntry(file)
	if err != nil {
		return nil, key, err
	}

	// Atomically store only if key doesn't exist (prevents race condition)
	actual, loaded := u.fs.LoadOrStore(key, newEntry)
	if loaded {
		// Another goroutine created the entry first - use theirs
		// Our newEntry was never used, cleanup will happen via GC
		actual.refCount.Add(1)
		actual.lastAccessed.Store(utils.NowUnix())
		return actual, key, nil
	}

	// We won the race - use our new entry
	newEntry.refCount.Add(1)
	newEntry.lastAccessed.Store(utils.NowUnix())
	return newEntry, key, nil
}

// releaseFS releases an fs entry using a pre-computed key (avoids redundant allocation).
func (u *Usenet) releaseFS(key string) {
	entry, ok := u.fs.Load(key)
	if !ok {
		return
	}

	entry.refCount.Add(-1)
	entry.lastAccessed.Store(utils.NowUnix())
}

// cleanupIdleFS removes sessions with refCount=0 that haven't been used recently
func (u *Usenet) cleanupIdleFS() {
	// Keep a warm reader through short pauses, then tear it down. Usenet segment
	// buffering is only for active latency hiding; stale buffers should disappear
	// quickly instead of behaving like a VFS cache.
	const idleThreshold = int64(120) // 2 minutes idle
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		now := utils.NowUnix()

		u.fs.Range(func(key string, entry *fsEntry) bool {
			if entry.refCount.Load() == 0 {
				lastUsed := entry.lastAccessed.Load()
				if now-lastUsed > idleThreshold {
					// Cleanup
					entry.cleanup()
					u.fs.Delete(key)
				}
			}
			return true
		})
	}
}

// Parse processes an NZB for download/streaming (quick parse, defers archive extraction)
func (u *Usenet) Parse(ctx context.Context, name string, content []byte, category string) (*storage.NZB, map[string]*parser.FileGroup, error) {
	return u.ParseWithID(ctx, "", name, content, category)
}

// ParseWithID parses an NZB using a caller-provided ID. Supplying the ID lets
// the manager expose a queued entry before the active-download worker starts.
func (u *Usenet) ParseWithID(ctx context.Context, id, name string, content []byte, category string) (*storage.NZB, map[string]*parser.FileGroup, error) {
	if len(content) == 0 {
		return nil, nil, fmt.Errorf("NZB content is empty")
	}

	// Validate NZB content
	if err := validateNZB(content); err != nil {
		return nil, nil, fmt.Errorf("invalid NZB content: %w", err)
	}

	// Create parser with the manager
	prs := parser.NewParser(u.nntp, u.processingMaxConnections, u.logger.With().Str("component", "parser").Logger())

	// Quick parse: defer archive extraction for async processing
	nzb, groups, err := prs.Parse(ctx, name, content)
	if err != nil {
		return nil, nil, err
	}
	if id != "" {
		nzb.ID = id
	}

	nzb.Category = category
	nzb.Status = NZBStatusParsing
	// Save NZB file to disk
	nzbPath, err := u.saveNZBFile(name, content)
	if err != nil {
		return nil, nil, err
	}
	nzb.Path = nzbPath

	// Mark as processing
	if err := u.markAsProcessing(nzb); err != nil {
		return nil, nil, fmt.Errorf("failed to mark NZB as processing: %w", err)
	}

	if err := u.nzbStorage.AddNZB(nzb); err != nil {
		return nil, nil, fmt.Errorf("failed to save NZB to storage: %w", err)
	}

	u.logger.Info().
		Str("nzb_id", nzb.ID).
		Str("name", nzb.Name).
		Int("groups", len(groups)).
		Msg("Successfully parsed NZB file")
	return nzb, groups, nil
}

// Process processes archive files in an NZB (full parse)
func (u *Usenet) Process(ctx context.Context, nzb *storage.NZB, groups map[string]*parser.FileGroup) (*storage.NZB, error) {
	u.logger.Info().
		Str("nzb_id", nzb.ID).
		Str("name", nzb.Name).
		Msg("Processing archive files in NZB")

	// Create parser with the manager
	prs := parser.NewParser(u.nntp, u.processingMaxConnections, u.logger.With().Str("component", "parser").Logger())
	// Process the groups (archives)
	updatedNZB, err := prs.Process(ctx, nzb, groups)
	if err != nil {
		// Mark as failed
		_ = u.markAsFailed(nzb, err)
		return nzb, fmt.Errorf("failed to process NZB archives: %w", err)
	}

	// Post-parse availability gate: probe a sample of each content file's
	// segments before declaring the NZB complete. Segments can go missing
	// between the original parse and now; without this gate they slip through
	// to Sonarr/Radarr and only surface later as failed ffprobes. Connection
	// errors are non-fatal here (CheckFileAvailability returns nil for those),
	// so a provider hiccup won't wrongly fail an import — only a definitively
	// missing segment (gone on every provider) fails the NZB.
	if err := u.checkNZBAvailability(ctx, updatedNZB); err != nil {
		_ = u.markAsFailed(updatedNZB, err)
		return updatedNZB, fmt.Errorf("availability check failed: %w", err)
	}

	// Mark as completed
	if err := u.markAsCompleted(updatedNZB); err != nil {
		return updatedNZB, fmt.Errorf("failed to mark NZB as completed: %w", err)
	}

	u.logger.Info().
		Str("nzb_id", updatedNZB.ID).
		Str("name", updatedNZB.Name).
		Int("files", len(updatedNZB.Files)).
		Msg("Successfully processed NZB archives (full parse)")
	return updatedNZB, nil
}

// checkAvailability samples each content file's segments (via the same
// repair-bank-gated BatchStat path as CheckFile) and returns an error if any
// file is definitively unavailable — i.e. a sampled segment is missing on
// every provider. Recovery/noise files (par2, ignore), deleted files, and
// segment-less entries are skipped so the gate fails only on genuinely missing
// playable content. Connection-only failures are treated as non-fatal by
// CheckFileAvailability, so they do not fail the NZB. It returns on the first
// definitively-missing file (fail fast).
func (u *Usenet) checkNZBAvailability(ctx context.Context, nzb *storage.NZB) error {
	samplePercent := config.Get().Usenet.ImportAvailabilitySamplePercent
	for i := range nzb.Files {
		file := &nzb.Files[i]
		if file.IsDeleted || len(file.Segments) == 0 {
			continue
		}
		switch file.FileType {
		case storage.NZBFileTypePar2, storage.NZBFileTypeIgnore:
			continue
		}
		if ctx.Err() != nil {
			// Cancelled/timed out: not a content failure — don't fail the NZB.
			return nil
		}
		if err := u.CheckFileAvailability(ctx, file, samplePercent); err != nil {
			u.logger.Warn().
				Err(err).
				Str("nzb_id", nzb.ID).
				Str("file", file.Name).
				Msg("Post-parse availability check failed; marking NZB unavailable")
			return fmt.Errorf("file %q unavailable: %w", file.Name, err)
		}
	}
	return nil
}

// CheckFile probes the availability of a single NZB file. Connection use is
// gated by the NNTP client's repair bank so concurrent probes don't starve
// streaming traffic. Deep-verify behavior follows configuration: it runs only
// when Usenet.DeepVerifySamplePercent > 0 (callers that need per-call control
// use CheckFileWithDeepVerify).
func (u *Usenet) CheckFile(ctx context.Context, nzoID, filename string) error {
	return u.CheckFileWithDeepVerify(ctx, nzoID, filename, -1)
}

// CheckFileWithDeepVerify is CheckFile with an explicit deep-verify percent.
// deepVerifyPercent < 0 means "use the configured Usenet.DeepVerifySamplePercent";
// >= 0 overrides it for this call (0 disables deep verify for this call).
func (u *Usenet) CheckFileWithDeepVerify(ctx context.Context, nzoID, filename string, deepVerifyPercent int) error {
	file, err := u.getFile(nzoID, filename)
	if err != nil {
		return fmt.Errorf("failed to get file: %w", err)
	}
	if len(file.Segments) == 0 {
		return fmt.Errorf("file has no Segments: %s", filename)
	}

	cfg := config.Get()
	samplePercent := cfg.Usenet.AvailabilitySamplePercent
	if deepVerifyPercent < 0 {
		deepVerifyPercent = cfg.Usenet.DeepVerifySamplePercent
	}
	err = u.checkFileAvailability(ctx, file, samplePercent, deepVerifyPercent)
	file.Segments = nil
	return err
}

// CheckFileAvailability runs the STAT-based availability sample only (no deep
// verify). Used by the import-time availability check, which is intentionally
// light. The repair path uses checkFileAvailability with a deep-verify percent.
func (u *Usenet) CheckFileAvailability(ctx context.Context, file *storage.NZBFile, samplePercent int) error {
	return u.checkFileAvailability(ctx, file, samplePercent, 0)
}

func (u *Usenet) checkFileAvailability(ctx context.Context, file *storage.NZBFile, samplePercent, deepVerifyPercent int) error {
	// Sample segments based on configured percentage
	messageIDs := u.sampleSegments(file.Segments, samplePercent)

	// Batch STAT checks. The NNTP client gates each worker through its internal
	// repair bank so concurrent availability checks don't starve streaming
	// connections.
	result, err := u.nntp.BatchStat(ctx, messageIDs)
	if err != nil {
		// Connection/system error - log and continue (don't fail availability check)
		u.logger.Warn().
			Err(err).
			Str("file", file.Name).
			Msg("Non-fatal error during availability check, ignoring")
		return nil
	}

	// Check if all sampled segments are available.
	// Distinguish genuine article-not-found from connection errors:
	//   TotalCount = FoundCount + notFoundCount + ErrorCount
	// Only treat a file as unavailable when segments are definitively missing
	// (notFoundCount > 0). Connection errors mean we couldn't check — treat
	// those the same as the top-level error path above (non-fatal, skip check).
	if !result.AllAvailable() {
		notFoundCount := result.TotalCount - result.FoundCount - result.ErrorCount
		if result.ErrorCount > 0 && notFoundCount == 0 {
			// All failures were connection errors, not missing articles.
			return nil
		}
		// At least some segments are definitively missing.
		u.logger.Warn().
			Str("file", file.Name).
			Int("total_segments", len(file.Segments)).
			Int("available_segments", result.FoundCount).
			Int("missing_segments", notFoundCount).
			Int("error_count", result.ErrorCount).
			Msg("File is unavailable - one or more segments are missing")
		return customerror.UsenetSegmentMissingError
	}

	// STAT says every sampled segment is present. Some providers keep the
	// overview entry (223 on STAT) alive after the body has been purged or
	// taken down, returning 430 on BODY. Such "head alive, body dead" files
	// pass the STAT sample at any percent yet fail every real read. When a
	// deep-verify percent is in effect, BODY-fetch a sample to catch them.
	if deepVerifyPercent > 0 {
		if err := u.deepVerifyFile(ctx, file, deepVerifyPercent); err != nil {
			return err
		}
	}

	return nil
}

// deepVerifyFile BODY-fetches a sample of the file's segments to confirm their
// bodies are actually retrievable, not merely present per STAT. Any segment
// that returns article-not-found marks the whole file unavailable. Connection
// errors are non-fatal (mirrors the STAT path): we only fail on a definitive
// missing body. Concurrency is bounded to the repair bank so deep verify does
// not starve streaming connections.
func (u *Usenet) deepVerifyFile(ctx context.Context, file *storage.NZBFile, percent int) error {
	messageIDs := u.sampleSegments(file.Segments, percent)
	if len(messageIDs) == 0 {
		return nil
	}

	limit := u.nntp.RepairBankCapacity()
	if limit <= 0 {
		limit = 4 // conservative default when no repair bank is configured
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(limit)

	var mu sync.Mutex
	var missing int
	var connErrors int
	var okCount int
	var sampleErr string

	for _, msgID := range messageIDs {
		msgID := msgID
		g.Go(func() error {
			if gctx.Err() != nil {
				return nil
			}
			err := u.nntp.VerifySegmentBody(gctx, msgID)
			if err == nil {
				mu.Lock()
				okCount++
				mu.Unlock()
				return nil
			}
			if nntp.IsArticleNotFoundError(err) || customerror.IsPermanentError(err) {
				mu.Lock()
				missing++
				mu.Unlock()
				// One definitively-missing body is enough to condemn the
				// file; cancel the rest of the sample.
				cancel()
				return nil
			}
			// Connection/transient error — not a missing body. Count but
			// don't condemn (matches the STAT path's treatment).
			mu.Lock()
			connErrors++
			if sampleErr == "" {
				sampleErr = err.Error()
			}
			mu.Unlock()
			return nil
		})
	}
	_ = g.Wait()

	u.logger.Debug().
		Str("file", file.Name).
		Int("total_segments", len(file.Segments)).
		Int("sampled", len(messageIDs)).
		Int("ok", okCount).
		Int("missing", missing).
		Int("conn_errors", connErrors).
		Str("sample_err", sampleErr).
		Msg("deepVerify outcome")

	if missing > 0 {
		u.logger.Warn().
			Str("file", file.Name).
			Int("total_segments", len(file.Segments)).
			Int("deep_verified", len(messageIDs)).
			Int("missing_bodies", missing).
			Int("conn_errors", connErrors).
			Msg("File is unavailable - segment body missing on deep verify (STAT-alive, BODY-dead)")
		return customerror.UsenetSegmentMissingError
	}

	return nil
}

// sampleSegments returns a sample of segment message IDs based on the given
// percentage. Dead bodies on a purged/taken-down file are almost always
// CONTIGUOUS (a provider drops a run of articles, not scattered singles). To
// catch a contiguous dead run, the gap between consecutively probed segments
// must be smaller than the run. Evenly-spaced single points are optimal for
// this per unit of budget, so the sample is uniform — but the stride is capped
// (maxStride) so that even at a low percent we never step over a dead run
// longer than that cap. The configured percent raises the budget (tighter
// stride, more reliable); the cap is a floor on reliability. First and last
// segments are always included (corruption frequently sits at boundaries).
func (u *Usenet) sampleSegments(segments []storage.NZBSegment, percent int) []string {
	total := len(segments)
	if total == 0 {
		return nil
	}

	// 100% = check all
	if percent >= 100 {
		messageIDs := make([]string, total)
		for i, seg := range segments {
			messageIDs[i] = seg.MessageID
		}
		return messageIDs
	}

	// For very small files, just check all
	if total <= 3 {
		messageIDs := make([]string, total)
		for i, seg := range segments {
			messageIDs[i] = seg.MessageID
		}
		return messageIDs
	}

	// Budget from the configured percent (minimum 2 for first+last).
	targetCount := (total * percent) / 100
	if targetCount < 2 {
		targetCount = 2
	}
	if targetCount > total {
		targetCount = total
	}

	// Reliability floor: never let the stride exceed maxStride, so any
	// contiguous dead run longer than maxStride is guaranteed to be hit even
	// if the percent budget alone would have strided further. This can raise
	// the effective sample above the configured percent on large files; that
	// is intentional — it is the cost of reliably catching short dead runs.
	const maxStride = 8
	minCountForStride := (total + maxStride - 1) / maxStride // ceil(total/maxStride)
	if targetCount < minCountForStride {
		targetCount = minCountForStride
	}
	if targetCount > total {
		targetCount = total
	}

	// Evenly spaced sample of targetCount points across [0, total-1].
	seen := make(map[int]struct{}, targetCount+2)
	order := make([]int, 0, targetCount+2)
	add := func(idx int) {
		if idx < 0 || idx >= total {
			return
		}
		if _, ok := seen[idx]; ok {
			return
		}
		seen[idx] = struct{}{}
		order = append(order, idx)
	}

	add(0)
	add(total - 1)

	if targetCount > 1 {
		step := float64(total-1) / float64(targetCount-1)
		for i := 0; i < targetCount; i++ {
			add(int(step * float64(i)))
		}
	}

	messageIDs := make([]string, 0, len(order))
	for _, idx := range order {
		messageIDs = append(messageIDs, segments[idx].MessageID)
	}
	return messageIDs
}

func (u *Usenet) Stop() {
	u.logger.Info().Msg("Stopping Usenet")
}

// Close closes all usenet resources including NNTP connections
func (u *Usenet) Close() error {
	u.logger.Info().Msg("Closing Usenet NNTP client")

	// Close NNTP client FIRST to force-close all active connections.
	// This unblocks any in-flight StreamBody/TCP reads in prefetch workers,
	// allowing SegmentFetcher.Close() (prefetchWg.Wait()) to complete without hanging.
	if u.nntp != nil {
		if err := u.nntp.Close(); err != nil {
			u.logger.Warn().Err(err).Msg("Failed to close NNTP client")
		}
	}

	// Cleanup all active FS entries (fetcher.Close() now completes quickly
	// because connections were already force-closed above)
	u.fs.Range(func(key string, entry *fsEntry) bool {
		entry.cleanup()
		return true
	})
	u.fs.Clear()

	u.logger.Info().Msg("Usenet closed")
	return nil
}

func (u *Usenet) getFile(nzoID, filename string) (*storage.NZBFile, error) {
	files, err := u.getFiles(nzoID, []string{filename})
	if err != nil {
		return nil, err
	}
	file := files[filename]
	if file == nil {
		return nil, fmt.Errorf("file %s not found in NZB %s", filename, nzoID)
	}
	return file, nil
}

func (u *Usenet) getFiles(nzoID string, filenames []string) (map[string]*storage.NZBFile, error) {
	nzb, err := u.nzbStorage.GetNZB(nzoID)
	if err != nil {
		return nil, fmt.Errorf("metadata load failed: %w", err)
	}

	requested := make(map[string]struct{}, len(filenames))
	for _, filename := range filenames {
		requested[filename] = struct{}{}
	}

	files := make(map[string]*storage.NZBFile, len(requested))
	for i := range nzb.Files {
		source := nzb.Files[i]
		if source.IsDeleted {
			continue
		}
		if _, ok := requested[source.Name]; !ok {
			continue
		}
		file := source
		if file.NzbID == "" {
			file.NzbID = nzoID
		}
		files[file.Name] = &file
	}
	return files, nil
}

func (u *Usenet) preStreamChecks(file *storage.NZBFile) error {
	// Check if we have Segments
	if len(file.Segments) == 0 {
		return fmt.Errorf("file has no Segments: %s", file.Name)
	}

	// Check if file was marked as failed previously
	if cause, ok := u.failedFiles.Load(fsKey(file.NzbID, file.Name)); ok {
		return customerror.NewSilentError(cause).Permanent()
	}

	return nil
}

// FailedFileCause returns the recorded permanent failure for a file (e.g. an
// article-not-found discovered during a prior read/prefetch), or nil if none.
// Lets higher layers surface the real cause instead of a generic "no data"
// error when a stream produces nothing because every segment is missing.
func (u *Usenet) FailedFileCause(nzoID, filename string) error {
	if cause, ok := u.failedFiles.Load(fsKey(nzoID, filename)); ok {
		return cause
	}
	return nil
}

// Stream streams a file using the new streaming system with caching and worker limiting
func (u *Usenet) Stream(ctx context.Context, nzoID, filename string, start, end int64, writer io.Writer) error {
	if start < 0 {
		start = 0
	}
	if end < start {
		return fmt.Errorf("invalid byte range %d-%d", start, end)
	}

	// Use getOrCreateEntry to get both entry and key in one call,
	// avoiding redundant key computation in releaseFS.
	ufsEntry, key, err := u.getOrCreateEntry(ctx, nzoID, filename)
	if err != nil {
		return fmt.Errorf("failed to get or create file system: %w", err)
	}
	defer u.releaseFS(key)

	// Use start/end directly - file segments are already positioned correctly
	rangeStart := start
	rangeEnd := end

	// Validate range against volume size
	if rangeEnd >= ufsEntry.volumes[0].Size {
		rangeEnd = ufsEntry.volumes[0].Size - 1
	}

	if rangeEnd < rangeStart {
		return fmt.Errorf("invalid resolved byte range %d-%d", rangeStart, rangeEnd)
	}

	// get shared reader from entry (created once, reused by all streams)
	readerAt, _, err := ufsEntry.getOrCreateReader()
	if err != nil {
		return fmt.Errorf("failed to get reader: %w", err)
	}

	length := rangeEnd - rangeStart + 1

	// Check context before starting
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Prefetch only a bounded read-ahead window from the requested start,
	// NOT the entire range. Queuing a whole multi-GB file would flood the
	// fixed-depth prefetch channel with head segments and starve reads that
	// land elsewhere (e.g. ffprobe seeking to the moov atom at EOF). The
	// per-read sliding window in readAtPlain advances this as playback
	// progresses; PreCache separately warms the head and tail.
	prefetchLen := length
	if u.prefetchSize > 0 && prefetchLen > u.prefetchSize {
		prefetchLen = u.prefetchSize
	}
	readerAt.Prefetch(ctx, rangeStart, prefetchLen)

	section := newContextSectionReader(ctx, readerAt, rangeStart, length)
	buf := acquireStreamBuffer()
	defer releaseStreamBuffer(buf)

	// Use a safe copy loop that checks context and validates read counts
	_, err = safeCopyBuffer(ctx, writer, section, buf)

	// Handle context cancellation explicitly
	if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}

	// Mark file as failed if article not found (permanent error)
	if err != nil && nntp.IsArticleNotFoundError(err) {
		u.failedFiles.Store(key, err) // Reuse pre-computed key
		// Wrap error to mark as permanent
		return customerror.NewArticleNotFoundError(err)
	}

	return err
}

// safeCopyBuffer copies from src to dst using buf, with context checking and
// validation of read counts to prevent panics from corrupted readers during shutdown.
func safeCopyBuffer(ctx context.Context, dst io.Writer, src io.Reader, buf []byte) (written int64, err error) {
	var release func()
	if len(buf) == 0 {
		buf = acquireStreamBuffer()
		release = func() { releaseStreamBuffer(buf) }
	}
	if release != nil {
		defer release()
	}
	bufLen := len(buf)

	for {
		// Check context before each read
		select {
		case <-ctx.Done():
			return written, ctx.Err()
		default:
		}

		nr, er := src.Read(buf)

		// Validate read count - this catches corrupted readers during shutdown
		if nr < 0 {
			return written, fmt.Errorf("reader returned negative count: %d", nr)
		}
		if nr > bufLen {
			// Reader returned more bytes than buffer capacity - this would panic
			// Return error instead of panicking
			return written, fmt.Errorf("reader returned invalid count %d (buffer size %d)", nr, bufLen)
		}

		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nw > nr {
				nw = 0
				if ew == nil {
					ew = fmt.Errorf("invalid write count: %d", nw)
				}
			}
			written += int64(nw)
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}

// Touch validates that the first segment of a file is available via NNTP STAT
func (u *Usenet) Touch(ctx context.Context, nzoID, filename string) error {
	file, err := u.getFile(nzoID, filename)
	if err != nil {
		return fmt.Errorf("failed to get file: %w", err)
	}

	if err := u.preStreamChecks(file); err != nil {
		return err
	}

	// Check if we have Segments
	if len(file.Segments) == 0 {
		return fmt.Errorf("file has no Segments: %s", filename)
	}

	// get first segment
	firstSeg := file.Segments[0]
	// Run STAT command to check if article exists
	_, _, err = u.nntp.Stat(ctx, firstSeg.MessageID)
	if err != nil {
		return fmt.Errorf("segment not available: %w", err)
	}
	return nil
}

// PreCache creates a file system entry and pre-fetches head and tail segments.
// This warms up the cache to reduce latency for subsequent reads (e.g. ffprobe).
// Uses the shared entry/reader so the cache is available for Stream calls.
func (u *Usenet) PreCache(ctx context.Context, nzoID, filename string) error {
	// Use shared entry (same as Stream)
	entry, key, err := u.getOrCreateEntry(ctx, nzoID, filename)
	if err != nil {
		return fmt.Errorf("failed to get or create entry: %w", err)
	}
	defer u.releaseFS(key)

	if len(entry.volumes) == 0 {
		return fmt.Errorf("no volumes available for file %s", filename)
	}

	fileSize := entry.volumes[0].Size

	// Calculate how much to read for head and tail
	headSize := int64(2 * 1024 * 1024) // 2MB head (~3 segments)
	tailSize := int64(2 * 1024 * 1024) // 2MB tail (~3 segments)

	if headSize > fileSize {
		headSize = fileSize
	}

	// get shared reader from entry
	readerAt, _, err := entry.getOrCreateReader()
	if err != nil {
		return fmt.Errorf("failed to get reader: %w", err)
	}

	// Pre-fetch head segments using Prefetch (non-blocking segment download)
	readerAt.Prefetch(ctx, 0, headSize)

	// Pre-fetch tail segments (if file is large enough)
	if fileSize > headSize+tailSize {
		tailOffset := fileSize - tailSize
		readerAt.Prefetch(ctx, tailOffset, tailSize)
	}

	return nil
}

// Stats returns nntp statistics
func (u *Usenet) Stats() map[string]interface{} {
	stats := u.nntp.Stats()
	stats["readers"] = u.fs.Size()
	stats["nzb_storage"] = u.nzbStorage.Stats()
	return stats
}

// GetNZB returns NZB metadata by ID
func (u *Usenet) GetNZB(id string) (*storage.NZB, error) {
	return u.nzbStorage.GetNZB(id)
}

// ForEachNZB iterates over all NZBs
func (u *Usenet) ForEachNZB(fn func(*storage.NZB) error) error {
	return u.nzbStorage.ForEachNZB(fn)
}

// NZBStorage returns the underlying NZB storage
func (u *Usenet) NZBStorage() *NZBStorage {
	return u.nzbStorage
}

// SpeedTest runs a speed test for a specific NNTP provider
// It finds a segment from a processed NZB to download for real speed measurement
func (u *Usenet) SpeedTest(ctx context.Context, providerHost string) nntp.SpeedTestResult {
	// Try to find a segment from any processed NZB for the speed test
	messageID := u.findTestSegment()
	return u.nntp.SpeedTest(ctx, providerHost, messageID)
}

// findTestSegment looks for a segment from any processed NZB to use for speed testing
func (u *Usenet) findTestSegment() string {
	var messageID string

	// Iterate through NZBs to find a usable segment
	_ = u.nzbStorage.ForEachNZB(func(nzb *storage.NZB) error {
		for _, file := range nzb.Files {
			if file.IsDeleted || len(file.Segments) == 0 {
				continue
			}
			// Use the first segment we find
			messageID = file.Segments[0].MessageID
			// Return an error to stop iteration (not a real error)
			return fmt.Errorf("found")
		}
		return nil
	})

	return messageID
}

// GetSpeedTestResults returns all stored speed test results
func (u *Usenet) GetSpeedTestResults() map[string]nntp.SpeedTestResult {
	return u.nntp.GetSpeedTestResults()
}

func (u *Usenet) saveNZBFile(name string, content []byte) (string, error) {
	path := filepath.Join(u.metadataDir, name)
	if err := os.WriteFile(path, content, 0644); err != nil {
		return "", fmt.Errorf("failed to save NZB file to disk: %w", err)
	}
	return path, nil
}

// StageNZB persists a queued NZB before an active-download worker starts.
func (u *Usenet) StageNZB(id string, content []byte) (string, error) {
	if id == "" {
		return "", fmt.Errorf("NZB ID is required")
	}
	// Keep the staged file off the .nzb extension so the metadata-directory
	// watcher does not treat a pending active-download job as an unmanaged import.
	path := filepath.Join(u.metadataDir, id+".queued")
	if err := os.WriteFile(path, content, 0644); err != nil {
		return "", fmt.Errorf("failed to stage NZB file: %w", err)
	}
	return path, nil
}

// RemoveStagedNZB removes a queued source file after it has been parsed.
func (u *Usenet) RemoveStagedNZB(path string) {
	if path != "" {
		_ = os.Remove(path)
	}
}

func (u *Usenet) markAsProcessing(nzb *storage.NZB) error {
	// Mark as processing by creating a marker file with the NZB ID
	markerPath := nzb.Path + ".processing"
	if err := os.WriteFile(markerPath, []byte(nzb.ID), 0644); err != nil {
		return fmt.Errorf("failed to create processing marker: %w", err)
	}
	return nil
}

func (u *Usenet) markAsCompleted(nzb *storage.NZB) error {
	nzb.Status = NZBStatusCompleted
	if err := u.nzbStorage.AddNZB(nzb); err != nil {
		return fmt.Errorf("failed to save NZB to storage: %w", err)
	}

	// Mark as processed by creating a marker file with the NZB ID
	markerPath := nzb.Path + ".processed"
	if err := os.WriteFile(markerPath, []byte(nzb.ID), 0644); err != nil {
		return fmt.Errorf("failed to create processed marker: %w", err)
	}

	// Remove processing marker if exists
	processingMarker := nzb.Path + ".processing"
	_ = os.Remove(processingMarker)
	return nil
}

func (u *Usenet) markAsFailed(nzb *storage.NZB, err error) error {
	// Mark as failed in storage
	nzb.Status = NZBStatusFailed
	nzb.FailMessage = err.Error()
	if err := u.nzbStorage.AddNZB(nzb); err != nil {
		return fmt.Errorf("failed to mark NZB as failed in storage: %w", err)
	}

	// Remove processing marker if exists
	processingMarker := nzb.Path + ".processing"
	_ = os.Remove(processingMarker)

	// Remove the nzb file itself, as it's considered failed
	if nzb.Path != "" {
		if err := os.Remove(nzb.Path); err != nil && !os.IsNotExist(err) {
			u.logger.Warn().Err(err).Str("path", nzb.Path).Msg("Failed to delete NZB file from disk after failure")
		}
	}
	return nil
}

func (u *Usenet) Delete(nzoID string) error {
	nzb, err := u.nzbStorage.GetNZB(nzoID)
	if err != nil {
		return fmt.Errorf("failed to get NZB: %w", err)
	}

	// Delete NZB XML file from disk
	if nzb.Path != "" {
		if err := os.Remove(nzb.Path); err != nil && !os.IsNotExist(err) {
			u.logger.Warn().Err(err).Str("path", nzb.Path).Msg("Failed to delete NZB file from disk")
		}

		// Delete marker files
		processedMarker := nzb.Path + ".processed"
		_ = os.Remove(processedMarker)
		failedMarker := nzb.Path + ".failed"
		_ = os.Remove(failedMarker)
	}

	// Delete from file-based storage
	if err := u.nzbStorage.DeleteNZB(nzoID); err != nil {
		return fmt.Errorf("failed to delete NZB from storage: %w", err)
	}
	return nil
}

// PendingNZB is an unmanaged NZB file claimed by the metadata-directory watcher.
type PendingNZB struct {
	Name    string
	Path    string
	Content []byte
}

// ClaimNewNZBs moves unmanaged NZB files out of the watched extension and
// returns them for submission to the shared active-download queue.
func (u *Usenet) ClaimNewNZBs() ([]PendingNZB, error) {
	entries, err := os.ReadDir(u.metadataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata dir: %w", err)
	}

	var pending []PendingNZB
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		claimedPath := filepath.Join(u.metadataDir, name)
		if strings.HasSuffix(name, ".nzb.importing") {
			name = strings.TrimSuffix(name, ".importing")
		} else {
			if filepath.Ext(name) != ".nzb" {
				continue
			}
			path := filepath.Join(u.metadataDir, name)
			if fileExists(path+".processed") || fileExists(path+".processing") || fileExists(path+".failed") {
				continue
			}
			claimedPath = path + ".importing"
			if err := os.Rename(path, claimedPath); err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return nil, fmt.Errorf("failed to claim NZB %s: %w", name, err)
			}
		}

		content, err := os.ReadFile(claimedPath)
		if err != nil {
			u.logger.Error().Err(err).Str("path", claimedPath).Msg("Failed to read claimed NZB")
			continue
		}
		pending = append(pending, PendingNZB{Name: name, Path: claimedPath, Content: content})
	}

	if len(pending) > 0 {
		u.logger.Info().Int("count", len(pending)).Msg("Found new NZB files to queue")
	}
	return pending, nil
}

// RemoveClaimedNZB removes a watched source after it has been staged by the queue.
func (u *Usenet) RemoveClaimedNZB(path string) {
	if path != "" {
		_ = os.Remove(path)
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
