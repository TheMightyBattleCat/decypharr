package usenet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
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
	"github.com/sirrobot01/decypharr/internal/nntp/yenc"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/fs"
	"github.com/sirrobot01/decypharr/pkg/usenet/fs/reader"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
	"github.com/sirrobot01/decypharr/pkg/usenet/types"
)

const (
	bufferSize = 256 * 1024 // 256KB buffer for streaming

	// verifyBufferSize is the streaming copy-buffer size for verification
	// reads (ffprobe import/sweep checks, padding disabled). Larger than
	// bufferSize so each safeCopyBuffer iteration spans several segments.
	// Real playback keeps the 256KB buffer.
	//
	// Kept small: verificationPrefetch keeps the segments warm ahead of the
	// read, so the foreground EnsureSegmentsConcurrent is usually a no-op and
	// the copy buffer only sizes the memcpy-out-of-cache granularity. It was
	// tried at 24MB with no gain when the fetch was still synchronous.
	verifyBufferSize = 4 * 1024 * 1024 // 4MB

	// verifyFirstRead is the most a verification range request's first read
	// asks the source for; each later read asks for twice the one before, up
	// to verifyBufferSize. A read fills what it asks for (fetching those
	// articles) and is charged to the probe's budget before a byte is sent,
	// but ffprobe abandons most range requests after about a megabyte: at
	// 4 MiB per first read, a 15-window spread of a 253 MB episode was
	// charged ~260 MiB for ~70 MB consumed (a production install, 2026-09-16). Ramping
	// reaches the full buffer after ~7.8 MiB, before the rolling prefetch
	// starts (verificationPrefetchRamp), so a long forward read is unchanged.
	// Measured locally against a 265 MB file with a 40 ms/MiB fetch: charged
	// 68.0 -> 28.5 MiB, spread 3.4 -> 0.8 s, frames and errors identical.
	verifyFirstRead = 256 * 1024

	// verificationPrefetchAhead is how far ahead of a verification read's
	// position Usenet.verificationPrefetch keeps segments fetched. Deep enough
	// (~3-4s of lead at observed REMUX scan rates) that a multi-second
	// provider/retry tail on one segment overlaps ffmpeg's consume of the
	// segments already fetched, instead of stalling the sequential read. The
	// overshoot past bufferMemorySize (64MB) spills to the disk-backed stream
	// file, which on an HDD still outruns the fetch rate.
	verificationPrefetchAhead = 128 * 1024 * 1024
	// verificationPrefetchRamp is how much a verification read must consume
	// before its prefetch starts: a seek or ffprobe's moov probe finishes
	// inside this and pays nothing; a sustained forward scan crosses it and
	// then gets the full look-ahead window.
	verificationPrefetchRamp = 8 * 1024 * 1024

	// failedFileTTL bounds how long a permanent-failure record in
	// failedFiles survives before preStreamChecks/FailedFileCause treat it
	// as expired and let the next read re-verify from scratch. Without
	// this, a transient provider-side 430 storm (an indexer/provider
	// briefly missing articles it actually has) could poison a file for
	// the entire process lifetime with no way back short of a restart or
	// an explicit Clear call.
	failedFileTTL = 15 * time.Minute
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

var verifyBufferPool = sync.Pool{
	New: func() any {
		return make([]byte, verifyBufferSize)
	},
}

func acquireVerifyBuffer() []byte {
	buf := verifyBufferPool.Get().([]byte)
	if cap(buf) < verifyBufferSize {
		buf = make([]byte, verifyBufferSize)
	}
	return buf[:verifyBufferSize]
}

func releaseVerifyBuffer(buf []byte) {
	if buf == nil {
		return
	}
	if cap(buf) < verifyBufferSize {
		return
	}
	verifyBufferPool.Put(buf[:verifyBufferSize])
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

	// overlayOpts wires the playback-padding/PAR2-patch store into the
	// single-volume reader (see getOrCreateReader). Built once at createEntry
	// time, when the owning nzbID and logical filename are known; nil when
	// overlay support is disabled (e.g. no usable overlay store).
	overlayOpts []reader.Option

	// streaming is the reader as a *reader.StreamingReader, once created -
	// published atomically so RefreshRepairedSegments can reach a live
	// reader without racing getOrCreateReader's Once. nil for the
	// multi-volume reader, which has no segment cache or overlay.
	streaming atomic.Pointer[reader.StreamingReader]

	// gone is set by Delete: the NZB this entry reads is deleted, and new
	// streams get ErrEntryGone instead of reusing the warm reader - which
	// kept fetching the deleted grab's articles and turning their 430s
	// into playback escalations. cleanupIdleFS reaps it once idle.
	gone atomic.Bool
}

// fsEntryTombstone marks an entry claimed for teardown. Once refCount holds
// this value no new stream can acquire the entry (see acquire), which is what
// makes cleanup safe against a concurrent Stream that already Load()ed the
// entry from the map.
const fsEntryTombstone = int32(-1 << 30)

func (fe *fsEntry) cleanup() {
	if fe.readerCleanup != nil {
		fe.readerCleanup()
		fe.readerCleanup = nil
		fe.reader = nil
	}
}

// acquire takes a reference unless the entry has been claimed for teardown.
func (fe *fsEntry) acquire() bool {
	for {
		n := fe.refCount.Load()
		if n < 0 {
			return false
		}
		if fe.refCount.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

// claimForCleanup atomically claims an idle (refCount == 0) entry for
// teardown, fencing out any future acquire.
func (fe *fsEntry) claimForCleanup() bool {
	return fe.refCount.CompareAndSwap(0, fsEntryTombstone)
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
			readerAt, size, cleanup, err = fe.fs.CreateReaderAtForVolume(fe.volumes[0], fe.overlayOpts...)
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
		if sr, ok := readerAt.(*reader.StreamingReader); ok {
			fe.streaming.Store(sr)
		}
	})

	if fe.readerErr != nil {
		return nil, 0, fe.readerErr
	}
	// cleanup() nils the reader after the Once has fired; a caller racing a
	// shutdown-path cleanup must get an error, not a nil interface.
	if fe.reader == nil {
		return nil, 0, fmt.Errorf("reader has been closed")
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

func (n *noPrefetchReader) FetchRange(ctx context.Context, off, length int64, concurrency int) error {
	// No-op for multi-volume readers - no dedicated fetcher/cache to drive.
	return nil
}

func (n *noPrefetchReader) FetchRangeWindowed(ctx context.Context, base, total int64, concurrency int, horizon func() int64) error {
	// No-op for multi-volume readers - no dedicated fetcher/cache to drive.
	return nil
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

// meteredReader wraps the verification-read source so Stream can report a
// decode probe's effective throughput. read_wait is the cumulative time spent
// blocked in the underlying ReadAt (i.e. waiting on EnsureSegmentsConcurrent),
// which for a verification read is essentially the whole copy - if it is far
// below dur the bottleneck is downstream (ffmpeg/CPU), not the fetch.
type meteredReader struct {
	inner    io.Reader
	reads    int
	bytes    int64
	readWait time.Duration
	// consumed is bytes delivered so far, relative to the request's range
	// start. Written by Read (single goroutine), read concurrently by
	// Usenet.verificationPrefetch, so it must be atomic.
	consumed atomic.Int64
	// budget, when non-nil, caps the bytes this probe may pull across every
	// range request it issues for the file (see reader.VerifyBudget). Once
	// blown, Read stops feeding the probe.
	budget *reader.VerifyBudget
	// next is the most the next read asks the source for (verifyFirstRead,
	// doubling); 0 before the first read.
	next int
}

func (m *meteredReader) Read(p []byte) (int, error) {
	// A budget already spent - by an earlier range request of this probe, or
	// declared spent by the checker - must not deliver another byte. Checking
	// only after the read would let every request ffprobe issues past the cut
	// fetch and deliver one more buffer before stopping.
	if m.budget.Exceeded() {
		return 0, reader.ErrVerifyBudgetExhausted
	}
	if m.next == 0 {
		m.next = verifyFirstRead
	}
	if len(p) > m.next {
		p = p[:m.next]
	}
	m.next = min(m.next*2, verifyBufferSize)
	t := time.Now()
	n, err := m.inner.Read(p)
	waited := time.Since(t)
	m.readWait += waited
	m.reads++
	m.bytes += int64(n)
	m.consumed.Add(int64(n))
	// Feed the budget's end-of-verification accounting (reads / wait /
	// throughput), which stands in for the per-range-request log line.
	m.budget.Observe(int64(n), waited)
	// Only credited on a clean read: bytes handed back alongside io.EOF end
	// the body anyway, so charging them could only turn the last read of a
	// finished probe into a spurious "budget blown".
	if err == nil && !m.budget.Add(int64(n)) {
		// Hand back what we already read, then stop. The caller (Stream)
		// recognises this sentinel and ends the response body cleanly - a
		// blown budget is a statement about the probe, not the file.
		return n, reader.ErrVerifyBudgetExhausted
	}
	return n, err
}

// ErrEntryGone indicates the backing NZB record for a stream request no
// longer exists - nzoID was deleted or superseded by a re-grab (see Delete)
// after a caller resolved a reference to it and before this read ran. The
// canonical case is a FUSE handle opened against an entry that playback
// repair then deleted+re-grabbed while the handle stayed open (see
// pkg/mount/dfs/vfs's Downloaders.staleEntry): the handle's *storage.Entry
// is captured once at open time and never refreshed, so every subsequent
// read resolves the same, now-gone nzoID forever. Distinct from a live NNTP
// article-not-found (which padding/PAR2 repair can address) - there is
// nothing to retry or repair here, only a fresh Open() against whatever
// entry (if any) now exists at the same path can recover.
var ErrEntryGone = errors.New("usenet: entry no longer exists")

type Usenet struct {
	nntp                     *nntp.Client
	logger                   zerolog.Logger
	metadataDir              string
	nzbStorage               *NZBStorage // File-based NZB metadata storage
	maxConnections           int         // Connections allocated per streaming file
	verificationConnections  int         // Concurrent fetches an ffprobe verification read's prefetch may drive
	processingMaxConnections int         // Connections allocated per file for parsing and NZB downloads
	prefetchSize             int64       // Streaming prefetch size in bytes
	failedFiles              *xsync.Map[string, failedFileRecord]

	// overlay is the playback-padding/PAR2-patch store. Nil only if it failed
	// to initialize (e.g. an unwritable data dir) - streaming then behaves
	// exactly as it did before the overlay feature existed.
	overlay *overlay.Store

	// deadPostings suppresses re-parsing/re-probing a re-grab of an NZB
	// whose content was already confirmed unavailable within the last
	// deadPostingTTL - see ParseWithID and checkNZBAvailability.
	deadPostings *deadPostingCache

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

	// One-time (idempotent) upgrade of any legacy protobuf meta files to the v2
	// codec. Runs in the background so it never blocks startup; atomic rewrites
	// keep concurrent reads safe throughout.
	go func() {
		if _, err := nzbStorage.MigrateLegacy(); err != nil {
			nzbStorage.logger.Warn().Err(err).Msg("Legacy NZB meta migration failed")
		}
	}()

	// Create NNTP client with retry configuration
	client, err := nntp.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	_logger.Info().Str("yenc_decoder", yenc.Backend()).Msg("yEnc decoder selected: " + yenc.Backend())

	maxConns := usenetConfig.MaxConnections
	if maxConns <= 0 {
		maxConns = 10
	}
	processingMaxConns := usenetConfig.ProcessingMaxConnections
	if processingMaxConns <= 0 {
		processingMaxConns = maxConns
	}

	// Width of a verification read's background prefetch. It gets its own
	// connection slots on top of maxConns (see NewSegmentFetcher), so this is
	// additive headroom, not a ceiling.
	verifyConns := cfg.Repair.VerificationConnections
	if verifyConns <= 0 {
		verifyConns = 32
	}

	prefetchSize, err := config.ParseSize(usenetConfig.ReadAhead)
	if err != nil {
		prefetchSize = 16 * 1024 * 1024 // Default to 16MB
	}

	// Rooted next to the stored-NZB records directory (nzbStorage.MetaDir,
	// i.e. .../usenet/meta), not under the DFS cache - the cache gets cleared
	// routinely and would silently forget every dead-segment/patch record.
	overlayRoot := filepath.Join(config.GetMainPath(), "usenet", "overlay")
	overlayStore, err := overlay.NewStore(overlayRoot, logger.New("usenet-overlay"))
	if err != nil {
		_logger.Warn().Err(err).Msg("Failed to initialize playback-padding overlay store; padding and PAR2 repair are disabled for this run")
		overlayStore = nil
	}
	if overlayStore != nil {
		overlayStore.SetPolicy(overlayPolicyFromConfig(cfg.Repair))
	}

	u := &Usenet{
		nzbStorage:               nzbStorage,
		nntp:                     client,
		logger:                   _logger,
		metadataDir:              metadataDir,
		maxConnections:           maxConns,
		verificationConnections:  verifyConns,
		processingMaxConnections: processingMaxConns,
		prefetchSize:             prefetchSize,
		fs:                       xsync.NewMap[string, *fsEntry](),
		failedFiles:              xsync.NewMap[string, failedFileRecord](),
		overlay:                  overlayStore,
		deadPostings:             newDeadPostingCache(),
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

	var overlayOpts []reader.Option
	if u.overlay != nil && file.NzbID != "" && config.Get().Repair.PlaybackPaddingEnabled() {
		overlayOpts = []reader.Option{reader.WithOverlay(u.overlay.Handle(file.NzbID), file.Name)}
	}

	return &fsEntry{
		fs:          usenetFS,
		volumes:     volumes,
		overlayOpts: overlayOpts,
	}, nil
}

// getOrCreateEntry returns the fsEntry and its cache key to avoid redundant key computation.
func (u *Usenet) getOrCreateEntry(ctx context.Context, nzoID, filename string) (*fsEntry, string, error) {
	key := fsKey(nzoID, filename)

	// Fast path: entry already exists and isn't being torn down. acquire() (a
	// CAS, not a blind Add) is what closes the race against cleanupIdleFS:
	// once the janitor claims an idle entry no new reference can be taken, so
	// a stream can never end up on an entry whose reader is being closed.
	if entry, ok := u.fs.Load(key); ok {
		if entry.gone.Load() {
			return nil, key, fmt.Errorf("%w: deleted while its reader was open", ErrEntryGone)
		}
		if entry.acquire() {
			entry.lastAccessed.Store(utils.NowUnix())
			return entry, key, nil
		}
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

	// Published already holding our reference: stored with refCount 0 and
	// lastAccessed 0, the janitor could claim the entry (refCount 0, idle
	// since the epoch) between the store and the increment and tear it
	// down under us.
	newEntry.refCount.Store(1)
	newEntry.lastAccessed.Store(utils.NowUnix())

	// Atomically store only if key doesn't exist (prevents race condition)
	for {
		actual, loaded := u.fs.LoadOrStore(key, newEntry)
		if !loaded {
			// We won the race - use our new entry
			return newEntry, key, nil
		}
		// Another goroutine created the entry first - use theirs.
		// Our newEntry was never used (readers are lazy), GC reclaims it.
		if actual.acquire() {
			actual.lastAccessed.Store(utils.NowUnix())
			return actual, key, nil
		}
		// The mapped entry is claimed for teardown; the janitor removes it
		// from the map immediately after claiming, so retry until our entry
		// can be stored.
		if err := ctx.Err(); err != nil {
			return nil, key, err
		}
		runtime.Gosched()
	}
}

// HasBandwidthHeadroom reports whether at least one non-backup provider
// currently has lead-tier capacity available. Used to gate deferrable bulk
// background work (Sonarr next-episode pre-caching) so it never eats into a
// provider's held-back reserve - see nntp.Client.HasLeadHeadroom.
func (u *Usenet) HasBandwidthHeadroom() bool {
	return u.nntp.HasLeadHeadroom()
}

// EvictCache immediately tears down the cached reader/disk buffer for one
// file, if it is currently idle (no active Stream holding a reference).
// Used by the next-episode precache feature to reclaim a pre-cached
// episode's disk footprint once it has actually been watched (see
// config.Precache.PrecacheEvictAfterWatched) instead of waiting for the
// normal idle-timeout cleanup (cleanupIdleFS). Returns false (no-op) if the
// entry doesn't exist or is currently in use.
func (u *Usenet) EvictCache(nzoID, filename string) bool {
	if u == nil || u.fs == nil {
		return false
	}
	key := fsKey(nzoID, filename)
	entry, ok := u.fs.Load(key)
	if !ok {
		return false
	}
	if !entry.claimForCleanup() {
		return false
	}
	u.fs.Delete(key)
	entry.cleanup()
	return true
}

// RefreshRepairedSegments makes a live reader of filename fetch segIdx again
// on its next read (see reader.StreamingReader.RefetchSegments), so segments
// a PAR2 repair just patched are served from the patch rather than the
// zero-fill the reader may still hold. Unlike EvictCache it works while a
// viewer is streaming, and it keeps every other cached segment. Returns
// false when no reader for the file is open.
func (u *Usenet) RefreshRepairedSegments(nzoID, filename string, segIdx []int) bool {
	if u == nil || u.fs == nil || len(segIdx) == 0 {
		return false
	}
	entry, ok := u.fs.Load(fsKey(nzoID, filename))
	if !ok {
		return false
	}
	sr := entry.streaming.Load()
	if sr == nil {
		return false
	}
	sr.RefetchSegments(segIdx)
	return true
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
					// Claim before touching anything: the CAS fences out a
					// concurrent Stream that already Load()ed this entry from
					// the map (it will fail acquire() and create a fresh
					// entry). Delete from the map before the (potentially
					// slow) cleanup so waiting creators aren't stalled.
					if entry.claimForCleanup() {
						u.fs.Delete(key)
						entry.cleanup()
					}
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

	// Reject a re-grab of a posting already confirmed unavailable within
	// deadPostingTTL without spending a single round trip on it - keyed on
	// content, not nzbID, so it catches the identical release coming back
	// under a fresh grab ID (the FLUX/ETHEL/Kitsune/RAWR cycling this cache
	// exists to stop).
	contentHash := hashNZBContent(content)
	// The posting identity also catches another indexer's NZB for the same
	// upload; the byte hash still covers records stored before it existed.
	postingKey := postingKeyFromNZB(content)
	if u.deadPostings.Check(contentHash) || u.deadPostings.Check(postingKey) {
		return nil, nil, fmt.Errorf("%q: %w (confirmed unavailable within the last %s)", name, parser.ErrReleaseUnavailable, deadPostingTTL)
	}

	// Create parser with the manager
	prs := parser.NewParser(u.nntp, u.processingMaxConnections, u.logger.With().Str("component", "parser").Logger())

	// Quick parse: defer archive extraction for async processing
	nzb, groups, err := prs.Parse(ctx, name, content)
	if err != nil {
		if errors.Is(err, parser.ErrReleaseUnavailable) {
			u.deadPostings.Mark(contentHash)
			u.deadPostings.Mark(postingKey)
		}
		return nil, nil, err
	}
	if id != "" {
		nzb.ID = id
	}

	// Persisted for the later dead-posting marks (Process's availability
	// gate, MarkPostingDeadByNZBID): the posting identity when the XML
	// yields one, so they cover every re-listing of the upload.
	nzb.ContentHash = contentHash
	if postingKey != "" {
		nzb.ContentHash = postingKey
	}
	nzb.Category = category
	nzb.Status = NZBStatusParsing
	// Save NZB file to disk
	nzbPath, err := u.saveNZBFile(nzb.ID, content)
	if err != nil {
		return nil, nil, err
	}
	nzb.Path = nzbPath

	// Mark as processing
	if err := u.markAsProcessing(nzb); err != nil {
		// Don't leave the source file orphaned; an un-marked .nzb would be
		// re-claimed by the refresh watcher on every scan.
		_ = os.Remove(nzbPath)
		return nil, nil, fmt.Errorf("failed to mark NZB as processing: %w", err)
	}

	if err := u.nzbStorage.AddNZB(nzb); err != nil {
		_ = os.Remove(nzbPath + ".processing")
		_ = os.Remove(nzbPath)
		return nil, nil, fmt.Errorf("failed to save NZB to storage: %w", err)
	}
	u.reviveNZB(nzb.ID) // an ID can be supplied by the caller, and so reused

	u.logger.Debug().
		Str("nzb_id", nzb.ID).
		Str("name", nzb.Name).
		Int("groups", len(groups)).
		Msg("Successfully parsed NZB file")
	return nzb, groups, nil
}

// Process processes archive files in an NZB (full parse)
func (u *Usenet) Process(ctx context.Context, nzb *storage.NZB, groups map[string]*parser.FileGroup) (*storage.NZB, error) {
	u.logger.Debug().
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
		// Same negative cache ParseWithID checks/marks for a STAT/PAR2-probe
		// abort - a re-grab of this exact content within deadPostingTTL is
		// rejected before it re-parses or re-probes the same dead articles.
		u.deadPostings.Mark(updatedNZB.ContentHash)
		return updatedNZB, fmt.Errorf("availability check failed: %w: %w", err, parser.ErrReleaseUnavailable)
	}

	// Mark as completed
	if err := u.markAsCompleted(updatedNZB); err != nil {
		return updatedNZB, fmt.Errorf("failed to mark NZB as completed: %w", err)
	}

	u.logger.Info().
		Str("nzb_id", updatedNZB.ID).
		Str("name", updatedNZB.Name).
		Int("files", len(updatedNZB.Files)).
		Str(logger.FieldStatus, logger.StatusOK).
		Int64(logger.FieldSize, updatedNZB.TotalSize).
		Str(logger.FieldNote, logger.Count(len(updatedNZB.Files), "file")).
		Msg("NZB parsed and available")
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
// streaming traffic.
func (u *Usenet) CheckFile(ctx context.Context, nzoID, filename string) error {
	// Repair/availability probes only need a sample of one file's message ids.
	// Decode just those (no numeric columns, no NZBSegment structs, no other
	// files) so a full sweep doesn't hold whole segment maps in memory.
	samplePercent := config.Get().Usenet.AvailabilitySamplePercent
	messageIDs, err := u.nzbStorage.SampleFileMessageIDs(nzoID, filename, samplePercent)
	if err != nil {
		return fmt.Errorf("failed to sample file segments: %w", err)
	}
	if len(messageIDs) == 0 {
		return fmt.Errorf("file has no Segments: %s", filename)
	}
	return u.checkAvailability(ctx, filename, messageIDs)
}

func (u *Usenet) CheckFileAvailability(ctx context.Context, file *storage.NZBFile, samplePercent int) error {
	return u.checkAvailability(ctx, file.Name, u.sampleSegments(file.Segments, samplePercent))
}

// RecordOverlayDead records a confirmed-dead segment against the overlay
// store for nzoID/filename. No-op (nil error) if the overlay store failed to
// initialize. Exposed so the repair sweep can persist damage it discovers
// without importing pkg/usenet/overlay directly.
func (u *Usenet) RecordOverlayDead(nzoID, filename string, segIndex int, msgID string, bytes int64) error {
	if u.overlay == nil {
		return nil
	}
	return u.overlay.RecordDead(nzoID, filename, segIndex, msgID, bytes)
}

// OverlayVerdict returns the overlay store's current damage verdict for
// nzoID/filename ("clean" if the overlay store is unavailable or the file has
// no recorded damage).
func (u *Usenet) OverlayVerdict(nzoID, filename string) overlay.Verdict {
	if u.overlay == nil {
		return overlay.VerdictClean
	}
	return u.overlay.Verdict(nzoID, filename)
}

// OverlayPendingRepair returns every file of nzoID with at least one
// non-patched dead segment recorded in the overlay store, keyed by logical
// filename. Empty (nil error) if the overlay store is unavailable or nothing
// is pending.
func (u *Usenet) OverlayPendingRepair(nzoID string) (map[string][]overlay.DeadSegment, error) {
	if u.overlay == nil {
		return nil, nil
	}
	return u.overlay.PendingRepair(nzoID)
}

// OverlayWritePatch stores repaired bytes for one segment and marks it
// patched - see overlay.Store.WritePatch.
func (u *Usenet) OverlayWritePatch(nzoID, filename string, segIndex int, data []byte) error {
	if u.overlay == nil {
		return fmt.Errorf("overlay store unavailable")
	}
	return u.overlay.WritePatch(nzoID, filename, segIndex, data)
}

// OverlayListNZBIDs returns every nzbID with recorded overlay state. Empty
// (nil error) if the overlay store is unavailable.
func (u *Usenet) OverlayListNZBIDs() ([]string, error) {
	if u.overlay == nil {
		return nil, nil
	}
	return u.overlay.ListNZBIDs()
}

// OverlayManifest returns nzoID's full overlay manifest - see
// overlay.Store.GetManifest.
func (u *Usenet) OverlayManifest(nzoID string) (*overlay.Manifest, error) {
	if u.overlay == nil {
		return nil, fmt.Errorf("overlay store unavailable")
	}
	return u.overlay.GetManifest(nzoID)
}

// OverlayDiskUsage returns nzoID's real on-disk overlay byte totals - see
// overlay.Store.DiskUsage.
func (u *Usenet) OverlayDiskUsage(nzoID string) (patchBytes, manifestBytes int64, err error) {
	if u.overlay == nil {
		return 0, 0, nil
	}
	return u.overlay.DiskUsage(nzoID)
}

// OverlayFilePatchBytes returns the real on-disk size of every patch blob
// written for file's patched segments - see overlay.Store.FilePatchBytes.
func (u *Usenet) OverlayFilePatchBytes(nzoID, filename string, fe *overlay.FileEntry) int64 {
	if u.overlay == nil {
		return 0
	}
	return u.overlay.FilePatchBytes(nzoID, filename, fe)
}

// OverlayPatchBytes returns the recovered bytes for a patched segment, if
// any - see overlay.Store.PatchBytes.
func (u *Usenet) OverlayPatchBytes(nzoID, filename string, segIndex int) ([]byte, bool) {
	if u.overlay == nil {
		return nil, false
	}
	return u.overlay.PatchBytes(nzoID, filename, segIndex)
}

// OverlayClearFileDamage removes dead/padded segment records and resets the
// verdict to clean while preserving any patched segments (PAR2-recovered
// bytes). Returns patchesPreserved=true when at least one patched segment
// was kept. No-op (nil error) if the overlay store is unavailable.
func (u *Usenet) OverlayClearFileDamage(nzoID, filename string) (patchesPreserved bool, err error) {
	if u.overlay == nil {
		return false, nil
	}
	return u.overlay.ClearFileDamage(nzoID, filename)
}

// OverlayDeleteFile removes filename's overlay record and patch blobs from
// nzoID's manifest, without touching any other file - see
// overlay.Store.DeleteFile. removed=false (nil error) if the overlay store
// is unavailable or held no matching record - callers must not treat that as
// success.
func (u *Usenet) OverlayDeleteFile(nzoID, filename string) (removed bool, err error) {
	if u.overlay == nil {
		return false, nil
	}
	return u.overlay.DeleteFile(nzoID, filename)
}

// OverlayDeleteEntry removes nzoID's entire overlay state (every file's
// manifest record and patch blobs) - see overlay.Store.DeleteEntry. No-op
// (nil error) if the overlay store is unavailable. Used by the overlay
// management API's orphan GC when nzoID's backing entry no longer exists at
// all, as opposed to OverlayDeleteFile's narrower per-file scope.
// OverlayEntryExists reports whether nzoID has an overlay directory on disk.
// (OverlayManifest cannot tell: a missing manifest loads as a fresh one.)
func (u *Usenet) OverlayEntryExists(nzoID string) bool {
	if u.overlay == nil {
		return false
	}
	return u.overlay.EntryExists(nzoID)
}

func (u *Usenet) OverlayDeleteEntry(nzoID string) error {
	if u.overlay == nil {
		return nil
	}
	return u.overlay.DeleteEntry(nzoID)
}

// OverlayMarkRejected marks nzoID as import-rejected so subsequent
// RecordDead/Decide calls for it silently no-op - see
// overlay.Store.MarkRejected. Call it right before OverlayDeleteEntry when
// tearing down a rejected import, so a straggling fetcher goroutine can't
// re-create the manifest the delete just removed. No-op if the overlay store
// is unavailable.
func (u *Usenet) OverlayMarkRejected(nzoID string) {
	if u.overlay == nil {
		return
	}
	u.overlay.MarkRejected(nzoID)
}

// OverlayMarkSweepEntry / OverlayClearSweepEntry / OverlayIsEntrySweepActive
// expose the overlay store's per-entry sweep set. While an entry is marked,
// the fetcher refuses to pad its dead segments (the real 430 propagates so
// ffprobe sees the corruption) and PAR2 auto-enqueue is deferred for it. The
// repair sweep marks each entry around its ffprobe probe and clears it after.
// No-ops / false if the overlay store is unavailable.
func (u *Usenet) OverlayMarkSweepEntry(nzoID string) {
	if u.overlay == nil {
		return
	}
	u.overlay.MarkSweepEntry(nzoID)
}

func (u *Usenet) OverlayClearSweepEntry(nzoID string) {
	if u.overlay == nil {
		return
	}
	u.overlay.ClearSweepEntry(nzoID)
}

func (u *Usenet) OverlayIsEntrySweepActive(nzoID string) bool {
	if u.overlay == nil {
		return false
	}
	return u.overlay.IsEntrySweepActive(nzoID)
}

// OverlayMarkSweepDead / OverlayIsSweepDead expose the per-entry deadSeen latch
// on the overlay's sweep set. The fetcher latches it when any confirmed-dead
// (430) segment surfaces for an entry that's currently under a probe; the
// repair sweep's probeFile reads it to short-circuit straight to a broken
// verdict rather than waiting on ffprobe. No-op / false if the overlay store
// is unavailable or the entry isn't sweep-active.
func (u *Usenet) OverlayMarkSweepDead(nzoID string) {
	if u.overlay == nil {
		return
	}
	u.overlay.MarkSweepDead(nzoID)
}

func (u *Usenet) OverlayIsSweepDead(nzoID string) bool {
	if u.overlay == nil {
		return false
	}
	return u.overlay.IsEntrySweepDead(nzoID)
}

// SetOverlayRepairEnqueuer installs the callback invoked whenever the reader
// pads a segment, so the manager-level PAR2 repair worker (pkg/manager) can
// be notified without the overlay/reader packages needing to know it exists.
func (u *Usenet) SetOverlayRepairEnqueuer(fn func(nzbID string, deadSegments int)) {
	if u.overlay == nil {
		return
	}
	u.overlay.SetRepairEnqueuer(fn)
}

// SetOverlayFailedNotifier installs the callback invoked whenever a file's
// overlay verdict freshly transitions to failed (needs re-grab), so the
// manager-level notifications service can be told without the overlay
// package needing to know it exists.
func (u *Usenet) SetOverlayFailedNotifier(fn func(nzoID, file string)) {
	if u.overlay == nil {
		return
	}
	u.overlay.SetFailedNotifier(fn)
}

// SetOverlayViewerPadNotifier installs the callback invoked each time a
// viewer's read is padded past the caps - see overlay.SetViewerPadNotifier.
func (u *Usenet) SetOverlayViewerPadNotifier(fn func(nzoID, file string)) {
	if u.overlay == nil {
		return
	}
	u.overlay.SetViewerPadNotifier(fn)
}

// overlayPolicyFromConfig derives the overlay padding-cap Policy from repair
// config - already clamped by config's load/save path (see
// RepairConfig.PadMaxRunSegments and friends), so this is a plain field copy.
func overlayPolicyFromConfig(repair config.RepairConfig) overlay.Policy {
	return overlay.Policy{
		MaxRunSegments:   repair.PadMaxRunSegments,
		MaxTotalSegments: repair.PadMaxTotalSegments,
		MaxByteRatio:     repair.PadMaxByteRatio,
	}
}

// ApplyOverlayPolicy re-reads the padding caps from the live config and
// installs them on the overlay store. Called after the repair config is
// updated live (see Repair.ApplyConfig) so a saved change applies without a
// restart.
//
// Also clears every cached permanent failure: raising the caps can turn a
// file the OLD, stricter policy condemned (verdict Failed, poisoning
// failedFiles per shouldPoisonFailedFile) into one the new policy would
// have padded instead. There's no cheap way to know which specific files
// the change affects, so this un-poisons everything and lets the next read
// of each re-verify against the new policy - the overlay's own persisted
// verdicts are untouched, only the in-memory short-circuit cache is reset.
func (u *Usenet) ApplyOverlayPolicy() {
	if u.overlay == nil {
		return
	}
	u.overlay.SetPolicy(overlayPolicyFromConfig(config.Get().Repair))
	u.failedFiles.Clear()
}

// RunDamageSample probes nzoID/filename's segment health using the stratified
// damage sampler. It loads the file's full segment list, collects the overlay's
// recorded-dead indices, and wires a fetchBody callback through the NNTP
// client (real BODY fetch with failover and bandwidth accounting). The caller
// passes a verification-style context (ContextForVerificationRead) so padding
// never masks a dead article.
func (u *Usenet) RunDamageSample(ctx context.Context, nzoID, filename string, opts SampleOpts) (SampleResult, error) {
	nzb, err := u.nzbStorage.GetNZB(nzoID)
	if err != nil {
		return SampleResult{Verdict: VerdictInconclusive}, fmt.Errorf("load NZB: %w", err)
	}
	file := nzb.GetFileByName(filename)
	if file == nil || len(file.Segments) == 0 {
		return SampleResult{Verdict: VerdictInconclusive}, fmt.Errorf("file %s has no segments", filename)
	}

	segments := make([]SegmentRef, len(file.Segments))
	for i, seg := range file.Segments {
		segments[i] = SegmentRef{Index: i, MessageID: seg.MessageID}
	}

	var recordedDead []int
	if u.overlay != nil {
		if m, merr := u.overlay.GetManifest(nzoID); merr == nil {
			if fe := m.Files[filename]; fe != nil {
				for _, d := range fe.DeadSegments {
					if d.Status != overlay.StatusPatched {
						recordedDead = append(recordedDead, d.Index)
					}
				}
			}
		}
	}

	fetchBody := func(ctx context.Context, seg SegmentRef) error {
		_, err := u.FetchArticle(ctx, seg.MessageID)
		return err
	}

	result := SampleFileDamage(ctx, segments, recordedDead, fetchBody, opts)
	if u.overlay != nil && result.CoverageFraction > 0 {
		_ = u.overlay.SetCoverageFraction(nzoID, filename, result.CoverageFraction)
	}
	return result, nil
}

// FetchArticle downloads and yEnc-decodes a single NNTP article, returning
// its full decoded body. Goes through the same client (and thus the same
// bandwidth accounting and provider tiering/failover) as normal streaming
// reads. Intended for cold, one-off reads (PAR2 index/recovery data, MD5-16k
// probes) - NOT the hot streaming path, which uses the cached/pooled
// SegmentFetcher instead.
func (u *Usenet) FetchArticle(ctx context.Context, messageID string) ([]byte, error) {
	return u.FetchArticleChecked(ctx, messageID, nil)
}

// ArticleCheck judges a decoded article by its yEnc headers (meta is nil when
// the article had none) and returns "" to accept it, or why it is not the
// article the caller asked for.
type ArticleCheck func(meta *nntp.YencMetadata) string

// FetchArticleChecked is FetchArticle with an identity check. Providers can
// hold a different upload's article under a reused Message-ID; it decodes
// cleanly and passes its own CRC, so only its yEnc headers give it away. An
// article check rejects is treated as a 430 from that provider, so failover
// asks the next one - the same rule the streaming reader applies (see
// reader.articleMismatch). A nil check accepts everything.
func (u *Usenet) FetchArticleChecked(ctx context.Context, messageID string, check ArticleCheck) ([]byte, error) {
	var buf bytes.Buffer
	err := u.nntp.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
		buf.Reset()
		_, meta, err := conn.StreamBodyMeta(messageID, &buf)
		if err != nil {
			return err
		}
		if check != nil {
			if reason := check(meta); reason != "" {
				u.logger.Debug().Str("message_id", messageID).Str("reason", reason).
					Msg("Provider returned a different upload's article; trying the next provider")
				return &nntp.Error{
					Type:    nntp.ErrorTypeArticleNotFound,
					Message: "article belongs to a different upload: " + reason,
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, nil
}

// FetchArticlePrefix returns the first n decoded bytes of one NNTP article,
// held in memory only: no segment cache, no DFS cache, no prefetch. The rest
// of the body is drained and discarded (the connection stays reusable), so
// it still costs one article of bandwidth.
func (u *Usenet) FetchArticlePrefix(ctx context.Context, messageID string, n int) ([]byte, error) {
	var prefix []byte
	err := u.nntp.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
		meta, err := conn.GetHeaderPrefix(messageID, n)
		if err != nil {
			return err
		}
		prefix = meta.Snippet
		return nil
	})
	if err != nil {
		return nil, err
	}
	return prefix, nil
}

// ArticleHead is an article's first decoded bytes and the posted file its
// yEnc header names.
type ArticleHead struct {
	Prefix []byte
	Name   string // yEnc name= of the posted file
	Part   int    // yEnc part= (1 for a posted file's first article)
}

// FetchArticleHead is FetchArticlePrefix plus the article's yEnc name and part
// number, which say which posted file the article belongs to - a check that
// a stored message ID still serves its own upload.
func (u *Usenet) FetchArticleHead(ctx context.Context, messageID string, n int) (ArticleHead, error) {
	var head ArticleHead
	err := u.nntp.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
		meta, err := conn.GetHeaderPrefix(messageID, n)
		if err != nil {
			return err
		}
		head = ArticleHead{Prefix: meta.Snippet, Name: meta.Name, Part: int(meta.Part)}
		return nil
	})
	return head, err
}

// MissingSegment identifies one confirmed-missing article found during a
// CheckFileDetailed probe, in enough detail to record against the overlay
// store (pkg/usenet/overlay.Store.RecordDead).
type MissingSegment struct {
	Index     int
	MessageID string
	Bytes     int64
}

// CheckFileDetailed is CheckFile plus the specific segments found
// definitively missing, for callers (the repair sweep) that need to persist
// that damage to the overlay store rather than just learn pass/fail. Unlike
// CheckFile's zero-alloc sampled-ids fast path, this decodes the file's full
// segment list so each sampled miss can be matched back to its index and
// byte length - acceptable here since it only runs from the repair sweep,
// not the streaming hot path.
func (u *Usenet) CheckFileDetailed(ctx context.Context, nzoID, filename string) (missing []MissingSegment, err error) {
	nzb, err := u.nzbStorage.GetNZB(nzoID)
	if err != nil {
		return nil, fmt.Errorf("failed to load NZB: %w", err)
	}
	file := nzb.GetFileByName(filename)
	if file == nil || len(file.Segments) == 0 {
		return nil, fmt.Errorf("file has no Segments: %s", filename)
	}

	samplePercent := config.Get().Usenet.AvailabilitySamplePercent
	idx := sampleIndices(len(file.Segments), samplePercent)
	if len(idx) == 0 {
		return nil, nil
	}
	messageIDs := make([]string, len(idx))
	for i, j := range idx {
		messageIDs[i] = file.Segments[j].MessageID
	}

	result, batchErr := u.nntp.BatchStat(ctx, messageIDs)
	if batchErr != nil {
		u.logger.Warn().Err(batchErr).Str("file", filename).Msg("Non-fatal error during detailed availability check, ignoring")
		return nil, nil
	}

	for i, r := range result.Results {
		if r.Available {
			continue
		}
		// Only a genuine article-not-found counts as confirmed-missing;
		// connection/protocol errors mean we couldn't check, not that the
		// article is gone.
		if r.Error != nil && !nntp.IsArticleNotFoundError(r.Error) {
			continue
		}
		seg := file.Segments[idx[i]]
		missing = append(missing, MissingSegment{Index: idx[i], MessageID: messageIDs[i], Bytes: seg.Bytes})
	}
	return missing, nil
}

// OverlayScreenResult is the read-only outcome of screening a file's TRUE
// damage extent - a full STAT over every segment, not just the nightly
// sweep's fixed ~10% sample - against the padding caps. Nothing about this
// screen is persisted: it doesn't call Decide, doesn't write StatusDead or
// StatusPadded, and doesn't touch the manifest. It only reports what Decide
// WOULD return if the newly-found missing segments were added to the ones
// already recorded.
//
// Like the live accept-path, this has no notion of WHERE in the file the
// damage sits relative to what's already played (CreatedAt/ImportedAt aren't
// surfaced here) - it judges purely by total segment extent, the same blind
// spot Decide already has.
type OverlayScreenResult struct {
	Entry string `json:"entry"`
	File  string `json:"file"`

	CurrentVerdict string `json:"current_verdict"` // the file's persisted verdict, unchanged by this screen

	TotalSegments  int   `json:"total_segments"`
	FileSize       int64 `json:"file_size"`
	AlreadyDead    int   `json:"already_dead"`    // recorded dead, not yet padded or patched
	AlreadyPadded  int   `json:"already_padded"`  // recorded dead and already padded
	AlreadyPatched int   `json:"already_patched"` // recorded dead, PAR2-recovered - excluded from the cap math
	NewlyMissing   int   `json:"newly_missing"`   // confirmed missing by this screen's full STAT, not yet recorded at all

	ProjectedTotalDead int     `json:"projected_total_dead"` // AlreadyDead + AlreadyPadded + NewlyMissing
	ProjectedRun       int     `json:"projected_run"`        // longest run of consecutive dead indices, projected
	ProjectedByteRatio float64 `json:"projected_byte_ratio"`

	WouldStayWithinCaps bool   `json:"would_stay_within_caps"`
	ProjectedVerdict    string `json:"projected_verdict"` // "degraded" (would stay padded) or "failed" (would exceed caps)
}

// screenHypothesis is the deduplicated union OverlayScreenFile projects
// against the padding caps, plus the already-known/newly-discovered counts
// its report surfaces. Building this is pure (no I/O), separated out from
// OverlayScreenFile so the one thing Phase 2's auto-regrab decision will
// depend on - that an already-recorded dead/padded segment showing up
// missing again in the full STAT is never counted as NEW damage - can be
// unit-tested directly against fabricated inputs, without a network or NZB
// storage double.
type screenHypothesis struct {
	deadSegments                               []overlay.DeadSegment
	newlyMissing                               int
	alreadyDead, alreadyPadded, alreadyPatched int
}

// buildScreenHypothesis merges recorded (already-persisted) dead segments
// with a full-STAT result into a deduplicated hypothetical dead-set: every
// recorded segment is kept exactly once, by index, however many times the
// STAT re-confirms it's still missing - it's already-known damage, not new
// decay. Only a segment whose index was NOT already recorded, and whose STAT
// result is a genuine article-not-found (not a connection/protocol error),
// counts as newly missing and gets added.
func buildScreenHypothesis(recorded []overlay.DeadSegment, segments []storage.NZBSegment, stat *nntp.BatchStatResult) screenHypothesis {
	h := screenHypothesis{deadSegments: append([]overlay.DeadSegment{}, recorded...)}

	knownIndex := make(map[int]bool, len(recorded))
	for _, d := range recorded {
		knownIndex[d.Index] = true
		switch d.Status {
		case overlay.StatusDead:
			h.alreadyDead++
		case overlay.StatusPadded:
			h.alreadyPadded++
		case overlay.StatusPatched:
			h.alreadyPatched++
		}
	}

	for i, r := range stat.Results {
		if r.Available || knownIndex[i] {
			continue
		}
		// Only a genuine article-not-found counts as confirmed-missing;
		// connection/protocol errors mean we couldn't check, not that the
		// article is gone.
		if r.Error != nil && !nntp.IsArticleNotFoundError(r.Error) {
			continue
		}
		h.newlyMissing++
		seg := segments[i]
		h.deadSegments = append(h.deadSegments, overlay.DeadSegment{
			Index: i, MessageID: seg.MessageID, Bytes: seg.Bytes, Status: overlay.StatusDead,
		})
	}
	return h
}

// OverlayScreenFile full-STATs every segment of nzoID/filename - not the
// fixed sample the nightly sweep is limited to - and reports whether the
// file's TRUE damage extent would still fit the padding caps. It is entirely
// read-only: no verdict is written, no segment is recorded as dead, and no
// repair or re-grab is triggered. Callers act on the report; this only
// produces it.
func (u *Usenet) OverlayScreenFile(ctx context.Context, nzoID, filename string) (OverlayScreenResult, error) {
	result := OverlayScreenResult{File: filename}

	nzb, err := u.nzbStorage.GetNZB(nzoID)
	if err != nil {
		return result, fmt.Errorf("failed to load NZB: %w", err)
	}
	file := nzb.GetFileByName(filename)
	if file == nil || len(file.Segments) == 0 {
		return result, fmt.Errorf("file has no Segments: %s", filename)
	}
	result.TotalSegments = len(file.Segments)
	result.FileSize = file.Size

	// This screen never mutates the real manifest - recorded is read once,
	// up front, and everything below works against a local hypothetical set.
	var recorded []overlay.DeadSegment
	if u.overlay != nil {
		if m, merr := u.overlay.GetManifest(nzoID); merr == nil {
			if fe := m.Files[filename]; fe != nil {
				result.CurrentVerdict = string(fe.Verdict)
				recorded = fe.DeadSegments
			}
		}
	}
	if result.CurrentVerdict == "" {
		result.CurrentVerdict = string(overlay.VerdictClean)
	}

	messageIDs := make([]string, len(file.Segments))
	for i, seg := range file.Segments {
		messageIDs[i] = seg.MessageID
	}
	stat, err := u.nntp.BatchStatComplete(ctx, messageIDs)
	if err != nil {
		return result, fmt.Errorf("full availability screen failed: %w", err)
	}

	h := buildScreenHypothesis(recorded, file.Segments, stat)
	result.AlreadyDead = h.alreadyDead
	result.AlreadyPadded = h.alreadyPadded
	result.AlreadyPatched = h.alreadyPatched
	result.NewlyMissing = h.newlyMissing

	policy := overlay.DefaultPolicy()
	if u.overlay != nil {
		policy = u.overlay.Policy()
	}
	eval, ok := overlay.WithinPadCaps(&overlay.FileEntry{DeadSegments: h.deadSegments}, file.Size, policy, len(file.Segments))
	result.ProjectedTotalDead = eval.TotalDead
	result.ProjectedRun = eval.LongestRun
	result.ProjectedByteRatio = eval.ByteRatio
	result.WouldStayWithinCaps = ok
	if ok {
		result.ProjectedVerdict = string(overlay.VerdictDegraded)
	} else {
		result.ProjectedVerdict = string(overlay.VerdictFailed)
	}
	return result, nil
}

// checkAvailability batch-STATs the given sampled message ids. The NNTP client
// gates each worker through its internal repair bank so concurrent availability
// checks don't starve streaming connections.
func (u *Usenet) checkAvailability(ctx context.Context, fileName string, messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}

	result, err := u.nntp.BatchStat(ctx, messageIDs)
	if err != nil {
		// Connection/system error - log and continue (don't fail availability check)
		u.logger.Warn().
			Err(err).
			Str("file", fileName).
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
			Str("file", fileName).
			Int("sampled_segments", len(messageIDs)).
			Int("available_segments", result.FoundCount).
			Int("missing_segments", notFoundCount).
			Int("error_count", result.ErrorCount).
			Msg("File is unavailable - one or more segments are missing")
		return customerror.UsenetSegmentMissingError
	}

	return nil
}

// sampleSegments returns a sample of segment message IDs based on the given
// percentage. Always includes first and last segments, then uniformly samples
// from the middle (see sampleIndices).
func (u *Usenet) sampleSegments(segments []storage.NZBSegment, percent int) []string {
	idx := sampleIndices(len(segments), percent)
	if len(idx) == 0 {
		return nil
	}
	out := make([]string, len(idx))
	for i, j := range idx {
		out[i] = segments[j].MessageID
	}
	return out
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
		// The NZB record itself is gone - nzoID was deleted or superseded
		// by a re-grab since this call was made to resolve it (e.g. a FUSE
		// handle opened before the entry was replaced). Distinct from a
		// live NNTP article-not-found: retrying/padding/repairing can never
		// fix this, only re-resolving against whatever entry (if any) now
		// exists at the same path can - see ErrEntryGone's doc comment.
		return nil, fmt.Errorf("%w: metadata load failed: %w", ErrEntryGone, err)
	}

	// GetFileByName picks among same-name records the way every check does,
	// so what streams is what was checked.
	files := make(map[string]*storage.NZBFile, len(filenames))
	for _, filename := range filenames {
		source := nzb.GetFileByName(filename)
		if source == nil {
			continue
		}
		file := *source
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
	if rec, ok := u.loadFailedFile(file.NzbID, file.Name); ok {
		u.logger.Debug().
			Str("nzb_id", file.NzbID).
			Str("file", file.Name).
			Err(rec.err).
			Msg("preStreamChecks: short-circuiting on cached permanent failure")
		return customerror.NewSilentError(rec.err).Permanent()
	}

	return nil
}

// FailedFileCause returns the recorded permanent failure for a file (e.g. an
// article-not-found discovered during a prior read/prefetch), or nil if none.
// Lets higher layers surface the real cause instead of a generic "no data"
// error when a stream produces nothing because every segment is missing.
func (u *Usenet) FailedFileCause(nzoID, filename string) error {
	if rec, ok := u.loadFailedFile(nzoID, filename); ok {
		return rec.err
	}
	return nil
}

// failedFileRecord is one permanently-failed (nzbID, filename)'s cached
// cause plus when it was recorded, so loadFailedFile can expire it after
// failedFileTTL instead of poisoning a file for the process lifetime.
type failedFileRecord struct {
	err        error
	recordedAt time.Time
}

// loadFailedFile returns (nzoID, filename)'s cached permanent-failure
// record, self-healing an expired one by deleting it on read so a stale
// entry never needs an explicit Clear call to eventually recover.
func (u *Usenet) loadFailedFile(nzoID, filename string) (failedFileRecord, bool) {
	key := fsKey(nzoID, filename)
	rec, ok := u.failedFiles.Load(key)
	if !ok {
		return failedFileRecord{}, false
	}
	if time.Since(rec.recordedAt) > failedFileTTL {
		u.failedFiles.Delete(key)
		return failedFileRecord{}, false
	}
	return rec, true
}

// ClearFailedFile un-poisons (nzoID, filename), letting the next read build
// a fresh reader and re-verify the file from scratch instead of
// short-circuiting on a stale cause. Called wherever the file's underlying
// damage may have changed since the record was written: a successful PAR2
// repair/patch, a manual "repair now", the overlay GUI's reclaim/research
// actions, and a raised padding-cap policy change (see ApplyOverlayPolicy).
func (u *Usenet) ClearFailedFile(nzoID, filename string) {
	u.failedFiles.Delete(fsKey(nzoID, filename))
}

// ClearFailedEntry un-poisons every file cached under nzoID. Called wherever
// an entry is deleted or superseded (see Delete) so a re-grab's fresh nzbID
// never inherits a stale cache key pointing at the old, now-gone grab.
func (u *Usenet) ClearFailedEntry(nzoID string) {
	prefix := nzoID + "::"
	u.failedFiles.Range(func(key string, _ failedFileRecord) bool {
		if strings.HasPrefix(key, prefix) {
			u.failedFiles.Delete(key)
		}
		return true
	})
}

// ContextForVerificationRead marks ctx so the segment fetcher treats a
// confirmed-dead segment as a hard failure instead of padding/patching it -
// see reader.ContextWithoutPadding. Callers pass the returned context into
// Stream for internal-bearer-token reads (ffprobe import/sweep checks), so a
// broken grab can never look healthy by way of the very padding that makes
// it playable during real playback.
func ContextForVerificationRead(ctx context.Context) context.Context {
	return reader.ContextWithoutPadding(ctx)
}

// ContextForYieldingVerification marks a verification read as background work
// whose prefetch narrows while playback of another file stalls - see
// reader.ContextForYieldingVerification.
func ContextForYieldingVerification(ctx context.Context) context.Context {
	return reader.ContextForYieldingVerification(ctx)
}

// DeadSegmentSignal is a one-way latch a verification read carries so the
// ffprobe checker that spawned it can tell, after the fact, whether the
// fetcher ever observed a confirmed-dead (430) segment - see
// reader.DeadSegmentSignal.
type DeadSegmentSignal = reader.DeadSegmentSignal

// NewDeadSegmentSignal returns a fresh, untripped signal.
func NewDeadSegmentSignal() *DeadSegmentSignal { return reader.NewDeadSegmentSignal() }

// ContextWithDeadSignal attaches sig to ctx so the segment fetcher trips it on
// a confirmed-dead segment during this read - see reader.ContextWithDeadSignal.
func ContextWithDeadSignal(ctx context.Context, sig *DeadSegmentSignal) context.Context {
	return reader.ContextWithDeadSignal(ctx, sig)
}

// VerifyBudget caps the bytes one ffprobe verification may pull for a file -
// see reader.VerifyBudget for why it exists.
type VerifyBudget = reader.VerifyBudget

// NewVerifyBudget returns a budget of limit bytes, or nil when limit <= 0.
func NewVerifyBudget(limit int64) *VerifyBudget { return reader.NewVerifyBudget(limit) }

// ContextWithVerifyBudget attaches b to ctx so the verification read path
// meters against it - see reader.ContextWithVerifyBudget.
func ContextWithVerifyBudget(ctx context.Context, b *VerifyBudget) context.Context {
	return reader.ContextWithVerifyBudget(ctx, b)
}

// IsVerificationRead reports whether ctx was marked by ContextForVerificationRead.
// Callers outside this package (pkg/manager) use this to skip triggering
// behavior meant only for real client playback - e.g. read-ahead precache -
// on decypharr's own internal ffprobe/sweep reads.
func IsVerificationRead(ctx context.Context) bool {
	return reader.PaddingDisabled(ctx)
}

// ContextForBurstDownload marks ctx so the segment fetcher still records a
// confirmed-dead segment in the overlay and queues its repair, but never
// fabricates zero-fill bytes for it - see reader.ContextForBurstDownload.
// Deliberately NOT the same marker ContextForVerificationRead uses: a
// verification read must suppress overlay recording entirely, while a
// precache burst still needs its own damage-detection to see the dead
// segment. Callers pass the returned context into ReadAhead for the
// next-episode pre-cache burst (see pkg/manager.Precache.precacheEpisodeFile),
// which has no viewer waiting and would rather fail a dead segment outright
// than risk zero-fill bytes being durably persisted into the DFS cache as if
// they were genuine data.
func ContextForBurstDownload(ctx context.Context) context.Context {
	return reader.ContextForBurstDownload(ctx)
}

// ContextForBufferedPlayback marks a stream whose reads fill a buffer in front
// of the client, so their waits don't count as playback stalls - see
// reader.ContextForBufferedPlayback and NotePlaybackWait.
func ContextForBufferedPlayback(ctx context.Context) context.Context {
	return reader.ContextForBufferedPlayback(ctx)
}

// NotePlaybackWait records that a client read of nzoID/filename, served from
// a buffer in front of this package (the DFS mount), waited d for its data -
// the player waiting, which pauses other files' read-ahead bursts once d is
// long enough. No-op when the file has no open entry or its reader can't
// track stalls (a multi-volume archive).
func (u *Usenet) NotePlaybackWait(nzoID, filename string, d time.Duration) {
	// A stall that can't be recorded is logged, so a signal that never
	// reaches a reader shows up as such rather than as calm playback.
	untracked := func(reason string) {
		if d >= reader.PlaybackStallAfter {
			u.logger.Debug().Str("nzb", nzoID).Str("file", filename).Dur("wait", d).Str("reason", reason).
				Msg("playback stall not tracked")
		}
	}
	key := fsKey(nzoID, filename)
	entry, ok := u.fs.Load(key)
	if !ok || !entry.acquire() {
		untracked("no open entry")
		return
	}
	defer u.releaseFS(key)
	r, _, err := entry.getOrCreateReader()
	if err != nil {
		untracked("no reader")
		return
	}
	s, ok := r.(interface {
		NotePlaybackWait(time.Duration, time.Time)
	})
	if !ok {
		untracked("reader does not track stalls")
		return
	}
	s.NotePlaybackWait(d, time.Now())
}

// Stream streams a file using the new streaming system with caching and worker limiting
func (u *Usenet) Stream(ctx context.Context, nzoID, filename string, start, end int64, writer io.Writer) error {
	// Every client stream comes through here; background reads don't. A slow
	// playback read pauses other files' read-ahead bursts - see
	// reader.ContextForPlayback. Verification reads are excluded by the
	// reader itself.
	ctx = reader.ContextForPlayback(ctx)
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
	verifyRead := reader.PaddingDisabled(ctx)
	if !verifyRead {
		// Normal read: hint the background prefetch workers. They fetch on
		// the fetcher's lifetime ctx, which is fine here - padding is allowed.
		// A verification read gets a no-pad rolling prefetch instead (below):
		// the background workers would pad a dead read-ahead segment and
		// AutoEnqueue it, defeating the point of the no-pad probe.
		readerAt.Prefetch(ctx, rangeStart, prefetchLen)
	}

	section := newContextSectionReader(ctx, readerAt, rangeStart, length)
	var buf []byte
	var meter *meteredReader
	if verifyRead {
		buf = acquireVerifyBuffer()
		defer releaseVerifyBuffer(buf)

		// Meter the verification path so a decode probe's throughput is
		// visible. ffprobe issues one HTTP range request per decode window,
		// so each log line below is one window - bucket by file for the
		// per-window breakdown.
		// The budget (if the probe caller registered one) is shared across
		// every range request this probe issues, so a runaway read is capped
		// for the file as a whole rather than per window.
		meter = &meteredReader{inner: section, budget: reader.VerifyBudgetFromContext(ctx)}

		// Rolling no-pad prefetch: keep segments fetched ahead of the read
		// position so the fetch overlaps ffmpeg's consume instead of running
		// synchronously per readAtPlain call (~3x slower). The deferred
		// cancel+wait guarantees the goroutine is fully stopped before Stream
		// returns and releaseFS drops this entry's reader refcount.
		pfCtx, pfCancel := context.WithCancel(ctx)
		pfDone := make(chan struct{})
		defer func() {
			pfCancel()
			// Bounded: a prefetch worker blocked in a dial/read timeout would
			// otherwise hold the WebDAV handler open for the full NNTP timeout.
			// The goroutine holds no reader ref of its own, so a brief linger
			// past this point is harmless (releaseFS has a grace period).
			select {
			case <-pfDone:
			case <-time.After(2 * time.Second):
			}
		}()
		go func() {
			defer close(pfDone)
			u.verificationPrefetch(pfCtx, readerAt, rangeStart, length, meter)
		}()
	} else {
		buf = acquireStreamBuffer()
		defer releaseStreamBuffer(buf)
	}

	var copySrc io.Reader = section
	if meter != nil {
		copySrc = meter
	}
	copyStart := time.Now()

	// Use a safe copy loop that checks context and validates read counts
	written, err := safeCopyBuffer(ctx, writer, copySrc, buf)
	if meter != nil {
		meter.budget.ObserveRequest(written)
	}

	// A blown verification budget ends the body early on purpose. It is not a
	// stream failure and must not reach the article-not-found / failedFiles
	// handling below: the probe caller sees Exceeded() and decides what the
	// cut means - an inconclusive verdict, or, for seek detection, a file
	// with no usable index.
	if meter != nil && errors.Is(err, reader.ErrVerifyBudgetExhausted) {
		err = nil
		// Debug once, for the request that actually crossed the cap. Every
		// request ffprobe issues after that is refused before its first byte,
		// and there can be many, so those are Trace.
		ev := u.logger.Debug()
		if meter.bytes == 0 {
			ev = u.logger.Trace()
		}
		ev.Str("nzb_id", nzoID).
			Str("file", filename).
			Int64("budget_bytes", meter.budget.Limit()).
			Int64("used_bytes", meter.budget.Used()).
			Msg("Repair: verification read hit its byte budget; ending body early")
	}

	if verifyRead && meter != nil && meter.bytes > 0 {
		dur := time.Since(copyStart)
		var mibs float64
		if dur > 0 {
			mibs = float64(meter.bytes) / (1024 * 1024) / dur.Seconds()
		}
		// One line per HTTP range request. ffprobe issues dozens to hundreds
		// of these per decode probe, so it is Trace, not Debug: the useful
		// per-verification totals (bytes, throughput, read_wait, whether the
		// budget was cut) are logged once by the ffprobe checker from the
		// VerifyBudget accounting. Flip log_level to trace to get the
		// per-range breakdown back (range_start progression = forward-scan vs
		// seek signature).
		u.logger.Trace().
			Str("nzb_id", nzoID).
			Str("file", filename).
			Int64("range_start", rangeStart).
			Int64("range_len", length).
			Int64("bytes", meter.bytes).
			Dur("dur", dur).
			Float64("throughput_mib_s", mibs).
			Int("reads", meter.reads).
			Dur("read_wait", meter.readWait).
			Msg("Repair: verification range served")
	}

	// Handle context cancellation explicitly
	if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}

	// Mark file as failed if article not found (permanent error)
	if err != nil && nntp.IsArticleNotFoundError(err) {
		if u.shouldPoisonFailedFile(ctx, nzoID, filename) {
			u.logger.Warn().
				Str("nzb_id", nzoID).
				Str("file", filename).
				Err(err).
				Msg("Stream: caching permanent failure, future reads will short-circuit")
			u.failedFiles.Store(key, failedFileRecord{err: err, recordedAt: time.Now()}) // Reuse pre-computed key
		}
		// Wrap error to mark as permanent
		return customerror.NewArticleNotFoundError(err)
	}

	return err
}

// verificationPrefetch keeps segments fetched ahead of a verification read's
// position so the NNTP fetch overlaps ffmpeg's decode instead of running
// synchronously per readAtPlain call. Measured on a production install 2026-09-09: the
// synchronous path sustained ~24 MiB/s on a REMUX while the normal (prefetched)
// read path did ~73, and the earlier chunk-at-a-time FetchRange loop only
// reached ~30 because each 8MB burst barriered on its slowest segment.
//
// It runs FetchRangeWindowed on the caller's ctx, which carries the no-pad
// marker and the dead-segment signal the WebDAV handler attached: a dead
// article in the read-ahead trips the broken verdict exactly as the foreground
// read would, rather than being padded. That is why the background prefetch
// workers (which fetch on the fetcher's lifetime ctx and would pad) are bypassed
// for this path. Non-ctx errors are swallowed - a failed segment stays
// StateFailed for the foreground read to surface.
//
// The horizon keeps the workers within verificationPrefetchAhead of the read
// position and holds them off entirely until the read has consumed
// verificationPrefetchRamp - a seek or moov probe finishes inside the ramp and
// costs nothing.
func (u *Usenet) verificationPrefetch(ctx context.Context, r fs.PrefetchableReaderAt, base, total int64, meter *meteredReader) {
	if total <= 0 || meter == nil {
		return
	}
	conc := u.verificationConnections
	if conc < 1 {
		conc = u.maxConnections
	}
	if conc < 1 {
		conc = 8
	}
	horizon := func() int64 {
		consumed := meter.consumed.Load()
		if consumed < verificationPrefetchRamp {
			return -1
		}
		return consumed + verificationPrefetchAhead
	}
	_ = r.FetchRangeWindowed(ctx, base, total, conc, horizon)
}

// shouldPoisonFailedFile decides whether an article-not-found from this read
// should permanently poison (nzoID, filename) for every future Stream call
// (see preStreamChecks). Two safety gates, in order:
//
//   - A verification read (ContextForVerificationRead - ffprobe import/sweep
//     checks) deliberately disables padding to observe the raw failure. That
//     tells the import gate the grab is broken; it says nothing about
//     whether normal playback - which pads within the overlay's caps -
//     could have survived the same dead segment. Never let it poison the
//     cache real playback consults.
//   - When padding is active for this file, the reader already pads
//     anything within the overlay's caps (see reader.SegmentFetcher.
//     handleConfirmedMissing) - an article-not-found only reaches here
//     after the overlay itself gave up, which means Decide already set the
//     file's verdict to Failed before returning. Poisoning on anything
//     less than Failed (Clean because padding never got a chance to run
//     yet, or Degraded because the damage is within caps) would
//     permanently block the padding path above from ever running again via
//     preStreamChecks - exactly the bug that let Odd Cousins S02E01 fail
//     for 69 minutes without a single padded segment: two verification
//     reads poisoned the file before real playback ever built a reader, so
//     the overlay verdict stayed Clean the whole time. If the overlay has
//     no record yet (Clean), the safe default is to NOT poison - the next
//     read builds the reader and records real damage instead.
func (u *Usenet) shouldPoisonFailedFile(ctx context.Context, nzoID, filename string) bool {
	if reader.PaddingDisabled(ctx) {
		return false
	}
	return u.OverlayVerdict(nzoID, filename) == overlay.VerdictFailed
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

// ReadCachedAt reads [off, off+len(p)) via nzoID/filename's shared
// entry/reader - the same one Stream/ReadAhead use - serving from whatever
// is already fetched without any of Stream's side effects (no failedFiles
// poisoning, no Observe re-entry, no Downloaders). Used by the next-episode
// precache pass to read back bytes a prior ReadAhead call already fetched,
// for a durable copy into the DFS cache (see
// pkg/manager.Precache.persistCleanRanges). Like ReadAtContext, this can
// still trigger a fetch (and padding/overlay recording) for a segment that
// isn't actually cached yet - callers that only want segments confirmed
// clean should check OverlayPendingRepair first (as persistCleanRanges
// does) rather than relying on this to skip damaged ranges itself.
func (u *Usenet) ReadCachedAt(ctx context.Context, nzoID, filename string, p []byte, off int64) (int, error) {
	entry, key, err := u.getOrCreateEntry(ctx, nzoID, filename)
	if err != nil {
		return 0, fmt.Errorf("failed to get or create entry: %w", err)
	}
	defer u.releaseFS(key)

	readerAt, _, err := entry.getOrCreateReader()
	if err != nil {
		return 0, fmt.Errorf("failed to get reader: %w", err)
	}
	return readerAt.ReadAtContext(ctx, p, off)
}

// ReadAhead aggressively fetches the remainder of a file - [from, EOF) -
// into the cache at up to concurrency parallel segment fetches, distinct
// from (and typically much higher than) the reader's normal steady-state
// prefetch window used during ordinary streaming. Intended to be triggered
// once a playing file's read position has crossed a configured threshold
// (see config.Precache), not on every read.
//
// Uses the same shared entry/reader as Stream/PreCache, so anything already
// cached is skipped and anything fetched here is immediately available to
// concurrent/subsequent Stream calls. As a side effect of going through the
// normal per-segment fetch path, any segment confirmed missing across every
// provider is recorded in the overlay (and padded, if playback padding is
// enabled) exactly as it would be for a live read - see
// pkg/usenet/fs/reader.SegmentFetcher.handleConfirmedMissing.
func (u *Usenet) ReadAhead(ctx context.Context, nzoID, filename string, from int64, concurrency int) error {
	return u.ReadAheadRange(ctx, nzoID, filename, from, -1, concurrency)
}

// ReadAheadRange is ReadAhead over [off, off+length) instead of to EOF; a
// negative length means to EOF. The fetched segments land in the reader's
// scratch SegmentCache, which holds only a few hundred MB, so a caller that
// wants them durably has to copy each range out before fetching much more -
// see pkg/manager.Precache.burstToDurable.
func (u *Usenet) ReadAheadRange(ctx context.Context, nzoID, filename string, off, length int64, concurrency int) error {
	entry, key, err := u.getOrCreateEntry(ctx, nzoID, filename)
	if err != nil {
		return fmt.Errorf("failed to get or create entry: %w", err)
	}
	defer u.releaseFS(key)

	if len(entry.volumes) == 0 {
		return fmt.Errorf("no volumes available for file %s", filename)
	}
	fileSize := entry.volumes[0].Size
	if off < 0 {
		off = 0
	}
	if off >= fileSize {
		return nil
	}
	if length < 0 || length > fileSize-off {
		length = fileSize - off
	}

	readerAt, _, err := entry.getOrCreateReader()
	if err != nil {
		return fmt.Errorf("failed to get reader: %w", err)
	}

	return readerAt.FetchRange(ctx, off, length, concurrency)
}

// Stats returns nntp statistics
func (u *Usenet) Stats() map[string]any {
	stats := u.nntp.Stats()
	stats["readers"] = u.fs.Size()
	stats["nzb_storage"] = u.nzbStorage.Stats()
	return stats
}

// GetNZB returns NZB metadata by ID
func (u *Usenet) GetNZB(id string) (*storage.NZB, error) {
	return u.nzbStorage.GetNZB(id)
}

// ProcessingMaxConnections returns the configured connection/concurrency
// bound for parsing and NZB-processing work (Download's fetch pool sizes
// off this same value) - exposed so other packages that build their own
// bounded concurrent fetch pool (e.g. the PAR2 repair worker's intact-slice
// reader) use the identical, single-source-of-truth limit rather than a
// second hardcoded or duplicated one.
func (u *Usenet) ProcessingMaxConnections() int {
	return u.processingMaxConnections
}

// GetNZBHeader returns NZB metadata without its segment map. Use this when only
// scalar fields or the file list are needed (status, path, sizes); it avoids
// decoding/allocating the multi-megabyte segment data.
func (u *Usenet) GetNZBHeader(id string) (*storage.NZB, error) {
	return u.nzbStorage.GetNZBHeader(id)
}

// ForEachNZB iterates over all NZBs
func (u *Usenet) ForEachNZB(fn func(*storage.NZB) error) error {
	return u.nzbStorage.ForEachNZB(fn)
}

// MarkPostingDeadByNZBID records the raw-content identity of nzoID's source
// NZB (its ContentHash - see hashNZBContent) in the dead-posting negative
// cache, so an identical re-list (same posted articles, fresh grab ID) is
// rejected at ParseWithID for deadPostingTTL instead of re-parsing,
// re-STATting and re-ffprobing its way to the same verdict.
//
// Intended for a caller that has *confirmed the posted articles are gone* -
// e.g. the import gate on a dead-segment (NNTP 430) ffprobe verdict - not for
// a transient or an ambiguous unreadable/decode failure, since a same-name
// repost with fresh message-IDs hashes differently and must not be
// suppressed.
//
// marked is false (a no-op) when the record can't be resolved or has no
// persisted ContentHash (NZBs parsed before ContentHash serialization); err
// is only the header-lookup error. The caller decides how loudly to log.
func (u *Usenet) MarkPostingDeadByNZBID(nzoID string) (marked bool, err error) {
	if u == nil || nzoID == "" {
		return false, nil
	}
	nzb, err := u.nzbStorage.GetNZBHeader(nzoID)
	if err != nil {
		return false, err
	}
	if nzb == nil || nzb.ContentHash == "" {
		return false, nil
	}
	u.deadPostings.Mark(nzb.ContentHash)
	return true, nil
}

// HasPar2Data reports whether nzoID's stored record already has retained
// PAR2 file references (Par2Files/Par2Source - see storage.NZB), via the
// cheap header-only decode. Does not attempt a backfill for a record that
// predates those fields - that's the PAR2 repair job's job, lazily, only
// once it actually needs them.
func (u *Usenet) HasPar2Data(nzoID string) bool {
	nzb, err := u.nzbStorage.GetNZBHeader(nzoID)
	if err != nil || nzb == nil {
		return false
	}
	return len(nzb.Par2Files) > 0
}

// NZBStorage returns the underlying NZB storage
func (u *Usenet) NZBStorage() *NZBStorage {
	return u.nzbStorage
}

// NZBsDir exposes the watched .nzb source directory for callers outside this
// package that need to reconcile it against another store (e.g. stale-entry
// cleanup scanning for on-disk files with no corresponding storage entry).
func (u *Usenet) NZBsDir() string {
	return u.metadataDir
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

func (u *Usenet) saveNZBFile(id string, content []byte) (string, error) {
	// Store the raw source keyed by the bounded NZB ID rather than the
	// (untrusted, arbitrarily long) display name. ext4 caps a path component at
	// 255 bytes; a long release name plus a ".processing"/".importing"/".queued"
	// marker suffix blew past that limit, which failed the rename, wedged the
	// refresh watcher, and left truncated fragment files behind. The UUID keeps
	// every derived name comfortably under the cap.
	path := filepath.Join(u.metadataDir, id+".nzb")
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

	// The parsed segment map (.meta) is the only artifact needed for streaming
	// and repair, so the raw .nzb source file is dead weight once the NZB
	// completes — delete it (and its processing marker) immediately. Path is
	// cleared so a later Delete()/watch scan ignores the now-absent file; with
	// the source gone there is nothing for ClaimNewNZBs to re-import, so no
	// .processed marker is needed.
	if nzb.Path != "" {
		if err := os.Remove(nzb.Path); err != nil && !os.IsNotExist(err) {
			u.logger.Warn().Err(err).Str("path", nzb.Path).Msg("Failed to delete NZB source file after completion")
		}
		_ = os.Remove(nzb.Path + ".processing")
		nzb.Path = ""
	}

	if err := u.nzbStorage.AddNZB(nzb); err != nil {
		return fmt.Errorf("failed to save NZB to storage: %w", err)
	}
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

// ErrBackfillImpossible marks a BackfillPar2Refs failure no retry can fix.
var ErrBackfillImpossible = errors.New("PAR2 refs cannot be backfilled")

// BackfillPar2Refs re-derives Par2Files/Par2Source for an NZB record whose
// stored meta blob predates those fields, by re-parsing the raw .nzb source
// file on disk (nzb.Path - see pkg/manager/stale_nzb.go for this location
// convention). Intended to be called lazily by the PAR2 repair job, right
// before it would otherwise report "par2 repair unavailable" for an entry
// that simply hasn't been re-parsed since PAR2 retention shipped.
//
// This only succeeds while the source file is still present. markAsCompleted
// deletes it once an NZB finishes downloading - the parsed .meta blob is the
// only artifact normal streaming/repair ever needed before this feature
// existed - so in practice this backfill only has something to work with for
// an entry that is still mid-processing or that failed before completing.
// A completed entry from before PAR2 retention shipped has no raw NZB left
// to re-derive from; it simply never gets PAR2 repair and falls straight to
// the legacy re-grab path, exactly like "no par2 available" does for any
// other entry.
//
// An error wrapping ErrBackfillImpossible means no retry can succeed (no
// source NZB, or it holds no PAR2 data); any other is transient.
func (u *Usenet) BackfillPar2Refs(ctx context.Context, nzoID string) error {
	nzb, err := u.nzbStorage.GetNZB(nzoID)
	if err != nil {
		return fmt.Errorf("failed to load NZB: %w", err)
	}
	if len(nzb.Par2Files) > 0 || len(nzb.Par2Source) > 0 {
		return nil
	}
	if nzb.Path == "" {
		return fmt.Errorf("source NZB file is no longer on disk; cannot backfill PAR2 refs for %s: %w", nzoID, ErrBackfillImpossible)
	}
	content, err := os.ReadFile(nzb.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("source NZB file is gone: %w: %w", err, ErrBackfillImpossible)
		}
		return fmt.Errorf("failed to read source NZB file: %w", err)
	}

	prs := parser.NewParser(u.nntp, u.processingMaxConnections, u.logger.With().Str("component", "par2-backfill").Logger())
	reparsed, _, err := prs.Parse(ctx, nzb.Name, content)
	if err != nil {
		return fmt.Errorf("failed to re-parse source NZB file: %w", err)
	}
	if len(reparsed.Par2Files) == 0 && len(reparsed.Par2Source) == 0 {
		return fmt.Errorf("re-parsed NZB has no PAR2/source file data for %s: %w", nzoID, ErrBackfillImpossible)
	}

	// The re-parse ran without the storage lock (it hits the network); the
	// write re-reads under it, so it neither re-creates a record deleted
	// meanwhile nor overwrites refs another writer stored.
	err = u.nzbStorage.Update(nzoID, func(cur *storage.NZB) (bool, error) {
		if len(cur.Par2Files) > 0 || len(cur.Par2Source) > 0 {
			return false, nil
		}
		cur.Par2Files = reparsed.Par2Files
		cur.Par2Source = reparsed.Par2Source
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("failed to save backfilled PAR2 refs: %w", err)
	}
	return nil
}

// SaveNZBPar2Match persists the posted-file -> PAR2 FileDesc match cache for
// nzoID (see storage.Par2MatchRef / NZB.Par2Match). Write-once: it is a
// no-op if refs is empty or a non-empty cache is already stored, so a later
// repair attempt never rewrites it. Concurrency matches BackfillPar2Refs -
// GetNZB/modify/AddNZB, safe here because at most one repair pass runs per
// nzbID at a time (see Par2Repair.running).
func (u *Usenet) SaveNZBPar2Match(nzoID string, refs []storage.Par2MatchRef) error {
	if len(refs) == 0 {
		return nil
	}
	err := u.nzbStorage.Update(nzoID, func(nzb *storage.NZB) (bool, error) {
		if len(nzb.Par2Match) > 0 {
			return false, nil
		}
		nzb.Par2Match = refs
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("failed to save par2 match cache: %w", err)
	}
	return nil
}

// StatSegments batch-STATs the given message IDs (header-only, no body
// download) across every configured provider and returns the per-ID result.
// A thin exported wrapper over the NNTP client's exhaustive
// BatchStatComplete, for repair-path callers that already hold a specific
// segment list (e.g. the PAR2 recovery-volume pre-census) rather than a
// whole file. Order of the returned slice is not guaranteed to match the
// input - callers should key on StatResult.MessageID.
func (u *Usenet) StatSegments(ctx context.Context, messageIDs []string) ([]nntp.StatResult, error) {
	if len(messageIDs) == 0 {
		return nil, nil
	}
	res, err := u.nntp.BatchStatComplete(ctx, messageIDs)
	if err != nil {
		return nil, err
	}
	return res.Results, nil
}

// StatFileHealth STATs every segment of nzoID/filename (header-only, no body
// download) and reports how many are confirmed dead - a genuine
// article-not-found across every configured provider, per
// nntp.IsArticleNotFoundError. Connection/protocol errors are not counted:
// they mean the segment couldn't be checked, not that it's gone.
//
// Unlike the overlay's padded-segment record - which only ever sees the
// fraction of a file actually fetched during the streaming import window -
// this checks the whole file deterministically. Used by the import STAT
// census gate (see Downloader.statImportGate).
//
// total is len(segments) whenever the segment list could be loaded, so a
// caller can still log a ratio when StatSegments itself errors (in which
// case that transport error is returned as-is with dead=0). A missing or
// segment-less NZB/file is a descriptive error with total=0.
func (u *Usenet) StatFileHealth(ctx context.Context, nzoID string, filename string) (dead int, total int, err error) {
	nzb, err := u.nzbStorage.GetNZB(nzoID)
	if err != nil {
		return 0, 0, fmt.Errorf("stat file health: failed to load NZB %s: %w", nzoID, err)
	}
	file := nzb.GetFileByName(filename)
	if file == nil || len(file.Segments) == 0 {
		return 0, 0, fmt.Errorf("stat file health: file %q has no segments in NZB %s", filename, nzoID)
	}

	msgIDs := make([]string, len(file.Segments))
	for i, seg := range file.Segments {
		msgIDs[i] = seg.MessageID
	}

	results, err := u.StatSegments(ctx, msgIDs)
	if err != nil {
		return 0, len(msgIDs), err
	}
	for _, r := range results {
		if !r.Available && nntp.IsArticleNotFoundError(r.Error) {
			dead++
		}
	}
	return dead, len(msgIDs), nil
}

func (u *Usenet) Delete(nzoID string) error {
	nzb, err := u.nzbStorage.GetNZBHeader(nzoID)
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

	// Readers still open on this NZB must stop serving it (see fsEntry.gone).
	u.retireReaders(nzoID)

	// Drop any recorded dead-segment/patch state for this NZB. Keyed by
	// nzbID (unique) rather than name, avoiding the name-twin hazard the DFS
	// cache has. Best-effort: an overlay cleanup failure shouldn't fail the
	// whole deletion, since the NZB record itself is already gone.
	if u.overlay != nil {
		// Reject first, as cleanupRejectedImport does: a fetch still draining
		// on an open reader would otherwise re-create the manifest via
		// RecordDead/Decide right after the RemoveAll below, leaving an
		// orphan record for an entry that no longer exists.
		u.overlay.MarkRejected(nzoID)
		if err := u.overlay.DeleteEntry(nzoID); err != nil {
			u.logger.Warn().Err(err).Str("nzb_id", nzoID).Msg("Failed to delete overlay entry")
		}
	}

	// Also drop any cached permanent failure under this nzbID. A re-grab
	// always mints a fresh nzbID, so this is mostly cheap insurance - but a
	// caller superseding-in-place (rare) must not have a fresh entry
	// inherit a stale short-circuit from the grab it's replacing.
	u.ClearFailedEntry(nzoID)
	return nil
}

// retireReaders marks every open reader of nzoID's files gone, and tears
// down those no stream holds right now; the rest go when idle.
func (u *Usenet) retireReaders(nzoID string) {
	if u.fs == nil || nzoID == "" {
		return
	}
	prefix := fsKey(nzoID, "")
	u.fs.Range(func(key string, entry *fsEntry) bool {
		if !strings.HasPrefix(key, prefix) {
			return true
		}
		entry.gone.Store(true)
		if entry.claimForCleanup() {
			u.fs.Delete(key)
			entry.cleanup()
		}
		return true
	})
}

// reviveNZB undoes Delete's fencing for an nzoID that is being added again:
// the overlay rejection, and any gone reader not yet reaped (one a stream
// still holds is left; that stream sees ErrEntryGone and the next open
// after it is reaped builds a fresh reader).
func (u *Usenet) reviveNZB(nzoID string) {
	if u.overlay != nil {
		u.overlay.ClearRejected(nzoID)
	}
	if u.fs == nil {
		return
	}
	prefix := fsKey(nzoID, "")
	u.fs.Range(func(key string, entry *fsEntry) bool {
		if strings.HasPrefix(key, prefix) && entry.gone.Load() && entry.claimForCleanup() {
			u.fs.Delete(key)
			entry.cleanup()
		}
		return true
	})
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
				// Skip this entry instead of aborting the whole scan. A single
				// poison file (e.g. a name so long that appending ".importing"
				// exceeds the filesystem limit) previously failed every refresh
				// and permanently blocked all other pending NZBs.
				u.logger.Error().Err(err).Str("name", name).Msg("Failed to claim NZB; skipping")
				continue
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
