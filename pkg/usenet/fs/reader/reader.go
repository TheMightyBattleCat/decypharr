package reader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/crypto"
	"github.com/sirrobot01/decypharr/internal/nntp"
)

// verificationFetchLogThreshold gates the "concurrent segment fetch blocked"
// debug line - only log when the foreground fetch in a verification read
// actually stalled (prefetch fell behind), not on every instant cache hit.
const verificationFetchLogThreshold = 150 * time.Millisecond

var decryptionBufPool = sync.Pool{}

func acquireDecryptionBuffer(size int) []byte {
	v := decryptionBufPool.Get()
	if v == nil {
		return make([]byte, size)
	}
	buf := v.([]byte)
	if cap(buf) < size {
		return make([]byte, size)
	}
	return buf[:size]
}

func releaseDecryptionBuffer(buf []byte) {
	decryptionBufPool.Put(buf)
}

// StreamingReader provides io.ReaderAt over NNTP segments with automatic
// caching, prefetching, and error recovery.
//
// Key features:
//   - Pin/Unpin pattern prevents the "chunk does not exist" race condition
//   - Disk caching with transparent re-download
//   - Request deduplication for concurrent reads of the same segment
//   - Background prefetching for smooth sequential reads
//   - AES-CBC decryption support for encrypted archives
//
// Usage:
//
//	reader, err := NewStreamingReader(ctx, client, segments, opts...)
//	defer reader.Close()
//	n, err := reader.ReadAt(buf, offset)
type StreamingReader struct {
	// Dependencies
	cache   *SegmentCache
	fetcher *SegmentFetcher
	config  Config

	// File metadata
	totalSize int64
	segCount  int

	// Encryption support
	encryption EncryptionConfig

	// Read position for io.Reader interface
	readOffset atomic.Int64

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	closed atomic.Bool
	logger zerolog.Logger

	// Stats
	stats *ReaderStats
}

// NewStreamingReader creates a new streaming reader for NNTP segments.
func NewStreamingReader(
	ctx context.Context,
	client *nntp.Client,
	segments []SegmentMeta,
	opts ...Option,
) (*StreamingReader, error) {
	if len(segments) == 0 {
		return nil, fmt.Errorf("no segments provided")
	}
	if client == nil {
		return nil, fmt.Errorf("NNTP client is required")
	}

	// Apply configuration options
	config := DefaultConfig()
	for _, opt := range opts {
		opt(&config)
	}

	ctx, cancel := context.WithCancel(ctx)
	logger := config.Logger

	stats := &ReaderStats{}

	// Create cache
	cache, err := NewSegmentCache(ctx, segments, config, stats, logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("create cache: %w", err)
	}

	// Create fetcher
	fetcher := NewSegmentFetcher(ctx, client, cache, config, stats, logger)

	sr := &StreamingReader{
		cache:     cache,
		fetcher:   fetcher,
		config:    config,
		totalSize: cache.TotalSize(),
		segCount:  cache.SegmentCount(),
		ctx:       ctx,
		cancel:    cancel,
		logger:    logger,
		stats:     stats,
	}

	return sr, nil
}

// NewStreamingReaderWithEncryption creates an encrypted reader.
func NewStreamingReaderWithEncryption(
	ctx context.Context,
	client *nntp.Client,
	segments []SegmentMeta,
	encConfig EncryptionConfig,
	opts ...Option,
) (*StreamingReader, error) {
	sr, err := NewStreamingReader(ctx, client, segments, opts...)
	if err != nil {
		return nil, err
	}
	sr.encryption = encConfig
	return sr, nil
}

// ReadAt implements io.ReaderAt with blocking semantics.
// Blocks until the requested byte range is available.
//
// THE CRITICAL PATH: Uses Pin/Unpin to prevent the race condition.
func (sr *StreamingReader) ReadAt(p []byte, off int64) (int, error) {
	return sr.ReadAtContext(context.Background(), p, off)
}

// ReadAtContext implements context-aware random reads. The context is carried
// into segment waits and NNTP fetches so DFS/FUSE read timeouts can cancel the
// underlying work instead of waiting for the reader lifetime to end.
func (sr *StreamingReader) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if sr.closed.Load() {
		return 0, io.ErrClosedPipe
	}

	if len(p) == 0 {
		return 0, nil
	}

	if off >= sr.totalSize {
		return 0, io.EOF
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	sr.stats.Reads.Add(1)

	if sr.encryption.Enabled {
		return sr.readAtEncrypted(ctx, p, off)
	}
	return sr.readAtPlain(ctx, p, off)
}

// readAtPlain handles non-encrypted reads.
func (sr *StreamingReader) readAtPlain(ctx context.Context, p []byte, off int64) (int, error) {
	// Clamp to file bounds
	readLen := int64(len(p))
	eofAfter := false
	if off+readLen > sr.totalSize {
		readLen = sr.totalSize - off
		eofAfter = true
	}

	// Determine which segments we need
	startSeg, endSeg := sr.cache.SegmentsForRange(off, readLen)

	// CRITICAL: Pin segments to prevent eviction during read
	sr.cache.PinRange(startSeg, endSeg)
	defer sr.cache.UnpinRange(startSeg, endSeg)

	// Queue prefetch for read-ahead (non-blocking). Skipped for verification
	// reads (ffprobe sweep/import checks, reconcileStickyFailed): the no-pad
	// marker rides only on this ctx, but prefetchOne runs on the fetcher's
	// lifetime context, so a dead article in the read-ahead window would be
	// padded and AutoEnqueue'd to the urgent PAR2 lane despite the caller
	// explicitly opting out of that. A seeky verification caller (ffprobe
	// jumps across decode windows) gets nothing useful from a sequential
	// read-ahead prediction anyway.
	if !paddingDisabled(ctx) {
		prefetchEnd := min(endSeg+sr.config.PrefetchAhead, sr.segCount-1)
		if prefetchEnd > endSeg {
			sr.fetcher.QueuePrefetchRange(endSeg+1, prefetchEnd)
		}
	}

	// Ensure all required segments are available (may block for downloads).
	// Verification reads (padding disabled) run with no prefetch, so a
	// multi-segment span is fetched via a worker pool; single-segment spans
	// and normal playback keep the serial path.
	var ensureErr error
	if paddingDisabled(ctx) && endSeg > startSeg {
		fetchStart := time.Now()
		ensureErr = sr.fetcher.EnsureSegmentsConcurrent(ctx, startSeg, endSeg)
		// Only log the slow ones. With the verification prefetch pipelining
		// ahead (Usenet.verificationPrefetch) the common case is an instant
		// cache hit, one line per readAtPlain call - pure noise. A fetch that
		// actually blocked here means the prefetch fell behind.
		if d := time.Since(fetchStart); d >= verificationFetchLogThreshold {
			sr.logger.Debug().
				Int("start_seg", startSeg).
				Int("end_seg", endSeg).
				Int("segments", endSeg-startSeg+1).
				Dur("fetch_dur", d).
				Msg("verification read: concurrent segment fetch blocked")
		}
	} else {
		ensureErr = sr.fetcher.EnsureSegments(ctx, startSeg, endSeg)
	}
	if ensureErr != nil {
		sr.stats.ReadErrors.Add(1)
		return 0, ensureErr
	}

	// Read data from cache
	n, err := sr.readFromCache(ctx, p[:readLen], off, startSeg, endSeg)
	// A short read (readFromCache stopping at a segment stored shorter than
	// its slot) is not a read ERROR: the bytes it did return are valid, and
	// the caller is expected to re-issue from off+n. Fall through so those
	// bytes still count towards BytesRead and MarkConsumed, and let the
	// io.ErrUnexpectedEOF ride out with them.
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		sr.stats.ReadErrors.Add(1)
		return n, err
	}

	sr.stats.BytesRead.Add(int64(n))

	// Tell the cache what we actually delivered so its sliding-window
	// sweeper can advance the back-window cutoff. Skip on zero-byte reads
	// (probe, short EOF) to avoid moving the high-water mark spuriously.
	if n > 0 {
		sr.cache.MarkConsumed(off, int64(n))
	}

	if eofAfter && int64(n) == readLen {
		return n, io.EOF
	}
	return n, err
}

// readFromCache reads data from the cache, handling segment boundaries.
//
// Uses ReadRangeInto so each pread fetches only the bytes the caller actually
// needs from that segment — no scratch buffer, no read amplification.
// Previously the code read entire segments (~750 KB) even for 4 KB reads,
// which filled the kernel page cache with mostly-unused data and caused
// progressive performance degradation on large files.
func (sr *StreamingReader) readFromCache(ctx context.Context, p []byte, off int64, startSeg, endSeg int) (int, error) {
	totalRead := 0

	for segIdx := startSeg; segIdx <= endSeg; segIdx++ {
		// Wait for segment to be ready
		if err := sr.cache.WaitForSegment(ctx, segIdx); err != nil {
			return totalRead, err
		}

		// Calculate the intersection of the caller's range with this segment.
		segStart := sr.cache.SegmentOffset(segIdx)
		segEnd := sr.cache.SegmentOffset(segIdx + 1)

		readStart := max(off, segStart)
		readEnd := min(off+int64(len(p)), segEnd)

		if readStart >= readEnd {
			continue
		}

		outOffset := readStart - off
		segDataOffset := readStart - segStart
		copyLen := readEnd - readStart

		// Read only the needed slice directly into the output buffer.
		// No intermediate scratch buffer — zero extra allocation, zero amplification.
		n, ok := sr.cache.ReadRangeInto(segIdx, segDataOffset, copyLen, p[outOffset:outOffset+copyLen])
		if !ok {
			// The slot says OnDisk but the bytes aren't readable. Force the
			// state back to Empty before re-fetching: otherwise Fetch's
			// OnDisk fast path would short-circuit and we'd loop straight to
			// the "still missing" error, leaving the segment wedged. This is
			// the self-heal for any segment that ends up OnDisk-but-empty.
			sr.logger.Warn().Int("segment", segIdx).Msg("segment data missing after wait, re-fetching")
			sr.cache.invalidateForRefetch(segIdx)
			if err := sr.fetcher.Fetch(ctx, segIdx); err != nil {
				return totalRead, fmt.Errorf("re-fetch segment %d: %w", segIdx, err)
			}
			n, ok = sr.cache.ReadRangeInto(segIdx, segDataOffset, copyLen, p[outOffset:outOffset+copyLen])
			if !ok {
				return totalRead, fmt.Errorf("segment %d still missing after re-fetch", segIdx)
			}
		}

		totalRead += n

		// ReadRangeInto clamps to the segment's RECORDED length and still
		// reports ok - a segment stored shorter than its slot (a truncated
		// body committed by bufferStreamWriter.Finalize, which commits on
		// written > 0 without checking it filled maxBytes) hands back fewer
		// bytes than this slice asked for.
		//
		// Everything after those bytes in p was never written by this
		// iteration. Carrying on to the next segment would write ITS bytes at
		// their own outOffset, leaving a stale gap in the middle of p while
		// totalRead - a plain sum - implies a contiguous run. Every
		// io.ReaderAt caller reads n as "the first n bytes are valid", so
		// that combination hands back a buffer whose contents don't match its
		// own length.
		//
		// Stop at the hole and return the contiguous prefix. Deliberately no
		// re-fetch here: these segments are deterministically short (measured
		// byte-identical across separate runs), so retrying them would only
		// build a retry storm. io.ErrUnexpectedEOF is this codebase's existing
		// short-read signal (see limitedReaderAt above, and skippableError in
		// the hanwen backend, which already unwraps it to a partial-data
		// success) and it lets persistDurableRanges tell a hole apart from a
		// segment that is merely cold.
		if int64(n) < copyLen {
			sr.logger.Debug().
				Int("segment", segIdx).
				Int("got", n).
				Int64("want", copyLen).
				Int64("segment_stored_size", sr.cache.SegmentDataSize(segIdx)).
				Msg("segment is short of its slot; returning contiguous prefix")
			return totalRead, io.ErrUnexpectedEOF
		}
	}

	return totalRead, nil
}

// readAtEncrypted handles AES-CBC encrypted reads.
func (sr *StreamingReader) readAtEncrypted(ctx context.Context, p []byte, off int64) (int, error) {
	// AES-CBC requires block-aligned reads
	alignedStart := (off / crypto.BlockSize) * crypto.BlockSize

	// Determine actual end
	reqEnd := min(off+int64(len(p)), sr.totalSize)

	// Round up to next block
	alignedEnd := reqEnd
	if remainder := alignedEnd % crypto.BlockSize; remainder != 0 {
		alignedEnd += crypto.BlockSize - remainder
	}

	bufLen := alignedEnd - alignedStart
	buf := acquireDecryptionBuffer(int(bufLen))
	defer releaseDecryptionBuffer(buf)

	// Read aligned data
	n, err := sr.readAtPlain(ctx, buf, alignedStart)
	if n > 0 && len(sr.encryption.Key) > 0 {
		// Handle partial last block
		decryptedLen := int64(n)
		if remainder := decryptedLen % crypto.BlockSize; remainder != 0 {
			// Pad with zeros for decryption
			if int64(len(buf)) >= decryptedLen+(crypto.BlockSize-remainder) {
				for i := int64(0); i < crypto.BlockSize-remainder; i++ {
					buf[decryptedLen+i] = 0
				}
				decryptedLen += crypto.BlockSize - remainder
			}
		}

		// Decrypt
		if decryptErr := sr.decryptInPlace(ctx, buf[:decryptedLen], alignedStart); decryptErr != nil {
			return 0, fmt.Errorf("decrypt: %w", decryptErr)
		}
	}

	if err != nil && err != io.EOF {
		return 0, err
	}

	// Copy requested data to p
	validDataEnd := min(int64(n), reqEnd-alignedStart)

	startOffset := off - alignedStart
	if startOffset >= validDataEnd {
		return 0, err
	}

	copied := copy(p, buf[startOffset:validDataEnd])

	if err == io.EOF && copied == len(p) {
		return copied, nil
	}

	return copied, err
}

// decryptInPlace decrypts data using AES-256-CBC.
func (sr *StreamingReader) decryptInPlace(ctx context.Context, data []byte, offset int64) error {
	if len(data) == 0 {
		return nil
	}

	if len(data)%crypto.BlockSize != 0 {
		return nil // Not block-aligned, can't decrypt
	}

	iv, err := sr.computeIVForOffset(ctx, offset)
	if err != nil {
		return err
	}

	return crypto.DecryptBlock(data, sr.encryption.Key, iv)
}

// computeIVForOffset computes the IV for decrypting at a given offset.
func (sr *StreamingReader) computeIVForOffset(ctx context.Context, offset int64) ([]byte, error) {
	blockOffset := (offset / crypto.BlockSize) * crypto.BlockSize

	if blockOffset == 0 {
		// First block uses original IV
		iv := make([]byte, crypto.BlockSize)
		copy(iv, sr.encryption.IV)
		return iv, nil
	}

	// For other blocks, IV is the previous ciphertext block
	prevBlockOffset := blockOffset - crypto.BlockSize
	iv := make([]byte, crypto.BlockSize)

	// Read previous block (raw, not decrypted)
	n, err := sr.readAtPlain(ctx, iv, prevBlockOffset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("read IV block at %d: %w", prevBlockOffset, err)
	}
	if n < crypto.BlockSize {
		return nil, fmt.Errorf("short read for IV: got %d, need %d", n, crypto.BlockSize)
	}

	return iv, nil
}

// FetchRange aggressively fetches every segment covering [off, off+length)
// into the cache using up to concurrency parallel fetch goroutines - distinct
// from (and typically much higher than) the reader's normal steady-state
// prefetch pool sized by Config.MaxConnections. Intended for a deliberate
// read-ahead burst (see pkg/manager.Precache), not the per-read sliding
// prefetch window Prefetch/readAtPlain drive during ordinary streaming.
//
// Blocks until every segment in range has been attempted. An individual
// segment's permanent failure (including a confirmed-missing article the
// overlay padded, or recorded for later PAR2 repair - see
// SegmentFetcher.handleConfirmedMissing) does not abort the rest of the
// range; only ctx cancellation does, and is returned once every already-
// in-flight fetch drains.
func (sr *StreamingReader) FetchRange(ctx context.Context, off, length int64, concurrency int) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if sr.closed.Load() {
		return io.ErrClosedPipe
	}
	if off < 0 {
		off = 0
	}
	if off >= sr.totalSize || length <= 0 {
		return nil
	}
	if off+length > sr.totalSize {
		length = sr.totalSize - off
	}
	if concurrency < 1 {
		concurrency = 1
	}

	startSeg, endSeg := sr.cache.SegmentsForRange(off, length)

	segCh := make(chan int)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error

	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for segIdx := range segCh {
				err := sr.fetcher.Fetch(ctx, segIdx)
				if err == nil {
					continue
				}
				if ctxErr := ctx.Err(); ctxErr != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = ctxErr
					}
					errMu.Unlock()
				}
				// Any other error (including permanent article-not-found)
				// has already been recorded/padded by the normal fetch path
				// where applicable; read-ahead keeps going for the rest of
				// the range rather than aborting on one bad segment.
			}
		}()
	}

sendLoop:
	for segIdx := startSeg; segIdx <= endSeg; segIdx++ {
		select {
		case segCh <- segIdx:
		case <-ctx.Done():
			errMu.Lock()
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			errMu.Unlock()
			break sendLoop
		}
	}
	close(segCh)
	wg.Wait()

	return firstErr
}

// FetchRangeWindowed fetches every segment covering [base, base+total) into the
// cache using `concurrency` persistent workers. Unlike FetchRange it has no
// per-batch barrier: each worker claims the next segment the instant it finishes
// one, so a single slow segment (retry ladder, slow provider) stalls only its
// own worker while the rest keep the connections full - the behaviour the normal
// prefetch workers have and the ffprobe verification read previously lacked.
//
// horizon() returns a byte offset measured from base; a worker will not fetch a
// segment whose start is past that offset, and blocks (ctx-aware) until horizon
// advances. A caller that consumes little - a seek, ffprobe's moov probe - keeps
// horizon low and fetches little; a negative horizon fetches nothing at all (the
// ramp before a scan proves itself a sustained forward read). Segment offsets
// are absolute in the file; the horizon comparison converts to the base frame.
//
// Non-ctx fetch errors are swallowed - a confirmed-missing article stays
// StateFailed for the foreground read to surface, exactly as FetchRange does.
// Returns on ctx cancellation or once every segment in range has been attempted.
func (sr *StreamingReader) FetchRangeWindowed(ctx context.Context, base, total int64, concurrency int, horizon func() int64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if sr.closed.Load() {
		return io.ErrClosedPipe
	}
	if horizon == nil {
		return nil
	}
	if base < 0 {
		base = 0
	}
	if base >= sr.totalSize || total <= 0 {
		return nil
	}
	if base+total > sr.totalSize {
		total = sr.totalSize - base
	}
	if concurrency < 1 {
		concurrency = 1
	}

	startSeg, endSeg := sr.cache.SegmentsForRange(base, total)

	var (
		nextSeg  atomic.Int64
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
	)
	nextSeg.Store(int64(startSeg))
	recordCtxErr := func() {
		if e := ctx.Err(); e != nil {
			errMu.Lock()
			if firstErr == nil {
				firstErr = e
			}
			errMu.Unlock()
		}
	}

	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					recordCtxErr()
					return
				}
				seg := int(nextSeg.Add(1)) - 1
				if seg > endSeg {
					return
				}
				segStart := sr.cache.SegmentOffset(seg) - base
				for segStart > horizon() {
					select {
					case <-ctx.Done():
						recordCtxErr()
						return
					case <-time.After(15 * time.Millisecond):
					}
				}
				if err := sr.fetcher.Fetch(ctx, seg); err != nil && ctx.Err() != nil {
					recordCtxErr()
					return
				}
			}
		}()
	}

	wg.Wait()
	return firstErr
}

// Prefetch triggers segment downloads for the given byte range without blocking.
func (sr *StreamingReader) Prefetch(ctx context.Context, off, length int64) {
	if sr.closed.Load() {
		return
	}
	if ctx != nil && ctx.Err() != nil {
		return
	}
	if off >= sr.totalSize {
		return
	}

	// Clamp to file bounds
	if off+length > sr.totalSize {
		length = sr.totalSize - off
	}

	startSeg, endSeg := sr.cache.SegmentsForRange(off, length)
	sr.fetcher.QueuePrefetchRange(startSeg, endSeg)
}

// Read implements io.Reader using ReadAt with tracked position.
func (sr *StreamingReader) Read(p []byte) (int, error) {
	if sr.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}

	offset := sr.readOffset.Load()
	if offset >= sr.totalSize {
		return 0, io.EOF
	}

	n, err := sr.ReadAt(p, offset)
	if n > 0 {
		sr.readOffset.Add(int64(n))
	}
	return n, err
}

// Seek implements io.Seeker.
func (sr *StreamingReader) Seek(offset int64, whence int) (int64, error) {
	if sr.closed.Load() {
		return 0, io.ErrClosedPipe
	}

	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = sr.readOffset.Load() + offset
	case io.SeekEnd:
		newPos = sr.totalSize + offset
	default:
		return 0, fmt.Errorf("invalid seek whence %d", whence)
	}

	if newPos < 0 {
		return 0, fmt.Errorf("seek before beginning")
	}
	if newPos > sr.totalSize {
		newPos = sr.totalSize
	}

	sr.readOffset.Store(newPos)
	return newPos, nil
}

// Size returns the total size of the virtual file.
func (sr *StreamingReader) Size() int64 {
	return sr.totalSize
}

// Stats returns a snapshot of current statistics.
func (sr *StreamingReader) Stats() map[string]int64 {
	return sr.stats.Snapshot()
}

// Close releases all resources.
func (sr *StreamingReader) Close() error {
	if sr.closed.Swap(true) {
		return nil
	}

	sr.cancel()

	// Close fetcher first (stops downloads)
	sr.fetcher.Close()

	// Then close cache (cleans up files)
	return sr.cache.Close()
}

// Pool manages a pool of readers for efficient resource sharing.
// This is useful when multiple files need to be read concurrently.
type Pool struct {
	client *nntp.Client
	config Config

	readers sync.Map // map[string]*StreamingReader
}

// GetReader returns a reader for the given segments, creating one if needed.
func (rp *Pool) GetReader(
	ctx context.Context,
	key string,
	segments []SegmentMeta,
	encryption EncryptionConfig,
) (*StreamingReader, error) {
	// Check if reader exists
	if v, ok := rp.readers.Load(key); ok {
		return v.(*StreamingReader), nil
	}

	// Create new reader
	var reader *StreamingReader
	var err error
	if encryption.Enabled {
		reader, err = NewStreamingReaderWithEncryption(
			ctx, rp.client, segments, encryption,
			WithMaxDisk(rp.config.MaxDisk),
			WithMaxConnections(rp.config.MaxConnections),
			WithPrefetchAhead(rp.config.PrefetchAhead),
		)
	} else {
		reader, err = NewStreamingReader(
			ctx, rp.client, segments,
			WithMaxDisk(rp.config.MaxDisk),
			WithMaxConnections(rp.config.MaxConnections),
			WithPrefetchAhead(rp.config.PrefetchAhead),
		)
	}
	if err != nil {
		return nil, err
	}

	// Store (race-safe: LoadOrStore)
	actual, loaded := rp.readers.LoadOrStore(key, reader)
	if loaded {
		// Another goroutine beat us, close ours
		_ = reader.Close()
		return actual.(*StreamingReader), nil
	}

	return reader, nil
}

// RemoveReader closes and removes a reader from the pool.
func (rp *Pool) RemoveReader(key string) {
	if v, ok := rp.readers.LoadAndDelete(key); ok {
		_ = v.(*StreamingReader).Close()
	}
}

// Close closes all readers in the pool.
func (rp *Pool) Close() {
	rp.readers.Range(func(key, value any) bool {
		_ = value.(*StreamingReader).Close()
		rp.readers.Delete(key)
		return true
	})
}
