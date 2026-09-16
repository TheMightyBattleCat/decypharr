package manager

import (
	"sync"

	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// VerifyBudget caps the bytes one ffprobe verification may pull for a single
// file - see usenet/fs/reader.VerifyBudget for the measurements behind it.
type VerifyBudget = usenet.VerifyBudget

// NewVerifyBudget returns a budget of limit bytes, or nil (unbounded) when
// limit <= 0 - mirrors NewDeadSegmentSignal for callers in this package.
func NewVerifyBudget(limit int64) *VerifyBudget { return usenet.NewVerifyBudget(limit) }

const (
	// verifyBudgetFloor is the smallest cap handed to any probe. The budget
	// is charged per read of the verification path's 4 MiB copy buffer, and
	// every range request ffprobe opens costs at least one such read, fetched
	// by the prefetch whether or not ffprobe consumes it. Measured on
	// a production install (1,513 healthy 15-window spreads, 2026-09-14..16): 3.7 MiB per
	// read, p50 38 reads / 140 MiB, p99 57 / 208 MiB, max 416 MiB. At the old
	// 256 MiB floor 40 spreads were cut, every one a file of 2.2 GB or less;
	// an episode whose windows need ~65 range requests (Fear Light & Clocks
	// S04E02, 253 MB, measured 46 requests reading 70 MB) could never pass.
	// 512 MiB clears the largest healthy pass, and a typical pass plus the
	// one retry checkConfirmed runs on the same budget after a broken verdict
	// (a retry after a 400 MiB pass is still cut, which ends inconclusive,
	// never broken). It still cuts a runaway read of a file over ~0.5 GB.
	//
	// This raises the cliff rather than removing its cause: a read is charged
	// a whole buffer however little of it ffprobe consumes (the 253 MB episode
	// above: ~70 MB consumed, ~260 MiB charged), so a file needing ~130 range
	// requests is still cut. Charging closer to what ffprobe pulls (a smaller
	// first read per request, ramping to the buffer size) is not done.
	verifyBudgetFloor = 512 * 1024 * 1024

	// verifyBudgetDivisor sets the cap at fileBytes/8 (12.5%) once that
	// exceeds the floor. Measured healthy probes land at 1.4-2.3% of the
	// file, so this is ~6-9x a good read - loose enough that tripping it
	// means something genuinely pathological, tight enough that the runaway
	// case (87-96% of the file, repeatedly) is cut by roughly 7x.
	verifyBudgetDivisor = 8
)

// verifyBudgetFor sizes a budget for a file of fileBytes. Returns nil - i.e.
// unbounded, exactly the pre-budget behaviour - when the size is unknown,
// rather than risk capping a read we cannot reason about.
func verifyBudgetFor(fileBytes int64) *VerifyBudget {
	if fileBytes <= 0 {
		return nil
	}
	limit := fileBytes / verifyBudgetDivisor
	if limit < verifyBudgetFloor {
		limit = verifyBudgetFloor
	}
	return usenet.NewVerifyBudget(limit)
}

// verifyBudgetRegistry bridges an in-flight ffprobe verification to the WebDAV
// handler serving it, for exactly the reason deadSignalRegistry exists: the
// probe caller and the read path are joined only by an HTTP request to the
// in-process WebDAV endpoint, so a context value set by the caller never
// reaches the reader. The caller registers a *VerifyBudget keyed by
// (infoHash, fileName) for the life of its probe; the handler looks it up on
// an internal-bearer read and attaches it to the request context the metered
// reader sees (see webdav.handleDownload).
var verifyBudgetRegistry sync.Map // map[string]*VerifyBudget

// registerVerifyBudget publishes b for the duration of a verification.
// No-op if b is nil or infoHash is empty (nothing to key on).
func registerVerifyBudget(infoHash, fileName string, b *VerifyBudget) {
	if b == nil || infoHash == "" {
		return
	}
	verifyBudgetRegistry.Store(deadSignalKey(infoHash, fileName), b)
}

// unregisterVerifyBudget removes b once the verification finishes.
// CompareAndDelete so a concurrent probe of the same file that registered its
// own budget is left untouched.
func unregisterVerifyBudget(infoHash, fileName string, b *VerifyBudget) {
	if b == nil || infoHash == "" {
		return
	}
	verifyBudgetRegistry.CompareAndDelete(deadSignalKey(infoHash, fileName), b)
}

// VerifyBudgetForVerificationRead returns the budget a range request for
// infoHash/fileName should meter against - the phase the probe's checker has
// open (see VerifyBudget.BeginPhase), else the file budget the caller
// registered - or nil when no verification is registered. Called by the WebDAV
// handler on every internal-bearer (ffprobe verification) request, so each
// request picks up whichever phase is open when it starts.
func VerifyBudgetForVerificationRead(infoHash, fileName string) *VerifyBudget {
	v, _ := verifyBudgetRegistry.Load(deadSignalKey(infoHash, fileName))
	b, _ := v.(*VerifyBudget)
	return b.ForRequest()
}
