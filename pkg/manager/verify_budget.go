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
	// verifyBudgetFloor is the smallest cap handed to any probe. A healthy
	// decode pass over a 3-4 GB episode consumes 45-75 MB, so this leaves
	// roughly 3-5x headroom before a well-behaved read could ever trip, and
	// keeps small files effectively unbounded.
	verifyBudgetFloor = 256 * 1024 * 1024

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
