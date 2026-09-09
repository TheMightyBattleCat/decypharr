package manager

import (
	"context"
	"sync"

	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// DeadSegmentSignal re-exports usenet.DeadSegmentSignal (ultimately
// reader.DeadSegmentSignal) so callers in this package and the WebDAV handler
// don't have to reach into the reader package directly.
type DeadSegmentSignal = usenet.DeadSegmentSignal

// NewDeadSegmentSignal returns a fresh, untripped signal.
func NewDeadSegmentSignal() *DeadSegmentSignal { return usenet.NewDeadSegmentSignal() }

// ContextWithDeadSignal attaches sig to ctx for the segment fetcher - see
// usenet.ContextWithDeadSignal.
func ContextWithDeadSignal(ctx context.Context, sig *DeadSegmentSignal) context.Context {
	return usenet.ContextWithDeadSignal(ctx, sig)
}

// deadSignalRegistry bridges an in-flight ffprobe verification read - running
// in the probe caller's goroutine (repair_sweep.probeFile or
// downloader.ffprobeImportGate) - to the fetcher goroutine that actually
// serves it. The two are joined only by an HTTP request to the in-process
// WebDAV endpoint, so a context value the caller sets never reaches the
// fetcher. Instead the caller registers a *DeadSegmentSignal keyed by
// (infoHash, fileName) for the life of its probe, and the WebDAV handler looks
// it up when it recognises the internal bearer token and attaches it to the
// request context the fetcher sees (see webdav.handleDownload). The fetcher
// trips it on a confirmed-dead segment; the caller then reads Detected().
var deadSignalRegistry sync.Map // map[string]*DeadSegmentSignal

func deadSignalKey(infoHash, fileName string) string {
	return infoHash + "\x00" + fileName
}

// registerDeadSignal publishes sig for the duration of a verification read.
// No-op if sig is nil or infoHash is empty (nothing to key on).
func registerDeadSignal(infoHash, fileName string, sig *DeadSegmentSignal) {
	if sig == nil || infoHash == "" {
		return
	}
	deadSignalRegistry.Store(deadSignalKey(infoHash, fileName), sig)
}

// unregisterDeadSignal removes sig once the read finishes. CompareAndDelete so
// a concurrent probe of the same file that registered its own signal is left
// untouched.
func unregisterDeadSignal(infoHash, fileName string, sig *DeadSegmentSignal) {
	if sig == nil || infoHash == "" {
		return
	}
	deadSignalRegistry.CompareAndDelete(deadSignalKey(infoHash, fileName), sig)
}

// DeadSignalForVerificationRead returns the signal a probe caller registered
// for infoHash/fileName, or nil. Called by the WebDAV handler on an
// internal-bearer (ffprobe verification) read.
func DeadSignalForVerificationRead(infoHash, fileName string) *DeadSegmentSignal {
	v, _ := deadSignalRegistry.Load(deadSignalKey(infoHash, fileName))
	sig, _ := v.(*DeadSegmentSignal)
	return sig
}

// ContextWithVerifyBudget attaches b to ctx for the metered verification read
// - see usenet.ContextWithVerifyBudget.
func ContextWithVerifyBudget(ctx context.Context, b *VerifyBudget) context.Context {
	return usenet.ContextWithVerifyBudget(ctx, b)
}
