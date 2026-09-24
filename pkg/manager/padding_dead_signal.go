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
//
// Two probes of one file can overlap (a manual recheck during the sweep, an
// import-gate retry). Keyed by file alone, the second probe's signal replaced
// the first's, so the first probe's later reads tripped nothing and it could
// pass a file whose dead segment it had read. So the reads get a per-file hub
// signal that trips every probe registered for the file; a probe that
// registers after the hub tripped starts tripped, since the dead segment is a
// fact about the file.
var (
	deadSignalMu       sync.Mutex
	deadSignalRegistry = map[string]*deadSignalShare{}
)

type deadSignalShare struct {
	hub     *DeadSegmentSignal
	members map[*DeadSegmentSignal]struct{}
}

func deadSignalKey(infoHash, fileName string) string {
	return infoHash + "\x00" + fileName
}

// registerDeadSignal publishes sig for the duration of a verification read.
// No-op if sig is nil or infoHash is empty (nothing to key on).
func registerDeadSignal(infoHash, fileName string, sig *DeadSegmentSignal) {
	if sig == nil || infoHash == "" {
		return
	}
	key := deadSignalKey(infoHash, fileName)
	deadSignalMu.Lock()
	defer deadSignalMu.Unlock()
	share := deadSignalRegistry[key]
	if share == nil {
		share = &deadSignalShare{members: make(map[*DeadSegmentSignal]struct{})}
		share.hub = usenet.NewForwardingDeadSegmentSignal(func() {
			deadSignalMu.Lock()
			defer deadSignalMu.Unlock()
			for m := range share.members {
				m.Trip()
			}
		})
		deadSignalRegistry[key] = share
	}
	share.members[sig] = struct{}{}
	if share.hub.Detected() {
		sig.Trip()
	}
}

// unregisterDeadSignal removes sig once the read finishes, leaving any other
// probe of the same file registered.
func unregisterDeadSignal(infoHash, fileName string, sig *DeadSegmentSignal) {
	if sig == nil || infoHash == "" {
		return
	}
	key := deadSignalKey(infoHash, fileName)
	deadSignalMu.Lock()
	defer deadSignalMu.Unlock()
	share := deadSignalRegistry[key]
	if share == nil {
		return
	}
	delete(share.members, sig)
	if len(share.members) == 0 {
		delete(deadSignalRegistry, key)
	}
}

// DeadSignalForVerificationRead returns the signal a verification read of
// infoHash/fileName carries - tripping it trips every probe registered for
// the file - or nil when none is. Called by the WebDAV handler on an
// internal-bearer (ffprobe verification) read.
func DeadSignalForVerificationRead(infoHash, fileName string) *DeadSegmentSignal {
	deadSignalMu.Lock()
	defer deadSignalMu.Unlock()
	if share := deadSignalRegistry[deadSignalKey(infoHash, fileName)]; share != nil {
		return share.hub
	}
	return nil
}

// sweepVerifyRegistry marks the files the repair sweep is probing, joined to
// the WebDAV handler the same way deadSignalRegistry is. A sweep's
// verification read is background work, so the handler marks it to narrow its
// prefetch while playback of another file stalls; an import's check, which
// never registers here, keeps full width.
var sweepVerifyRegistry sync.Map // map[string]*struct{}

// registerSweepVerification marks infoHash/fileName as probed by the sweep
// and returns the call that unmarks it.
func registerSweepVerification(infoHash, fileName string) func() {
	if infoHash == "" {
		return func() {}
	}
	key, tok := deadSignalKey(infoHash, fileName), new(struct{})
	sweepVerifyRegistry.Store(key, tok)
	return func() { sweepVerifyRegistry.CompareAndDelete(key, tok) }
}

// ContextForVerificationReadOf is ContextForVerificationRead for a read of
// infoHash/fileName, adding the sweep's yield mark when the sweep is the one
// probing it.
func ContextForVerificationReadOf(ctx context.Context, infoHash, fileName string) context.Context {
	ctx = ContextForVerificationRead(ctx)
	if _, ok := sweepVerifyRegistry.Load(deadSignalKey(infoHash, fileName)); ok {
		ctx = usenet.ContextForYieldingVerification(ctx)
	}
	return ctx
}

// ContextWithVerifyBudget attaches b to ctx for the metered verification read
// - see usenet.ContextWithVerifyBudget.
func ContextWithVerifyBudget(ctx context.Context, b *VerifyBudget) context.Context {
	return usenet.ContextWithVerifyBudget(ctx, b)
}
