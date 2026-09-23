package manager

import (
	"context"
	"time"
)

var (
	// idleDeletePoll is how often a deferred delete checks whether the
	// entry's stream has closed.
	idleDeletePoll = 30 * time.Second

	// idleDeleteMaxWait bounds the wait, in case a stream record is never
	// cleared; the entry is then deleted anyway.
	idleDeleteMaxWait = 12 * time.Hour
)

// deleteEntryWhenIdle deletes a re-grabbed or superseded entry - now if no
// client is streaming it, otherwise once the stream closes. deleted reports
// whether it was deleted now. Deleting under a viewer ends their stream: the
// entry's readers stop serving it (see usenet.Delete), while the Arr having
// removed the file's symlink does not touch a handle already open. The
// replacement is found by name meanwhile, so waiting costs nothing but the
// old grab's disk and records a little longer.
func (r *Repair) deleteEntryWhenIdle(hash, name string) (deleted bool, err error) {
	if !r.manager.Streaming(hash, "") {
		return true, r.deleteNow(hash)
	}
	if _, pending := r.idleDeletes.LoadOrStore(hash, struct{}{}); pending {
		return false, nil
	}
	r.logger.Info().Str("entry", name).Str("infohash", hash).
		Msg("Repair: entry is being streamed; deleting it once the stream closes")
	go r.awaitIdleDelete(hash, name)
	return false, nil
}

func (r *Repair) awaitIdleDelete(hash, name string) {
	defer r.idleDeletes.Delete(hash)
	ctx := r.parentCtx
	if ctx == nil {
		ctx = context.Background()
	}
	deadline := time.Now().Add(idleDeleteMaxWait)
	for r.manager.Streaming(hash, "") && time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(idleDeletePoll):
		}
	}
	if err := r.deleteNow(hash); err != nil {
		r.logger.Warn().Err(err).Str("entry", name).Str("infohash", hash).Msg("Repair: deferred delete failed")
		return
	}
	r.logger.Info().Str("entry", name).Str("infohash", hash).Msg("Repair: deleted entry after its stream closed")
}

// deleteNow is Manager.DeleteEntry, or r.deleteEntryFn in tests.
func (r *Repair) deleteNow(hash string) error {
	if r.deleteEntryFn != nil {
		return r.deleteEntryFn(hash)
	}
	return r.manager.DeleteEntry(hash, true)
}
