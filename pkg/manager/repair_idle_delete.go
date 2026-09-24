package manager

import (
	"context"
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
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
	// Persisted so a restart during the wait still deletes it (see
	// resumeIdleDeletes): the Arr file row and the broken record are gone
	// by now, so nothing else would ever come back for it.
	if st := r.manager.storage; st != nil {
		if err := st.SavePendingDelete(&storage.PendingDelete{InfoHash: hash, Name: name, Since: time.Now()}); err != nil {
			r.logger.Warn().Err(err).Str("entry", name).Msg("Repair: could not persist a deferred delete")
		}
	}
	r.logger.Info().Str("entry", name).Str("infohash", hash).
		Msg("Repair: entry is being streamed; deleting it once the stream closes")
	go r.awaitIdleDelete(hash, name)
	return false, nil
}

// resumeIdleDeletes replays the deferred deletes a restart interrupted: each
// still-stored entry is deleted now, or once its stream closes.
func (r *Repair) resumeIdleDeletes() {
	st := r.manager.storage
	if st == nil {
		return
	}
	var pending []*storage.PendingDelete
	_ = st.ForEachPendingDelete(func(pd *storage.PendingDelete) { pending = append(pending, pd) })
	for _, pd := range pending {
		if e, err := r.manager.GetEntry(pd.InfoHash); err != nil || e == nil {
			_ = st.DeletePendingDelete(pd.InfoHash) // already gone
			continue
		}
		r.logger.Info().Str("entry", pd.Name).Str("infohash", pd.InfoHash).
			Msg("Repair: resuming a deferred delete interrupted by a restart")
		if deleted, err := r.deleteEntryWhenIdle(pd.InfoHash, pd.Name); err != nil {
			r.logger.Warn().Err(err).Str("entry", pd.Name).Msg("Repair: resumed deferred delete failed")
		} else if deleted {
			_ = st.DeletePendingDelete(pd.InfoHash)
		}
	}
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
		return // kept persisted: the next start retries it
	}
	if st := r.manager.storage; st != nil {
		_ = st.DeletePendingDelete(hash)
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
