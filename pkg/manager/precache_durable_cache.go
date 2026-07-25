package manager

// dfsCacheRangeWriter is satisfied by the DFS mount's manager.MountManager
// implementation (pkg/mount/dfs.Manager) - the write-side mirror of
// dfsCacheRangeReader (see par2_cache_source.go for why this is a
// type-asserted interface rather than a direct import: pkg/mount/dfs
// already imports this package via *manager.Manager, so a direct import
// back would cycle). A failed assertion (rclone mode, no mount, mount not
// ready) just means there's nowhere to durably persist pre-cached bytes
// this run - the ephemeral per-stream SegmentCache still serves the
// read-ahead/damage-detection pass exactly as before this existed.
type dfsCacheRangeWriter interface {
	// WriteCachedRange durably writes p at [off, off+len(p)) into filename's
	// cache item under entryName, creating it (sized by fileSize) if it
	// doesn't exist yet. Pure disk write: no fetch, no padding, no Stream,
	// no Downloaders - callers must only ever pass bytes already known
	// correct (see Precache.persistCleanRanges).
	WriteCachedRange(entryName, filename string, fileSize int64, p []byte, off int64) error
}

// cacheWriter resolves the DFS write seam via the same MountManager()
// type-assertion dfsCacheRangeReader uses. nil (not an error) when no DFS
// mount is available - callers treat that as "nothing to durably write to
// this run" and fall back to their pre-existing behavior.
func (p *Precache) cacheWriter() dfsCacheRangeWriter {
	mgr := p.manager.MountManager()
	if mgr == nil {
		return nil
	}
	writer, _ := mgr.(dfsCacheRangeWriter)
	return writer
}
