package reader

import (
	"sync"

	"github.com/sirrobot01/decypharr/internal/buffer"
	"github.com/sirrobot01/decypharr/internal/config"
)

// usenet owns its streaming-buffer pool here rather than the buffer package
// owning a "usenet" singleton — the buffer package stays generic. The pool is
// created once with the configured usenet RAM budget and shared across every
// SegmentCache. Disk is bounded per-stream by the sliding-window sweep
// (see SegmentCache.sweepWindow), so the pool runs no disk backstop.
var (
	bufPoolOnce sync.Once
	bufPool     *buffer.Pool
)

func usenetBufferPool() *buffer.Pool {
	bufPoolOnce.Do(func() {
		bufPool = buffer.NewPool(buffer.PoolConfig{
			Name:         "usenet",
			MemoryBudget: config.Get().Usenet.BufferMemoryBytes(),
		})
	})
	return bufPool
}

// ScratchStats reports where the bytes fetched into the scratch caches went,
// cumulatively since start: how much was written in, how much of that also
// reached a scratch file on disk (flushed out of RAM, or written straight
// through because RAM was full), and how much was released without ever
// touching one. Two snapshots subtracted give the share of a period's
// downloads that cost a scratch disk write.
func ScratchStats() map[string]any {
	st := usenetBufferPool().Stats()
	return map[string]any{
		"memory_in_use":         st.MemoryInUse,
		"memory_budget":         st.MemoryBudget,
		"disk_in_use":           st.DiskInUse,
		"buffers":               st.Buffers,
		"written_bytes":         st.WrittenBytes,
		"flushed_bytes":         st.FlushedBytes,
		"write_through_bytes":   st.WriteThroughBytes,
		"released_in_ram_bytes": st.ReleasedInRAMBytes,
	}
}
