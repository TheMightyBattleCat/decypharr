//go:build !windows

package backup

import (
	"os"
	"syscall"
)

// diskInfo returns the free bytes where dir lives and whether dir shares a
// filesystem with dataDir (one failed disk would take both).
func diskInfo(dir, dataDir string) (free int64, same bool) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(dir, &fs); err == nil {
		free = int64(fs.Bavail) * int64(fs.Bsize)
	}
	a, errA := os.Stat(dir)
	b, errB := os.Stat(dataDir)
	if errA == nil && errB == nil {
		sa, okA := a.Sys().(*syscall.Stat_t)
		sb, okB := b.Sys().(*syscall.Stat_t)
		same = okA && okB && sa.Dev == sb.Dev
	}
	return free, same
}
