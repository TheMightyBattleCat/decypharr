//go:build linux || darwin

package backup

import (
	"time"

	"golang.org/x/sys/unix"
)

// setLinkTime sets the modification (and access) time of the link itself,
// never of what it points at.
func setLinkTime(p string, t time.Time) error {
	ts := unix.NsecToTimespec(t.UnixNano())
	return unix.UtimesNanoAt(unix.AT_FDCWD, p, []unix.Timespec{ts, ts}, unix.AT_SYMLINK_NOFOLLOW)
}
