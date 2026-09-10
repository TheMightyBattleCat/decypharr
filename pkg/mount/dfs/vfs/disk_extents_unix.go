//go:build linux || darwin

package vfs

import (
	"errors"
	"os"

	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs/ranges"
	"golang.org/x/sys/unix"
)

// dataExtents returns the byte ranges of the file at path that are backed by
// data on disk, walking it with SEEK_DATA/SEEK_HOLE. ok is false when the file
// can't be opened or the filesystem can't report holes; callers must then treat
// every byte as possibly present.
func dataExtents(path string) (extents ranges.Ranges, ok bool) {
	f, err := os.Open(path)
	if err != nil {
		return nil, false
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, false
	}

	fd := int(f.Fd())
	for off := int64(0); off < info.Size(); {
		data, err := unix.Seek(fd, off, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			break // no data at or after off
		}
		if err != nil {
			return nil, false
		}
		hole, err := unix.Seek(fd, data, unix.SEEK_HOLE)
		if err != nil {
			return nil, false
		}
		extents = append(extents, ranges.Range{Pos: data, Size: hole - data})
		off = hole
	}
	return extents, true
}
