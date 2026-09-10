//go:build !linux && !darwin

package vfs

import "github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs/ranges"

// dataExtents can't see holes on this platform, so nothing is ever reported
// missing.
func dataExtents(string) (ranges.Ranges, bool) { return nil, false }
