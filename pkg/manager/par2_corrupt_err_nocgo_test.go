//go:build !cgo

package manager

// corruptErr is nil without cgo: the pure-Go decoder checks no CRC.
var corruptErr error
