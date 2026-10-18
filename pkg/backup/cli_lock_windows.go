//go:build windows

package backup

import (
	"errors"
	"os"
	"syscall"
	"unsafe"
)

var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = kernel32.NewProc("LockFileEx")
	procUnlockFileEx = kernel32.NewProc("UnlockFileEx")
)

// tryLock takes the lock on path without waiting. It returns busy when
// another holder has it, and otherwise a release func that drops the lock.
// A missing file has nothing to lock: release is a no-op.
func tryLock(path string) (release func(), busy bool, err error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if errors.Is(err, os.ErrNotExist) {
		return func() {}, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var ov syscall.Overlapped
	r, _, callErr := procLockFileEx.Call(f.Fd(), 0x3, 0, 1, 0, uintptr(unsafe.Pointer(&ov)))
	if r == 0 {
		_ = f.Close()
		if errno, ok := callErr.(syscall.Errno); ok && errno == 33 {
			return nil, true, nil
		}
		return nil, false, callErr
	}
	return func() {
		var ov2 syscall.Overlapped
		procUnlockFileEx.Call(f.Fd(), 0, 1, 0, uintptr(unsafe.Pointer(&ov2)))
		_ = f.Close()
	}, false, nil
}

// lockHeld reports whether another process holds the lock on path.
func lockHeld(path string) (bool, error) {
	release, busy, err := tryLock(path)
	if err != nil || busy {
		return busy, err
	}
	release()
	return false, nil
}
