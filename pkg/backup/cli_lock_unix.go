//go:build !windows

package backup

import (
	"errors"
	"os"
	"syscall"
)

// tryLock takes the flock on path without waiting. It returns busy when
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
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
		_ = f.Close()
		return nil, true, nil
	}
	if err != nil {
		_ = f.Close()
		return nil, false, err
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, false, nil
}

// lockHeld reports whether another process holds the flock on path.
func lockHeld(path string) (bool, error) {
	release, busy, err := tryLock(path)
	if err != nil || busy {
		return busy, err
	}
	release()
	return false, nil
}
