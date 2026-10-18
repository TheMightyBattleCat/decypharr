package backup

// SetDuringRestoreHook lets the external tests watch the restore command
// between the database swap and the release of the store locks.
func SetDuringRestoreHook(f func(dataDir string)) { duringRestoreHook = f }

// LockHeld exposes lockHeld to the external tests.
func LockHeld(path string) (bool, error) { return lockHeld(path) }
