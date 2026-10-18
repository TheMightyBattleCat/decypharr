package backup

import "fmt"

// HoldReleaser is implemented by the real Service. The API releases the
// post-restore hold through it (the Service contract has no such method).
type HoldReleaser interface {
	ReleaseHold() error
	HoldActive() bool
}

var _ HoldReleaser = (*BackupService)(nil)

// HoldActive reports the post-restore hold: on from the first start after a
// database swap until the user releases it.
func (s *BackupService) HoldActive() bool { return PostRestoreHoldActive(s.deps.DataDir) }

// ReleaseHold ends the post-restore hold, which lets overlay reap, Plex
// reap, repair auto-fix, stale-NZB cleanup and webhook deletes run again.
func (s *BackupService) ReleaseHold() error {
	if !s.HoldActive() {
		return nil
	}
	if err := ReleasePostRestoreHold(s.deps.DataDir); err != nil {
		return fmt.Errorf("backup: releasing the post-restore hold: %w", err)
	}
	s.log.Info().Msg("Post-restore hold released")
	return nil
}
