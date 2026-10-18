package backup

import "fmt"

// RunStopper is implemented by the real Service. The API stops a running
// backup job through it (the Service contract has no such method).
type RunStopper interface {
	StopRun() error
}

var _ RunStopper = (*BackupService)(nil)

// StopRun cancels the backup job in progress. Its snapshot is not written;
// the blobs it already stored are reused by the next run or removed by
// retention. Pre-action snapshots are not jobs and cannot be stopped.
func (s *BackupService) StopRun() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runCancel == nil {
		return fmt.Errorf("%w: no backup job is running", ErrNotFound)
	}
	s.stopRequested = true
	s.runCancel()
	return nil
}
