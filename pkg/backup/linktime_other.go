//go:build !linux && !darwin

package backup

import "time"

// setLinkTime is a no-op where the link's own time cannot be set.
func setLinkTime(string, time.Time) error { return nil }
