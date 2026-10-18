package backup

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// LinkRestoreAppliedFile is the marker, in the data folder, that a manual
// link restore has created or retargeted at least one link. Automatic
// re-creation stays off the Settings page until it exists.
const LinkRestoreAppliedFile = "links-restore-applied.json"

// LinkRestoreApplied reports whether the marker exists in dataDir.
func LinkRestoreApplied(dataDir string) bool {
	if dataDir == "" {
		return false
	}
	_, err := os.Stat(filepath.Join(dataDir, LinkRestoreAppliedFile))
	return err == nil
}

func markLinkRestoreApplied(dataDir string, at time.Time) error {
	if dataDir == "" {
		return nil
	}
	b, err := json.Marshal(struct {
		At time.Time `json:"at"`
	}{at})
	if err != nil {
		return err
	}
	p := filepath.Join(dataDir, LinkRestoreAppliedFile)
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}
