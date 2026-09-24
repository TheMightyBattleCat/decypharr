package storage

import (
	"fmt"
	"time"

	json "github.com/bytedance/sonic"
)

// RegrabGuardRecord is one logical file's automatic re-grab history (see
// pkg/manager.regrabGuard), persisted so a restart doesn't wipe it: the guard
// exists to stop a loop that spans days and re-grabs, and the process
// restarts far more often than that.
type RegrabGuardRecord struct {
	Identity       string               `json:"identity"`
	Attempts       []time.Time          `json:"attempts,omitempty"`
	Terminal       bool                 `json:"terminal,omitempty"`
	TerminalAt     time.Time            `json:"terminal_at,omitempty"`
	Reason         string               `json:"reason,omitempty"`
	RecentReleases map[string]time.Time `json:"recent_releases,omitempty"`
	// KeepRelease records, per posting identity, when an automatic re-grab
	// away from that posting (a file assembled wrong at import, re-grabbed
	// without blocklisting) was last allowed.
	KeepRelease map[string]KeepReleaseMark `json:"keep_release,omitempty"`
}

// KeepReleaseMark is one keep-release re-grab: when, and from which entry.
type KeepReleaseMark struct {
	At      time.Time `json:"at"`
	FromNZB string    `json:"from_nzb,omitempty"`
}

func (s *Storage) SaveRegrabGuardRecord(rec *RegrabGuardRecord) error {
	if rec == nil || rec.Identity == "" {
		return fmt.Errorf("regrab guard record is missing identity")
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return s.regrabGuard.Put(rec.Identity, data, nil)
}

func (s *Storage) DeleteRegrabGuardRecord(identity string) error {
	if identity == "" {
		return nil
	}
	return s.regrabGuard.Delete(identity)
}

// ForEachRegrabGuardRecord calls fn for every persisted record. A record
// that fails to decode is skipped.
func (s *Storage) ForEachRegrabGuardRecord(fn func(*RegrabGuardRecord)) error {
	return s.regrabGuard.ForEach(func(key string, value []byte) error {
		var rec RegrabGuardRecord
		if err := json.Unmarshal(value, &rec); err != nil {
			return nil
		}
		if rec.Identity == "" {
			rec.Identity = key
		}
		fn(&rec)
		return nil
	})
}
