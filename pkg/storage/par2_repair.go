package storage

import (
	"fmt"
	"sort"
	"time"

	json "github.com/bytedance/sonic"
)

// Par2RepairOutcome is the terminal state of one PAR2 repair attempt.
type Par2RepairOutcome string

const (
	Par2RepairOutcomeCompleted   Par2RepairOutcome = "completed"
	Par2RepairOutcomeFailed      Par2RepairOutcome = "failed"
	Par2RepairOutcomeUnavailable Par2RepairOutcome = "unavailable"
)

// Par2RepairAttempt is the append-only, compact history record produced by a
// single PAR2 repair pass (see pkg/manager.Par2Repair.runJob). Distinct from
// RepairRun: a RepairRun is one repair-sweep pass over many entries, while a
// Par2RepairAttempt is one PAR2 reconstruction pass over a single NZB.
type Par2RepairAttempt struct {
	ID        string            `json:"id"`
	EntryName string            `json:"entry_name"`
	NzbID     string            `json:"nzb_id"`
	StartedAt time.Time         `json:"started_at"`
	Duration  time.Duration     `json:"duration"`
	Outcome   Par2RepairOutcome `json:"outcome"`

	ReadBytes       int64 `json:"read_bytes,omitempty"`
	SlicesRepaired  int   `json:"slices_repaired,omitempty"`
	SegmentsPatched int   `json:"segments_patched,omitempty"`

	FailReason string `json:"fail_reason,omitempty"`
	// CRCCanary marks a failure whose cause was a reconstructed (or
	// supposedly-intact) slice failing its own IFSC MD5+CRC32 verification -
	// see pkg/usenet/par2.ErrChecksumMismatch. This is the important failure
	// mode to surface distinctly: it means the repair pass produced bytes it
	// could prove were wrong (or the offset mapping has drifted), not merely
	// that not enough recovery data was available.
	CRCCanary bool `json:"crc_canary,omitempty"`
}

func (s *Storage) SavePar2RepairAttempt(a *Par2RepairAttempt) error {
	if a == nil || a.ID == "" {
		return fmt.Errorf("par2 repair attempt is missing id")
	}
	data, err := json.Marshal(a)
	if err != nil {
		return err
	}
	return s.par2Repairs.Put(a.ID, data, nil)
}

func (s *Storage) GetPar2RepairAttempt(id string) (*Par2RepairAttempt, error) {
	if id == "" {
		return nil, fmt.Errorf("par2 repair attempt id is empty")
	}
	data, err := s.par2Repairs.Get(id)
	if err != nil {
		return nil, err
	}
	var a Par2RepairAttempt
	if err := json.Unmarshal(data, &a); err != nil {
		return nil, err
	}
	if a.ID == "" {
		a.ID = id
	}
	return &a, nil
}

// ListPar2RepairAttempts returns every attempt, sorted newest-first.
func (s *Storage) ListPar2RepairAttempts() ([]*Par2RepairAttempt, error) {
	attempts := make([]*Par2RepairAttempt, 0)
	err := s.par2Repairs.ForEach(func(key string, value []byte) error {
		var a Par2RepairAttempt
		if err := json.Unmarshal(value, &a); err != nil {
			return nil
		}
		if a.ID == "" {
			a.ID = key
		}
		attempts = append(attempts, &a)
		return nil
	})
	sort.Slice(attempts, func(i, j int) bool {
		return attempts[i].StartedAt.After(attempts[j].StartedAt)
	})
	return attempts, err
}

// LatestPar2RepairAttemptForNzb returns the most recent attempt recorded for
// nzbID, or nil (nil error) if none exists. Used by the overlay management
// API to explain a file's current repair status from history when there's no
// live queued/running job.
func (s *Storage) LatestPar2RepairAttemptForNzb(nzbID string) (*Par2RepairAttempt, error) {
	attempts, err := s.ListPar2RepairAttempts()
	if err != nil {
		return nil, err
	}
	for _, a := range attempts {
		if a.NzbID == nzbID {
			return a, nil
		}
	}
	return nil, nil
}

// ClearPar2RepairAttempts deletes every persisted attempt.
func (s *Storage) ClearPar2RepairAttempts() error {
	attempts, err := s.ListPar2RepairAttempts()
	if err != nil {
		return err
	}
	for _, a := range attempts {
		_ = s.par2Repairs.Delete(a.ID)
	}
	return nil
}

// PrunePar2RepairAttempts keeps the newest `keep` attempts and deletes the
// rest.
func (s *Storage) PrunePar2RepairAttempts(keep int) error {
	if keep <= 0 {
		keep = 100
	}
	attempts, err := s.ListPar2RepairAttempts()
	if err != nil {
		return err
	}
	if len(attempts) <= keep {
		return nil
	}
	for _, a := range attempts[keep:] {
		_ = s.par2Repairs.Delete(a.ID)
	}
	return nil
}
