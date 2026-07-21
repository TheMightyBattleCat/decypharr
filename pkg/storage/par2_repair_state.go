package storage

import (
	"fmt"
	"time"

	json "github.com/bytedance/sonic"
)

// Par2RepairState is the per-nzbID backoff/terminal-failure state that gates
// automatic PAR2 repair re-enqueue (see pkg/manager.Par2Repair.Enqueue).
// Distinct from Par2RepairAttempt (the append-only history log): this is one
// mutable record per nzbID, updated after every attempt, that only the
// automatic-trigger path consults - a manual "repair now" (Par2Repair.RunNow)
// never checks it, matching "manual always overrides".
type Par2RepairState struct {
	NzbID        string    `json:"nzb_id"`
	LastAttempt  time.Time `json:"last_attempt"`
	AttemptCount int       `json:"attempt_count"`
	LastError    string    `json:"last_error,omitempty"`

	// Terminal, once true, means the last failure was classified as one no
	// amount of retrying can fix on its own (see
	// pkg/manager.classifyPar2Failure) - the automatic path stops
	// re-enqueuing entirely until a manual repair-now (which re-evaluates
	// from scratch) changes this.
	Terminal       bool   `json:"terminal,omitempty"`
	TerminalReason string `json:"terminal_reason,omitempty"`

	// NextRetryAt is the earliest time the automatic path may re-enqueue
	// after a transient failure (exponential backoff). Zero means no backoff
	// in effect (never attempted, or the last attempt succeeded).
	NextRetryAt time.Time `json:"next_retry_at,omitempty"`
}

func (s *Storage) SavePar2RepairState(st *Par2RepairState) error {
	if st == nil || st.NzbID == "" {
		return fmt.Errorf("par2 repair state is missing nzb_id")
	}
	data, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return s.par2RepairState.Put(st.NzbID, data, nil)
}

// GetPar2RepairState returns nzbID's persisted state. Like GetEntryHealth
// and friends elsewhere in this package, any error (including "no such
// key" - there is no exported not-found sentinel to distinguish it from a
// real I/O error) means the caller should treat this as "no state yet" -
// the common case for a first attempt.
func (s *Storage) GetPar2RepairState(nzbID string) (*Par2RepairState, error) {
	if nzbID == "" {
		return nil, fmt.Errorf("nzb_id is empty")
	}
	data, err := s.par2RepairState.Get(nzbID)
	if err != nil {
		return nil, err
	}
	var st Par2RepairState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, err
	}
	if st.NzbID == "" {
		st.NzbID = nzbID
	}
	return &st, nil
}

func (s *Storage) DeletePar2RepairState(nzbID string) error {
	if nzbID == "" {
		return nil
	}
	return s.par2RepairState.Delete(nzbID)
}
