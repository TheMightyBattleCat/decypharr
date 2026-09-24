package storage

import (
	"fmt"
	"time"

	json "github.com/bytedance/sonic"
)

// PendingDelete is an entry whose delete waits for a client stream to close
// (pkg/manager.Repair.deleteEntryWhenIdle), persisted so a restart during the
// wait does not leave the superseded grab behind for good.
type PendingDelete struct {
	InfoHash string    `json:"info_hash"`
	Name     string    `json:"name"`
	Since    time.Time `json:"since"`
}

func (s *Storage) SavePendingDelete(pd *PendingDelete) error {
	if pd == nil || pd.InfoHash == "" {
		return fmt.Errorf("pending delete is missing info hash")
	}
	data, err := json.Marshal(pd)
	if err != nil {
		return err
	}
	return s.pendingDeletes.Put(pd.InfoHash, data, nil)
}

func (s *Storage) DeletePendingDelete(infoHash string) error {
	if infoHash == "" {
		return nil
	}
	return s.pendingDeletes.Delete(infoHash)
}

// ForEachPendingDelete calls fn for every persisted pending delete. A record
// that fails to decode is skipped.
func (s *Storage) ForEachPendingDelete(fn func(*PendingDelete)) error {
	return s.pendingDeletes.ForEach(func(key string, value []byte) error {
		var pd PendingDelete
		if err := json.Unmarshal(value, &pd); err != nil {
			return nil
		}
		if pd.InfoHash == "" {
			pd.InfoHash = key
		}
		fn(&pd)
		return nil
	})
}
