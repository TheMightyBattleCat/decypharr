package usenet

import (
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// NewWithOverlayForTest builds a minimal Usenet backed by store, with NO providers,
// NO nntp client, and NO background goroutines — for tests in OTHER packages that need
// overlay teardown wired (OverlayDeleteFile/OverlayDeleteEntry) without the cost and
// goroutine leaks of New(). NOT for production use; a Usenet built this way cannot fetch.
func NewWithOverlayForTest(store *overlay.Store) *Usenet {
	return &Usenet{
		overlay:     store,
		failedFiles: xsync.NewMap[string, failedFileRecord](),
		logger:      zerolog.Nop(),
	}
}

// NewWithNZBStorageForTest is NewWithOverlayForTest with NZB record storage
// under config.GetMainPath() (point config at a temp dir first), for tests of
// code that reads NZB records (GetNZB, GetNZBHeader). NOT for production use.
func NewWithNZBStorageForTest(store *overlay.Store) (*Usenet, error) {
	nzbs, err := NewNZBStorage()
	if err != nil {
		return nil, err
	}
	u := NewWithOverlayForTest(store)
	u.nzbStorage = nzbs
	return u, nil
}

// AddNZBForTest stores nzb as NewWithNZBStorageForTest's record store would
// after a parse. NOT for production use.
func (u *Usenet) AddNZBForTest(nzb *storage.NZB) error {
	return u.nzbStorage.AddNZB(nzb)
}
