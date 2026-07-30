package usenet

import (
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
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
