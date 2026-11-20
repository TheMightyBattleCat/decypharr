package decypharr

import (
	"errors"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

type fakeResetter struct {
	err   error
	calls int
}

func (f *fakeResetter) Reset() error {
	f.calls++
	return f.err
}

// A restart whose manager reopen fails must hand the error back, so Start
// returns it (non-zero exit) instead of starting the services again on a
// closed store.
func TestResetForRestartReturnsTheReopenError(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	reopen := errors.New("failed to reopen storage after reset: entries.db damaged")
	mgr := &fakeResetter{err: reopen}
	err := resetForRestart(mgr)
	if mgr.calls != 1 {
		t.Fatalf("Reset called %d times, want 1", mgr.calls)
	}
	if !errors.Is(err, reopen) {
		t.Fatalf("resetForRestart = %v, want the reopen error", err)
	}

	ok := &fakeResetter{}
	if err := resetForRestart(ok); err != nil || ok.calls != 1 {
		t.Fatalf("resetForRestart on a good reopen = %v (calls %d), want nil after one Reset", err, ok.calls)
	}
}
