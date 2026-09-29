package customerror_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/manager/link"
)

func TestRetryClassificationUsesWrappedMetadata(t *testing.T) {
	for _, tc := range []struct {
		name             string
		err              error
		retry, permanent bool
	}{
		{"permanent timeout", link.NewPermanentError(errors.New("i/o timeout"), ""), false, true},
		{"custom permanent", customerror.NewPermanentError(errors.New("broken pipe")), false, true},
		// Not retryable is not permanent: a permanent verdict during playback
		// deletes and re-searches the release.
		{"nntp yenc decode", &nntp.Error{Type: nntp.ErrorTypeYencDecode, Message: "crc mismatch"}, false, false},
		{"plain not found", errors.New("not found"), false, true},
		{"plain timeout", errors.New("i/o timeout"), true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, err := range []error{tc.err, fmt.Errorf("fetch: %w", tc.err), errors.Join(errors.New("context"), tc.err)} {
				if got := customerror.IsRetriableError(err); got != tc.retry {
					t.Errorf("IsRetriableError(%v) = %v, want %v", err, got, tc.retry)
				}
				if got := customerror.IsPermanentError(err); got != tc.permanent {
					t.Errorf("IsPermanentError(%v) = %v, want %v", err, got, tc.permanent)
				}
			}
		})
	}
}
