package link

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// A link the provider no longer serves is repaired by re-inserting the
// entry; an outage or limit is not, since a re-insert during one can mark the
// entry Bad and send it to a blocklisting re-grab.
func TestHandleBadLinkReinsertsOnlyWhenTheProviderLostTheFile(t *testing.T) {
	oldPath := config.GetMainPath()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(func() { config.SetConfigPath(oldPath) })

	for _, tc := range []struct {
		name     string
		err      error
		reinsert bool
	}{
		{"hoster unavailable", fmt.Errorf("get download link for realdebrid: %w", errors.Join(customerror.HosterUnavailableError)), true},
		{"empty link", fmt.Errorf("realdebrid API error: download link not found: %w", types.EmptyDownloadLinkError), true},
		{"gateway error", errors.New("realdebrid API error: Status: 503 || Code: 25"), false},
		{"traffic exceeded", customerror.TrafficExceededError, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reinserts := 0
			s := &Service{
				repairer: func(_ context.Context, entry *storage.Entry) error {
					reinserts++
					entry.Bad = true // stop after one re-insert
					return nil
				},
				logger: zerolog.Nop(),
			}
			entry := &storage.Entry{InfoHash: "abc", Name: "release"}

			_, err := s.handleBadLink(t.Context(), tc.err, entry, types.DownloadLink{Filename: "file.mkv"}, 0)
			if err == nil {
				t.Fatal("handleBadLink returned no error for a link it could not serve")
			}
			if got := reinserts == 1; got != tc.reinsert {
				t.Fatalf("re-inserts = %d, want re-insert %v", reinserts, tc.reinsert)
			}
		})
	}
}
