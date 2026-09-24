package parser

// A first-segment STAT that failed for a transient reason must not be tagged
// ErrReleaseUnavailable: ParseWithID would mark the content hash dead and
// AddNewNZB would queue it pre-failed for the Arr to blocklist.

import (
	"context"
	"errors"
	"testing"

	"github.com/Tensai75/nzbparser"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/nntp"
)

func TestTransientStatNotTaggedReleaseUnavailable(t *testing.T) {
	fileGroups := map[string]*FileGroup{
		"release": {
			BaseName:       "release",
			ActualFilename: "release.rar",
			Files: []nzbparser.NzbFile{
				{Filename: "release.rar", Segments: nzbparser.NzbSegments{{Number: 1, Bytes: 1000, Id: "<seg1>"}}},
			},
		},
	}
	rawFiles := nzbparser.NzbFiles{
		{Filename: "release.rar", Bytes: 1000, Segments: nzbparser.NzbSegments{{Number: 1, Bytes: 1000, Id: "<seg1>"}}},
	}
	fetch := func(_ context.Context, _ string) (*nntp.YencMetadata, error) {
		return &nntp.YencMetadata{Size: 970, Begin: 0, End: 969}, nil
	}

	p := &NZBParser{logger: zerolog.Nop(), maxConcurrent: 4}
	cases := map[string]error{
		// ExecuteWithFailover returns ctx.Err() when the request context ends
		// (SABnzbd add runs on r.Context()).
		"context canceled": context.Canceled,
		"nntp timeout":     &nntp.Error{Type: nntp.ErrorTypeTimeout, Message: "i/o timeout"},
		// getAnyAvailableConnection when every primary is at its soft quota cap.
		"no eligible providers": errors.New("no eligible providers available"),
		"connection refused":    &nntp.Error{Type: nntp.ErrorTypeConnection, Message: "dial tcp: connection refused"},
	}
	for name, statErr := range cases {
		t.Run(name, func(t *testing.T) {
			stat := func(_ context.Context, _ string) error { return statErr }
			_, _, err := availabilityThenPar2Refs(context.Background(), zerolog.Nop(), 4, fileGroups, rawFiles, p.detectFileType, stat, fetch)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !errors.Is(err, statErr) {
				t.Fatalf("error %v does not wrap the STAT error", err)
			}
			if errors.Is(err, ErrReleaseUnavailable) {
				t.Errorf("transient STAT failure (%v) tagged ErrReleaseUnavailable", statErr)
			}
		})
	}

	// A genuine 430 on every provider is still tagged.
	notFound := &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: "no such article"}
	stat := func(_ context.Context, _ string) error { return notFound }
	_, _, err := availabilityThenPar2Refs(context.Background(), zerolog.Nop(), 4, fileGroups, rawFiles, p.detectFileType, stat, fetch)
	if !errors.Is(err, ErrReleaseUnavailable) || !nntp.IsArticleNotFoundError(err) {
		t.Errorf("430: err=%v, want ErrReleaseUnavailable wrapping an article-not-found", err)
	}
}
