package parser

// Failover settles on one provider's 430 while another provider could not be
// dialled, sat in its dial cooldown or was over its quota, and marks it
// partial (nntp.PartialNotFoundError). At the first STAT of a new NZB that
// must be "could not check" (retried, nothing recorded), not
// ErrReleaseUnavailable: that verdict marks the posting dead, reports it to
// hearsay and fails the entry so the Arr blocklists a release the unasked
// provider may hold.

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/Tensai75/nzbparser"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/nntp"
)

func partialNotFound() error {
	return &nntp.PartialNotFoundError{
		Err:     &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: "no such article"},
		Unasked: []string{"down.example"},
	}
}

func TestPartialNotFoundAtFirstStatIsCouldNotCheck(t *testing.T) {
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
	var fetches int32
	fetch := func(_ context.Context, _ string) (*nntp.YencMetadata, error) {
		atomic.AddInt32(&fetches, 1)
		return &nntp.YencMetadata{Size: 970, Begin: 0, End: 969}, nil
	}
	p := &NZBParser{logger: zerolog.Nop(), maxConcurrent: 4}
	run := func(statErr error) error {
		stat := func(_ context.Context, _ string) error { return statErr }
		_, _, err := availabilityThenPar2Refs(context.Background(), zerolog.Nop(), 4, fileGroups, rawFiles, p.detectFileType, stat, fetch, nil)
		return err
	}

	t.Run("partial", func(t *testing.T) {
		statErr := partialNotFound()
		err := run(statErr)
		if err == nil {
			t.Fatal("expected an error: the availability check did not pass")
		}
		if errors.Is(err, ErrReleaseUnavailable) {
			t.Fatalf("partial 430 tagged ErrReleaseUnavailable (posting marked dead, Arr blocklists): %v", err)
		}
		// Not ErrReleaseUnavailable is what retryNZBParse needs to parse the
		// NZB again; the cause stays readable for the log.
		if !errors.Is(err, statErr) || !nntp.IsPartialNotFound(err) {
			t.Fatalf("error %v does not wrap the partial STAT error", err)
		}
		if got := atomic.LoadInt32(&fetches); got != 0 {
			t.Fatalf("PAR2 probing ran %d fetches after a failed availability check", got)
		}
	})

	// Control: a 430 every provider gave still rejects the release.
	t.Run("complete", func(t *testing.T) {
		err := run(&nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: "no such article"})
		if !errors.Is(err, ErrReleaseUnavailable) || !nntp.IsArticleNotFoundError(err) {
			t.Fatalf("complete 430: err=%v, want ErrReleaseUnavailable wrapping an article-not-found", err)
		}
	})
}

// The PAR2 source-size probe counts only a complete 430 toward its early
// abort. Its production fetch asks one provider (ExecuteOnce) and never
// returns a partial 430, so this pins the rule for a fetch that does.
func TestPartialNotFoundProbeDoesNotCountTowardAbort(t *testing.T) {
	const nFiles = par2ProbeMaxFailedFetches + 5
	files := make(nzbparser.NzbFiles, 0, nFiles)
	for i := 0; i < nFiles; i++ {
		files = append(files, nzbparser.NzbFile{
			Filename: fmt.Sprintf("file%02d.rar", i),
			Segments: nzbparser.NzbSegments{{Number: 1, Bytes: 1000, Id: fmt.Sprintf("<seg%d>", i)}},
		})
	}
	p := &NZBParser{logger: zerolog.Nop()}

	var calls int32
	partial := func(_ context.Context, _ string) (*nntp.YencMetadata, error) {
		atomic.AddInt32(&calls, 1)
		return nil, partialNotFound()
	}
	_, source, aborted := buildPar2RefsWithFetch(context.Background(), p.logger, 1, files, p.detectFileType, partial, nil)
	if aborted {
		t.Fatalf("aborted after %d partial 430s; a provider never asked may hold those articles", atomic.LoadInt32(&calls))
	}
	if got := atomic.LoadInt32(&calls); got != nFiles {
		t.Fatalf("fetch called %d times, want every file probed (%d)", got, nFiles)
	}
	if len(source) != nFiles {
		t.Fatalf("source = %d files, want %d", len(source), nFiles)
	}

	// Control: the same count of complete 430s aborts.
	complete := func(_ context.Context, _ string) (*nntp.YencMetadata, error) {
		return nil, &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: "no such article"}
	}
	if _, _, aborted := buildPar2RefsWithFetch(context.Background(), p.logger, 1, files, p.detectFileType, complete, nil); !aborted {
		t.Fatal("complete 430s did not abort probing")
	}
}
