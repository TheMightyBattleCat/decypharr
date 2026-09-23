package manager

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// The live reader must be refreshed BEFORE the DFS ranges are forgotten:
// forgetting first let a read in between re-stream the reader's stale pad,
// which the DFS cache then persisted as final bytes. A second forget after
// the grace delay covers a stream that read the pad just before the refresh.
func TestInvalidateRepairedRefreshesReaderBeforeForgettingDFS(t *testing.T) {
	nzb := &storage.NZB{Files: []storage.NZBFile{{
		Name: "movie.mkv",
		Segments: []storage.NZBSegment{
			{StartOffset: 0, EndOffset: 99},
			{StartOffset: 100, EndOffset: 199},
			{StartOffset: 200, EndOffset: 299},
		},
	}}}
	pending := map[string][]overlay.DeadSegment{"movie.mkv": {{Index: 1}, {Index: 2}}}

	var calls []string
	var delayed func()
	var delay time.Duration
	invalidateRepaired(nzb, pending,
		func(file string, segIdx []int) { calls = append(calls, fmt.Sprintf("refresh %s %v", file, segIdx)) },
		func(file string, off, length int64) {
			calls = append(calls, fmt.Sprintf("forget %s %d+%d", file, off, length))
		},
		func(d time.Duration, f func()) { delay, delayed = d, f },
	)

	want := []string{
		"refresh movie.mkv [1 2]",
		"forget movie.mkv 100+100",
		"forget movie.mkv 200+100",
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %q, want %q", calls, want)
	}
	if delayed == nil || delay != repairedRangeReforgetDelay {
		t.Fatalf("second forget scheduled = %v after %v, want after %v", delayed != nil, delay, repairedRangeReforgetDelay)
	}
	calls = nil
	delayed()
	if !reflect.DeepEqual(calls, want[1:]) {
		t.Fatalf("delayed calls = %q, want %q", calls, want[1:])
	}
}

func TestInvalidateRepairedWithoutDFSStillRefreshesReader(t *testing.T) {
	nzb := &storage.NZB{Files: []storage.NZBFile{{Name: "movie.mkv", Segments: []storage.NZBSegment{{StartOffset: 0, EndOffset: 99}}}}}
	refreshed := false
	invalidateRepaired(nzb, map[string][]overlay.DeadSegment{"movie.mkv": {{Index: 0}}},
		func(string, []int) { refreshed = true }, nil,
		func(time.Duration, func()) { t.Fatal("no DFS mount: nothing to forget later") })
	if !refreshed {
		t.Fatal("reader not refreshed without a DFS mount")
	}
}
