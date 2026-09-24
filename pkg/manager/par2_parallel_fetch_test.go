package manager

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

var parallelFetchNopLogger = zerolog.Nop()

func par2NotFoundErr() error {
	return &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430}
}

func par2TransportErr() error {
	return fmt.Errorf("connection reset by peer")
}

func makeTestVol(name string, count uint32, msgIDs ...string) par2Volume {
	segs := make([]storage.Par2SegmentRef, len(msgIDs))
	for i, id := range msgIDs {
		segs[i] = storage.Par2SegmentRef{MessageID: id, Bytes: 1000}
	}
	return par2Volume{
		ref:   storage.Par2FileRef{Name: name, Segments: segs},
		count: count,
	}
}

// mockVolumeFetch returns an articleFetchFunc: a message ID in successes
// returns its bytes, one in notFound 430s, anything else is a transport
// error.
func mockVolumeFetch(successes map[string][]byte, notFound map[string]bool) articleFetchFunc {
	return func(ctx context.Context, messageID string) ([]byte, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if notFound[messageID] {
			return nil, par2NotFoundErr()
		}
		if data, ok := successes[messageID]; ok {
			return data, nil
		}
		return nil, par2TransportErr()
	}
}

func sourceNames(sources []par2.Source) []string {
	out := make([]string, len(sources))
	for i, s := range sources {
		out[i] = s.Name
	}
	return out
}

// ─── computeRecoveryBatch ───

func TestComputeRecoveryBatch(t *testing.T) {
	vols := []par2Volume{
		makeTestVol("vol000+01.par2", 1),
		makeTestVol("vol001+02.par2", 2),
		makeTestVol("vol003+04.par2", 4),
		makeTestVol("vol007+08.par2", 8),
		makeTestVol("vol015+16.par2", 16),
	}

	tests := []struct {
		name      string
		shortfall uint32
		wantLen   int
	}{
		{"zero", 0, 0},
		{"need_1_gets_slack", 1, 2},
		{"need_3_exact_plus_slack", 3, 3},
		{"need_7_exact_plus_slack", 7, 4},
		{"need_15_exact_plus_slack", 15, 5},
		{"need_31_all_no_slack", 31, 5},
		{"exceeds_available", 100, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := computeRecoveryBatch(vols, tt.shortfall)
			if len(batch) != tt.wantLen {
				t.Fatalf("computeRecoveryBatch(shortfall=%d) len = %d, want %d", tt.shortfall, len(batch), tt.wantLen)
			}
			if tt.wantLen == 0 && batch != nil {
				t.Errorf("want nil batch, got %v", batch)
			}
		})
	}
}

func TestComputeRecoveryBatch_Empty(t *testing.T) {
	if b := computeRecoveryBatch(nil, 10); b != nil {
		t.Errorf("nil vols: want nil, got %v", b)
	}
	if b := computeRecoveryBatch([]par2Volume{}, 5); b != nil {
		t.Errorf("empty vols: want nil, got %v", b)
	}
}

// ─── fetchMoreVolumes ───

func TestFetchMoreVolumes_AllSuccess(t *testing.T) {
	vols := []par2Volume{
		makeTestVol("v0.par2", 1, "a@news"),
		makeTestVol("v1.par2", 2, "b@news"),
		makeTestVol("v2.par2", 4, "c@news"),
	}
	fetch := mockVolumeFetch(map[string][]byte{
		"a@news": []byte("d0"),
		"b@news": []byte("d1"),
		"c@news": []byte("d2"),
	}, nil)

	nextIdx, fetched, sources, added, err := fetchMoreVolumes(
		context.Background(), parallelFetchNopLogger, fetch, vols, 0, 7, 0, nil, "test-entry", 4,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nextIdx != 3 || fetched != 7 || added != 3 {
		t.Fatalf("got nextIdx=%d fetched=%d added=%d, want 3/7/3", nextIdx, fetched, added)
	}
	if got := sourceNames(sources); len(got) != 3 || got[0] != "v0.par2" || got[1] != "v1.par2" || got[2] != "v2.par2" {
		t.Fatalf("sources = %v, want [v0 v1 v2] in order", got)
	}
}

func TestFetchMoreVolumes_MixedNotFound(t *testing.T) {
	vols := []par2Volume{
		makeTestVol("v0.par2", 1, "a@news"),
		makeTestVol("v1.par2", 2, "b@news"),
		makeTestVol("v2.par2", 4, "c@news"),
	}
	fetch := mockVolumeFetch(
		map[string][]byte{"a@news": []byte("d0"), "c@news": []byte("d2")},
		map[string]bool{"b@news": true},
	)

	nextIdx, fetched, sources, added, err := fetchMoreVolumes(
		context.Background(), parallelFetchNopLogger, fetch, vols, 0, 7, 0, nil, "test-entry", 4,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nextIdx != 3 || fetched != 5 || added != 2 {
		t.Fatalf("got nextIdx=%d fetched=%d added=%d, want 3/5/2", nextIdx, fetched, added)
	}
	if got := sourceNames(sources); len(got) != 2 || got[0] != "v0.par2" || got[1] != "v2.par2" {
		t.Fatalf("sources = %v, want [v0 v2] in order", got)
	}
}

func TestFetchMoreVolumes_ThreeConsecutiveNotFoundAborts(t *testing.T) {
	vols := []par2Volume{
		makeTestVol("v0.par2", 1, "a@news"),
		makeTestVol("v1.par2", 2, "b@news"),
		makeTestVol("v2.par2", 4, "c@news"),
		makeTestVol("v3.par2", 8, "d@news"),
		makeTestVol("v4.par2", 16, "e@news"),
	}
	fetch := mockVolumeFetch(nil, map[string]bool{
		"a@news": true, "b@news": true, "c@news": true,
		"d@news": true, "e@news": true,
	})

	_, fetched, sources, added, err := fetchMoreVolumes(
		context.Background(), parallelFetchNopLogger, fetch, vols, 0, 31, 0, nil, "test-entry", 2,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if added != 0 || fetched != 0 || len(sources) != 0 {
		t.Fatalf("got added=%d fetched=%d sources=%d, want 0/0/0", added, fetched, len(sources))
	}
}

func TestFetchMoreVolumes_SuccessResetsStreak(t *testing.T) {
	// 430, 430, success, 430, 430 — streak never reaches 3.
	vols := []par2Volume{
		makeTestVol("v0.par2", 1, "a@news"),
		makeTestVol("v1.par2", 2, "b@news"),
		makeTestVol("v2.par2", 4, "c@news"),
		makeTestVol("v3.par2", 8, "d@news"),
		makeTestVol("v4.par2", 16, "e@news"),
	}
	fetch := mockVolumeFetch(
		map[string][]byte{"c@news": []byte("d2")},
		map[string]bool{"a@news": true, "b@news": true, "d@news": true, "e@news": true},
	)

	nextIdx, fetched, sources, added, err := fetchMoreVolumes(
		context.Background(), parallelFetchNopLogger, fetch, vols, 0, 31, 0, nil, "test-entry", 2,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if added != 1 || fetched != 4 || nextIdx != 5 {
		t.Fatalf("got added=%d fetched=%d nextIdx=%d, want 1/4/5", added, fetched, nextIdx)
	}
	if got := sourceNames(sources); len(got) != 1 || got[0] != "v2.par2" {
		t.Fatalf("sources = %v, want [v2]", got)
	}
}

func TestFetchMoreVolumes_TransportErrorResetsStreak(t *testing.T) {
	// 430, 430, transport, 430, 430 — transport resets the streak, so it
	// never reaches 3 and the whole batch is walked.
	vols := []par2Volume{
		makeTestVol("v0.par2", 1, "a@news"),
		makeTestVol("v1.par2", 2, "b@news"),
		makeTestVol("v2.par2", 4, "c@news"), // transport error (in neither map)
		makeTestVol("v3.par2", 8, "d@news"),
		makeTestVol("v4.par2", 16, "e@news"),
	}
	fetch := mockVolumeFetch(
		nil,
		map[string]bool{"a@news": true, "b@news": true, "d@news": true, "e@news": true},
	)

	_, fetched, _, added, err := fetchMoreVolumes(
		context.Background(), parallelFetchNopLogger, fetch, vols, 0, 31, 0, nil, "test-entry", 2,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if added != 0 || fetched != 0 {
		t.Fatalf("got added=%d fetched=%d, want 0/0", added, fetched)
	}
}

func TestFetchMoreVolumes_ResumeFromNextVolIdx(t *testing.T) {
	vols := []par2Volume{
		makeTestVol("v0.par2", 1, "a@news"),
		makeTestVol("v1.par2", 2, "b@news"),
		makeTestVol("v2.par2", 4, "c@news"),
		makeTestVol("v3.par2", 8, "d@news"),
	}
	fetch := mockVolumeFetch(
		map[string][]byte{"c@news": []byte("d2"), "d@news": []byte("d3")},
		nil,
	)

	nextIdx, fetched, sources, added, err := fetchMoreVolumes(
		context.Background(), parallelFetchNopLogger, fetch, vols, 2, 12, 0, nil, "test-entry", 4,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nextIdx != 4 || fetched != 12 || added != 2 {
		t.Fatalf("got nextIdx=%d fetched=%d added=%d, want 4/12/2", nextIdx, fetched, added)
	}
	if got := sourceNames(sources); len(got) != 2 || got[0] != "v2.par2" || got[1] != "v3.par2" {
		t.Fatalf("sources = %v, want [v2 v3]", got)
	}
}

func TestFetchMoreVolumes_ZeroShortfall(t *testing.T) {
	vols := []par2Volume{makeTestVol("v0.par2", 1, "a@news")}
	fetch := mockVolumeFetch(nil, nil)

	nextIdx, fetched, sources, added, err := fetchMoreVolumes(
		context.Background(), parallelFetchNopLogger, fetch, vols, 0, 5, 10, nil, "test-entry", 4,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nextIdx != 0 || fetched != 10 || added != 0 || len(sources) != 0 {
		t.Fatalf("got nextIdx=%d fetched=%d added=%d sources=%d, want 0/10/0/0", nextIdx, fetched, added, len(sources))
	}
}

func TestFetchMoreVolumes_PreCancelledCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	vols := []par2Volume{makeTestVol("v0.par2", 1, "a@news")}
	fetch := mockVolumeFetch(map[string][]byte{"a@news": []byte("x")}, nil)

	_, fetched, sources, added, err := fetchMoreVolumes(
		ctx, parallelFetchNopLogger, fetch, vols, 0, 1, 0, nil, "test-entry", 4,
	)

	// A cut-off fetch reports why it came back short, so the caller's
	// shortfall is classified transient rather than terminal.
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if added != 0 || fetched != 0 || len(sources) != 0 {
		t.Fatalf("got added=%d fetched=%d sources=%d, want 0/0/0", added, fetched, len(sources))
	}
}

func TestFetchMoreVolumes_WaveBoundaryStreakCarry(t *testing.T) {
	// maxConc=2: wave 1=[v0,v1], wave 2=[v2,v3].
	// v0=430, v1=430 (streak=2), v2=430 (streak=3) → abort.
	// v3 would succeed but is past the abort point.
	vols := []par2Volume{
		makeTestVol("v0.par2", 1, "a@news"),
		makeTestVol("v1.par2", 2, "b@news"),
		makeTestVol("v2.par2", 4, "c@news"),
		makeTestVol("v3.par2", 8, "d@news"),
	}
	fetch := mockVolumeFetch(
		map[string][]byte{"d@news": []byte("d3")},
		map[string]bool{"a@news": true, "b@news": true, "c@news": true},
	)

	_, fetched, sources, added, err := fetchMoreVolumes(
		context.Background(), parallelFetchNopLogger, fetch, vols, 0, 15, 0, nil, "test-entry", 2,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if added != 0 || fetched != 0 || len(sources) != 0 {
		t.Fatalf("got added=%d fetched=%d sources=%d, want 0/0/0", added, fetched, len(sources))
	}
}

func TestFetchMoreVolumes_StopsWhenNeededMet(t *testing.T) {
	// needed=3, fetchedSlices=0. batch = [v0,v1,v2] (shortfall 3 covered by
	// v0+v1, plus one slack). maxConc=2: wave 1 = [v0,v1] fetches 4 slices
	// >= needed(3) → stop before wave 2. nextVolIdx advances past the
	// attempted wave only (2), not the un-fetched slack volume.
	vols := []par2Volume{
		makeTestVol("v0.par2", 2, "a@news"),
		makeTestVol("v1.par2", 2, "b@news"),
		makeTestVol("v2.par2", 4, "c@news"),
		makeTestVol("v3.par2", 8, "d@news"),
	}
	fetch := mockVolumeFetch(map[string][]byte{
		"a@news": []byte("d0"), "b@news": []byte("d1"),
		"c@news": []byte("d2"), "d@news": []byte("d3"),
	}, nil)

	nextIdx, fetched, sources, added, err := fetchMoreVolumes(
		context.Background(), parallelFetchNopLogger, fetch, vols, 0, 3, 0, nil, "test-entry", 2,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if added != 2 || fetched != 4 || len(sources) != 2 || nextIdx != 2 {
		t.Fatalf("got added=%d fetched=%d sources=%d nextIdx=%d, want 2/4/2/2", added, fetched, len(sources), nextIdx)
	}
}
