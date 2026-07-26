package usenet

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func makeSegments(n int) []SegmentRef {
	segs := make([]SegmentRef, n)
	for i := range n {
		segs[i] = SegmentRef{Index: i, MessageID: fmt.Sprintf("msg-%d@test", i)}
	}
	return segs
}

func alwaysPass(_ context.Context, _ SegmentRef) error { return nil }

func alwaysFail(_ context.Context, _ SegmentRef) error { return errors.New("430") }

func failSet(dead map[int]struct{}) func(context.Context, SegmentRef) error {
	return func(_ context.Context, seg SegmentRef) error {
		if _, ok := dead[seg.Index]; ok {
			return errors.New("430")
		}
		return nil
	}
}

func TestStratificationCoversFullRange(t *testing.T) {
	segs := makeSegments(1000)
	dead := []int{50, 500, 950}

	selected := selectSample(segs, dead, 64)

	seen := make(map[int]struct{}, len(selected))
	for _, s := range selected {
		seen[s.Index] = struct{}{}
	}

	for _, d := range dead {
		if _, ok := seen[d]; !ok {
			t.Errorf("recorded-dead segment %d missing from sample", d)
		}
	}

	var minIdx, maxIdx int
	for _, s := range selected {
		if s.Index < minIdx {
			minIdx = s.Index
		}
		if s.Index > maxIdx {
			maxIdx = s.Index
		}
	}

	if maxIdx < 900 {
		t.Errorf("sample max index %d does not reach the tail of 1000 segments", maxIdx)
	}
	if minIdx > 10 {
		t.Errorf("sample min index %d does not cover the header region", minIdx)
	}
}

func TestStratificationAlwaysIncludesRecordedDead(t *testing.T) {
	segs := makeSegments(200)
	dead := []int{0, 5, 100, 199}

	selected := selectSample(segs, dead, 10)

	seen := make(map[int]struct{}, len(selected))
	for _, s := range selected {
		seen[s.Index] = struct{}{}
	}

	for _, d := range dead {
		if _, ok := seen[d]; !ok {
			t.Errorf("recorded-dead segment %d missing from sample", d)
		}
	}
}

func TestEarlyAbortFailureLimit(t *testing.T) {
	segs := makeSegments(500)

	result := SampleFileDamage(context.Background(), segs, nil, alwaysFail, SampleOpts{
		SampleSize:         64,
		EarlyAbortFailures: 3,
		EarlyAbortPasses:   32,
	})

	if result.Verdict != VerdictBroken {
		t.Errorf("expected BROKEN, got %s", result.Verdict)
	}
	if result.EarlyAbortReason != AbortFailureLimit {
		t.Errorf("expected abort reason failure_limit, got %s", result.EarlyAbortReason)
	}
	if result.SegmentsFailed < 3 {
		t.Errorf("expected at least 3 failures, got %d", result.SegmentsFailed)
	}
}

func TestEarlyAbortCleanStreakWithDeadRecovered(t *testing.T) {
	segs := makeSegments(500)
	dead := []int{10, 20}

	result := SampleFileDamage(context.Background(), segs, dead, alwaysPass, SampleOpts{
		SampleSize:         64,
		EarlyAbortFailures: 3,
		EarlyAbortPasses:   32,
	})

	if result.Verdict != VerdictClean {
		t.Errorf("expected CLEAN, got %s", result.Verdict)
	}
	if result.EarlyAbortReason != AbortCleanStreakDead {
		t.Errorf("expected abort reason clean_streak_with_dead_recovered, got %s", result.EarlyAbortReason)
	}
	if result.RecordedDeadRecovered != 2 {
		t.Errorf("expected 2 dead recovered, got %d", result.RecordedDeadRecovered)
	}
}

func TestMixedResultsNoEarlyAbort(t *testing.T) {
	segs := makeSegments(100)
	// Fail just 2 segments — below the early-abort threshold of 3.
	deadIndices := map[int]struct{}{25: {}, 75: {}}

	result := SampleFileDamage(context.Background(), segs, nil, failSet(deadIndices), SampleOpts{
		SampleSize:         100,
		EarlyAbortFailures: 3,
		EarlyAbortPasses:   200,
	})

	if result.EarlyAbortReason != AbortNone {
		t.Errorf("expected no early abort, got %s", result.EarlyAbortReason)
	}
	if result.SegmentsFailed != 2 {
		t.Errorf("expected 2 failures, got %d", result.SegmentsFailed)
	}
	if result.SegmentsPassed != 98 {
		t.Errorf("expected 98 passes, got %d", result.SegmentsPassed)
	}

	expectedExtrapolated := 2 // (2/100)*100 = 2
	if result.ExtrapolatedDeadCount != expectedExtrapolated {
		t.Errorf("expected extrapolated %d, got %d", expectedExtrapolated, result.ExtrapolatedDeadCount)
	}
}

func TestEmptyRecordedDead(t *testing.T) {
	segs := makeSegments(200)

	result := SampleFileDamage(context.Background(), segs, nil, alwaysPass, SampleOpts{
		SampleSize:         32,
		EarlyAbortFailures: 3,
		EarlyAbortPasses:   32,
	})

	if result.Verdict != VerdictClean {
		t.Errorf("expected CLEAN, got %s", result.Verdict)
	}
	if result.SegmentsSampled != 32 {
		t.Errorf("expected 32 sampled, got %d", result.SegmentsSampled)
	}
}

func TestSingleSegmentFile(t *testing.T) {
	segs := makeSegments(1)

	t.Run("pass", func(t *testing.T) {
		result := SampleFileDamage(context.Background(), segs, nil, alwaysPass, SampleOpts{})
		if result.Verdict != VerdictClean {
			t.Errorf("expected CLEAN, got %s", result.Verdict)
		}
		if result.SegmentsSampled != 1 {
			t.Errorf("expected 1 sampled, got %d", result.SegmentsSampled)
		}
	})

	t.Run("fail", func(t *testing.T) {
		result := SampleFileDamage(context.Background(), segs, nil, alwaysFail, SampleOpts{})
		if result.Verdict != VerdictBroken {
			t.Errorf("expected BROKEN, got %s", result.Verdict)
		}
	})

	t.Run("single_dead", func(t *testing.T) {
		result := SampleFileDamage(context.Background(), segs, []int{0}, alwaysPass, SampleOpts{})
		if result.Verdict != VerdictClean {
			t.Errorf("expected CLEAN, got %s", result.Verdict)
		}
		if result.RecordedDeadRecovered != 1 {
			t.Errorf("expected 1 dead recovered, got %d", result.RecordedDeadRecovered)
		}
	})
}

func TestDeduplicationNearStart(t *testing.T) {
	segs := makeSegments(200)
	// Dead segment at index 0 — which the header region also covers.
	dead := []int{0}

	selected := selectSample(segs, dead, 64)

	counts := make(map[int]int)
	for _, s := range selected {
		counts[s.Index]++
	}

	for idx, n := range counts {
		if n > 1 {
			t.Errorf("segment %d appears %d times in sample (should be 1)", idx, n)
		}
	}

	if _, ok := counts[0]; !ok {
		t.Error("dead segment 0 missing from sample")
	}
}

func TestEmptySegments(t *testing.T) {
	result := SampleFileDamage(context.Background(), nil, nil, alwaysPass, SampleOpts{})
	if result.Verdict != VerdictClean {
		t.Errorf("expected CLEAN for empty input, got %s", result.Verdict)
	}
	if result.SegmentsSampled != 0 {
		t.Errorf("expected 0 sampled for empty input, got %d", result.SegmentsSampled)
	}
}

func TestExtrapolationWithLargeSampleSize(t *testing.T) {
	segs := makeSegments(1000)
	// Fail every 10th segment.
	deadSet := make(map[int]struct{})
	for i := 0; i < 1000; i += 10 {
		deadSet[i] = struct{}{}
	}

	result := SampleFileDamage(context.Background(), segs, nil, failSet(deadSet), SampleOpts{
		SampleSize:         1000,
		EarlyAbortFailures: 200,
		EarlyAbortPasses:   2000,
	})

	if result.ExtrapolatedDeadCount != 100 {
		t.Errorf("expected extrapolated 100, got %d", result.ExtrapolatedDeadCount)
	}
}

func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	segs := makeSegments(100)
	result := SampleFileDamage(ctx, segs, nil, func(ctx context.Context, _ SegmentRef) error {
		return ctx.Err()
	}, SampleOpts{SampleSize: 64, EarlyAbortFailures: 3})

	if result.Verdict == VerdictClean {
		t.Error("cancelled context should not produce CLEAN verdict")
	}
}
