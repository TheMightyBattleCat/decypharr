package usenet

import (
	"context"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
)

type SampleVerdict string

const (
	VerdictClean       SampleVerdict = "clean"
	VerdictBroken      SampleVerdict = "broken"
	VerdictInconclusive SampleVerdict = "inconclusive"
)

type SegmentRef struct {
	Index     int
	MessageID string
}

type SampleOpts struct {
	SampleSize         int
	EarlyAbortFailures int
	EarlyAbortPasses   int
	MaxBudgetBytes     int64
}

func (o SampleOpts) withDefaults() SampleOpts {
	if o.SampleSize <= 0 {
		o.SampleSize = 64
	}
	if o.EarlyAbortFailures <= 0 {
		o.EarlyAbortFailures = 3
	}
	if o.EarlyAbortPasses <= 0 {
		o.EarlyAbortPasses = 32
	}
	return o
}

type AbortReason string

const (
	AbortNone           AbortReason = ""
	AbortFailureLimit   AbortReason = "failure_limit"
	AbortCleanStreakDead AbortReason = "clean_streak_with_dead_recovered"
)

type SampleResult struct {
	Verdict              SampleVerdict
	SegmentsSampled      int
	SegmentsFailed       int
	SegmentsPassed       int
	RecordedDeadRecovered int
	ExtrapolatedDeadCount int
	CoverageFraction     float64
	EarlyAbortReason     AbortReason
}

// SampleFileDamage probes a file's segment health by fetching a stratified
// sample of its segments through the provided callback. It always tests every
// known-dead segment first (the recovery question), then fills remaining
// sample slots with segments spread evenly across the full range so dead-
// article runs—which cluster on disk—are detected more efficiently per byte
// than a contiguous head-region sample would.
func SampleFileDamage(
	ctx context.Context,
	segments []SegmentRef,
	recordedDead []int,
	fetchBody func(ctx context.Context, seg SegmentRef) error,
	opts SampleOpts,
) SampleResult {
	opts = opts.withDefaults()
	total := len(segments)

	if total == 0 {
		return SampleResult{Verdict: VerdictClean}
	}

	selected := selectSample(segments, recordedDead, opts.SampleSize)

	deadSet := make(map[int]struct{}, len(recordedDead))
	for _, idx := range recordedDead {
		deadSet[idx] = struct{}{}
	}

	type probeResult struct {
		index  int
		failed bool
	}

	const concurrency = 4
	work := make(chan SegmentRef, len(selected))
	results := make(chan probeResult, len(selected))

	// Feed the work channel: dead segments first (unshuffled), then the
	// shuffled non-dead portion. selectSample returns them in this order.
	for _, seg := range selected {
		work <- seg
	}
	close(work)

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	var aborted atomic.Bool

	var wg sync.WaitGroup
	for range min(concurrency, len(selected)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seg := range work {
				if aborted.Load() {
					return
				}
				err := fetchBody(ctx, seg)
				results <- probeResult{index: seg.Index, failed: err != nil}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var (
		sampled           int
		failed            int
		passed            int
		deadRecovered     int
		consecutivePasses int
		allDeadPassed     = true
		abortReason       AbortReason
	)

	deadChecked := make(map[int]bool, len(deadSet))

	for r := range results {
		sampled++

		if r.failed {
			failed++
			consecutivePasses = 0
			if _, isDead := deadSet[r.index]; isDead {
				deadChecked[r.index] = false
				allDeadPassed = false
			}

			if failed >= opts.EarlyAbortFailures {
				abortReason = AbortFailureLimit
				aborted.Store(true)
				cancel()
				// Drain remaining results already in flight.
				for extra := range results {
					sampled++
					if extra.failed {
						failed++
					} else {
						passed++
						if _, isDead := deadSet[extra.index]; isDead {
							if _, seen := deadChecked[extra.index]; !seen {
								deadChecked[extra.index] = true
								deadRecovered++
							}
						}
					}
				}
				break
			}
		} else {
			passed++
			consecutivePasses++

			if _, isDead := deadSet[r.index]; isDead {
				if _, seen := deadChecked[r.index]; !seen {
					deadChecked[r.index] = true
					deadRecovered++
				}
			}

			// Recompute allDeadPassed: true only if every dead segment we've
			// seen so far has passed.
			if allDeadPassed {
				for _, ok := range deadChecked {
					if !ok {
						allDeadPassed = false
						break
					}
				}
			}

			allDeadSeen := len(deadChecked) == len(deadSet)
			deadCondition := len(deadSet) == 0 || (allDeadSeen && allDeadPassed)

			if consecutivePasses >= opts.EarlyAbortPasses && deadCondition {
				abortReason = AbortCleanStreakDead
				aborted.Store(true)
				cancel()
				for extra := range results {
					sampled++
					if extra.failed {
						failed++
					} else {
						passed++
						if _, isDead := deadSet[extra.index]; isDead {
							if _, seen := deadChecked[extra.index]; !seen {
								deadChecked[extra.index] = true
								deadRecovered++
							}
						}
					}
				}
				break
			}
		}
	}

	coverage := float64(sampled) / float64(total)
	extrapolated := 0
	if sampled > 0 {
		rate := float64(failed) / float64(sampled)
		extrapolated = int(math.Round(rate * float64(total)))
	}

	verdict := VerdictInconclusive
	switch {
	case abortReason == AbortFailureLimit:
		verdict = VerdictBroken
	case abortReason == AbortCleanStreakDead:
		verdict = VerdictClean
	case failed == 0:
		verdict = VerdictClean
	case failed > 0 && float64(failed)/float64(sampled) > 0.5:
		verdict = VerdictBroken
	}

	return SampleResult{
		Verdict:               verdict,
		SegmentsSampled:       sampled,
		SegmentsFailed:        failed,
		SegmentsPassed:        passed,
		RecordedDeadRecovered: deadRecovered,
		ExtrapolatedDeadCount: extrapolated,
		CoverageFraction:      coverage,
		EarlyAbortReason:      abortReason,
	}
}

// selectSample builds a deduplicated, ordered list of segments to probe.
// Dead segments come first (in their original order), followed by a shuffled
// spread sample drawn from the rest of the range.
func selectSample(segments []SegmentRef, recordedDead []int, sampleSize int) []SegmentRef {
	total := len(segments)
	if total == 0 {
		return nil
	}

	indexMap := make(map[int]SegmentRef, total)
	for _, s := range segments {
		indexMap[s.Index] = s
	}

	chosen := make(map[int]struct{}, sampleSize)
	var deadPart []SegmentRef

	// Always include every recorded-dead segment.
	for _, idx := range recordedDead {
		if _, exists := indexMap[idx]; !exists {
			continue
		}
		if _, dup := chosen[idx]; dup {
			continue
		}
		chosen[idx] = struct{}{}
		deadPart = append(deadPart, indexMap[idx])
	}

	remaining := sampleSize - len(chosen)
	if remaining <= 0 {
		return deadPart
	}

	// Reserve a few slots for the header region (first ~1% of segments).
	headerEnd := max(1, total/100)
	headerSlots := max(1, min(remaining/8, headerEnd))

	addIfNew := func(idx int) {
		if _, dup := chosen[idx]; dup {
			return
		}
		if _, exists := indexMap[idx]; !exists {
			return
		}
		chosen[idx] = struct{}{}
	}

	// Header region: spread within [0, headerEnd).
	for i := range headerSlots {
		idx := 0
		if headerEnd > 1 {
			idx = i * (headerEnd - 1) / max(1, headerSlots-1)
		}
		if idx >= total {
			continue
		}
		addIfNew(idx)
	}

	// Spread evenly across the full range for the rest.
	spreadSlots := sampleSize - len(chosen)
	if spreadSlots > 0 {
		step := float64(total-1) / float64(spreadSlots)
		for i := range spreadSlots {
			idx := int(math.Round(float64(i) * step))
			if idx >= total {
				idx = total - 1
			}
			addIfNew(idx)
		}
	}

	// If we still have room (dedup removed some), fill gaps.
	if len(chosen) < sampleSize && len(chosen) < total {
		for idx := range total {
			if len(chosen) >= sampleSize {
				break
			}
			addIfNew(idx)
		}
	}

	var spreadPart []SegmentRef
	for idx := range chosen {
		if _, isDead := func() (struct{}, bool) {
			for _, d := range recordedDead {
				if d == idx {
					return struct{}{}, true
				}
			}
			return struct{}{}, false
		}(); isDead {
			continue
		}
		spreadPart = append(spreadPart, indexMap[idx])
	}

	rand.Shuffle(len(spreadPart), func(i, j int) {
		spreadPart[i], spreadPart[j] = spreadPart[j], spreadPart[i]
	})

	return append(deadPart, spreadPart...)
}

