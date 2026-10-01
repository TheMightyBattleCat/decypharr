package nntp

import "testing"

// statRoundTrips is how many pipelined exchanges one chunk of n STATs costs
// on a provider: one per statPipelineDepth IDs, the last one part filled.
func statRoundTrips(n int) int {
	return (n + statPipelineDepth - 1) / statPipelineDepth
}

// Does lowering the chunk ceiling from 50 to 48 save round trips? Only when
// the ceiling is the size picked, and pickStatBatchSize picks it only for a
// call of at least workers*3*ceiling IDs. The table shows the chunk size and
// the round trips per 1,000 IDs for a range of file sizes, for the ceiling
// as it is (50), the proposed 48, and a third option the numbers point to:
// rounding whatever size is picked to a whole number of pipeline windows.
func TestFollowupStatBatchCeiling(t *testing.T) {
	const (
		workers = 330 // the pool on the production box, to the nearest ten
		minSize = 10
	)
	aligned := func(size int) int {
		// nearest whole number of windows, at least one
		w := (size + statPipelineDepth/2) / statPipelineDepth
		return max(w, 1) * statPipelineDepth
	}
	perThousand := func(size int) float64 {
		return 1000 * float64(statRoundTrips(size)) / float64(size)
	}

	files := []struct {
		name string
		ids  int
	}{
		{"0.7 GB episode", 1000},
		{"2 GB episode", 3000},
		{"8 GB film", 11500},
		{"25 GB REMUX", 35000},
		{"40 GB REMUX", 57000},
		{"70 GB UHD REMUX", 100000},
	}
	changed48 := 0
	for _, f := range files {
		s50 := pickStatBatchSize(f.ids, workers, 50, minSize)
		s48 := pickStatBatchSize(f.ids, workers, 48, minSize)
		sa := aligned(s50)
		t.Logf("%-16s ids=%-6d ceiling 50: chunk %2d, %5.1f trips/1000 | ceiling 48: chunk %2d, %5.1f | whole windows: chunk %2d, %5.1f",
			f.name, f.ids, s50, perThousand(s50), s48, perThousand(s48), sa, perThousand(sa))
		if s48 != s50 {
			changed48++
		}
	}
	// 48 only changes files of about 50,000 articles and up (35 GB+).
	if changed48 != 2 {
		t.Errorf("ceiling 48 changed %d of %d sizes, recorded 2", changed48, len(files))
	}
	// A chunk of 10 (every file up to about 7 GB) is one part-filled window
	// either way; a mid-size chunk wastes most of a window.
	if got := statRoundTrips(pickStatBatchSize(35000, workers, 50, minSize)); got != 3 {
		t.Errorf("25 GB REMUX chunk costs %d round trips, recorded 3", got)
	}
	if got := statRoundTrips(aligned(pickStatBatchSize(35000, workers, 50, minSize))); got != 2 {
		t.Errorf("aligned 25 GB REMUX chunk costs %d round trips, recorded 2", got)
	}
}

// Why the hint does not help a file's first chunks, and why a small probe
// needs a rule of its own. A call's chunks all start before any has an
// answer, so each reads an empty hint and asks its home provider first.
func TestFollowupStatHintEmptyWhenChunksStartTogether(t *testing.T) {
	const absentHost, chunkSize = "absent.example", 10
	newHint := func() *statHint {
		return &statHint{byHost: map[string]*statHintCounts{absentHost: {}}}
	}

	// 80 chunks start together (80 workers homed on the absent provider).
	hint := newHint()
	paid := 0
	for range 80 {
		if !hint.absent(absentHost) {
			paid += chunkSize // this chunk asks the absent provider first
		}
	}
	for range 80 {
		hint.add(absentHost, chunkSize, 0) // the answers arrive afterwards
	}
	if paid != 800 {
		t.Fatalf("slow misses paid before the hint could act = %d, recorded 800", paid)
	}
	if !hint.absent(absentHost) {
		t.Fatal("hint should mark the provider absent once the answers are in")
	}
	t.Logf("burst start: %d slow misses paid; in the 2026-10-01 sweep the hint fired with a median of 800 answers", paid)

	// A probe of 4 IDs before the fan-out does not reach statHintMinAnswers,
	// so fed through the hint as it stands it changes nothing.
	probe := newHint()
	probe.add(absentHost, 4, 0)
	if probe.absent(absentHost) {
		t.Fatal("a 4-ID probe now marks a provider absent; update this note")
	}
	t.Logf("a 4-ID probe leaves the hint unset (needs %d answers): the probe needs its own rule, e.g. another provider answered a hit first", statHintMinAnswers)
}
