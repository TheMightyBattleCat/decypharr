package manager

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/rs/zerolog"
)

// The patched total is what the pass wrote, not what the overlay had
// recorded when it started: a pass that began with one recorded segment and
// rebuilt many more it found itself used to report 1.
func TestPar2PassTallyCountsEverySegmentWritten(t *testing.T) {
	tally := &par2PassTally{recorded: 3}
	tally.addHealed(1)
	tally.addPatchedRecorded(1)
	tally.addPatchedRecorded(1)
	tally.addPatchedDiscovered(33)
	tally.setRounds(2)
	tally.setStatSweep("incomplete")

	if got := tally.patched(); got != 36 {
		t.Fatalf("patched() = %d, want 36 (1 healed + 2 recorded + 33 discovered)", got)
	}

	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	logger.Info().Func(tally.logFields).Msg("done")
	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("log line is not JSON: %v (%s)", err, buf.String())
	}
	want := map[string]any{
		"segments_patched":   float64(36),
		"segments_recorded":  float64(3),
		"patched_recorded":   float64(2),
		"healed":             float64(1),
		"patched_discovered": float64(33),
		"rounds":             float64(2),
		"stat_sweep":         "incomplete",
		// The journal prints only this on an info line.
		"note": "3 of 3 recorded patched • 1 fetched intact • 33 more found and patched • 2 full reads • damage check incomplete",
	}
	for k, v := range want {
		if line[k] != v {
			t.Errorf("log field %s = %v, want %v", k, line[k], v)
		}
	}
	parts := line["patched_recorded"].(float64) + line["healed"].(float64) + line["patched_discovered"].(float64)
	if parts != line["segments_patched"].(float64) {
		t.Errorf("segments_patched %v is not the sum of its parts %v", line["segments_patched"], parts)
	}
}

// Callers that do not want a tally (the live dry-run harness) pass nil.
func TestPar2PassTallyNilIsSafe(t *testing.T) {
	var tally *par2PassTally
	tally.addHealed(1)
	tally.addPatchedRecorded(1)
	tally.addPatchedDiscovered(1)
	tally.setRounds(1)
	tally.setStatSweep("complete")
	if got := tally.patched(); got != 0 {
		t.Fatalf("nil tally patched() = %d, want 0", got)
	}
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	logger.Info().Func(tally.logFields).Msg("done")
	if bytes.Contains(buf.Bytes(), []byte("segments_patched")) {
		t.Fatalf("nil tally added fields: %s", buf.String())
	}
}
