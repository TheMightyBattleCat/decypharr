package nntp

import (
	"errors"
	"testing"
)

// checkAvailability reads Total - Found - Error as the missing count, so a
// result with neither Available nor an Error must count as an error ("could
// not check"), never as a confirmed miss.
func TestTallyStatResults(t *testing.T) {
	results := []StatResult{
		{MessageID: "a", Available: true},
		{MessageID: "b", Error: classifyNNTPError(430, "no such article")},
		{MessageID: "c", Error: NewConnectionError(errors.New("reset"))},
		{MessageID: "d"}, // zero value: neither found nor an error
	}
	found, errs := tallyStatResults(results)
	if found != 1 || errs != 2 {
		t.Fatalf("found=%d errs=%d, want found=1 errs=2 (the 430 is the only miss)", found, errs)
	}
}
