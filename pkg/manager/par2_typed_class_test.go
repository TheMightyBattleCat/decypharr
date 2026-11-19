package manager

import (
	"errors"
	"testing"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// PI-2: runRepair's failures carry their class. The text carries names taken
// from the release, and a name must not flip the verdict either way.

func TestPar2ClassIdentityMissIgnoresReleaseNames(t *testing.T) {
	const hostile = "Movie (transient: lol).mkv"

	err := par2UnmatchedDeadSegmentErr("<a@x>", hostile, hostile, false, nil)
	if c := classifyPar2Failure(err); !c.terminal || c.suspect {
		t.Errorf("identity miss on a file named %q: got %+v, want terminal", hostile, c)
	}
	if c := classifyPar2Failure(err); c.reason != err.Error() {
		t.Errorf("reason %q, want the error text %q", c.reason, err.Error())
	}

	// The retryable variants stay transient.
	if c := classifyPar2Failure(par2UnmatchedDeadSegmentErr("<a@x>", "Movie.mkv", "Movie.mkv", true, nil)); c.terminal || c.suspect {
		t.Errorf("posted file skipped by matching: got %+v, want transient", c)
	}
	indexErr := errors.New(`index file "x.par2" failed to fetch without a 430: i/o timeout`)
	if c := classifyPar2Failure(par2UnmatchedDeadSegmentErr("<a@x>", hostile, hostile, false, indexErr)); c.terminal || c.suspect {
		t.Errorf("index file failed to fetch: got %+v, want transient", c)
	}
}

func TestPar2ClassUnmeasuredGeometryTimeoutIgnoresReleaseNames(t *testing.T) {
	const name = "Show.S01E01.recovery slice.mkv"
	fileID := [16]byte{0x0c}
	refs := []par2DeadRef{{
		file: name,
		seg:  overlay.DeadSegment{Index: 0, MessageID: "<a@x>", Bytes: 50},
		rng:  postedRange{fileID: fileID, start: 0, end: 50},
	}}

	timeout := &nntp.Error{Type: nntp.ErrorTypeTimeout, Message: "i/o timeout"}
	_, err := splitPlaceableDeadRefs(map[[16]byte]*postedFileFetcher{fileID: {geometryErr: timeout}}, refs)
	if err == nil {
		t.Fatal("no error for a dead segment on unmeasured geometry")
	}
	if c := classifyPar2Failure(err); c.terminal || c.suspect {
		t.Errorf("geometry probe timed out on a file named %q: got %+v, want transient", name, c)
	}

	// A dead head article (430) under the same transient class stays terminal.
	gone := &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: "no such article"}
	_, err = splitPlaceableDeadRefs(map[[16]byte]*postedFileFetcher{fileID: {geometryErr: gone}}, refs)
	if c := classifyPar2Failure(err); !c.terminal {
		t.Errorf("geometry probe 430 on a file named %q: got %+v, want terminal", name, c)
	}
}

func TestPar2ClassMissingFetcherIgnoresReleaseNames(t *testing.T) {
	fid := [16]byte{0x0d}
	terminal := func([16]byte) (string, bool) {
		return `posted file "x (transient: y).7z.010": backing article confirmed missing across all providers`, true
	}
	if c := classifyPar2Failure(missingFetcherErr(fid, terminal)); !c.terminal {
		t.Errorf("structural miss naming a file with the transient tag: got %+v, want terminal", c)
	}
	transient := func([16]byte) (string, bool) {
		return `posted file "more damage than recorded.mkv" failed to fetch/hash during matching`, false
	}
	if c := classifyPar2Failure(missingFetcherErr(fid, transient)); c.terminal || c.suspect {
		t.Errorf("retryable miss naming a file with a terminal phrase: got %+v, want transient", c)
	}
}

// A round stop is classed by its causes, and a stream that stopped on a
// retryable error stays transient, as the "(transient:" tag made it before.
func TestPar2ClassRoundCapCauses(t *testing.T) {
	base := errors.New("par2: 3 intact slices unavailable, more than the 1 spare recovery slices can cover")
	cases := []struct {
		name                              string
		missing, corrupt, short, checksum int
		repairErr                         error
		wantTerminal, wantSuspect         bool
	}{
		{"confirmed missing", 2, 0, 0, 0, base, true, false},
		{"corrupt", 0, 1, 0, 0, base, true, false},
		{"short read only", 0, 0, 3, 0, base, false, true},
		{"bad checksum only", 0, 0, 0, 2, base, false, true},
		{"short read and missing", 1, 0, 3, 0, base, true, false},
		{"missing, stream stopped on a retryable miss", 2, 0, 0, 0,
			missingFetcherErr([16]byte{1}, func([16]byte) (string, bool) { return "posted file \"a\" failed to fetch/hash during matching", false }),
			false, false},
	}
	for _, tc := range cases {
		c := classifyPar2Failure(par2RoundCapError(tc.missing, tc.corrupt, tc.short, tc.checksum, tc.repairErr))
		if c.terminal != tc.wantTerminal || c.suspect != tc.wantSuspect {
			t.Errorf("%s: got terminal=%v suspect=%v, want terminal=%v suspect=%v",
				tc.name, c.terminal, c.suspect, tc.wantTerminal, tc.wantSuspect)
		}
	}
}
