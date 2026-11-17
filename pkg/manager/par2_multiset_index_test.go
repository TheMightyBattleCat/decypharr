package manager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// P-13: a season pack with one PAR2 set per episode. The index file of the
// set protecting the damaged episode did not fetch, so the first choice sees
// only episode 1's set and changes nothing. Once a recovery volume of episode
// 2's set is among the fetched files the pass must choose episode 2's set,
// narrow the volumes to it and parse an index that holds the damaged file.
// Before, the pass parsed every fetched file as one set: "belongs to a
// different recovery set", or episode 1's set and "not part of any matched
// posted file", both terminal and a re-grab of the whole pack.
func TestPar2SetChosenFromVolumesWhenAnIndexIsMissing(t *testing.T) {
	sources, set1, set2, vols, posted := multisetFixture(t)
	ep1Index := sources[1]
	// The damage is in episode 2.
	pending := map[string][]overlay.DeadSegment{"e02.rar": {{Index: 0, MessageID: "<e02-1>"}}}

	// Episode 2's index failed: the first choice, on index files alone, has
	// one set and leaves everything as it is.
	first := choosePar2Set(context.Background(), []par2.Source{ep1Index}, vols, posted, pending)
	if first.setID != ([16]byte{}) || first.unattributed != 0 {
		t.Fatalf("first choice on one index: set %x unattributed %d, want no choice", first.setID, first.unattributed)
	}
	if _, _, ok := par2SetAfterVolumes(context.Background(), []par2.Source{ep1Index}, vols, 0, posted, pending); ok {
		t.Fatal("one set visible: par2SetAfterVolumes should change nothing")
	}

	// The first fetch took vols[0] (episode 1's) and vols[1] (episode 2's);
	// episode 2's volume carries its set's Main and FileDesc packets.
	ep2Vol, _ := otherSetIndex("Show.S01E02.vol00+01.par2", "e02.rar")
	fetched := []par2.Source{ep1Index, ep2Vol}
	c, next, ok := par2SetAfterVolumes(context.Background(), fetched, vols, 2, posted, pending)
	if !ok || c.setID != set2 {
		t.Fatalf("after the volumes: ok=%v set %x, want episode 2's set %x", ok, c.setID, set2)
	}
	if len(c.sources) != 1 || c.sources[0].Name != ep2Vol.Name {
		t.Fatalf("sources = %v, want only episode 2's volume", par2SourceNames(c.sources))
	}
	if len(c.vols) != 1 || c.vols[0].ref.Name != "Show.S01E02.vol00+01.par2" || next != 1 {
		t.Fatalf("vols = %v next=%d, want episode 2's one volume, already fetched", c.vols, next)
	}
	if len(c.pending["e02.rar"]) != 1 || c.deferred != 0 {
		t.Fatalf("pending = %v deferred=%d, want the e02.rar segment kept", c.pending, c.deferred)
	}
	idx, err := par2.ParseIndexSet(context.Background(), c.sources, c.setID)
	if err != nil {
		t.Fatalf("parse PAR2 index on the chosen set: %v", err)
	}
	if !par2IndexHasFile(idx, "e02.rar") {
		t.Fatal("the parsed index does not protect e02.rar")
	}

	// Damage in episode 1 instead: its set is chosen, episode 2's volume is
	// dropped, and later fetches resume after episode 1's one tried volume
	// (vols[1] was episode 2's and is gone from the narrowed list).
	pending1 := map[string][]overlay.DeadSegment{"a": {{MessageID: posted[0].Segments[0].MessageID}}}
	c, next, ok = par2SetAfterVolumes(context.Background(), fetched, vols, 2, posted, pending1)
	if !ok || c.setID != set1 {
		t.Fatalf("damage in episode 1: ok=%v set %x, want %x", ok, c.setID, set1)
	}
	if len(c.vols) != 2 || c.vols[0].ref.Name != "fixture.vol0+1.par2" || c.vols[1].ref.Name != "fixture.vol1+2.par2" || next != 1 {
		t.Fatalf("vols = %v next=%d, want episode 1's two volumes with the first already fetched", c.vols, next)
	}
}

// Without the re-choice, the volumes' mixed list parses as one set (the
// first Main packet's) and the damaged file is not in it. This pins the
// parse half: a mixed list no longer fails outright.
func TestPar2MixedSourcesParseAsTheFirstMainPacketsSet(t *testing.T) {
	sources, set1, _, _, _ := multisetFixture(t)
	ep1Index := sources[1]
	ep2Vol, _ := otherSetIndex("Show.S01E02.vol00+01.par2", "e02.rar")
	idx, err := par2.ParseIndexSet(context.Background(), []par2.Source{ep1Index, ep2Vol}, [16]byte{})
	if err != nil {
		t.Fatalf("parse PAR2 index over a mixed list: %v", err)
	}
	if idx.SetID != set1 || par2IndexHasFile(idx, "e02.rar") || idx.OtherSetPackets == 0 {
		t.Fatalf("set %x e02=%v other=%d, want episode 1's set with episode 2's packets skipped", idx.SetID, par2IndexHasFile(idx, "e02.rar"), idx.OtherSetPackets)
	}
}

// An index file that times out says nothing about whether the set it
// describes exists: the set-dependent failures that follow are transient.
// An index file whose articles are all 430 is gone, and they stay terminal.
func TestPar2IndexFetchTimeoutMakesSetFailuresTransient(t *testing.T) {
	old := par2ArticleFetchTimeout
	par2ArticleFetchTimeout = 50 * time.Millisecond
	t.Cleanup(func() { par2ArticleFetchTimeout = old })

	gone := storage.Par2FileRef{Name: "Show.S01E01.par2", Size: 100, Segments: []storage.Par2SegmentRef{{MessageID: "<gone>", Bytes: 100}}}
	slow := storage.Par2FileRef{Name: "Show.S01E02.par2", Size: 100, Segments: []storage.Par2SegmentRef{{MessageID: "<slow>", Bytes: 100}}}
	good := storage.Par2FileRef{Name: "Show.S01E03.par2", Size: 4, Segments: []storage.Par2SegmentRef{{MessageID: "<good>", Bytes: 4}}}
	fetch := func(ctx context.Context, id string) ([]byte, error) {
		switch id {
		case "<gone>":
			return nil, &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: "no such article"}
		case "<slow>":
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return []byte("PAR2"), nil
	}
	ctx := context.Background()

	srcs, indexErr := fetchPar2IndexFiles(ctx, zerolog.Nop(), fetch, []storage.Par2FileRef{gone, good}, "e")
	if indexErr != nil || len(srcs) != 1 || srcs[0].Name != good.Name {
		t.Fatalf("430 index: sources %v err %v, want the good file and no error", par2SourceNames(srcs), indexErr)
	}
	for _, e := range par2SetDependentFailures() {
		if class := classifyPar2Failure(par2IndexFetchTransient(e, indexErr)); !class.terminal {
			t.Errorf("index 430'd: %q classified %+v, want terminal", e, class)
		}
	}

	srcs, indexErr = fetchPar2IndexFiles(ctx, zerolog.Nop(), fetch, []storage.Par2FileRef{gone, slow, good}, "e")
	if indexErr == nil || !strings.Contains(indexErr.Error(), slow.Name) || len(srcs) != 1 {
		t.Fatalf("timed-out index: sources %v err %v, want the good file and an error naming %s", par2SourceNames(srcs), indexErr, slow.Name)
	}
	for _, e := range par2SetDependentFailures() {
		tagged := par2IndexFetchTransient(e, indexErr)
		if class := classifyPar2Failure(tagged); class.terminal || class.suspect {
			t.Errorf("index timed out: %q classified %+v, want transient", tagged, class)
		}
		if !strings.HasPrefix(tagged.Error(), e.Error()) {
			t.Errorf("tagged %q lost the original message %q", tagged, e)
		}
	}

	// A job that was cancelled is not an index fetch failure.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, indexErr = fetchPar2IndexFiles(cctx, zerolog.Nop(), fetch, []storage.Par2FileRef{slow}, "e"); indexErr != nil {
		t.Fatalf("cancelled job: indexErr %v, want nil", indexErr)
	}
}

// The three runRepair failures that depend on which set the pass works on.
func par2SetDependentFailures() []error {
	return []error{
		fmt.Errorf("parse PAR2 index: %w", errors.New("par2: no Main packet found")),
		errors.New("no posted file matched the PAR2 recovery set"),
		fmt.Errorf("dead segment %s (file %q) is not part of any matched posted file", "<e02-1>", "e02.rar"),
	}
}

func par2IndexHasFile(idx *par2.Index, name string) bool {
	for _, fd := range idx.Files {
		if fd.Name == name {
			return true
		}
	}
	return false
}

func par2SourceNames(srcs []par2.Source) []string {
	out := make([]string, len(srcs))
	for i, s := range srcs {
		out[i] = s.Name
	}
	return out
}

// Verify checks one posted file against its recovery set. With several sets
// and none tied to the file it must not parse the first set it meets: a
// same-length file of another episode there would match by length and turn
// an unchecked file into a "whole-file MD5 mismatch".
func TestPar2VerifySetRefusesAnUntiedMix(t *testing.T) {
	sources, set1, set2, _, posted := multisetFixture(t)
	e02 := posted[len(posted)-1]

	if got, err := par2VerifySet(context.Background(), sources, posted, e02, "e02.rar"); err != nil || got != set2 {
		t.Fatalf("tied to episode 2: set %x err %v, want %x", got, err, set2)
	}
	if got, err := par2VerifySet(context.Background(), sources, posted, posted[0], posted[0].Name); err != nil || got != set1 {
		t.Fatalf("tied to episode 1: set %x err %v, want %x", got, err, set1)
	}
	if got, err := par2VerifySet(context.Background(), sources[1:], posted, posted[0], posted[0].Name); err != nil || got != ([16]byte{}) {
		t.Fatalf("one set: set %x err %v, want zero and no error", got, err)
	}
	stray := storage.PostedFileRef{Name: "e09.rar", Size: 1, Segments: []storage.Par2SegmentRef{{MessageID: "<e09-1>"}}}
	if _, err := par2VerifySet(context.Background(), sources, append(posted, stray), stray, stray.Name); err == nil || !strings.Contains(err.Error(), "2 PAR2 recovery sets") {
		t.Fatalf("untied file over two sets: err %v, want the several-sets error", err)
	}
}
