package manager

// A whole urgent pass on an obfuscated posting: the PAR2 files are posted
// under random names with no ".volNN+MM" marker, so nothing but their sizes
// tells the index from a recovery volume.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// The fixture's volumes are far under par2UnnamedVolumeFloor, so the census
// keeps every PAR2 file as an index file and their recovery slices arrive
// with the index. The pass must count those slices as recovery data in hand
// and repair, not stop at "no PAR2 recovery volumes retained".
func TestP2EPar2ObfuscatedPostingRepairs(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	srv := servers[0]
	f1, f2 := fx.posted[0], fx.posted[1]

	nzb := fx.nzb("p2e-obfuscated")
	for i := range nzb.Par2Files {
		pf := fx.par2[i]
		if nzb.Par2Files[i].Name != pf.name {
			t.Fatalf("Par2Files[%d] is %s, want %s (fixture order changed)", i, nzb.Par2Files[i].Name, pf.name)
		}
		name := fmt.Sprintf("%08x%08x.par2", 0x5eed0000+i*7919, 0xc0ffee^(i*104729))
		nzb.Par2Files[i].Name = name
		// Re-post the articles under the obfuscated name, so the yEnc
		// headers agree with the NZB record.
		for part, id := range pf.ids {
			off := int64(part * p2eArticleSize)
			srv.addArticle(&p2eArticle{
				id:   id,
				data: pf.payloads[part],
				body: p2eYenc(name, int64(len(pf.data)), part+1, len(pf.ids), off, pf.payloads[part]),
			})
		}
	}
	if vols, idx := censusPar2Volumes(nzb.Par2Files); len(vols) != 0 || len(idx) != len(nzb.Par2Files) {
		t.Fatalf("census: %d volumes, %d index files; want every obfuscated file kept as an index file (fixture volumes are under the floor)", len(vols), len(idx))
	}

	h := newP2eHarnessNZB(t, srv, fx, nzb)
	h.recordDead(p2eSeg{f1, 1}, p2eSeg{f2, 3})
	for _, s := range h.dead {
		h.srv.set(s.file.ids[s.idx], p2eNotFound)
	}
	o := h.run(nil)
	h.report(o)
	p2eCompleted(2).check(t, h, o)
	if o.Server.Body == 0 {
		t.Errorf("no BODY served; the pass did not run against the server")
	}
}

// p2eObfuscate re-posts every PAR2 file of nzb under a random-looking name
// with no ".volNN+MM" marker, so the yEnc headers agree with the NZB record.
func p2eObfuscate(t *testing.T, srv *p2eServer, fx *p2eFixture, nzb *storage.NZB) {
	t.Helper()
	for i := range nzb.Par2Files {
		pf := fx.par2[i]
		if nzb.Par2Files[i].Name != pf.name {
			t.Fatalf("Par2Files[%d] is %s, want %s (fixture order changed)", i, nzb.Par2Files[i].Name, pf.name)
		}
		name := fmt.Sprintf("%08x%08x.par2", 0x5eed0000+i*7919, 0xc0ffee^(i*104729))
		nzb.Par2Files[i].Name = name
		for part, id := range pf.ids {
			off := int64(part * p2eArticleSize)
			srv.addArticle(&p2eArticle{
				id:   id,
				data: pf.payloads[part],
				body: p2eYenc(name, int64(len(pf.data)), part+1, len(pf.ids), off, pf.payloads[part]),
			})
		}
	}
}

// p2eObfuscatedLargeVolumes is the release with one PAR2 set of an index
// and two equal-size volumes of 280 recovery slices of 16 KiB (about 4.6 MB
// each, over par2UnnamedVolumeFloor), posted under obfuscated names. It
// returns the NZB record and the volumes' article IDs.
func p2eObfuscatedLargeVolumes(t *testing.T, nzbID string) (*p2eServer, *p2eFixture, *storage.NZB, []string) {
	t.Helper()
	servers, fx := newP2eEnvSets(t, 1, p2eSet{
		base:  p2eRelease,
		files: []int{0, 1, 2},
		args:  []string{"-s16384", "-c560", "-n2"},
	})
	srv := servers[0]
	var volArticles []string
	for _, pf := range fx.par2 {
		if strings.Contains(pf.name, ".vol") {
			if len(pf.data) < par2UnnamedVolumeFloor {
				t.Fatalf("fixture volume %s is %d bytes, want at least the %d floor", pf.name, len(pf.data), par2UnnamedVolumeFloor)
			}
			volArticles = append(volArticles, pf.ids...)
		}
	}
	if len(fx.par2) != 3 || len(volArticles) == 0 {
		t.Fatalf("fixture has %d PAR2 files, want an index and two volumes", len(fx.par2))
	}

	nzb := fx.nzb(nzbID)
	p2eObfuscate(t, srv, fx, nzb)
	vols, idx := censusPar2Volumes(nzb.Par2Files)
	if len(vols) != 2 || len(idx) != 1 {
		t.Fatalf("census: %d volumes, %d index files; want both large obfuscated files as volumes and one index file", len(vols), len(idx))
	}
	if idx[0].Name != nzb.Par2Files[0].Name {
		t.Fatalf("census kept %s as the index file, want %s", idx[0].Name, nzb.Par2Files[0].Name)
	}
	return srv, fx, nzb, volArticles
}

// runP2eObfuscatedLargeVolumes kills dead on the server, runs the pass and
// checks it repaired with the volumes.
func runP2eObfuscatedLargeVolumes(t *testing.T, srv *p2eServer, fx *p2eFixture, nzb *storage.NZB, volArticles []string, dead ...p2eSeg) *p2eHarness {
	t.Helper()
	h := newP2eHarnessNZB(t, srv, fx, nzb)
	h.recordDead(dead...)
	for _, s := range h.dead {
		h.srv.set(s.file.ids[s.idx], p2eNotFound)
	}
	o := h.run(nil)
	h.report(o)
	p2eCompleted(len(dead)).check(t, h, o)
	var volBodies int64
	for _, id := range volArticles {
		volBodies += srv.bodyCount(id)
	}
	if volBodies == 0 {
		t.Errorf("no recovery-volume article was fetched; the repair did not use the volumes")
	}
	return h
}

// Recovery volumes over par2UnnamedVolumeFloor on an obfuscated posting are
// split out by size: the census takes them as recovery volumes (fetched
// smallest first, as far as the damage needs) and the index file alone as
// an index file. The census counts equal-size volumes at one slice each, far
// fewer than they hold. The damage here (two files of a length tie, so the
// early gate can't count it) is found in the solve; the pass must gate on
// the slices it parsed, not on the census, and repair.
func TestP2EPar2ObfuscatedPostingLargeVolumesRepairs(t *testing.T) {
	srv, fx, nzb, volArticles := p2eObfuscatedLargeVolumes(t, "p2e-obfuscated-large")
	runP2eObfuscatedLargeVolumes(t, srv, fx, nzb, volArticles, p2eSeg{fx.posted[0], 1}, p2eSeg{fx.posted[1], 3})
}

// The same posting with the damage on the one file of a unique length, so
// the early arithmetic gate counts it from the index before any volume is
// fetched: more damaged slices than the census's one slice per volume.
// Counted from the parsed slice size, the volumes hold 280 each and the
// gate lets the pass through to repair.
func TestP2EPar2ObfuscatedPostingLargeVolumesPassEarlyGate(t *testing.T) {
	srv, fx, nzb, volArticles := p2eObfuscatedLargeVolumes(t, "p2e-obfuscated-large-early")
	f3 := fx.posted[2]
	dead := p2eSeg{f3, 0}

	// The early gate must see this damage, and the census alone must be
	// short of it: else this case does not exercise the gate.
	index, err := par2.ParseIndex(context.Background(), []par2.Source{{Name: fx.par2[0].name, Data: fx.par2[0].data}})
	if err != nil {
		t.Fatalf("parse the fixture's index file: %v", err)
	}
	vols, _ := censusPar2Volumes(nzb.Par2Files)
	var census uint32
	for _, v := range vols {
		census += v.count
	}
	pending := map[string][]overlay.DeadSegment{f3.name: {{MessageID: f3.ids[dead.idx]}}}
	earlyK := earlyDamagedSliceCheck(index, pending, nzb.Par2Source, nzb.Par2Match, zerolog.Nop())
	if earlyK == 0 || uint32(earlyK) <= census {
		t.Fatalf("early check counts %d damaged slices against a census of %d; want more than the census", earlyK, census)
	}
	h := runP2eObfuscatedLargeVolumes(t, srv, fx, nzb, volArticles, dead)
	for _, ln := range h.logs.lines() {
		if msg, _ := ln["message"].(string); strings.Contains(msg, "early arithmetic check") {
			t.Errorf("the early gate declined: %v", ln)
		}
	}
}
