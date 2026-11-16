package manager

import (
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// An urgent pass over a two-file release: part 1's only recorded dead
// article is alive on the second provider, so the pass patches it; part 2 has
// more dead articles than the recovery set can rebuild, so the pass ends
// terminal. The immediate re-grab must cover part 2 only: part 1 was repaired
// by this same pass and keeps its import.
func TestPar2TerminalRegrabSkipsFilesThePassPatched(t *testing.T) {
	servers, fx := newP2eEnv(t, 2)
	h := newP2eHarness(t, servers[0], fx, "p2e-terminal-regrab")
	f1, f2 := fx.posted[0], fx.posted[1]

	// An Arr for the entry's category, so regrabOnTerminal reaches its
	// per-file loop. Every call is answered 404: the re-grab itself stops in
	// repairPlaybackFileNow, after the file has reached it.
	arrSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.NotFound(w, nil)
	}))
	t.Cleanup(arrSrv.Close)
	h.p.manager.arr.AddOrUpdate(arr.New("p2e", arrSrv.URL, "token", false, nil, "", ""))

	h.recordDead(p2eSeg{f1, 1}, p2eSeg{f2, 1})
	servers[1].set(f1.ids[1], p2eNotFound) // recorded dead: gone on one provider, alive on the other
	for _, srv := range servers {
		for _, id := range f2.ids[1:4] { // three articles: more slices than the recovery set holds
			srv.set(id, p2eNotFound)
		}
	}

	o := h.run(nil)
	h.report(o)

	if o.LogMsg != "par2 repair unavailable" || o.Class != "terminal" {
		t.Fatalf("pass ended %q class=%q err=%q, want unavailable and terminal", o.LogMsg, o.Class, o.Err)
	}
	pend, err := h.u.OverlayPendingRepair(h.nzbID)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pend[f1.name]) != 0 || len(pend[f2.name]) == 0 {
		t.Fatalf("pending after the pass = %+v, want %s healed and %s still damaged", pend, f1.name, f2.name)
	}

	// Each file regrabOnTerminal hands to repairPlaybackFileNow logs one
	// "initiating" line before the call and one outcome line after it.
	var initiated, reached []string
	for _, ln := range h.logs.lines() {
		file, _ := ln["file"].(string)
		switch ln["message"] {
		case "par2 repair: terminal verdict on urgent lane; initiating immediate regrab":
			initiated = append(initiated, file)
		case "par2 repair: immediate regrab did not proceed", "par2 repair: immediate regrab skipped":
			reached = append(reached, file)
		}
	}
	sort.Strings(initiated)
	sort.Strings(reached)
	want := []string{f2.name}
	if strings.Join(initiated, ",") != strings.Join(want, ",") || strings.Join(reached, ",") != strings.Join(want, ",") {
		t.Fatalf("re-grab initiated for %v, reached repairPlaybackFileNow for %v; want only %v (the file the pass could not repair)", initiated, reached, want)
	}
}

// When the pass patched every recorded dead segment before ending terminal,
// the re-read finds nothing pending and nothing is re-grabbed: the entry's
// name is not used in place of a file.
func TestPar2TerminalRegrabWithNothingPendingRegrabsNothing(t *testing.T) {
	p, _ := newTestPar2Repair(t)
	logs := &p2eLogBuf{}
	p.logger = zerolog.New(logs).Level(zerolog.DebugLevel)
	arrSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.NotFound(w, nil)
	}))
	t.Cleanup(arrSrv.Close)
	p.manager.arr = arr.NewStorage()
	p.manager.arr.AddOrUpdate(arr.New("p2e", arrSrv.URL, "token", false, nil, "", ""))

	p.regrabOnTerminal(&storage.Entry{InfoHash: "nzb-all-patched", Name: p2eRelease, Category: "p2e"}, p2eRelease, nil)

	for _, ln := range logs.lines() {
		if ln["message"] == "par2 repair: terminal verdict on urgent lane; initiating immediate regrab" {
			t.Fatalf("re-grab initiated for %v with nothing pending, want none", ln["file"])
		}
	}
}
