package manager

import (
	"context"
	"slices"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Every way decodeWindows ends without a verdict records why, and a verdict
// records nothing.
func TestDecodeWindows_RecordsWhyNoVerdict(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name     string
		ctx      context.Context
		size     int64
		spent    bool
		outcomes map[string]probeOutcome
		want     string
	}{
		{"clean spread", context.Background(), testBigFile, false, nil, ""},
		{"decode error is a verdict", context.Background(), testBigFile, false,
			map[string]probeOutcome{decodePhaseDetect: {ok: false, reason: ffprobeReasonDecodeError, conclusive: true}}, ""},
		{"detect timed out", context.Background(), testBigFile, false,
			map[string]probeOutcome{decodePhaseDetect: {ok: true, conclusive: false}}, unverifiedTimeout},
		{"detect decoded through errors", context.Background(), testBigFile, false,
			map[string]probeOutcome{decodePhaseDetect: {ok: true, reason: ffprobeReasonDecodedThrough, conclusive: false}}, unverifiedDecodeErrors},
		{"head scan cut", context.Background(), testBigFile, false,
			map[string]probeOutcome{decodePhaseDetect: unseekable, decodePhaseHead: {ok: true, conclusive: false, cut: true}}, unverifiedNoSeekIndex},
		{"head scan timed out", context.Background(), testBigFile, false,
			map[string]probeOutcome{decodePhaseDetect: unseekable, decodePhaseHead: {ok: true, conclusive: false}}, unverifiedNoSeekIndex},
		{"head scan decoded through errors", context.Background(), testBigFile, false,
			map[string]probeOutcome{decodePhaseDetect: unseekable, decodePhaseHead: {ok: true, reason: ffprobeReasonDecodedThrough, conclusive: false}}, unverifiedDecodeErrors},
		{"head scan stopped with the run", cancelled, testBigFile, false,
			map[string]probeOutcome{decodePhaseDetect: unseekable, decodePhaseHead: {ok: true, conclusive: false, cut: true}}, unverifiedCancelled},
		{"spread decoded through errors", context.Background(), testSmall, false,
			map[string]probeOutcome{decodePhaseSpread: {ok: true, reason: ffprobeReasonDecodedThrough, conclusive: false}}, unverifiedDecodeErrors},
		{"verification already spent", context.Background(), testBigFile, true, nil, unverifiedReadBudget},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := verifyBudgetFor(tc.size)
			if tc.spent {
				parent.Exhaust()
			}
			f, _ := newTreeChecker(parent, tc.outcomes)
			cause := &unverifiedCause{}
			ok, _, conclusive, _ := f.decodeWindows(contextWithUnverifiedCause(tc.ctx, cause), "E", "f.mkv", testDuration, tc.size, parent)
			if tc.want == "" {
				if cause.reason != "" {
					t.Fatalf("recorded %q for a verdict (ok=%v conclusive=%v)", cause.reason, ok, conclusive)
				}
				return
			}
			if !ok || conclusive {
				t.Fatalf("got ok=%v conclusive=%v, want no verdict", ok, conclusive)
			}
			if cause.reason != tc.want {
				t.Fatalf("recorded %q, want %q", cause.reason, tc.want)
			}
		})
	}
}

func TestUnverifiedCause(t *testing.T) {
	var none *unverifiedCause
	if got := none.get(); got != unverifiedInconclusive {
		t.Fatalf("nil cause = %q, want %q", got, unverifiedInconclusive)
	}
	if got := (&unverifiedCause{}).get(); got != unverifiedInconclusive {
		t.Fatalf("unrecorded cause = %q, want %q", got, unverifiedInconclusive)
	}
	noteUnverified(context.Background(), unverifiedTimeout) // no holder: must not panic

	if got := timeoutCause(context.Background()); got != unverifiedTimeout {
		t.Fatalf("live parent = %q, want %q", got, unverifiedTimeout)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if got := timeoutCause(cancelled); got != unverifiedCancelled {
		t.Fatalf("cancelled parent = %q, want %q", got, unverifiedCancelled)
	}
}

func TestUnverifiedFiles(t *testing.T) {
	c := &candidate{item: &storage.EntryItem{Files: map[string]*storage.File{
		"b.mkv": {Name: "b.mkv", Size: 2000},
		"a.mkv": {Name: "a.mkv", Size: 1000},
	}}}
	results := []fileResult{
		{name: "b.mkv", infoHash: "h", healthy: true, unverifiedReason: unverifiedNoSeekIndex},
		{name: "a.mkv", infoHash: "h", healthy: true, unverifiedReason: reasonTailTruncated, shortBytes: 15208},
		{name: "verified.mkv", healthy: true},
		{name: "stopped.mkv", healthy: true, unverifiedReason: unverifiedCancelled},
		{name: "broken.mkv", broken: true, unverifiedReason: unverifiedTimeout},
	}
	got := unverifiedFiles(c, results)
	want := []storage.UnverifiedFile{
		{FileName: "a.mkv", InfoHash: "h", Reason: reasonTailTruncated, Size: 1000, ShortBytes: 15208},
		{FileName: "b.mkv", InfoHash: "h", Reason: unverifiedNoSeekIndex, Size: 2000},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}

func TestEntryHealthIsUnverified(t *testing.T) {
	files := []storage.UnverifiedFile{{FileName: "a.mkv", Reason: reasonTailTruncated}}
	for _, tc := range []struct {
		name string
		h    *storage.EntryHealth
		want bool
	}{
		{"healthy with a file", &storage.EntryHealth{Status: storage.HealthHealthy, UnverifiedFiles: files}, true},
		{"healthy, all verified", &storage.EntryHealth{Status: storage.HealthHealthy}, false},
		{"gone broken since", &storage.EntryHealth{Status: storage.HealthBroken, UnverifiedFiles: files}, false},
		{"nil", nil, false},
	} {
		if got := tc.h.IsUnverified(); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

// saveUnverified stores an entry named name and a healthy record listing its
// files unverified for the given reasons, in file order.
func saveUnverified(t *testing.T, repair *Repair, hash, name string, reasons ...string) {
	t.Helper()
	e := torrentEntry(hash, name, "realdebrid", "realdebrid")
	h := &storage.EntryHealth{EntryName: name, Status: storage.HealthHealthy, FileCount: len(reasons)}
	for i, reason := range reasons {
		file := name + ".mkv"
		if i > 0 {
			file = name + ".extra" + string(rune('0'+i)) + ".mkv"
			e.Files[file] = &storage.File{Name: file, Size: 1000, InfoHash: hash}
		}
		h.UnverifiedFiles = append(h.UnverifiedFiles, storage.UnverifiedFile{FileName: file, InfoHash: hash, Reason: reason, Size: 1000})
	}
	if err := repair.manager.storage.AddOrUpdate(e); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}
	if err := repair.manager.storage.SaveEntryHealth(h); err != nil {
		t.Fatalf("SaveEntryHealth: %v", err)
	}
}

func health(t *testing.T, repair *Repair, name string) *storage.EntryHealth {
	t.Helper()
	h, err := repair.manager.storage.GetEntryHealth(name)
	if err != nil || h == nil {
		t.Fatalf("%s: no health (%v)", name, err)
	}
	return h
}

func TestListUnverified(t *testing.T) {
	repair := newTestRepairForFix(t)
	saveUnverified(t, repair, "h2", "Zed", unverifiedTimeout)
	saveUnverified(t, repair, "h1", "Alpha", reasonTailTruncated)
	// A record whose entry was deleted must not be listed.
	if err := repair.manager.storage.SaveEntryHealth(&storage.EntryHealth{
		EntryName: "Gone", Status: storage.HealthHealthy,
		UnverifiedFiles: []storage.UnverifiedFile{{FileName: "Gone.mkv", Reason: reasonTailTruncated}},
	}); err != nil {
		t.Fatal(err)
	}
	list, err := repair.ListUnverified()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, h := range list {
		names = append(names, h.EntryName)
	}
	if !slices.Equal(names, []string{"Alpha", "Zed"}) {
		t.Fatalf("listed %v, want [Alpha Zed]", names)
	}
	if _, _, unverified := repair.manager.storage.CountDecodeVerified(repair.manager.EntryNameHasBackingEntry); unverified != 2 {
		t.Fatalf("status count %d, want 2, matching the list", unverified)
	}
}

// Replace marks only tail-truncated files broken, with the keep-release
// reason, and runs Fix on them; other unverified files stay listed.
func TestReplaceUnverified(t *testing.T) {
	repair := newTestRepairForFix(t)
	saveUnverified(t, repair, "h1", "Movie", reasonTailTruncated, unverifiedNoSeekIndex)
	saveUnverified(t, repair, "h2", "Slow", unverifiedTimeout)

	res, err := repair.ReplaceUnverified(context.Background(), nil, 0)
	if err != nil {
		t.Fatalf("ReplaceUnverified: %v", err)
	}
	repair.runWG.Wait()
	if res.Eligible != 1 || !slices.Equal(res.Queued, []string{"Movie"}) || res.Remaining != 0 || res.Run == nil {
		t.Fatalf("result %+v, want Movie queued with a run", res)
	}

	h := health(t, repair, "Movie")
	if h.Status != storage.HealthBroken || h.FailureReason != reasonTailTruncated || len(h.BrokenFiles) != 1 ||
		h.BrokenFiles[0].FileName != "Movie.mkv" || h.BrokenFiles[0].Reason != reasonTailTruncated || h.BrokenFiles[0].InfoHash != "h1" {
		t.Fatalf("Movie health %+v, want its truncated file broken", h)
	}
	if !keepReleaseReason(h.BrokenFiles[0].Reason) {
		t.Fatal("a replaced tail-truncated file would blocklist its release")
	}
	if len(h.UnverifiedFiles) != 1 || h.UnverifiedFiles[0].Reason != unverifiedNoSeekIndex {
		t.Fatalf("Movie unverified files %+v, want the no-seek-index file kept", h.UnverifiedFiles)
	}
	if s := health(t, repair, "Slow"); s.Status != storage.HealthHealthy || len(s.UnverifiedFiles) != 1 {
		t.Fatalf("a timed-out file was touched: %+v", s)
	}

	if _, err := repair.ReplaceUnverified(context.Background(), nil, 0); err == nil {
		t.Fatal("a second Replace found something to do; only timed-out files are left")
	}
}

func TestReplaceUnverifiedBatchesAndNames(t *testing.T) {
	repair := newTestRepairForFix(t)
	for _, name := range []string{"C", "A", "B"} {
		saveUnverified(t, repair, "h-"+name, name, reasonTailTruncated)
	}

	res, err := repair.ReplaceUnverified(context.Background(), []string{"B", "C"}, 1)
	if err != nil {
		t.Fatalf("ReplaceUnverified: %v", err)
	}
	repair.runWG.Wait()
	if res.Eligible != 2 || !slices.Equal(res.Queued, []string{"B"}) || res.Remaining != 1 {
		t.Fatalf("result %+v, want B queued and C remaining", res)
	}
	for name, want := range map[string]storage.HealthStatus{"A": storage.HealthHealthy, "B": storage.HealthBroken, "C": storage.HealthHealthy} {
		if got := health(t, repair, name).Status; got != want {
			t.Errorf("%s: status %q, want %q", name, got, want)
		}
	}
}

// While a run is active Replace changes nothing.
func TestReplaceUnverifiedDuringARun(t *testing.T) {
	repair := newTestRepairForFix(t)
	saveUnverified(t, repair, "h1", "Movie", reasonTailTruncated)
	repair.activeRunID = "sweep-1"

	if _, err := repair.ReplaceUnverified(context.Background(), nil, 0); err == nil {
		t.Fatal("Replace ran during another run")
	}
	if h := health(t, repair, "Movie"); !h.IsUnverified() || len(h.BrokenFiles) != 0 {
		t.Fatalf("record changed during a run: %+v", h)
	}
}

// A replaced tail-truncated file still played, so the entry is kept after the
// re-search: a re-grab that never lands leaves a copy to import by hand.
func TestFinalizeEntryRepairKeepsTailTruncatedEntry(t *testing.T) {
	repair := newTestRepairForFix(t)
	e := torrentEntry("h1", "Movie", "realdebrid", "realdebrid")
	if err := repair.manager.storage.AddOrUpdate(e); err != nil {
		t.Fatal(err)
	}
	h := &storage.EntryHealth{
		EntryName: "Movie",
		Status:    storage.HealthBroken,
		FileCount: 1,
		BrokenFiles: []storage.BrokenFile{{
			EntryName: "Movie", FileName: "Movie.mkv", InfoHash: "h1", Reason: reasonTailTruncated,
			ArrName: "radarr", MediaID: 1, ArrFileID: 101,
		}},
		BrokenCount: 1,
	}
	repair.finalizeEntryRepair("Movie", h, map[string]struct{}{"radarr": {}})

	if got, err := repair.manager.storage.Get("h1"); err != nil || got == nil {
		t.Fatalf("entry deleted after replacing a tail-truncated file (%v)", err)
	}
	if got := health(t, repair, "Movie"); got.LastRepairAt.IsZero() {
		t.Fatalf("repair not stamped: %+v", got)
	}
}
