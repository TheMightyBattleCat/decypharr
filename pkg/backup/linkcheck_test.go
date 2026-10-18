package backup

import (
	"bytes"
	"context"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
)

// recFS is the real filesystem that records every call. Stat is the one that
// would follow a link, so the tests assert it is never reached.
type recFS struct {
	OSLinkFS
	mu    sync.Mutex
	stats []string
}

func (r *recFS) Stat(p string) (fs.FileInfo, error) {
	r.mu.Lock()
	r.stats = append(r.stats, p)
	r.mu.Unlock()
	return r.OSLinkFS.Stat(p)
}

type fakeLookup struct {
	files map[string]map[string]bool // entry -> files
	ids   map[string]string          // id -> entry
}

func (l fakeLookup) EntryExists(e string) bool { _, ok := l.files[e]; return ok }
func (l fakeLookup) FileInEntry(e, f string) bool {
	return l.files[e][f]
}
func (l fakeLookup) EntryByID(id string) (string, bool) { e, ok := l.ids[id]; return e, ok }
func (l fakeLookup) EntryID(e string) string {
	for id, n := range l.ids {
		if n == e {
			return id
		}
	}
	return ""
}

type fakeArr struct {
	roots []arr.RootFolder
	files []arr.MediaFile
	err   error
	resc  []int
}

func (f *fakeArr) RootFolders(context.Context) ([]arr.RootFolder, error) { return f.roots, f.err }
func (f *fakeArr) MediaFiles(context.Context) ([]arr.MediaFile, error)   { return f.files, f.err }
func (f *fakeArr) Rescan(_ context.Context, id int) error                { f.resc = append(f.resc, id); return nil }

type linkEnv struct {
	dir, mount, tv, movies, unused string
	fs                             *recFS
	lk                             fakeLookup
}

func mustLink(t *testing.T, target, p string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, p); err != nil {
		t.Fatal(err)
	}
}

func newLinkEnv(t *testing.T) *linkEnv {
	t.Helper()
	d := t.TempDir()
	e := &linkEnv{dir: d, mount: "/mnt/decy", tv: filepath.Join(d, "tv"), movies: filepath.Join(d, "movies"),
		unused: filepath.Join(d, "old"), fs: &recFS{}}
	e.lk = fakeLookup{
		files: map[string]map[string]bool{"EntryA": {"a.mkv": true}, "EntryB": {"b.mkv": true}, "EntryNew": {"c.mkv": true}},
		ids:   map[string]string{"idA": "EntryA", "idB": "EntryB", "idC": "EntryNew"},
	}
	// targets never exist on disk: /mnt/decy is not real.
	mustLink(t, e.mount+"/__all__/EntryA/a.mkv", filepath.Join(e.tv, "Show", "S01E01.mkv"))    // live
	mustLink(t, e.mount+"/__all__/EntryB/gone.mkv", filepath.Join(e.tv, "Show", "S01E02.mkv")) // target gone
	mustLink(t, e.mount+"/__all__/Removed/x.mkv", filepath.Join(e.tv, "Show", "S01E03.mkv"))   // entry gone
	mustLink(t, "/other/mount/file.mkv", filepath.Join(e.movies, "Film", "Film.mkv"))          // external
	mustLink(t, e.mount+"/__all__/EntryA/a.mkv", filepath.Join(e.unused, "Dead", "d.mkv"))     // live link, root unused
	mustLink(t, e.mount+"/__all__/Removed/y.mkv", filepath.Join(e.unused, "Dead", "e.mkv"))    // dead link
	return e
}

func (e *linkEnv) arrSrc() (*fakeArr, ArrSource) {
	fa := &fakeArr{
		roots: []arr.RootFolder{{ID: 1, Path: "/arrtv"}},
		files: []arr.MediaFile{
			{Path: "/arrtv/Show/S01E01.mkv", FileID: 11, ItemID: 7, Title: "Show"},
			{Path: "/arrtv/Show/S01E04.mkv", FileID: 14, ItemID: 7, Title: "Show"}, // no link on disk
		},
	}
	return fa, ArrSource{Name: "sonarr", Client: fa, PathMap: []PathMapping{{Arr: "/arrtv", Local: e.tv}}}
}

func (e *linkEnv) manifest(t *testing.T) (*Manifest, *fakeArr) {
	t.Helper()
	fa, src := e.arrSrc()
	rs := DiscoverRoots(context.Background(), []ArrSource{src}, []string{e.movies, e.unused, e.tv + "/"})
	m, err := BuildManifest(context.Background(), ManifestInput{FS: e.fs, Lookup: e.lk, MountRoot: e.mount, Roots: rs, Arrs: []ArrSource{src}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return m, fa
}

func rowByName(m *Manifest, name string) LinkRow {
	for _, r := range m.Rows {
		if filepath.Base(r.Path) == name {
			return r
		}
	}
	return LinkRow{}
}

func TestDiscoverRootsDedupAndPathMap(t *testing.T) {
	e := newLinkEnv(t)
	_, src := e.arrSrc()
	src2 := ArrSource{Name: "radarr", Client: &fakeArr{roots: []arr.RootFolder{{Path: e.tv}, {Path: e.movies}}}}
	rs := DiscoverRoots(context.Background(), []ArrSource{src, src2, {Name: "bad", Client: &fakeArr{err: os.ErrPermission}}}, []string{e.tv, e.unused})
	if len(rs.Roots) != 3 {
		t.Fatalf("roots = %+v", rs.Roots)
	}
	for _, r := range rs.Roots {
		if r.Path == e.tv && len(r.Arrs) != 2 {
			t.Errorf("tv arrs = %v", r.Arrs)
		}
	}
	if len(rs.PathMaps["sonarr"]) != 1 || len(rs.Errors) != 1 {
		t.Errorf("maps %v errors %v", rs.PathMaps, rs.Errors)
	}
	if got := src.ToArr(filepath.Join(e.tv, "Show", "x.mkv")); got != "/arrtv/Show/x.mkv" {
		t.Errorf("ToArr = %s", got)
	}
	if got := src.ToLocal("/arrtvx/a"); got != "/arrtvx/a" {
		t.Errorf("prefix must respect directory boundary: %s", got)
	}
}

func TestManifestStatesAndMissing(t *testing.T) {
	e := newLinkEnv(t)
	m, _ := e.manifest(t)
	want := map[string]LinkState{"S01E01.mkv": LinkLive, "S01E02.mkv": LinkTargetGone, "S01E03.mkv": LinkEntryGone,
		"Film.mkv": LinkExternal, "d.mkv": LinkLive, "e.mkv": LinkEntryGone}
	if len(m.Rows) != len(want) {
		t.Fatalf("rows = %d", len(m.Rows))
	}
	for name, st := range want {
		if got := rowByName(m, name); got.State != st {
			t.Errorf("%s state = %q want %q", name, got.State, st)
		}
	}
	r := rowByName(m, "S01E01.mkv")
	if r.Entry != "EntryA" || r.File != "a.mkv" || r.EntryID != "idA" || r.MountRoot != e.mount || r.Root != e.tv {
		t.Errorf("row = %+v", r)
	}
	if r.Arr != "sonarr" || r.ArrItemID != 7 || r.ArrFileID != 11 || r.ArrPath != "/arrtv/Show/S01E01.mkv" {
		t.Errorf("arr fields = %+v", r)
	}
	if ext := rowByName(m, "Film.mkv"); ext.MountRoot != "" || ext.Entry != "" {
		t.Errorf("external row = %+v", ext)
	}
	if len(m.Missing) != 1 || m.Missing[0].Path != filepath.Join(e.tv, "Show", "S01E04.mkv") || m.Missing[0].ArrItemID != 7 {
		t.Errorf("missing = %+v", m.Missing)
	}
	// movies and old have no Arr file; tv is used by the live link and the missing file.
	if len(m.UnusedRoots) != 2 {
		t.Errorf("unused = %v", m.UnusedRoots)
	}
	if len(e.fs.stats) != 0 {
		t.Fatalf("Stat called on %v", e.fs.stats)
	}
}

func TestUnusedRootWhenOnlyDeadLinks(t *testing.T) {
	e := newLinkEnv(t)
	fa, src := e.arrSrc()
	// the only Arr file under tv is a dead link.
	fa.files = []arr.MediaFile{{Path: "/arrtv/Show/S01E03.mkv", FileID: 1, ItemID: 7}}
	rs := DiscoverRoots(context.Background(), []ArrSource{src}, nil)
	m, err := BuildManifest(context.Background(), ManifestInput{FS: e.fs, Lookup: e.lk, MountRoot: e.mount, Roots: rs, Arrs: []ArrSource{src}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.UnusedRoots) != 1 || m.UnusedRoots[0] != e.tv {
		t.Errorf("unused = %v", m.UnusedRoots)
	}
	// An Arr that fails means unused cannot be known.
	fa.err = os.ErrPermission
	m, _ = BuildManifest(context.Background(), ManifestInput{FS: e.fs, Lookup: e.lk, MountRoot: e.mount,
		Roots: RootSet{Roots: []LinkRoot{{Path: e.tv}}}, Arrs: []ArrSource{src}}, nil)
	if len(m.UnusedRoots) != 0 || len(m.Errors) == 0 {
		t.Errorf("unused %v errors %v", m.UnusedRoots, m.Errors)
	}
}

func TestManifestRoundTripAndSummary(t *testing.T) {
	e := newLinkEnv(t)
	m, _ := e.manifest(t)
	m.Rows[0].Path += "\tweird\nname"
	m.Rows[0].MTime = time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
	var buf bytes.Buffer
	if err := EncodeManifest(&buf, m); err != nil {
		t.Fatal(err)
	}
	got, err := DecodeManifest(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Rows) != len(m.Rows) || len(got.Missing) != 1 || len(got.Roots) != len(m.Roots) || len(got.UnusedRoots) != 2 {
		t.Fatalf("decoded %d rows %d missing", len(got.Rows), len(got.Missing))
	}
	for i := range m.Rows {
		if got.Rows[i] != m.Rows[i] {
			t.Errorf("row %d: %+v != %+v", i, got.Rows[i], m.Rows[i])
		}
	}
	if _, err := DecodeManifest(bytes.NewReader([]byte("junk"))); err == nil {
		t.Error("junk decoded")
	}
	res := Summarise(got, time.Unix(100, 0))
	if res.Live != 3 || res.Dead != 3 || res.Missing != 1 || len(res.Roots) != 3 {
		t.Fatalf("result = %+v", res)
	}
	for _, r := range res.Roots {
		switch r.Root {
		case e.tv:
			if r.Total != 3 || r.Live != 1 || r.Target != 1 || r.Entry != 1 || r.Unused {
				t.Errorf("tv = %+v", r)
			}
		case e.movies:
			if r.External != 1 || !r.Unused {
				t.Errorf("movies = %+v", r)
			}
		}
	}
	if tp := res.Trend(); tp.Live != 3 || tp.Dead != 3 {
		t.Errorf("trend = %+v", tp)
	}
}

func TestLinkCheckerWithHTTPArr(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	e := newLinkEnv(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v3/rootfolder":
			_, _ = w.Write([]byte(`[{"id":1,"path":"` + e.tv + `"}]`))
		case "/api/v3/series":
			_, _ = w.Write([]byte(`[{"id":7,"title":"Show"}]`))
		case "/api/v3/episodefile":
			_, _ = w.Write([]byte(`[{"id":11,"path":"` + e.tv + `/Show/S01E01.mkv"},{"id":12,"path":"` + e.tv + `/Show/S01E09.mkv"}]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	a := arr.New("sonarr", srv.URL, "k", false, nil, "", "manual")
	a.Type = arr.Sonarr
	var got *Manifest
	chk := NewLinkChecker(LinkCheckConfig{FS: e.fs, Lookup: e.lk,
		MountRoot: func() string { return e.mount },
		Arrs:      func() []ArrSource { return []ArrSource{{Name: "sonarr", Client: NewArrClient(a)}} },
		OnResult:  func(m *Manifest, _ LinkCheckResult) { got = m },
	})
	st, err := chk(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got.Rows) != 3 || len(got.Missing) != 1 || st.Missing != 1 || st.LastCheck == nil || len(st.Roots) != 1 {
		t.Fatalf("status %+v manifest %+v", st, got)
	}
	if len(e.fs.stats) != 0 {
		t.Fatalf("Stat called on %v", e.fs.stats)
	}
}

func planFor(t *testing.T, e *linkEnv, m *Manifest, sc LinkScope) LinkPlan {
	t.Helper()
	return BuildLinkPlan(LinkPlanInput{FS: e.fs, Lookup: e.lk, MountRoot: e.mount, Rows: m.Rows, Scope: sc, SnapshotID: "snap"})
}

func TestPlanClassesAndApply(t *testing.T) {
	e := newLinkEnv(t)
	m, _ := e.manifest(t)
	// Build the "after disaster" state.
	tgone := filepath.Join(e.tv, "Show", "S01E02.mkv")
	egone := filepath.Join(e.tv, "Show", "S01E03.mkv")
	film := filepath.Join(e.movies, "Film", "Film.mkv")
	d := filepath.Join(e.unused, "Dead", "d.mkv")
	for _, p := range []string{tgone, egone, d, filepath.Join(e.unused, "Dead", "e.mkv")} {
		_ = os.Remove(p)
	}
	_ = os.Remove(film)
	if err := os.WriteFile(film, []byte("real"), 0o644); err != nil { // regular file -> conflict
		t.Fatal(err)
	}
	mustLink(t, "/somewhere/else", d) // different target -> retarget
	// The recorded entry for S01E02 was replaced: new folder, same id.
	e.lk.files["EntryB2"] = map[string]bool{"gone.mkv": true}
	e.lk.ids["idB"] = "EntryB2"
	delete(e.lk.files, "EntryB")
	for i := range m.Rows {
		if filepath.Base(m.Rows[i].Path) == "d.mkv" {
			m.Rows[i].Arr, m.Rows[i].ArrItemID = "sonarr", 9
		}
		if filepath.Base(m.Rows[i].Path) == "S01E02.mkv" {
			m.Rows[i].EntryID = "idB"
			m.Rows[i].Entry = "EntryB"
		}
	}
	plan := planFor(t, e, m, LinkScope{})
	cls := map[string]LinkPlanRow{}
	for _, r := range plan.Rows {
		cls[filepath.Base(r.Path)] = r
	}
	check := func(name string, c PlanClass) LinkPlanRow {
		t.Helper()
		if cls[name].Class != c {
			t.Errorf("%s = %q want %q (%+v)", name, cls[name].Class, c, cls[name])
		}
		return cls[name]
	}
	check("S01E01.mkv", PlanOK)
	r2 := check("S01E02.mkv", PlanCreate)
	if r2.Target != e.mount+"/__all__/EntryB2/gone.mkv" || r2.Note == "" {
		t.Errorf("replaced entry target = %q", r2.Target)
	}
	check("S01E03.mkv", PlanUnresolved)
	check("Film.mkv", PlanConflict)
	rt := check("d.mkv", PlanRetarget)
	if rt.Current != "/somewhere/else" || rt.Target != e.mount+"/__all__/EntryA/a.mkv" {
		t.Errorf("retarget = %+v", rt)
	}
	check("e.mkv", PlanUnresolved)
	if plan.Counts[PlanUnresolved] != 2 || plan.Counts[PlanOK] != 1 || plan.PlanID == "" {
		t.Errorf("counts = %v", plan.Counts)
	}

	// Apply without retarget or dangling.
	res := ApplyLinkPlan(context.Background(), e.fs, plan, LinkApplyOptions{})
	if res.Created != 1 || res.Retarget != 0 || res.Failed != 0 {
		t.Fatalf("apply = %+v", res)
	}
	if b, _ := os.ReadFile(film); string(b) != "real" {
		t.Fatal("regular file was replaced")
	}
	if cur, _ := os.Readlink(d); cur != "/somewhere/else" {
		t.Errorf("retargeted without being asked: %s", cur)
	}
	if _, err := os.Lstat(egone); err == nil {
		t.Error("dangling link created without being asked")
	}
	// A file appears after the plan: create must not clobber it.
	_ = os.Remove(tgone)
	_ = os.WriteFile(tgone, []byte("late"), 0o644)
	// Apply with everything ticked.
	fa := &fakeArr{}
	res = ApplyLinkPlan(context.Background(), e.fs, plan, LinkApplyOptions{Retarget: true, Dangling: true, Rescan: true,
		Rescanner: func(ctx context.Context, name string, id int) error { return fa.Rescan(ctx, id) }})
	if b, _ := os.ReadFile(tgone); string(b) != "late" {
		t.Fatal("late file replaced")
	}
	if res.Retarget != 1 || res.Created != 2 || res.Failed != 0 {
		t.Fatalf("apply2 = %+v", res)
	}
	if cur, _ := os.Readlink(d); cur != e.mount+"/__all__/EntryA/a.mkv" {
		t.Errorf("retarget result %s", cur)
	}
	if cur, _ := os.Readlink(egone); cur != e.mount+"/__all__/Removed/x.mkv" {
		t.Errorf("dangling result %s", cur)
	}
	ents, _ := os.ReadDir(filepath.Dir(d))
	for _, en := range ents {
		if filepath.Ext(en.Name()) == ".tmp" {
			t.Errorf("temp link left: %s", en.Name())
		}
	}
	if len(fa.resc) == 0 || res.Rescans != len(fa.resc) {
		t.Errorf("rescans %d %v", res.Rescans, fa.resc)
	}
	if len(e.fs.stats) != 0 {
		t.Fatalf("Stat called on %v", e.fs.stats)
	}
}

func TestPlanCreatesParentsAndRewritesMount(t *testing.T) {
	e := newLinkEnv(t)
	m, _ := e.manifest(t)
	for _, p := range []string{e.tv, e.movies, e.unused} {
		if err := os.RemoveAll(p); err != nil {
			t.Fatal(err)
		}
	}
	newMount := "/mnt/new"
	plan := BuildLinkPlan(LinkPlanInput{FS: e.fs, Lookup: e.lk, MountRoot: newMount, Rows: m.Rows})
	byName := map[string]LinkPlanRow{}
	for _, r := range plan.Rows {
		byName[filepath.Base(r.Path)] = r
	}
	if r := byName["S01E01.mkv"]; r.Class != PlanCreate || r.Target != newMount+"/__all__/EntryA/a.mkv" || r.Recorded != e.mount+"/__all__/EntryA/a.mkv" {
		t.Errorf("rewritten = %+v", r)
	}
	if r := byName["Film.mkv"]; r.Class != PlanCreate || r.Target != "/other/mount/file.mkv" {
		t.Errorf("external must be verbatim: %+v", r)
	}
	res := ApplyLinkPlan(context.Background(), e.fs, plan, LinkApplyOptions{})
	if res.Created != 3 || res.Failed != 0 {
		t.Fatalf("apply = %+v", res)
	}
	if cur, err := os.Readlink(filepath.Join(e.movies, "Film", "Film.mkv")); err != nil || cur != "/other/mount/file.mkv" {
		t.Errorf("external link = %q %v", cur, err)
	}
}

func TestPlanScopes(t *testing.T) {
	e := newLinkEnv(t)
	m, _ := e.manifest(t)
	count := func(sc LinkScope) int { return len(planFor(t, e, m, sc).Rows) }
	if n := count(LinkScope{}); n != 6 {
		t.Errorf("all = %d", n)
	}
	if n := count(LinkScope{Root: e.tv}); n != 3 {
		t.Errorf("root = %d", n)
	}
	if n := count(LinkScope{Series: "show"}); n != 3 {
		t.Errorf("series = %d", n)
	}
	if n := count(LinkScope{Series: "Film"}); n != 1 {
		t.Errorf("movie = %d", n)
	}
	if n := count(LinkScope{Prefix: filepath.Join(e.tv, "Show", "S01E0")}); n != 0 {
		t.Errorf("prefix must respect directory boundary, got %d", n)
	}
	if n := count(LinkScope{Prefix: filepath.Join(e.tv, "Show")}); n != 3 {
		t.Errorf("prefix = %d", n)
	}
	if n := count(LinkScope{Root: e.tv, Series: "Film"}); n != 0 {
		t.Errorf("combined = %d", n)
	}
}

func TestCleanupPlanAndApply(t *testing.T) {
	e := newLinkEnv(t)
	m, _ := e.manifest(t)
	plan := BuildCleanupPlan(m.Rows, LinksCleanupFilter{}, time.Time{})
	if plan.Total != 3 || len(plan.Groups) != 3 {
		t.Fatalf("plan = %+v", plan)
	}
	if p := BuildCleanupPlan(m.Rows, LinksCleanupFilter{States: []LinkState{LinkLive, LinkExternal}}, time.Time{}); p.Total != 0 {
		t.Errorf("live/external must never be selected: %+v", p)
	}
	if p := BuildCleanupPlan(m.Rows, LinksCleanupFilter{Root: e.unused}, time.Time{}); p.Total != 1 {
		t.Errorf("root filter = %+v", p)
	}
	if p := BuildCleanupPlan(m.Rows, LinksCleanupFilter{States: []LinkState{LinkEntryGone}, Series: "Show"}, time.Time{}); p.Total != 1 {
		t.Errorf("state+series filter = %+v", p)
	}
	// q is a substring of the path or entry, like the list's search box.
	if p := BuildCleanupPlan(m.Rows, LinksCleanupFilter{Q: "sho"}, time.Time{}); p.Total != 2 {
		t.Errorf("q substring of a folder name = %+v", p)
	}
	if p := BuildCleanupPlan(m.Rows, LinksCleanupFilter{Q: "S01E03"}, time.Time{}); p.Total != 1 {
		t.Errorf("q on a file name = %+v", p)
	}
	if p := BuildCleanupPlan(m.Rows, LinksCleanupFilter{Q: "nothing-like-this"}, time.Time{}); p.Total != 0 {
		t.Errorf("q without a match = %+v", p)
	}
	if r := ApplyCleanup(context.Background(), e.fs, plan, nil); r.Removed != 0 {
		t.Errorf("nothing ticked removed %d", r.Removed)
	}
	// One dead link is replaced by a real file before apply.
	egone := filepath.Join(e.tv, "Show", "S01E03.mkv")
	_ = os.Remove(egone)
	_ = os.WriteFile(egone, []byte("keep"), 0o644)
	var keys []string
	for _, g := range plan.Groups {
		keys = append(keys, g.Key)
	}
	r := ApplyCleanup(context.Background(), e.fs, plan, keys)
	if r.Removed != 2 || r.Failed != 0 {
		t.Fatalf("result = %+v", r)
	}
	if b, _ := os.ReadFile(egone); string(b) != "keep" {
		t.Error("regular file removed")
	}
	if _, err := os.Lstat(filepath.Join(e.tv, "Show", "S01E01.mkv")); err != nil {
		t.Error("live link removed")
	}
}

func TestCleanupSkipsLinkRetargetedSincePlan(t *testing.T) {
	e := newLinkEnv(t)
	m, _ := e.manifest(t)
	plan := BuildCleanupPlan(m.Rows, LinksCleanupFilter{}, time.Time{})
	// A dead link is re-pointed at something else after the plan.
	egone := filepath.Join(e.tv, "Show", "S01E03.mkv")
	other := filepath.Join(e.tv, "Show", "S01E01.mkv")
	_ = os.Remove(egone)
	if err := os.Symlink(other, egone); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, g := range plan.Groups {
		keys = append(keys, g.Key)
	}
	r := ApplyCleanup(context.Background(), e.fs, plan, keys)
	if r.Removed != 2 || r.Failed != 0 {
		t.Fatalf("result = %+v", r)
	}
	if _, err := os.Lstat(egone); err != nil {
		t.Error("retargeted link was removed")
	}
}

func TestCrossCheckDoesNotLstatThroughFolderLink(t *testing.T) {
	e := newLinkEnv(t)
	// A folder link inside the tv root pointing at a real folder that does
	// not hold the file the Arr lists.
	away := filepath.Join(e.dir, "away")
	if err := os.MkdirAll(away, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(away, filepath.Join(e.tv, "Linked")); err != nil {
		t.Fatal(err)
	}
	fa, src := e.arrSrc()
	fa.files = append(fa.files, arr.MediaFile{Path: "/arrtv/Linked/S01E01.mkv", FileID: 21, ItemID: 9, Title: "Linked"})
	rs := DiscoverRoots(context.Background(), []ArrSource{src}, []string{e.tv})
	m, err := BuildManifest(context.Background(), ManifestInput{FS: e.fs, Lookup: e.lk, MountRoot: e.mount, Roots: rs, Arrs: []ArrSource{src}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, ms := range m.Missing {
		if strings.Contains(ms.Path, "Linked") {
			t.Fatalf("file under a folder link was resolved through it: %+v", ms)
		}
	}
}

func TestServiceCleanupDropsRemovedLinksFromStoredCheck(t *testing.T) {
	e := newLinkEnv(t)
	m, _ := e.manifest(t)
	te := newEnv(t, nil)
	te.svc.deps.FS = e.fs
	te.cfg.PreAction.Enabled = false // the snapshot would re-walk the (empty) roots
	te.svc.recordCheck(m)
	ctx := context.Background()
	before, _ := te.svc.LinksStatus(ctx)
	if len(before.Trend) != 1 || before.Trend[0].Dead != 3 {
		t.Fatalf("trend before = %+v", before.Trend)
	}
	totalBefore := len(m.Rows)

	plan, err := te.svc.PlanLinksCleanup(ctx, LinksCleanupPlanRequest{})
	if err != nil || plan.Total != 3 {
		t.Fatalf("plan = %+v, %v", plan, err)
	}
	// One dead link is re-pointed after the plan: it must stay on disk and listed.
	kept := filepath.Join(e.tv, "Show", "S01E03.mkv")
	_ = os.Remove(kept)
	if err := os.Symlink(filepath.Join(e.tv, "Show", "S01E01.mkv"), kept); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, g := range plan.Groups {
		keys = append(keys, g.Key)
	}
	res, err := te.svc.ApplyLinksCleanup(ctx, LinksCleanupApplyRequest{PlanID: plan.PlanID, Groups: keys})
	if err != nil || res.Removed != 2 || len(res.RemovedPaths) != 2 {
		t.Fatalf("result = %+v, %v", res, err)
	}

	page, _ := te.svc.ListLinks(ctx, LinksQuery{PageSize: 1000})
	if page.Total != totalBefore-2 {
		t.Errorf("rows = %d, want %d", page.Total, totalBefore-2)
	}
	var sawKept bool
	for _, r := range page.Rows {
		for _, p := range res.RemovedPaths {
			if r.Path == p {
				t.Errorf("removed path still listed: %s", p)
			}
		}
		sawKept = sawKept || r.Path == kept
	}
	if !sawKept {
		t.Error("re-pointed link must stay listed")
	}
	after, _ := te.svc.LinksStatus(ctx)
	var dead, rowsTotal int
	for _, r := range after.Roots {
		dead += r.Target + r.Entry
		rowsTotal += r.Total
	}
	if dead != 1 || rowsTotal != totalBefore-2 {
		t.Errorf("roots = %+v", after.Roots)
	}
	if len(after.Trend) != 1 || after.Trend[0].Dead != 1 || after.Trend[0].Live != before.Trend[0].Live {
		t.Errorf("trend = %+v", after.Trend)
	}
	if after.LastCheck == nil || !after.LastCheck.Equal(*before.LastCheck) {
		t.Errorf("last check moved: %v", after.LastCheck)
	}
	// The next plan offers only the link that is still listed as dead.
	plan2, _ := te.svc.PlanLinksCleanup(ctx, LinksCleanupPlanRequest{})
	if plan2.Total != 1 {
		t.Fatalf("second plan = %+v", plan2)
	}
	for _, g := range plan2.Groups {
		for _, p := range g.Paths {
			if p != kept {
				t.Errorf("second plan offers %s", p)
			}
		}
	}
	// Remove the re-pointed link's dead row from consideration: a plan
	// built from nothing removed leaves the stored check alone.
	te.svc.dropRemovedLinks(nil)
	if p, _ := te.svc.ListLinks(ctx, LinksQuery{PageSize: 1000}); p.Total != totalBefore-2 {
		t.Errorf("rows changed on empty drop: %d", p.Total)
	}
}
