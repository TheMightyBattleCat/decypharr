package backup

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

type storeLookup struct{ st *storage.Storage }

func (l storeLookup) find(folder string) (*storage.Entry, bool) {
	var found *storage.Entry
	_ = l.st.ForEach(func(e *storage.Entry) error {
		if e.GetFolder() == folder {
			found = e
		}
		return nil
	})
	return found, found != nil
}
func (l storeLookup) EntryExists(e string) bool { _, ok := l.find(e); return ok }
func (l storeLookup) FileInEntry(e, f string) bool {
	en, ok := l.find(e)
	if !ok {
		return false
	}
	file, ok := en.Files[f]
	return ok && !file.Deleted
}
func (l storeLookup) EntryByID(id string) (string, bool) {
	en, err := l.st.Get(id)
	if err != nil {
		return "", false
	}
	return en.GetFolder(), true
}
func (l storeLookup) EntryID(e string) string {
	if en, ok := l.find(e); ok {
		return en.InfoHash
	}
	return ""
}

type undeleteEnv struct {
	repo    *FileRepo
	st      *storage.Storage
	u       *Undelete
	meta    string
	overlay string
	libDir  string
	linkP   string
	entry   *storage.Entry
}

func newUndeleteEnv(t *testing.T) *undeleteEnv {
	t.Helper()
	ctx := context.Background()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	config.Get()

	root := t.TempDir()
	repo, err := OpenRepo(filepath.Join(root, "repo"))
	if err != nil {
		t.Fatal(err)
	}
	st, err := storage.NewStorage(filepath.Join(root, "db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	e := &storage.Entry{
		Protocol: "nzb", InfoHash: "abc123", Name: "Some.Show.S01E01", Category: "sonarr", Size: 4096,
		Files: map[string]*storage.File{"ep.mkv": {Name: "ep.mkv", Size: 4096}},
	}
	if err := st.AddOrUpdate(e); err != nil {
		t.Fatal(err)
	}
	folder := e.GetFolder()
	if err := st.UpdateItem(&storage.EntryItem{Name: folder, Size: 4096, Files: map[string]*storage.File{"ep.mkv": {Name: "ep.mkv", Size: 4096}}}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveEntryHealth(&storage.EntryHealth{EntryName: folder, Status: "healthy"}); err != nil {
		t.Fatal(err)
	}
	// A second entry stays live throughout.
	if err := st.AddOrUpdate(&storage.Entry{Protocol: "nzb", InfoHash: "keep1", Name: "Other", Files: map[string]*storage.File{"o.mkv": {Name: "o.mkv", Size: 1}}}); err != nil {
		t.Fatal(err)
	}

	meta := filepath.Join(root, "usenet", "meta")
	overlay := filepath.Join(root, "usenet", "overlay")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.MkdirAll(filepath.Join(overlay, "abc123"), 0o755))
	must(os.MkdirAll(filepath.Join(overlay, "keep1"), 0o755))
	must(os.MkdirAll(meta, 0o755))
	mt := time.Unix(1_700_000_000, 0)
	writeFile(t, filepath.Join(meta, "abc123.meta"), "meta-abc", mt)
	writeFile(t, filepath.Join(meta, "keep1.meta"), "meta-keep", mt)
	writeFile(t, filepath.Join(overlay, "abc123", "manifest.json"), "overlay-abc", mt)
	writeFile(t, filepath.Join(overlay, "keep1", "manifest.json"), "overlay-keep", mt)

	lib := filepath.Join(root, "lib")
	must(os.MkdirAll(filepath.Join(lib, "Show"), 0o755))
	linkP := filepath.Join(lib, "Show", "ep.mkv")
	mount := "/mnt/dec"
	man := &Manifest{Roots: []string{lib}, Rows: []LinkRow{
		{Path: linkP, Target: mount + "/__all__/" + folder + "/ep.mkv", MountRoot: mount, Entry: folder, File: "ep.mkv", EntryID: "abc123", State: LinkLive},
		{Path: filepath.Join(lib, "Show", "other.mkv"), Target: mount + "/__all__/Other/o.mkv", MountRoot: mount, Entry: "Other", File: "o.mkv", EntryID: "keep1", State: LinkLive},
	}}
	var mb bytes.Buffer
	must(EncodeManifest(&mb, man))
	mref, _, err := repo.PutBlob(ctx, &mb, CompressDefault)
	must(err)

	nzbWalk, err := WalkTree(ctx, repo, WalkOptions{Root: meta, Component: ComponentNZB})
	must(err)
	ovWalk, err := WalkTree(ctx, repo, WalkOptions{Root: overlay, Component: ComponentOverlay})
	must(err)
	dbEntry, err := NewDBExporter(st)(ctx, repo, nil)
	must(err)

	t0 := time.Date(2026, 9, 1, 4, 0, 0, 0, time.UTC)
	must(repo.WriteSnapshot(ctx, &Snapshot{ID: "snap-1", CreatedAt: t0, Trigger: "manual:full", Decypharr: BuildInfo{DBFormat: storage.DBFormat},
		Components: map[Component]ComponentEntry{
			ComponentDB:      dbEntry,
			ComponentNZB:     {Tree: nzbWalk.Tree.SHA256, FileCount: nzbWalk.FileCount},
			ComponentOverlay: {Tree: ovWalk.Tree.SHA256, FileCount: ovWalk.FileCount},
			ComponentLinks:   {Manifest: mref.SHA256, Rows: 2},
		}}))

	// The entry is removed, together with its files and link; a later
	// snapshot shows it gone.
	must(st.Delete("abc123"))
	must(os.RemoveAll(filepath.Join(overlay, "abc123")))
	must(os.Remove(filepath.Join(meta, "abc123.meta")))
	dbEntry2, err := NewDBExporter(st)(ctx, repo, nil)
	must(err)
	must(repo.WriteSnapshot(ctx, &Snapshot{ID: "snap-2", CreatedAt: t0.Add(24 * time.Hour), Trigger: "manual:quick", Decypharr: BuildInfo{DBFormat: storage.DBFormat},
		Components: map[Component]ComponentEntry{ComponentDB: dbEntry2}}))

	u := NewUndelete(UndeleteOptions{
		Repo: repo, Store: st, TempDir: t.TempDir(), MetaDir: meta, OverlayDir: overlay,
		Lookup: storeLookup{st}, MountRoot: mount,
	})
	return &undeleteEnv{repo: repo, st: st, u: u, meta: meta, overlay: overlay, libDir: lib, linkP: linkP, entry: e}
}

func TestUndeleteListAndRoundTrip(t *testing.T) {
	ctx := context.Background()
	env := newUndeleteEnv(t)

	list, err := env.u.ListRemoved(ctx, RemovedQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("removed = %+v, want one entry", list)
	}
	r := list[0]
	if r.Key != "abc123" || r.Name != env.entry.GetFolder() || r.Category != "sonarr" || r.LastSnapshotID != "snap-1" ||
		r.LinkCount != 1 || r.Size != 4096 || r.FirstMissing.IsZero() || !r.FirstMissing.After(r.LastSeen) {
		t.Fatalf("removed row = %+v", r)
	}
	if l, _ := env.u.ListRemoved(ctx, RemovedQuery{Name: "nomatch"}); len(l) != 0 {
		t.Fatalf("name filter kept %d", len(l))
	}
	if l, _ := env.u.ListRemoved(ctx, RemovedQuery{Category: "radarr"}); len(l) != 0 {
		t.Fatalf("category filter kept %d", len(l))
	}
	if l, _ := env.u.ListRemoved(ctx, RemovedQuery{Since: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)}); len(l) != 0 {
		t.Fatalf("date filter kept %d", len(l))
	}

	// Dry run changes nothing.
	dry, err := env.u.Restore(ctx, "abc123", RestoreRemovedRequest{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if dry.Applied || dry.Links == nil || dry.Links.Counts[PlanCreate] != 1 {
		t.Fatalf("dry run = %+v", dry)
	}
	if ok, _ := env.st.Exists("abc123"); ok {
		t.Fatal("dry run wrote the entry")
	}
	if _, err := os.Lstat(env.linkP); err == nil {
		t.Fatal("dry run created the link")
	}

	resp, err := env.u.Restore(ctx, "abc123", RestoreRemovedRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Applied || resp.LinkResult == nil || resp.LinkResult.Created != 1 {
		t.Fatalf("restore = %+v", resp)
	}
	got, err := env.st.Get("abc123")
	if err != nil || got.Name != env.entry.Name || got.Files["ep.mkv"] == nil {
		t.Fatalf("entry = %+v err=%v", got, err)
	}
	folder := env.entry.GetFolder()
	if item, err := env.st.GetEntryItem(folder); err != nil || item.Files["ep.mkv"] == nil {
		t.Fatalf("item = %+v err=%v", item, err)
	}
	if h, err := env.st.GetEntryHealth(folder); err != nil || h.Status != "healthy" {
		t.Fatalf("health = %+v err=%v", h, err)
	}
	for p, want := range map[string]string{
		filepath.Join(env.meta, "abc123.meta"):                "meta-abc",
		filepath.Join(env.overlay, "abc123", "manifest.json"): "overlay-abc",
	} {
		if b, err := os.ReadFile(p); err != nil || string(b) != want {
			t.Fatalf("%s = %q err=%v", p, b, err)
		}
	}
	if tgt, err := os.Readlink(env.linkP); err != nil || tgt != "/mnt/dec/__all__/"+folder+"/ep.mkv" {
		t.Fatalf("link = %q err=%v", tgt, err)
	}
	// The other entry's link was not touched.
	if _, err := os.Lstat(filepath.Join(env.libDir, "Show", "other.mkv")); err == nil {
		t.Fatal("another entry's link was created")
	}
	if l, _ := env.u.ListRemoved(ctx, RemovedQuery{}); len(l) != 0 {
		t.Fatalf("still listed after restore: %+v", l)
	}
}

func TestUndeleteRefusesNameAndIDClash(t *testing.T) {
	ctx := context.Background()
	env := newUndeleteEnv(t)
	folder := env.entry.GetFolder()

	// Same name, different id.
	if err := env.st.AddOrUpdate(&storage.Entry{Protocol: "nzb", InfoHash: "other-id", Name: env.entry.Name,
		Files: map[string]*storage.File{"ep.mkv": {Name: "ep.mkv", Size: 1}}}); err != nil {
		t.Fatal(err)
	}
	resp, err := env.u.Restore(ctx, "abc123", RestoreRemovedRequest{})
	if !errors.Is(err, ErrEntryExists) || !errors.Is(err, ErrInvalid) || resp.Existing != folder || resp.Applied {
		t.Fatalf("name clash: resp=%+v err=%v", resp, err)
	}
	if ok, _ := env.st.Exists("abc123"); ok {
		t.Fatal("a refused restore wrote the entry")
	}
	if _, err := os.Stat(filepath.Join(env.meta, "abc123.meta")); err == nil {
		t.Fatal("a refused restore wrote the meta file")
	}

	// Same id, different name.
	if err := env.st.Delete("other-id"); err != nil {
		t.Fatal(err)
	}
	if err := env.st.AddOrUpdate(&storage.Entry{Protocol: "nzb", InfoHash: "abc123", Name: "Renamed",
		Files: map[string]*storage.File{"ep.mkv": {Name: "ep.mkv", Size: 1}}}); err != nil {
		t.Fatal(err)
	}
	resp, err = env.u.Restore(ctx, "abc123", RestoreRemovedRequest{})
	if !errors.Is(err, ErrEntryExists) || resp.Existing != "Renamed" {
		t.Fatalf("id clash: resp=%+v err=%v", resp, err)
	}

	if _, err := env.u.Restore(ctx, "nope", RestoreRemovedRequest{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown key: %v", err)
	}
}
