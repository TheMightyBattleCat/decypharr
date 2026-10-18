package backup

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

func newTestHistory(t *testing.T, mod func(*HistoryOptions)) *History {
	t.Helper()
	blobs, err := OpenRepo(filepath.Join(t.TempDir(), "hist-blobs"))
	if err != nil {
		t.Fatal(err)
	}
	o := HistoryOptions{Dir: t.TempDir(), Blobs: blobs}
	if mod != nil {
		mod(&o)
	}
	return NewHistory(o)
}

func TestHistoryUnchangedSaveAddsNothing(t *testing.T) {
	ctx := context.Background()
	h := newTestHistory(t, nil)
	if _, added, err := h.Record(ctx, []byte(`{"port":"1"}`), "a"); err != nil || !added {
		t.Fatalf("first record: added=%v err=%v", added, err)
	}
	if _, added, err := h.Record(ctx, []byte(`{"port":"1"}`), "b"); err != nil || added {
		t.Fatalf("unchanged record: added=%v err=%v", added, err)
	}
	if _, added, _ := h.Record(ctx, []byte(`{"port":"2"}`), "c"); !added {
		t.Fatal("a changed save was not recorded")
	}
	list, _ := h.List(ctx)
	if len(list) != 2 || list[0].Source != "c" {
		t.Fatalf("list = %+v, want 2 versions newest first", list)
	}
}

func TestHistoryDiffMasksSecrets(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	h := newTestHistory(t, func(o *HistoryOptions) {
		o.Now = func() time.Time { now = now.Add(time.Second); return now }
	})
	a := `{"port":"1","debrids":[{"name":"x","api_key":"OLD-KEY-VALUE"}],"discord":{"webhook_url":"https://discord.test/api/webhooks/1/OLDHOOK"},"auth":{"password":"OLD-PASS"},"use_auth":true}`
	b := `{"port":"2","debrids":[{"name":"x","api_key":"NEW-KEY-VALUE"}],"discord":{"webhook_url":"https://discord.test/api/webhooks/1/NEWHOOK"},"auth":{"password":"NEW-PASS"},"use_auth":false}`
	va, _, _ := h.Record(ctx, []byte(a), "a")
	vb, _, _ := h.Record(ctx, []byte(b), "b")

	for _, against := range []string{"previous", va.ID} {
		d, err := h.Diff(ctx, vb.ID, against)
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range d.Lines {
			for _, bad := range []string{"KEY-VALUE", "HOOK", "PASS"} {
				if strings.Contains(l.Old+l.New+l.Path, bad) {
					t.Fatalf("secret leaked in %+v", l)
				}
			}
		}
		got := map[string]ConfigDiffLine{}
		for _, l := range d.Lines {
			got[l.Path] = l
		}
		if l := got["port"]; l.Old != "1" || l.New != "2" || l.Secret {
			t.Fatalf("port line = %+v", l)
		}
		if l := got["use_auth"]; l.Secret || l.New != "false" {
			t.Fatalf("use_auth is a flag, not a secret: %+v", l)
		}
		for _, p := range []string{"debrids[0].api_key", "discord.webhook_url", "auth.password"} {
			if l := got[p]; !l.Secret || !l.Changed || l.Old != "" || l.New != "" {
				t.Fatalf("%s line = %+v", p, l)
			}
		}
	}
	// The first version diffs against nothing.
	if _, err := h.Diff(ctx, va.ID, "previous"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Diff(ctx, "missing", "previous"); err == nil {
		t.Fatal("unknown id did not fail")
	}
}

func TestHistoryCurrentDiffAndUnchangedSecret(t *testing.T) {
	ctx := context.Background()
	h := newTestHistory(t, func(o *HistoryOptions) {
		o.Current = func() ([]byte, error) { return []byte(`{"port":"9","token":"same"}`), nil }
	})
	v, _, _ := h.Record(ctx, []byte(`{"port":"1","token":"same"}`), "a")
	d, err := h.Diff(ctx, v.ID, "current")
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range d.Lines {
		if l.Path == "token" && (!l.Secret || l.Changed) {
			t.Fatalf("unchanged secret = %+v", l)
		}
	}
}

func TestHistoryTrimKeepsPinnedVersions(t *testing.T) {
	ctx := context.Background()
	repo, err := OpenRepo(filepath.Join(t.TempDir(), "repo"))
	if err != nil {
		t.Fatal(err)
	}
	h := newTestHistory(t, func(o *HistoryOptions) { o.Keep = 3; o.Snapshots = repo })
	var first ConfigVersion
	for i := 0; i < 6; i++ {
		v, _, err := h.Record(ctx, []byte(`{"n":`+string(rune('0'+i))+`}`), "s")
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = v
		}
		if i == 1 {
			// A kept snapshot references version 1's config blob.
			if err := repo.WriteSnapshot(ctx, &Snapshot{ID: "snap-1", CreatedAt: time.Now().UTC(), Trigger: "manual:quick",
				Components: map[Component]ComponentEntry{ComponentConfig: {Files: map[string]string{"config.json": v.SHA256}}}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	list, _ := h.List(ctx)
	if len(list) != 4 {
		t.Fatalf("kept %d versions, want 3 newest plus the pinned one", len(list))
	}
	for _, v := range list {
		if v.ID == first.ID {
			t.Fatal("unpinned oldest version survived")
		}
	}
}

func TestHistoryRevertWritesThroughSave(t *testing.T) {
	ctx := context.Background()
	t.Setenv("DECYPHARR_PORT", "")
	config.Reset()
	dir := t.TempDir()
	config.SetConfigPath(dir)
	t.Cleanup(func() { config.OnSaved = nil; config.Reset() })
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{"port":"8282","log_level":"info"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	config.Get()

	h := newTestHistory(t, nil)
	raw, _ := os.ReadFile(filepath.Join(dir, "config.json"))
	first, _, err := h.Record(ctx, raw, "initial")
	if err != nil {
		t.Fatal(err)
	}
	config.OnSaved = h.OnSaved

	if _, err := config.Update(func(c *config.Config) error { c.Port = "9999"; return nil }); err != nil {
		t.Fatal(err)
	}
	list, _ := h.List(ctx)
	if len(list) != 2 {
		t.Fatalf("a settings save left %d versions, want 2", len(list))
	}

	resp, err := h.Revert(ctx, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := config.Get().Port; got != "8282" {
		t.Fatalf("port after revert = %q", got)
	}
	onDisk, _ := os.ReadFile(filepath.Join(dir, "config.json"))
	if !strings.Contains(string(onDisk), `"8282"`) {
		t.Fatalf("config.json not rewritten: %s", onDisk)
	}
	list, _ = h.List(ctx)
	if len(list) != 3 || resp.NewID != list[0].ID {
		t.Fatalf("revert did not become a new version: list=%d resp=%+v", len(list), resp)
	}
	if !resp.RestartRequired {
		t.Log("port change reported as hot")
	}
	if _, err := h.Revert(ctx, "nope"); err == nil {
		t.Fatal("revert of an unknown id succeeded")
	}
}

func TestHistoryRevertCallsApplied(t *testing.T) {
	ctx := context.Background()
	t.Setenv("DECYPHARR_PORT", "")
	config.Reset()
	dir := t.TempDir()
	config.SetConfigPath(dir)
	t.Cleanup(func() { config.OnSaved = nil; config.Reset() })
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{"port":"8282","log_level":"info"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	config.Get()

	var gotPrev, gotNow string
	h := newTestHistory(t, func(o *HistoryOptions) {
		o.Applied = func(prev, pub *config.Config) { gotPrev, gotNow = prev.Port, pub.Port }
	})
	raw, _ := os.ReadFile(filepath.Join(dir, "config.json"))
	first, _, err := h.Record(ctx, raw, "initial")
	if err != nil {
		t.Fatal(err)
	}
	config.OnSaved = h.OnSaved
	if _, err := config.Update(func(c *config.Config) error { c.Port = "9999"; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Revert(ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	if gotPrev != "9999" || gotNow != "8282" {
		t.Fatalf("Applied saw %q -> %q, want 9999 -> 8282", gotPrev, gotNow)
	}
}

func TestHistoryTrimsWhenBackupsAreNotConfigured(t *testing.T) {
	ctx := context.Background()
	live := LiveRepo{Open: func() (Repo, error) { return nil, ErrNotConfigured }}
	h := newTestHistory(t, func(o *HistoryOptions) { o.Keep = 3; o.Snapshots = live })
	for i := 0; i < 6; i++ {
		if _, _, err := h.Record(ctx, []byte(`{"n":`+string(rune('0'+i))+`}`), "s"); err != nil {
			t.Fatal(err)
		}
	}
	if list, _ := h.List(ctx); len(list) != 3 {
		t.Fatalf("kept %d versions with no snapshot repository, want 3", len(list))
	}
}

func TestHistoryKeepFollowsTheSavedSetting(t *testing.T) {
	h := newTestHistory(t, func(o *HistoryOptions) { o.Keep = 5 })
	if h.keep() != 5 {
		t.Fatalf("keep = %d, want 5", h.keep())
	}
	h.OnSaved([]byte(`{"backup":{"config_history_max":2}}`))
	if h.keep() != 2 {
		t.Fatalf("keep = %d after a save, want 2", h.keep())
	}
}
