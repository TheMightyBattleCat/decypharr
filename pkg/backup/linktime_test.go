//go:build linux || darwin

package backup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type failTimeFS struct{ recFS }

func (*failTimeFS) SetLinkTime(string, time.Time) error { return errors.New("denied") }

func TestApplyLinkPlanSetsRecordedTimes(t *testing.T) {
	d := t.TempDir()
	rec := time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC)
	fsys := &recFS{}
	created := filepath.Join(d, "a", "created.mkv")
	retarget := filepath.Join(d, "retarget.mkv")
	zero := filepath.Join(d, "zero.mkv")
	file := filepath.Join(d, "file.mkv")
	mustLink(t, "/nowhere/old", retarget)
	if err := os.WriteFile(file, []byte("real"), 0o644); err != nil {
		t.Fatal(err)
	}
	plan := LinkPlan{Rows: []LinkPlanRow{
		{Path: created, Class: PlanCreate, Target: "/nowhere/new", MTime: rec},
		{Path: retarget, Class: PlanRetarget, Target: "/nowhere/new2", MTime: rec},
		{Path: zero, Class: PlanCreate, Target: "/nowhere/z"},
		{Path: file, Class: PlanConflict, Target: "/nowhere/f", MTime: rec},
	}}
	res := ApplyLinkPlan(context.Background(), fsys, plan, LinkApplyOptions{Retarget: true})
	if res.Created != 2 || res.Retarget != 1 || res.Failed != 0 || len(res.Warnings) != 0 {
		t.Fatalf("apply = %+v", res)
	}
	for _, p := range []string{created, retarget} {
		fi, err := os.Lstat(p)
		if err != nil || !fi.ModTime().Equal(rec) {
			t.Errorf("%s mtime = %v %v", p, fi, err)
		}
	}
	if fi, _ := os.Lstat(zero); time.Since(fi.ModTime()) > time.Minute {
		t.Errorf("zero MTime must leave the time alone: %v", fi.ModTime())
	}
	if b, _ := os.ReadFile(file); string(b) != "real" {
		t.Errorf("regular file replaced")
	}
	if fi, _ := os.Lstat(file); fi.ModTime().Equal(rec) {
		t.Errorf("regular file time changed")
	}
	if len(fsys.stats) != 0 {
		t.Fatalf("Stat called on %v", fsys.stats)
	}
}

func TestApplyLinkPlanTimeFailureIsAWarning(t *testing.T) {
	d := t.TempDir()
	rec := time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC)
	p := filepath.Join(d, "l.mkv")
	plan := LinkPlan{Rows: []LinkPlanRow{{Path: p, Class: PlanCreate, Target: "/nowhere", MTime: rec}}}
	res := ApplyLinkPlan(context.Background(), &failTimeFS{}, plan, LinkApplyOptions{})
	if res.Created != 1 || res.Failed != 0 || len(res.Errors) != 0 || len(res.Warnings) != 1 {
		t.Fatalf("apply = %+v", res)
	}
	if _, err := os.Readlink(p); err != nil {
		t.Errorf("link missing: %v", err)
	}
}

func TestBuildLinkPlanCarriesRecordedTime(t *testing.T) {
	d := t.TempDir()
	rec := time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC)
	rows := []LinkRow{{Path: filepath.Join(d, "x.mkv"), Target: "/external/x", MTime: rec}}
	plan := BuildLinkPlan(LinkPlanInput{FS: &recFS{}, Lookup: fakeLookup{}, MountRoot: "/mnt/decy", Rows: rows})
	if len(plan.Rows) != 1 || !plan.Rows[0].MTime.Equal(rec) {
		t.Fatalf("plan = %+v", plan.Rows)
	}
}
