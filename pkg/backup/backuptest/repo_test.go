package backuptest

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/pkg/backup"
)

func TestMemRepoBlobsAndPrune(t *testing.T) {
	ctx := context.Background()
	r := NewMemRepo()
	a, isNew, err := r.PutBlob(ctx, strings.NewReader("alpha"), backup.CompressDefault)
	if err != nil || !isNew {
		t.Fatalf("put: %v new=%v", err, isNew)
	}
	if _, isNew, _ = r.PutBlob(ctx, strings.NewReader("alpha"), backup.CompressDefault); isNew {
		t.Fatal("second put of the same bytes reported new")
	}
	b, _, _ := r.PutBlob(ctx, strings.NewReader("beta"), backup.CompressDefault)
	t0 := time.Date(2026, 10, 1, 3, 0, 0, 0, time.UTC)
	for i, sha := range []string{a.SHA256, b.SHA256} {
		_ = r.WriteSnapshot(ctx, &backup.Snapshot{
			ID: string(rune('a' + i)), CreatedAt: t0.Add(time.Duration(i) * time.Hour),
			Components: map[backup.Component]backup.ComponentEntry{backup.ComponentConfig: {Files: map[string]string{"config.json": sha}}},
		})
	}
	plan, _ := r.PrunePlan(ctx, backup.RetentionPolicy{Last: 1})
	if len(plan.DeleteSnapshot) != 1 || plan.DeleteSnapshot[0].ID != "a" || plan.BlobCount != 1 {
		t.Fatalf("plan = %+v", plan)
	}
	if res, _ := r.Prune(ctx, plan); res.DeletedSnapshots != 1 || res.DeletedBlobs != 1 {
		t.Fatalf("prune = %+v", res)
	}
	rc, err := r.OpenBlob(ctx, b.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := io.ReadAll(rc); string(got) != "beta" {
		t.Fatalf("blob = %q", got)
	}
	if rep, _ := r.Verify(ctx, backup.VerifyOptions{Full: true}, nil); !rep.OK {
		t.Fatalf("verify = %+v", rep)
	}
}
