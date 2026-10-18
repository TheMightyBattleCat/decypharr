package backup

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func newTestRepo(t *testing.T) *FileRepo {
	t.Helper()
	r, err := OpenRepo(filepath.Join(t.TempDir(), "repo"))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func put(t *testing.T, r *FileRepo, data []byte) BlobRef {
	t.Helper()
	ref, _, err := r.PutBlob(context.Background(), bytes.NewReader(data), CompressDefault)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestPutBlobDedup(t *testing.T) {
	r := newTestRepo(t)
	data := bytes.Repeat([]byte("hello world "), 1000)
	a, created, err := r.PutBlob(context.Background(), bytes.NewReader(data), CompressDefault)
	if err != nil || !created {
		t.Fatalf("first put: created=%v err=%v", created, err)
	}
	b, created, err := r.PutBlob(context.Background(), bytes.NewReader(data), CompressBest)
	if err != nil || created {
		t.Fatalf("second put: created=%v err=%v", created, err)
	}
	if a.SHA256 != b.SHA256 || b.Stored != a.Stored || a.Stored >= a.Size {
		t.Fatalf("refs differ: %+v %+v", a, b)
	}
	blobs, _ := r.listBlobs(context.Background())
	if len(blobs) != 1 {
		t.Fatalf("want 1 blob on disk, got %d", len(blobs))
	}
	rc, err := r.OpenBlob(context.Background(), a.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, data) {
		t.Fatal("round trip mismatch")
	}
}

func TestPutBlobCodecByLevel(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()
	for _, c := range []struct {
		level CompressionLevel
		ext   string
	}{{CompressDefault, ".xz"}, {CompressBest, ".xz"}, {CompressFast, ".zst"}} {
		data := bytes.Repeat([]byte("codec "+c.ext+strconv.Itoa(int(c.level))+" "), 2000)
		ref, _, err := r.PutBlob(ctx, bytes.NewReader(data), c.level)
		if err != nil {
			t.Fatal(err)
		}
		p, raw, err := r.blobPath(ref.SHA256)
		if err != nil || raw || filepath.Ext(p) != c.ext || ref.Stored >= ref.Size {
			t.Fatalf("level %d: path=%s raw=%v ref=%+v err=%v", c.level, p, raw, ref, err)
		}
		rc, err := r.OpenBlob(ctx, ref.SHA256)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(got, data) {
			t.Fatalf("level %d: round trip mismatch", c.level)
		}
	}
	if rep, err := r.Verify(ctx, VerifyOptions{Full: true}, nil); err != nil || rep.Corrupt != nil {
		t.Fatalf("full verify: %v %+v", err, rep)
	}
}

func TestFastStoreForcesZstd(t *testing.T) {
	r := newTestRepo(t)
	cs := &countingStore{BlobStore: r, fast: true}
	ref, _, err := cs.PutBlob(context.Background(), bytes.NewReader(bytes.Repeat([]byte("pre-action "), 2000)), CompressBest)
	if err != nil {
		t.Fatal(err)
	}
	if p, _, _ := r.blobPath(ref.SHA256); filepath.Ext(p) != ".zst" {
		t.Fatalf("pre-action blob stored as %s, want .zst", p)
	}
}

func TestOpenRepoUpgradesFormat1(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "repo")
	for _, sub := range []string{"blobs/ab", "snapshots", "index", "tmp"} {
		_ = os.MkdirAll(filepath.Join(dir, sub), 0o700)
	}
	_ = os.WriteFile(filepath.Join(dir, "repo.json"), []byte(`{"format":1,"compression":"zstd","min_saving_percent":3}`), 0o600)
	// A blob a format 1 build wrote.
	data := bytes.Repeat([]byte("old zstd blob "), 500)
	z, err := zstdCompress(data, CompressDefault)
	if err != nil {
		t.Fatal(err)
	}
	sha := "ab" + strings.Repeat("0", 62)
	_ = os.WriteFile(filepath.Join(dir, "blobs", "ab", sha+".zst"), z, 0o600)

	r, err := OpenRepo(dir)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(filepath.Join(dir, "repo.json"))
	if !strings.Contains(string(b), `"format": 2`) || !strings.Contains(string(b), `"compression": "xz"`) {
		t.Fatalf("repo.json not upgraded: %s", b)
	}
	rc, err := r.OpenBlob(context.Background(), sha)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, data) {
		t.Fatal("format 1 zstd blob no longer reads")
	}

	_ = os.WriteFile(filepath.Join(dir, "repo.json"), []byte(`{"format":3}`), 0o600)
	if _, err := OpenRepo(dir); err == nil {
		t.Fatal("a newer format should be refused")
	}
}

func TestRawStoreWhenIncompressible(t *testing.T) {
	r := newTestRepo(t)
	data := make([]byte, 64<<10)
	_, _ = rand.Read(data)
	ref := put(t, r, data)
	if ref.Stored != ref.Size {
		t.Fatalf("random data should be stored raw: %+v", ref)
	}
	p, raw, err := r.blobPath(ref.SHA256)
	if err != nil || !raw || !strings.HasSuffix(p, ".raw") {
		t.Fatalf("path=%s raw=%v err=%v", p, raw, err)
	}
	rc, _ := r.OpenBlob(context.Background(), ref.SHA256)
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, data) {
		t.Fatal("raw round trip mismatch")
	}
	// Empty content is stored too.
	e := put(t, r, nil)
	if e.Size != 0 {
		t.Fatalf("empty: %+v", e)
	}
}

func TestPermissionsAndLock(t *testing.T) {
	r := newTestRepo(t)
	ref := put(t, r, []byte("secret"))
	p, _, _ := r.blobPath(ref.SHA256)
	if st, _ := os.Stat(p); st.Mode().Perm() != 0o600 {
		t.Fatalf("blob mode %v", st.Mode().Perm())
	}
	if st, _ := os.Stat(r.Dir()); st.Mode().Perm() != 0o700 {
		t.Fatalf("dir mode %v", st.Mode().Perm())
	}
	ctx := context.Background()
	un, err := r.Lock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Lock(ctx); !errors.Is(err, ErrBusy) {
		t.Fatalf("second lock: %v", err)
	}
	// A second handle on the same folder sees the lock file too.
	r2, _ := OpenRepo(r.Dir())
	if _, err := r2.Lock(ctx); !errors.Is(err, ErrBusy) {
		t.Fatalf("other handle lock: %v", err)
	}
	un()
	un2, err := r.Lock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	un2()
}

func TestStaleLockReplaced(t *testing.T) {
	r := newTestRepo(t)
	if err := os.WriteFile(r.lockPath(), []byte("not-a-pid"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Minute)
	if err := os.Chtimes(r.lockPath(), old, old); err != nil {
		t.Fatal(err)
	}
	un, err := r.Lock(context.Background())
	if err != nil {
		t.Fatalf("stale lock should be replaced: %v", err)
	}
	un()
}

func TestFreshUnreadableLockIsBusy(t *testing.T) {
	r := newTestRepo(t)
	if err := os.WriteFile(r.lockPath(), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Lock(context.Background()); !errors.Is(err, ErrBusy) {
		t.Fatalf("a lock file just created must count as held, got %v", err)
	}
}

func TestLockWithOurPIDFromEarlierRunIsStale(t *testing.T) {
	r := newTestRepo(t)
	if err := os.WriteFile(r.lockPath(), []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		t.Fatal(err)
	}
	un, err := r.Lock(context.Background())
	if err != nil {
		t.Fatalf("lock left by an earlier process with our PID should be replaced: %v", err)
	}
	defer un()
	r2, _ := OpenRepo(r.Dir())
	if _, err := r2.Lock(context.Background()); !errors.Is(err, ErrBusy) {
		t.Fatalf("lock we hold must stay busy, got %v", err)
	}
}

func TestOpenRepoRefusesForeignFolder(t *testing.T) {
	d := t.TempDir()
	_ = os.WriteFile(filepath.Join(d, "x.txt"), []byte("x"), 0o600)
	if _, err := OpenRepo(d); !errors.Is(err, ErrInvalid) {
		t.Fatalf("want ErrInvalid, got %v", err)
	}
}

func snap(id string, at time.Time, trigger string, c map[Component]ComponentEntry) *Snapshot {
	return &Snapshot{ID: id, CreatedAt: at, Trigger: trigger, Components: c, Result: "ok"}
}

func TestSnapshotRoundTrip(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()
	s := &Snapshot{Trigger: "manual:quick", Components: map[Component]ComponentEntry{ComponentConfig: {Files: map[string]string{"config.json": put(t, r, []byte("{}")).SHA256}}}}
	if err := r.WriteSnapshot(ctx, s); err != nil {
		t.Fatal(err)
	}
	if s.ID == "" || s.CreatedAt.IsZero() {
		t.Fatal("id and time not filled")
	}
	got, err := r.ReadSnapshot(ctx, s.ID)
	if err != nil || got.Trigger != "manual:quick" {
		t.Fatalf("read: %v %+v", err, got)
	}
	list, _ := r.ListSnapshots(ctx)
	if len(list) != 1 || list[0].Components[0] != ComponentConfig {
		t.Fatalf("list: %+v", list)
	}
	if _, err := r.ReadSnapshot(ctx, "../etc"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("traversal: %v", err)
	}
	if err := r.DeleteSnapshot(ctx, s.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadSnapshot(ctx, s.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after delete: %v", err)
	}
}

// pruneFixture: three snapshots a day apart. Old uses blobs A (own) and S
// (shared); mid uses S; new uses S via a tree blob and N.
func pruneFixture(t *testing.T) (r *FileRepo, shaA, shaS string, oldSize int) {
	r = newTestRepo(t)
	ctx := context.Background()
	a := bytes.Repeat([]byte("only-in-old "), 500)
	refA := put(t, r, a)
	refS := put(t, r, bytes.Repeat([]byte("shared "), 500))
	refN := put(t, r, bytes.Repeat([]byte("newest "), 500))
	treeJSON := `{"files":{"f.meta":"` + refS.SHA256 + `","g.meta":"` + refN.SHA256 + `"}}`
	tree := put(t, r, []byte(treeJSON))
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	must := func(s *Snapshot) {
		if err := r.WriteSnapshot(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	must(snap("old", base, "schedule:quick", map[Component]ComponentEntry{ComponentConfig: {Files: map[string]string{"a": refA.SHA256, "s": refS.SHA256}}}))
	must(snap("mid", base.Add(24*time.Hour), "schedule:quick", map[Component]ComponentEntry{ComponentConfig: {Files: map[string]string{"s": refS.SHA256}}}))
	must(snap("new", base.Add(48*time.Hour), "schedule:quick", map[Component]ComponentEntry{ComponentNZB: {Tree: tree.SHA256}}))
	r.now = func() time.Time { return base.Add(72 * time.Hour) }
	p, _, _ := r.blobPath(refA.SHA256)
	st, _ := os.Stat(p)
	return r, refA.SHA256, refS.SHA256, int(st.Size())
}

func TestPrunePlanFreedBytesAndSharedBlob(t *testing.T) {
	r, shaA, shaS, aSize := pruneFixture(t)
	ctx := context.Background()
	plan, err := r.PrunePlan(ctx, RetentionPolicy{Last: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.DeleteSnapshot) != 1 || plan.DeleteSnapshot[0].ID != "old" || plan.KeepSnapshots != 2 {
		t.Fatalf("plan: %+v", plan)
	}
	if plan.BlobCount != 1 || plan.DeleteBlobs[0] != shaA || plan.FreedBytes != int64(aSize) {
		t.Fatalf("blobs: count=%d %v freed=%d want %d", plan.BlobCount, plan.DeleteBlobs, plan.FreedBytes, aSize)
	}
	// Plan alone deletes nothing.
	if ok, _ := r.HasBlob(ctx, shaA); !ok {
		t.Fatal("plan deleted a blob")
	}
	res, err := r.Prune(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if res.DeletedSnapshots != 1 || res.DeletedBlobs != 1 || res.FreedBytes != int64(aSize) {
		t.Fatalf("result: %+v", res)
	}
	if ok, _ := r.HasBlob(ctx, shaA); ok {
		t.Fatal("blob only the deleted snapshot used should be gone")
	}
	if ok, _ := r.HasBlob(ctx, shaS); !ok {
		t.Fatal("blob shared with a kept snapshot (via mid and a tree) was deleted")
	}
	// Everything still verifies.
	rep, err := r.Verify(ctx, VerifyOptions{Full: true}, nil)
	if err != nil || !rep.OK {
		t.Fatalf("verify after prune: %v %+v", err, rep)
	}
}

func TestPruneKeepsBlobsOfTreeChildren(t *testing.T) {
	// Keep only "new": S is reachable only through its tree blob.
	r, _, shaS, _ := pruneFixture(t)
	ctx := context.Background()
	plan, _ := r.PrunePlan(ctx, RetentionPolicy{Last: 1})
	for _, sha := range plan.DeleteBlobs {
		if sha == shaS {
			t.Fatal("tree child marked for deletion")
		}
	}
	if _, err := r.Prune(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if ok, _ := r.HasBlob(ctx, shaS); !ok {
		t.Fatal("tree child deleted")
	}
}

func TestPruneRetentionRules(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()
	base := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return base }
	for i := 0; i < 6; i++ { // six 6-hourly "schedule:quick" snaps on the 10th, 9th..
		at := base.Add(-time.Duration(i) * 12 * time.Hour)
		_ = r.WriteSnapshot(ctx, snap("q"+string(rune('a'+i)), at, "schedule:quick", nil))
	}
	_ = r.WriteSnapshot(ctx, snap("other", base.Add(-100*24*time.Hour), "manual:full", nil))
	plan, err := r.PrunePlan(ctx, RetentionPolicy{Last: 1, Daily: 2, TriggerPrefix: "schedule:quick"})
	if err != nil {
		t.Fatal(err)
	}
	// qa (10th 12:00) last+day10; qb(10th 00:00) dup day; qc (9th 12:00) day9; others dropped.
	del := map[string]bool{}
	for _, s := range plan.DeleteSnapshot {
		del[s.ID] = true
	}
	if del["qa"] || del["qc"] || del["other"] || !del["qb"] || !del["qd"] || len(del) != 4 {
		t.Fatalf("delete set: %v", del)
	}
	age, _ := r.PrunePlan(ctx, RetentionPolicy{Last: 10, MaxAgeDays: 1, TriggerPrefix: "schedule:quick"})
	for _, s := range age.DeleteSnapshot {
		if s.ID == "qa" || s.ID == "other" {
			t.Fatalf("wrong age deletion %s", s.ID)
		}
	}
	if len(age.DeleteSnapshot) != 3 { // qd, qe, qf are over a day old
		t.Fatalf("age plan: %+v", age.DeleteSnapshot)
	}
}

func TestPruneRefusesOnUnreadableManifest(t *testing.T) {
	r, _, _, _ := pruneFixture(t)
	_ = os.WriteFile(filepath.Join(r.Dir(), "snapshots", "junk.json.zst"), []byte("garbage"), 0o600)
	if _, err := r.PrunePlan(context.Background(), RetentionPolicy{Last: 1}); err == nil {
		t.Fatal("prune plan must not run with an unreadable manifest")
	}
}

func TestVerifyCatchesCorruptAndMissing(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()
	good := put(t, r, bytes.Repeat([]byte("good "), 400))
	bad := put(t, r, bytes.Repeat([]byte("bad! "), 400))
	gone := put(t, r, bytes.Repeat([]byte("gone "), 400))
	_ = r.WriteSnapshot(ctx, snap("s1", time.Now(), "manual:x", map[Component]ComponentEntry{
		ComponentConfig: {Files: map[string]string{"g": good.SHA256, "b": bad.SHA256, "m": gone.SHA256}}}))
	rep, err := r.Verify(ctx, VerifyOptions{Full: true}, nil)
	if err != nil || !rep.OK || rep.BlobsChecked != 3 {
		t.Fatalf("clean verify: %v %+v", err, rep)
	}
	// Flip bytes in the middle of one blob, delete another.
	p, _, _ := r.blobPath(bad.SHA256)
	b, _ := os.ReadFile(p)
	for i := len(b) / 2; i < len(b)/2+4 && i < len(b); i++ {
		b[i] ^= 0xff
	}
	_ = os.WriteFile(p, b, 0o600)
	gp, _, _ := r.blobPath(gone.SHA256)
	_ = os.Remove(gp)

	quick, _ := r.Verify(ctx, VerifyOptions{}, nil)
	if quick.OK || len(quick.Missing) != 1 || quick.Missing[0] != gone.SHA256 || len(quick.Corrupt) != 0 {
		t.Fatalf("quick: %+v", quick)
	}
	full, _ := r.Verify(ctx, VerifyOptions{Full: true, SnapshotID: "s1"}, nil)
	if full.OK || len(full.Missing) != 1 || len(full.Corrupt) != 1 || full.Corrupt[0] != bad.SHA256 {
		t.Fatalf("full: %+v", full)
	}
	if _, err := r.Verify(ctx, VerifyOptions{SnapshotID: "nope"}, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown snapshot: %v", err)
	}
}

func TestPruneJobPrefixMatchesOnlyThatJob(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()
	base := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return base }
	_ = r.WriteSnapshot(ctx, snap("d1", base, "schedule:daily", nil))
	_ = r.WriteSnapshot(ctx, snap("d0", base.Add(-time.Hour), "schedule:daily", nil))
	_ = r.WriteSnapshot(ctx, snap("f0", base.Add(-2*time.Hour), "schedule:daily-full", nil))
	_ = r.WriteSnapshot(ctx, snap("t0", base.Add(-3*time.Hour), "schedule:daily2", nil))
	plan, err := r.PrunePlan(ctx, RetentionPolicy{Last: 1, TriggerPrefix: "schedule:daily"})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.DeleteSnapshot) != 1 || plan.DeleteSnapshot[0].ID != "d0" {
		t.Fatalf("delete set: %+v", plan.DeleteSnapshot)
	}
}
