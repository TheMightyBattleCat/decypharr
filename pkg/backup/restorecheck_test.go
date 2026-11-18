//go:build !windows

package backup_test

import (
	"context"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/backup"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// checkRepo writes, into a repository folder, a snapshot of an install that
// ran under infohash naming: two entries with their items and a health
// record for each, keyed by the infohash folder, plus health records for
// the extra names given (entries the snapshot does not hold).
func checkRepo(t *testing.T, dbFormat int, extraHealth ...string) string {
	return checkRepoWith(t, dbFormat, true, extraHealth...)
}

// checkRepoWith is checkRepo; withConfig false leaves the config out of
// the snapshot, as a database-only job saves it.
func checkRepoWith(t *testing.T, dbFormat int, withConfig bool, extraHealth ...string) string {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	setNaming(t, config.WebdavUseHash)
	st, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	for _, e := range []*storage.Entry{
		{InfoHash: "aaa", Name: "One", Category: "movies", Status: "downloaded", Protocol: config.ProtocolTorrent,
			Files: map[string]*storage.File{"one.mkv": {Name: "one.mkv", InfoHash: "aaa", Size: 1}}},
		{InfoHash: "bbb", Name: "Two", Category: "tv", Status: "downloaded", Protocol: config.ProtocolNZB,
			Files: map[string]*storage.File{"two.mkv": {Name: "two.mkv", InfoHash: "bbb", Size: 1}}},
	} {
		if err := st.AddOrUpdate(e); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range append([]string{"aaa", "bbb"}, extraHealth...) {
		if err := st.SaveEntryHealth(&storage.EntryHealth{EntryName: name, Status: storage.HealthStatus("healthy"),
			DecodeVerifiedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	if n := st.StoreCounts()["repair_state"]; n != 2+len(extraHealth) {
		t.Fatalf("source has %d health records, want %d", n, 2+len(extraHealth))
	}
	if n := st.StoreCounts()["items"]; n != 2 {
		t.Fatalf("source has %d items, want 2", n)
	}

	dir := t.TempDir()
	repo, err := backup.OpenRepo(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	dbEntry, err := backup.NewDBExporter(st)(ctx, repo, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, _, err := repo.PutBlob(ctx, strings.NewReader(`{"folder_naming":"infohash"}`), backup.CompressDefault)
	if err != nil {
		t.Fatal(err)
	}
	snap := &backup.Snapshot{
		ID: "20260101-000000-check", CreatedAt: time.Now(), Trigger: "manual:full",
		Decypharr: backup.BuildInfo{Commit: "abc1234", DBFormat: dbFormat},
		Components: map[backup.Component]backup.ComponentEntry{
			backup.ComponentDB:     dbEntry,
			backup.ComponentConfig: {Files: map[string]string{"config.json": ref.SHA256}},
		},
	}
	if !withConfig {
		delete(snap.Components, backup.ComponentConfig)
	}
	if err := repo.WriteSnapshot(ctx, snap); err != nil {
		t.Fatal(err)
	}
	return dir
}

// checkSandbox runs the install under the default naming (a fresh install
// restoring a backup taken under another naming), points the system temp
// folder at an empty folder and moves into another, so the test can see
// what the check leaves behind. Like the command in a fresh process, it
// starts with no config loaded or located.
func checkSandbox(t *testing.T) (tmp, cwd string) {
	t.Helper()
	setNaming(t, "")
	config.SetConfigPath("")
	config.Reset()
	tmp, cwd = t.TempDir(), t.TempDir()
	t.Setenv("TMPDIR", tmp)
	t.Chdir(cwd)
	return tmp, cwd
}

func assertEmpty(t *testing.T, what, dir string) {
	t.Helper()
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Fatalf("the check left %v in the %s", names, what)
	}
}

func row(label string, inSnapshot, restored int) *regexp.Regexp {
	return regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(label) + `\s+` +
		strconv.Itoa(inSnapshot) + `\s+` + strconv.Itoa(restored) + `\s*$`)
}

// A snapshot under infohash naming, checked on an install running the
// default naming: the dry run indexes under the snapshot's own naming and
// reports every health record kept.
func TestCLIRestoreCheckKeepsHealthRecordsUnderSnapshotNaming(t *testing.T) {
	repo := checkRepo(t, storage.DBFormat)
	tmp, cwd := checkSandbox(t)

	for _, args := range [][]string{
		{"restore", "--check", "--repo", repo, "--snapshot", "latest"},
		{"restore", "--check", "latest", "--repo", repo},
	} {
		code, out, e := runCLI(args...)
		if code != 0 {
			t.Fatalf("%v: %d\n%s%s", args, code, out, e)
		}
		for _, want := range []*regexp.Regexp{
			row("entries", 2, 2), row("items", 2, 2), row("health records", 2, 2),
			regexp.MustCompile(`folder_naming "infohash", from the snapshot's config`),
			regexp.MustCompile(`(?m)^result: ok$`),
		} {
			if !want.MatchString(out) {
				t.Fatalf("%v: output lacks %s:\n%s", args, want, out)
			}
		}
		if strings.Contains(out, "mismatch") {
			t.Fatalf("%v: reported a mismatch:\n%s", args, out)
		}
		assertEmpty(t, "system temp folder", tmp)
		assertEmpty(t, "working folder", cwd)
	}
}

// A health record whose entry is not in the snapshot is dropped by the
// restore: the check says how many were kept and exits non-zero.
func TestCLIRestoreCheckReportsDroppedHealthRecords(t *testing.T) {
	repo := checkRepo(t, storage.DBFormat, "Ghost")
	tmp, cwd := checkSandbox(t)

	code, out, e := runCLI("restore", "--check", "--repo", repo)
	if code != 1 {
		t.Fatalf("exit %d, want 1\n%s%s", code, out, e)
	}
	for _, want := range []*regexp.Regexp{
		row("health records", 3, 2),
		regexp.MustCompile(`(?m)^mismatch: health records: 2 of 3 kept$`),
		regexp.MustCompile(`(?m)^note: dropped 1 health records whose entry is not in the snapshot$`),
		regexp.MustCompile(`(?m)^result: MISMATCH$`),
	} {
		if !want.MatchString(out) {
			t.Fatalf("output lacks %s:\n%s", want, out)
		}
	}
	assertEmpty(t, "system temp folder", tmp)
	assertEmpty(t, "working folder", cwd)
}

// A check that cannot run still removes its scratch folder.
func TestCLIRestoreCheckRemovesScratchOnError(t *testing.T) {
	repo := checkRepo(t, storage.DBFormat+1)
	tmp, cwd := checkSandbox(t)

	code, out, e := runCLI("restore", "--check", "--repo", repo)
	if code != 1 || !strings.Contains(e, "newer build") {
		t.Fatalf("exit %d\n%s%s", code, out, e)
	}
	assertEmpty(t, "system temp folder", tmp)
	assertEmpty(t, "working folder", cwd)
}

// --check never takes a data folder or a restore's other options.
func TestCLIRestoreCheckRefusesDataFolderOptions(t *testing.T) {
	repo := checkRepo(t, storage.DBFormat)
	data := t.TempDir()
	tmp, _ := checkSandbox(t)
	for _, extra := range [][]string{
		{"--config", data}, {"--only", "db"}, {"--links-plan"}, {"--file", "x.tar.zst"},
		{"--snapshot", "latest", "latest"},
	} {
		args := append([]string{"restore", "--check", "--repo", repo}, extra...)
		if code, out, e := runCLI(args...); code != 2 {
			t.Fatalf("%v: exit %d\n%s%s", args, code, out, e)
		}
	}
	assertEmpty(t, "data folder", data)
	assertEmpty(t, "system temp folder", tmp)
}

// A database-only snapshot has no config to say which naming it was taken
// under. Checked under the default naming its items do not match the
// folders, but a database-only restore runs under the install's own naming,
// so the check notes it and does not fail.
func TestCLIRestoreCheckNotesUnknownNamingWithoutConfig(t *testing.T) {
	repo := checkRepoWith(t, storage.DBFormat, false)
	tmp, cwd := checkSandbox(t)

	code, out, e := runCLI("restore", "--check", "--repo", repo)
	if code != 0 {
		t.Fatalf("exit %d, want 0\n%s%s", code, out, e)
	}
	for _, want := range []*regexp.Regexp{
		row("items", 2, 2), row("health records", 2, 2),
		regexp.MustCompile(`folder_naming "filename", the snapshot has no config.json, so the default`),
		regexp.MustCompile(`(?m)^note: 2 entries would open blank under folder_naming "filename"; the snapshot has no config.json`),
		regexp.MustCompile(`(?m)^result: ok$`),
	} {
		if !want.MatchString(out) {
			t.Fatalf("output lacks %s:\n%s", want, out)
		}
	}
	assertEmpty(t, "system temp folder", tmp)
	assertEmpty(t, "working folder", cwd)
}
