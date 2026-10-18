package backup

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// ErrStoreLocked is returned when a Decypharr process still has the data
// folder's stores open.
var ErrStoreLocked = errors.New("backup: the data folder is in use by a running Decypharr; stop it first")

// RestoreOptions says what RestoreToDataDir puts back.
type RestoreOptions struct {
	SnapshotID string
	DataDir    string
	// Only limits the restore to these components; empty means every
	// component except links, which the caller restores through a Service.
	Only []Component
	// ApplyDB swaps the staged database in at once instead of leaving the
	// swap for the next start. Only safe when nothing runs on DataDir.
	ApplyDB bool
}

// RestoreReport is what RestoreToDataDir did.
type RestoreReport struct {
	SnapshotID string
	Restored   map[Component]int // files written per component
	Skipped    map[Component]string
	DB         *RestoreDBResponse
	Warnings   []string
}

// CheckStoresFree fails with ErrStoreLocked while another process holds any
// store lock under dataDir/db. On success it keeps every lock and returns a
// func that releases them (safe to call more than once), so nothing can open
// the stores between the check and the end of the restore.
func CheckStoresFree(dataDir string) (release func(), err error) {
	matches, err := filepath.Glob(filepath.Join(dataDir, "db", "*.lock"))
	if err != nil {
		return nil, err
	}
	var releases []func()
	var once sync.Once
	releaseAll := func() {
		once.Do(func() {
			for _, r := range releases {
				r()
			}
		})
	}
	for _, p := range matches {
		rel, busy, err := tryLock(p)
		if err != nil {
			releaseAll()
			return nil, fmt.Errorf("check store lock %s: %w", filepath.Base(p), err)
		}
		if busy {
			releaseAll()
			return nil, fmt.Errorf("%w (%s is locked)", ErrStoreLocked, filepath.Base(p))
		}
		releases = append(releases, rel)
	}
	return releaseAll, nil
}

func wanted(only []Component, c Component) bool {
	if len(only) == 0 {
		return c != ComponentLinks
	}
	for _, o := range only {
		if o == c {
			return true
		}
	}
	return false
}

// ParseComponents turns "db,config" into components, rejecting unknown names.
func ParseComponents(s string) ([]Component, error) {
	var out []Component
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		c := Component(p)
		if !c.Valid() {
			return nil, fmt.Errorf("%w: unknown component %q", ErrInvalid, p)
		}
		out = append(out, c)
	}
	return out, nil
}

// ResolveSnapshotID turns "latest" (or empty) into the newest snapshot's id.
func ResolveSnapshotID(ctx context.Context, repo Repo, id string) (string, error) {
	if id != "" && id != "latest" {
		return id, nil
	}
	list, err := repo.ListSnapshots(ctx)
	if err != nil {
		return "", err
	}
	if len(list) == 0 {
		return "", fmt.Errorf("%w: the repository has no snapshots", ErrNotFound)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].CreatedAt.After(list[j].CreatedAt) })
	return list[0].ID, nil
}

// RestoreToDataDir writes a snapshot's config, state, hearsay, NZB and
// overlay files into opts.DataDir and stages (or, with ApplyDB, swaps in)
// the database. Existing config and state files are moved aside, never
// deleted; NZB and overlay files that already exist are kept. The caller
// guarantees nothing is running on the folder (see CheckStoresFree).
func RestoreToDataDir(ctx context.Context, repo Repo, opts RestoreOptions) (RestoreReport, error) {
	rep := RestoreReport{SnapshotID: opts.SnapshotID, Restored: map[Component]int{}, Skipped: map[Component]string{}}
	snap, err := repo.ReadSnapshot(ctx, opts.SnapshotID)
	if err != nil {
		return rep, err
	}
	rep.SnapshotID = snap.ID
	if opts.DataDir == "" {
		return rep, fmt.Errorf("%w: a data folder is required", ErrInvalid)
	}
	if err := os.MkdirAll(opts.DataDir, 0o755); err != nil {
		return rep, err
	}
	stamp := time.Now().Format("20060102-150405")

	// The database goes first: it is the step that can still refuse (newer
	// format, a restore already staged, counts that differ), and nothing else
	// is written until it has passed.
	staged, filesDone := false, false
	if wanted(opts.Only, ComponentDB) {
		if _, ok := snap.Components[ComponentDB]; !ok {
			rep.Skipped[ComponentDB] = "not in the snapshot"
		} else {
			resp, err := StageDBRestore(repo, snap.ID, opts.DataDir)
			if err != nil {
				return rep, err
			}
			rep.DB = &resp
			rep.Warnings = append(rep.Warnings, resp.Warnings...)
			staged = true
			// A later failure must not leave a staged restore that blocks the
			// next attempt.
			defer func() {
				if !filesDone {
					_ = os.Remove(filepath.Join(opts.DataDir, PendingRestoreFile))
					_ = os.RemoveAll(resp.StagedDir)
				}
			}()
		}
	}

	for _, c := range []Component{ComponentConfig, ComponentState, ComponentHearsay} {
		if !wanted(opts.Only, c) {
			continue
		}
		ce, ok := snap.Components[c]
		if !ok || len(ce.Files) == 0 {
			rep.Skipped[c] = "not in the snapshot"
			continue
		}
		names := make([]string, 0, len(ce.Files))
		for n := range ce.Files {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			if !safeRel(n) {
				return rep, fmt.Errorf("%w: snapshot names an unsafe file %q", ErrInvalid, n)
			}
			dst := filepath.Join(opts.DataDir, filepath.FromSlash(n))
			if _, err := os.Lstat(dst); err == nil {
				if err := os.Rename(dst, dst+".before-restore-"+stamp); err != nil {
					return rep, fmt.Errorf("move %s aside: %w", n, err)
				}
			}
			if err := writeBlobFile(ctx, repo, ce.Files[n], dst, 0o600); err != nil {
				return rep, fmt.Errorf("restore %s: %w", n, err)
			}
			rep.Restored[c]++
		}
	}

	for _, c := range []Component{ComponentNZB, ComponentOverlay} {
		if !wanted(opts.Only, c) {
			continue
		}
		ce, ok := snap.Components[c]
		if !ok || ce.Tree == "" {
			rep.Skipped[c] = "not in the snapshot"
			continue
		}
		t, err := ReadTree(ctx, repo, ce.Tree)
		if err != nil {
			return rep, err
		}
		base := filepath.Join(opts.DataDir, "usenet", "meta")
		for rel, sha := range t.Files {
			if !safeRel(rel) {
				return rep, fmt.Errorf("%w: snapshot names an unsafe file %q", ErrInvalid, rel)
			}
			root := base
			if c == ComponentOverlay {
				// The overlay tree keeps its two folders under their own names.
				root = filepath.Join(opts.DataDir, "usenet")
			}
			dst := filepath.Join(root, filepath.FromSlash(rel))
			if _, err := os.Lstat(dst); err == nil {
				continue
			}
			if err := writeBlobFile(ctx, repo, sha, dst, 0o644); err != nil {
				return rep, fmt.Errorf("restore %s: %w", rel, err)
			}
			rep.Restored[c]++
		}
	}

	filesDone = true
	if staged {
		if opts.ApplyDB {
			if err := ApplyPendingRestore(opts.DataDir); err != nil {
				return rep, err
			}
			rep.DB.RestartRequired = false
		}
		total := 0
		for _, n := range rep.DB.Counts {
			total += n
		}
		rep.Restored[ComponentDB] = total
	}
	return rep, nil
}

// writeBlobFile copies a blob to dst through a temp file.
func writeBlobFile(ctx context.Context, bs BlobStore, sha, dst string, mode os.FileMode) error {
	rc, err := bs.OpenBlob(ctx, sha)
	if err != nil {
		return err
	}
	defer rc.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".restore-*.tmp")
	if err != nil {
		return err
	}
	_, cerr := io.Copy(tmp, rc)
	if err := errors.Join(cerr, tmp.Close()); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := os.Chmod(tmp.Name(), mode); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := os.Rename(tmp.Name(), dst); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	return nil
}

// ---- command line ----

// CLIEnv carries what the command needs from outside this package.
type CLIEnv struct {
	Out, Err io.Writer
	// NewService builds the same Service the server runs, for the data
	// folder and the repository folder given. The returned func releases
	// whatever it opened. Needed only for the links step.
	NewService func(dataDir, repoDir string) (*BackupService, func(), error)
}

// duringRestoreHook, when set by a test, runs after the restore and the swap
// and before the store locks are released.
var duringRestoreHook func(dataDir string)

// IsCLICommand reports whether args (os.Args[1:]) start the backup command.
func IsCLICommand(args []string) bool { return len(args) > 0 && args[0] == "backup" }

const cliUsage = `usage:
  decypharr backup list    --repo <dir>
  decypharr backup verify  --repo <dir> [--snapshot <id>] [--full]
  decypharr backup restore --repo <dir> --snapshot <id|latest> --config <data dir>
                           [--only db,config,state,nzb,overlay,hearsay,links] [--links-plan|--links-apply]
                           (--links-plan and --links-apply alone restore only the links)
  decypharr backup restore --file <path.tar.zst> --config <data dir> [--only db,config,state,hearsay]
                           (a downloaded archive instead of --repo; no links, nzb or overlay)
`

// RunCLI runs "backup <verb> ..." (args exclude the word "backup") and
// returns the process exit code.
func RunCLI(ctx context.Context, args []string, env CLIEnv) int {
	if env.Out == nil {
		env.Out = io.Discard
	}
	if env.Err == nil {
		env.Err = io.Discard
	}
	if len(args) == 0 {
		fmt.Fprint(env.Err, cliUsage)
		return 2
	}
	verb, rest := args[0], args[1:]
	fs := flag.NewFlagSet("backup "+verb, flag.ContinueOnError)
	fs.SetOutput(env.Err)
	repoDir := fs.String("repo", "", "backup folder")
	snapshot := fs.String("snapshot", "", "snapshot id or latest")
	full := fs.Bool("full", false, "re-hash every blob")
	dataDir := fs.String("config", "", "data folder to restore into")
	only := fs.String("only", "", "comma-separated components")
	linksPlan := fs.Bool("links-plan", false, "write a link plan file (default for links)")
	linksApply := fs.Bool("links-apply", false, "create the links")
	file := fs.String("file", "", "downloaded .tar.zst to restore (instead of --repo)")
	if err := fs.Parse(rest); err != nil {
		return 2
	}
	if verb == "restore" && *file != "" {
		if *repoDir != "" {
			fmt.Fprintln(env.Err, "backup: choose --repo or --file, not both")
			return 2
		}
		if *snapshot != "" || *linksPlan || *linksApply {
			fmt.Fprintln(env.Err, "backup: --snapshot, --links-plan and --links-apply need --repo")
			return 2
		}
		return cliRestoreFile(ctx, env, *file, *dataDir, *only)
	}
	if *repoDir == "" {
		fmt.Fprintln(env.Err, "backup: --repo is required")
		return 2
	}
	repo, err := OpenExistingRepo(*repoDir)
	if err != nil {
		fmt.Fprintln(env.Err, "backup:", err)
		return 1
	}
	fail := func(err error) int {
		fmt.Fprintln(env.Err, "backup:", err)
		return 1
	}

	switch verb {
	case "list":
		list, err := repo.ListSnapshots(ctx)
		if err != nil {
			return fail(err)
		}
		sort.Slice(list, func(i, j int) bool { return list[i].CreatedAt.After(list[j].CreatedAt) })
		tw := tabwriter.NewWriter(env.Out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tCREATED\tTRIGGER\tCOMPONENTS")
		for _, s := range list {
			cs := make([]string, len(s.Components))
			for i, c := range s.Components {
				cs[i] = string(c)
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", s.ID, s.CreatedAt.UTC().Format(time.RFC3339), s.Trigger, strings.Join(cs, ","))
		}
		_ = tw.Flush()
		return 0

	case "verify":
		id := *snapshot
		if id == "latest" {
			if id, err = ResolveSnapshotID(ctx, repo, id); err != nil {
				return fail(err)
			}
		}
		r, err := repo.Verify(ctx, VerifyOptions{SnapshotID: id, Full: *full}, nil)
		if err != nil {
			return fail(err)
		}
		fmt.Fprintf(env.Out, "snapshots: %d\nblobs checked: %d\nmissing: %d\ncorrupt: %d\n", r.Snapshots, r.BlobsChecked, len(r.Missing), len(r.Corrupt))
		if !r.OK {
			fmt.Fprintln(env.Out, "result: PROBLEMS FOUND")
			return 1
		}
		fmt.Fprintln(env.Out, "result: ok")
		return 0

	case "restore":
		if *dataDir == "" {
			fmt.Fprintln(env.Err, "backup: --config <data dir> is required")
			return 2
		}
		if *linksPlan && *linksApply {
			fmt.Fprintln(env.Err, "backup: choose --links-plan or --links-apply, not both")
			return 2
		}
		// Storage opens the logger, which loads the config: point it at the
		// data folder first, or a default config and logs/ land in the cwd.
		config.SetConfigPath(*dataDir)
		comps, err := ParseComponents(*only)
		if err != nil {
			return fail(err)
		}
		// Planning or applying links on its own must not re-run the whole
		// restore, so the links flags narrow it to links unless --only says
		// otherwise.
		if (*linksPlan || *linksApply) && strings.TrimSpace(*only) == "" {
			comps = []Component{ComponentLinks}
		}
		// The locks stay held until the restore and the database swap are
		// done; they are released before the links step opens the stores.
		releaseStores, err := CheckStoresFree(*dataDir)
		if err != nil {
			return fail(err)
		}
		defer releaseStores()
		id, err := ResolveSnapshotID(ctx, repo, *snapshot)
		if err != nil {
			return fail(err)
		}
		withLinks := *linksPlan || *linksApply
		for _, c := range comps {
			if c == ComponentLinks {
				withLinks = true
			}
		}
		rep, err := RestoreToDataDir(ctx, repo, RestoreOptions{SnapshotID: id, DataDir: *dataDir, Only: comps, ApplyDB: true})
		if duringRestoreHook != nil {
			duringRestoreHook(*dataDir)
		}
		releaseStores()
		if err != nil {
			return fail(err)
		}
		fmt.Fprintf(env.Out, "restored from snapshot %s into %s\n", rep.SnapshotID, *dataDir)
		printRestoreReport(env, rep)
		if withLinks {
			return cliLinks(ctx, env, *dataDir, *repoDir, id, *linksApply)
		}
		return 0
	}
	fmt.Fprintf(env.Err, "backup: unknown command %q\n%s", verb, cliUsage)
	return 2
}

func printRestoreReport(env CLIEnv, rep RestoreReport) {
	for _, c := range AllComponents() {
		if n, ok := rep.Restored[c]; ok {
			fmt.Fprintf(env.Out, "  %-8s %d\n", c, n)
		}
		if why, ok := rep.Skipped[c]; ok {
			fmt.Fprintf(env.Out, "  %-8s skipped: %s\n", c, why)
		}
	}
	for _, w := range rep.Warnings {
		fmt.Fprintln(env.Out, "  warning:", w)
	}
	if rep.DB != nil {
		fmt.Fprintln(env.Out, "the database is swapped in and the post-restore hold is on; release it from the Backups page after checking the install")
	}
}

// cliRestoreFile restores a downloaded .tar.zst into dataDir. It takes the
// same store locks and refusals as the repository restore.
func cliRestoreFile(ctx context.Context, env CLIEnv, file, dataDir, only string) int {
	fail := func(err error) int {
		fmt.Fprintln(env.Err, "backup:", err)
		return 1
	}
	if dataDir == "" {
		fmt.Fprintln(env.Err, "backup: --config <data dir> is required")
		return 2
	}
	config.SetConfigPath(dataDir)
	comps, err := ParseComponents(only)
	if err != nil {
		return fail(err)
	}
	for _, c := range comps {
		if c == ComponentLinks || c == ComponentNZB || c == ComponentOverlay {
			return fail(fmt.Errorf("%w: an archive cannot restore %s; use --repo", ErrInvalid, c))
		}
	}
	releaseStores, err := CheckStoresFree(dataDir)
	if err != nil {
		return fail(err)
	}
	defer releaseStores()
	rep, err := RestoreFromArchiveFile(ctx, file, ArchiveRestoreOptions{DataDir: dataDir, Only: comps, ApplyDB: true})
	if duringRestoreHook != nil {
		duringRestoreHook(dataDir)
	}
	releaseStores()
	if err != nil {
		return fail(err)
	}
	fmt.Fprintf(env.Out, "restored from %s into %s\n", file, dataDir)
	printRestoreReport(env, rep)
	return 0
}

// cliLinks plans (and with apply, creates) the snapshot's links.
func cliLinks(ctx context.Context, env CLIEnv, dataDir, repoDir, id string, apply bool) int {
	if env.NewService == nil {
		fmt.Fprintln(env.Err, "backup: links need the full service, which this build did not provide")
		return 1
	}
	svc, closeFn, err := env.NewService(dataDir, repoDir)
	if err != nil {
		fmt.Fprintln(env.Err, "backup:", err)
		return 1
	}
	defer closeFn()
	plan, err := svc.PlanLinksRestore(ctx, id, LinksPlanRequest{})
	if err != nil {
		fmt.Fprintln(env.Err, "backup:", err)
		return 1
	}
	if !apply {
		path := filepath.Join(dataDir, "links-plan-"+id+".json")
		b, _ := json.MarshalIndent(plan, "", "  ")
		if err := os.WriteFile(path, b, 0o644); err != nil {
			fmt.Fprintln(env.Err, "backup:", err)
			return 1
		}
		fmt.Fprintf(env.Out, "link plan written to %s (%d rows); run the same command with --links-apply to create them\n", path, len(plan.Rows))
		return 0
	}
	res, err := svc.ApplyLinksRestore(ctx, id, LinksApplyRequest{PlanID: plan.PlanID})
	if err != nil {
		fmt.Fprintln(env.Err, "backup:", err)
		return 1
	}
	fmt.Fprintf(env.Out, "links: %d created, %d skipped, %d failed\n", res.Created, res.Skipped, res.Failed)
	for _, w := range res.Warnings {
		fmt.Fprintln(env.Err, "warning:", w)
	}
	if res.Failed > 0 {
		return 1
	}
	return 0
}
