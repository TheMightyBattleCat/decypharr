package backup

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// LinkCheckConfig wires NewLinkChecker.
type LinkCheckConfig struct {
	FS        LinkFS
	Lookup    EntryLookup
	MountRoot func() string
	Arrs      func() []ArrSource
	// ExtraRoots are the folders configured in Settings.
	ExtraRoots func() []string
	// OnResult receives the manifest and counts of each finished check. The
	// caller keeps the manifest and the trend history.
	OnResult func(*Manifest, LinkCheckResult)
}

// NewLinkChecker returns the LinkChecker the job runner calls.
func NewLinkChecker(cfg LinkCheckConfig) LinkChecker {
	return func(ctx context.Context, p Progress) (LinksStatusResponse, error) {
		fsys := cfg.FS
		if fsys == nil {
			fsys = OSLinkFS{}
		}
		var arrs []ArrSource
		if cfg.Arrs != nil {
			arrs = cfg.Arrs()
		}
		var extra []string
		if cfg.ExtraRoots != nil {
			extra = cfg.ExtraRoots()
		}
		mount := ""
		if cfg.MountRoot != nil {
			mount = cfg.MountRoot()
		}
		m, err := BuildManifest(ctx, ManifestInput{
			FS: fsys, Lookup: cfg.Lookup, MountRoot: mount,
			Roots: DiscoverRoots(ctx, arrs, extra), Arrs: arrs,
		}, p)
		if err != nil {
			return LinksStatusResponse{}, err
		}
		res := Summarise(m, time.Now().UTC())
		if cfg.OnResult != nil {
			cfg.OnResult(m, res)
		}
		return res.Status(), nil
	}
}

func newPlanID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// inScope reports whether a row is selected by the scope.
func inScope(sc LinkScope, pth, root string) bool {
	if sc.Root != "" {
		if _, ok := trimDirPrefix(pth, sc.Root); !ok && filepath.Clean(root) != filepath.Clean(sc.Root) {
			return false
		}
	}
	if sc.Prefix != "" {
		if _, ok := trimDirPrefix(pth, sc.Prefix); !ok {
			return false
		}
	}
	if sc.Series != "" {
		found := false
		for _, part := range strings.Split(filepath.ToSlash(filepath.Dir(pth)), "/") {
			if strings.EqualFold(part, sc.Series) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// LinkPlanInput is the input of BuildLinkPlan.
type LinkPlanInput struct {
	FS     LinkFS
	Lookup EntryLookup
	// MountRoot is the mount path now, which may differ from the recorded one.
	MountRoot  string
	Rows       []LinkRow
	Scope      LinkScope
	SnapshotID string
	Now        time.Time
}

// resolveTarget works out what restore would write for a row, and whether
// the database still knows it.
func resolveTarget(r LinkRow, mountRoot string, lk EntryLookup) (target, note string, resolved bool) {
	if r.Entry == "" || r.State == LinkExternal {
		return r.Target, "", true
	}
	mountRoot = strings.TrimRight(mountRoot, "/")
	build := func(entry string) string {
		t := mountRoot + "/" + allFolder + "/" + entry
		if r.File != "" {
			t += "/" + r.File
		}
		return t
	}
	okEntry := func(e string) bool { return lk.EntryExists(e) && (r.File == "" || lk.FileInEntry(e, r.File)) }
	if okEntry(r.Entry) {
		return build(r.Entry), "", true
	}
	if r.EntryID != "" {
		if e, ok := lk.EntryByID(r.EntryID); ok && okEntry(e) {
			return build(e), "entry was renamed or replaced", true
		}
	}
	if lk.EntryExists(r.Entry) {
		return build(r.Entry), "file is not in the entry any more", false
	}
	return build(r.Entry), "entry is no longer in the database", false
}

// BuildLinkPlan classifies each manifest row in scope. It reads the disk with
// Lstat and Readlink only.
func BuildLinkPlan(in LinkPlanInput) LinkPlan {
	now := in.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	plan := LinkPlan{PlanID: newPlanID(), SnapshotID: in.SnapshotID, Scope: in.Scope,
		Counts: map[PlanClass]int{}, CreatedAt: now}
	for _, r := range in.Rows {
		if !inScope(in.Scope, r.Path, r.Root) {
			continue
		}
		target, note, resolved := resolveTarget(r, in.MountRoot, in.Lookup)
		row := LinkPlanRow{Path: r.Path, Target: target, Recorded: r.Target, EntryID: r.EntryID,
			Arr: r.Arr, ArrItemID: r.ArrItemID, Note: note, MTime: r.MTime}
		fi, err := in.FS.Lstat(r.Path)
		switch {
		case err == nil && fi.Mode()&fs.ModeSymlink != 0:
			cur, rerr := in.FS.Readlink(r.Path)
			if rerr != nil {
				row.Class, row.Note = PlanConflict, "cannot read the existing link: "+rerr.Error()
				break
			}
			row.Current = cur
			switch {
			case !resolved && cur == target:
				row.Class = PlanOK
			case !resolved:
				row.Class = PlanUnresolved
			case cur == target:
				row.Class, row.Current = PlanOK, ""
			default:
				row.Class = PlanRetarget
			}
		case err == nil:
			row.Class, row.Note = PlanConflict, "a file or folder is already here and is never replaced"
		case errors.Is(err, fs.ErrNotExist):
			if resolved {
				row.Class = PlanCreate
			} else {
				row.Class = PlanUnresolved
			}
		default:
			row.Class, row.Note = PlanConflict, "cannot inspect the path: "+err.Error()
		}
		plan.Counts[row.Class]++
		plan.Rows = append(plan.Rows, row)
	}
	sort.Slice(plan.Rows, func(i, j int) bool { return plan.Rows[i].Path < plan.Rows[j].Path })
	return plan
}

// LinkApplyOptions selects what ApplyLinkPlan does beyond creating links.
type LinkApplyOptions struct {
	Retarget bool
	Dangling bool
	Rescan   bool
	// Rescanner asks an Arr (by name) to re-read an item.
	Rescanner func(ctx context.Context, arrName string, itemID int) error
}

const linkDirMode fs.FileMode = 0o755

// ApplyLinkPlan carries out a plan. A create uses a single symlink call,
// which is atomic and fails if anything appeared at the path since the plan.
// A retarget makes a temporary link and renames it over the old link, after
// confirming the path is still a link. A regular file is never replaced.
func ApplyLinkPlan(ctx context.Context, fsys LinkFS, plan LinkPlan, opts LinkApplyOptions) LinksApplyResponse {
	var res LinksApplyResponse
	type item struct {
		arr string
		id  int
	}
	rescans := map[item]bool{}
	fail := func(path string, err error) {
		res.Failed++
		res.Errors = append(res.Errors, fmt.Sprintf("%s: %v", path, err))
	}
	stamp := func(row LinkPlanRow, p string) {
		if row.MTime.IsZero() {
			return
		}
		if err := fsys.SetLinkTime(p, row.MTime); err != nil {
			res.Warnings = append(res.Warnings, fmt.Sprintf("%s: could not set the modification time: %v", row.Path, err))
		}
	}
	for _, row := range plan.Rows {
		if ctx.Err() != nil {
			fail(row.Path, ctx.Err())
			break
		}
		doCreate := row.Class == PlanCreate || (row.Class == PlanUnresolved && opts.Dangling)
		switch {
		case doCreate:
			if err := fsys.MkdirAll(filepath.Dir(row.Path), linkDirMode); err != nil {
				fail(row.Path, err)
				continue
			}
			if err := fsys.Symlink(row.Target, row.Path); err != nil {
				if errors.Is(err, fs.ErrExist) {
					res.Skipped++
				} else {
					fail(row.Path, err)
				}
				continue
			}
			stamp(row, row.Path)
			res.Created++
		case row.Class == PlanRetarget && opts.Retarget:
			fi, err := fsys.Lstat(row.Path)
			if err != nil || fi.Mode()&fs.ModeSymlink == 0 {
				res.Skipped++
				continue
			}
			tmp := fmt.Sprintf("%s.decypharr-%s.tmp", row.Path, newPlanID()[:6])
			if err := fsys.Symlink(row.Target, tmp); err != nil {
				fail(row.Path, err)
				continue
			}
			stamp(row, tmp)
			if err := fsys.Rename(tmp, row.Path); err != nil {
				_ = fsys.Remove(tmp)
				fail(row.Path, err)
				continue
			}
			res.Retarget++
		default:
			res.Skipped++
			continue
		}
		if row.Arr != "" && row.ArrItemID != 0 {
			rescans[item{row.Arr, row.ArrItemID}] = true
		}
	}
	if opts.Rescan && opts.Rescanner != nil {
		keys := make([]item, 0, len(rescans))
		for k := range rescans {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].arr != keys[j].arr {
				return keys[i].arr < keys[j].arr
			}
			return keys[i].id < keys[j].id
		})
		for _, k := range keys {
			if err := opts.Rescanner(ctx, k.arr, k.id); err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("rescan %s %d: %v", k.arr, k.id, err))
				continue
			}
			res.Rescans++
		}
	}
	return res
}

// The contract's CleanupGroup carries paths only, so the target each dead
// link had when the plan was built is kept here by PlanID. ApplyCleanup
// removes a link only while it still points at that target. Only the most
// recent plans are kept.
const maxRememberedPlans = 16

var cleanupTargets = struct {
	sync.Mutex
	byPlan map[string]map[string]string
	order  []string
}{byPlan: map[string]map[string]string{}}

func rememberCleanupTargets(planID string, targets map[string]string) {
	c := &cleanupTargets
	c.Lock()
	defer c.Unlock()
	c.byPlan[planID] = targets
	c.order = append(c.order, planID)
	for len(c.order) > maxRememberedPlans {
		delete(c.byPlan, c.order[0])
		c.order = c.order[1:]
	}
}

func cleanupTargetsFor(planID string) (map[string]string, bool) {
	c := &cleanupTargets
	c.Lock()
	defer c.Unlock()
	t, ok := c.byPlan[planID]
	return t, ok
}

// BuildCleanupPlan groups dead links by root and state. Live and external
// links are never selected. With no States in the filter both dead states are
// selected.
// matchesQuery is the substring match of the link list's search box.
func matchesQuery(r LinkRow, q string) bool {
	return strings.Contains(strings.ToLower(r.Path+" "+r.Entry), strings.ToLower(q))
}

func BuildCleanupPlan(rows []LinkRow, f LinksCleanupFilter, now time.Time) CleanupPlan {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	want := map[LinkState]bool{}
	for _, s := range f.States {
		if s == LinkTargetGone || s == LinkEntryGone {
			want[s] = true
		}
	}
	if len(f.States) == 0 {
		want[LinkTargetGone], want[LinkEntryGone] = true, true
	}
	plan := CleanupPlan{PlanID: newPlanID(), CreatedAt: now}
	groups := map[string]*CleanupGroup{}
	targets := map[string]string{}
	for _, r := range rows {
		if !want[r.State] || !inScope(LinkScope{Root: f.Root, Series: f.Series}, r.Path, r.Root) {
			continue
		}
		if f.Q != "" && !matchesQuery(r, f.Q) {
			continue
		}
		key := r.Root + "|" + string(r.State)
		g := groups[key]
		if g == nil {
			g = &CleanupGroup{Key: key, Root: r.Root, State: r.State}
			groups[key] = g
		}
		g.Count++
		g.Paths = append(g.Paths, r.Path)
		targets[r.Path] = r.Target
		plan.Total++
	}
	rememberCleanupTargets(plan.PlanID, targets)
	for _, g := range groups {
		sort.Strings(g.Paths)
		plan.Groups = append(plan.Groups, *g)
	}
	sort.Slice(plan.Groups, func(i, j int) bool { return plan.Groups[i].Key < plan.Groups[j].Key })
	return plan
}

// ApplyCleanup removes the links of the ticked groups. A path that is no
// longer a symlink, or that no longer points at the target recorded in the
// plan, is left alone. A plan that was not built by this process is refused.
func ApplyCleanup(ctx context.Context, fsys LinkFS, plan CleanupPlan, groups []string) CleanupResult {
	var res CleanupResult
	targets, known := cleanupTargetsFor(plan.PlanID)
	if !known {
		if len(groups) > 0 {
			res.Failed++
			res.Errors = append(res.Errors, "cleanup plan is no longer known; build a new plan")
		}
		return res
	}
	ticked := map[string]bool{}
	for _, g := range groups {
		ticked[g] = true
	}
	for _, g := range plan.Groups {
		if !ticked[g.Key] {
			continue
		}
		for _, p := range g.Paths {
			if ctx.Err() != nil {
				res.Failed++
				res.Errors = append(res.Errors, ctx.Err().Error())
				return res
			}
			fi, err := fsys.Lstat(p)
			if err != nil {
				if !errors.Is(err, fs.ErrNotExist) {
					res.Failed++
					res.Errors = append(res.Errors, fmt.Sprintf("%s: %v", p, err))
				}
				continue
			}
			if fi.Mode()&fs.ModeSymlink == 0 {
				continue
			}
			if cur, err := fsys.Readlink(p); err != nil || cur != targets[p] {
				continue
			}
			if err := fsys.Remove(p); err != nil {
				res.Failed++
				res.Errors = append(res.Errors, fmt.Sprintf("%s: %v", p, err))
				continue
			}
			res.Removed++
			res.RemovedPaths = append(res.RemovedPaths, p)
		}
	}
	return res
}
