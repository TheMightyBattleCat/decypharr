package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// ErrEntryExists is returned (with a response naming the live entry) when an
// entry with the same ID or name is already in the database. It wraps
// ErrInvalid.
var ErrEntryExists = fmt.Errorf("%w: an entry with that name or id already exists", ErrInvalid)

// UndeleteOptions configures the recently-removed list and entry restore.
type UndeleteOptions struct {
	Repo Repo
	// Store is the live database.
	Store *storage.Storage
	// TempDir is where a snapshot's database is staged to be read; empty
	// uses the system temp folder.
	TempDir string
	// MetaDir, OverlayDir and VolumeOrderDir are the live usenet folders
	// (usenet/meta, usenet/overlay, usenet/volume-order-backup).
	MetaDir        string
	OverlayDir     string
	VolumeOrderDir string
	// Lookup answers link-plan questions from the live database.
	Lookup EntryLookup
	// FS is the filesystem links are checked and created on; nil uses the
	// real one.
	FS LinkFS
	// MountRoot is the mount path now.
	MountRoot string
	// Rescanner asks an Arr to re-read an item; nil makes a requested
	// rescan a no-op.
	Rescanner func(ctx context.Context, arrName string, itemID int) error
}

// Undelete implements UndeleteService.
type Undelete struct {
	o UndeleteOptions

	mu    sync.Mutex
	cache map[string]*snapSummary // by snapshot id; snapshots never change
	// restoreMu serialises restores so two cannot race the clash check.
	restoreMu sync.Mutex
}

var _ UndeleteService = (*Undelete)(nil)

// NewUndelete returns the undelete service for o.
func NewUndelete(o UndeleteOptions) *Undelete {
	if o.FS == nil {
		o.FS = OSLinkFS{}
	}
	return &Undelete{o: o, cache: map[string]*snapSummary{}}
}

type removedInfo struct {
	name, category string
	size           int64
	links          int
}

// snapSummary is what the list needs from one snapshot.
type snapSummary struct {
	id      string
	at      time.Time
	entries map[string]removedInfo // by entry id
	folders map[string]struct{}
}

// stagedDB is a snapshot's database opened in a scratch folder.
type stagedDB struct {
	*storage.Storage
	dir string
}

func (s *stagedDB) close() {
	_ = s.Storage.Close()
	_ = os.RemoveAll(s.dir)
}

func (u *Undelete) stage(ctx context.Context, snap *Snapshot) (*stagedDB, error) {
	ce, ok := snap.Components[ComponentDB]
	if !ok || len(ce.StoreBlobs) == 0 {
		return nil, fmt.Errorf("%w: snapshot %s has no database", ErrNotFound, snap.ID)
	}
	if snap.Decypharr.DBFormat > storage.DBFormat {
		return nil, fmt.Errorf("%w: snapshot %s was written by a newer build (db format %d)", ErrInvalid, snap.ID, snap.Decypharr.DBFormat)
	}
	scratch, err := os.MkdirTemp(u.o.TempDir, "decypharr-undelete-")
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(scratch, "db")
	_, err = storage.ImportStores(dir, func(store string) (io.ReadCloser, error) {
		sha := ce.StoreBlobs[store]
		if sha == "" {
			return nil, nil
		}
		return u.o.Repo.OpenBlob(ctx, sha)
	})
	if err != nil {
		_ = os.RemoveAll(scratch)
		return nil, err
	}
	st, err := storage.NewStorage(dir)
	if err != nil {
		_ = os.RemoveAll(scratch)
		return nil, err
	}
	return &stagedDB{Storage: st, dir: scratch}, nil
}

func (u *Undelete) readManifest(ctx context.Context, snap *Snapshot) *Manifest {
	ce, ok := snap.Components[ComponentLinks]
	if !ok || ce.Manifest == "" {
		return nil
	}
	rc, err := u.o.Repo.OpenBlob(ctx, ce.Manifest)
	if err != nil {
		return nil
	}
	defer rc.Close()
	m, err := DecodeManifest(rc)
	if err != nil {
		return nil
	}
	return m
}

func (u *Undelete) summarise(ctx context.Context, snap *Snapshot) (*snapSummary, error) {
	u.mu.Lock()
	if s, ok := u.cache[snap.ID]; ok {
		u.mu.Unlock()
		return s, nil
	}
	u.mu.Unlock()

	db, err := u.stage(ctx, snap)
	if err != nil {
		return nil, err
	}
	defer db.close()
	s := &snapSummary{id: snap.ID, at: snap.CreatedAt, entries: map[string]removedInfo{}, folders: map[string]struct{}{}}
	err = db.ForEachMeta(func(m *storage.EntryMetaInfo) error {
		s.entries[m.InfoHash] = removedInfo{name: m.Name, category: m.Category, size: m.Size}
		s.folders[m.Name] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if m := u.readManifest(ctx, snap); m != nil {
		byFolder := map[string]string{}
		for id, in := range s.entries {
			byFolder[in.name] = id
		}
		for _, r := range m.Rows {
			id := r.EntryID
			if _, ok := s.entries[id]; !ok {
				id = byFolder[r.Entry]
			}
			if in, ok := s.entries[id]; ok && id != "" {
				in.links++
				s.entries[id] = in
			}
		}
	}
	u.mu.Lock()
	u.cache[snap.ID] = s
	u.mu.Unlock()
	return s, nil
}

// liveSets returns the ids and folder names in the live database.
func (u *Undelete) liveSets() (ids, names map[string]string, err error) {
	ids, names = map[string]string{}, map[string]string{}
	err = u.o.Store.ForEachMeta(func(m *storage.EntryMetaInfo) error {
		ids[m.InfoHash] = m.Name
		names[m.Name] = m.InfoHash
		return nil
	})
	return ids, names, err
}

// dbSnapshots returns the snapshots that hold a database, oldest first.
func (u *Undelete) dbSnapshots(ctx context.Context) ([]*Snapshot, error) {
	infos, err := u.o.Repo.ListSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	var out []*Snapshot
	for _, in := range infos {
		s, err := u.o.Repo.ReadSnapshot(ctx, in.ID)
		if err != nil {
			return nil, err
		}
		if _, ok := s.Components[ComponentDB]; ok {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// ListRemoved lists entries that an older snapshot holds and the live
// database does not (by id or by folder name), newest removal first.
func (u *Undelete) ListRemoved(ctx context.Context, q RemovedQuery) ([]RemovedEntry, error) {
	snaps, err := u.dbSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	liveIDs, liveNames, err := u.liveSets()
	if err != nil {
		return nil, err
	}
	type seen struct {
		info removedInfo
		idx  int
	}
	last := map[string]seen{}
	sums := make([]*snapSummary, len(snaps))
	for i, snap := range snaps {
		s, err := u.summarise(ctx, snap)
		if err != nil {
			return nil, err
		}
		sums[i] = s
		for id, in := range s.entries {
			last[id] = seen{info: in, idx: i}
		}
	}
	needle := strings.ToLower(q.Name)
	now := time.Now().UTC()
	var out []RemovedEntry
	for id, sn := range last {
		if _, live := liveIDs[id]; live {
			continue
		}
		if _, live := liveNames[sn.info.name]; live {
			continue
		}
		e := RemovedEntry{
			Key: id, Name: sn.info.name, Category: sn.info.category,
			LastSnapshotID: sums[sn.idx].id, LastSeen: sums[sn.idx].at,
			Size: sn.info.size, LinkCount: sn.info.links,
		}
		if sn.idx+1 < len(sums) {
			e.FirstMissing = sums[sn.idx+1].at
		}
		if needle != "" && !strings.Contains(strings.ToLower(e.Name), needle) {
			continue
		}
		if q.Category != "" && !strings.EqualFold(e.Category, q.Category) {
			continue
		}
		// The entry went missing somewhere in (LastSeen, FirstMissing]; keep
		// it when that window overlaps the one asked for.
		end := e.FirstMissing
		if end.IsZero() {
			end = now
		}
		if !q.Since.IsZero() && end.Before(q.Since) {
			continue
		}
		if !q.Until.IsZero() && e.LastSeen.After(q.Until) {
			continue
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].LastSeen.Equal(out[j].LastSeen) {
			return out[i].LastSeen.After(out[j].LastSeen)
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// restoringLookup answers as though the entry being restored were already in
// the database, so its links plan the same before and after the restore.
type restoringLookup struct {
	base   EntryLookup
	id     string
	folder string
	files  map[string]bool
}

func (l restoringLookup) EntryExists(e string) bool {
	return e == l.folder || l.base.EntryExists(e)
}
func (l restoringLookup) FileInEntry(e, f string) bool {
	if e == l.folder {
		return l.files[f]
	}
	return l.base.FileInEntry(e, f)
}
func (l restoringLookup) EntryByID(id string) (string, bool) {
	if id == l.id {
		return l.folder, true
	}
	return l.base.EntryByID(id)
}
func (l restoringLookup) EntryID(e string) string {
	if e == l.folder {
		return l.id
	}
	return l.base.EntryID(e)
}

// fileCopy is a file to bring back from a blob.
type fileCopy struct {
	dst string
	sha string
}

// planFiles lists the meta, overlay and volume-order files of an entry in
// the snapshot, with their destinations.
func (u *Undelete) planFiles(ctx context.Context, snap *Snapshot, id string) ([]fileCopy, error) {
	var out []fileCopy
	if ce, ok := snap.Components[ComponentNZB]; ok && ce.Tree != "" && u.o.MetaDir != "" {
		t, err := ReadTree(ctx, u.o.Repo, ce.Tree)
		if err != nil {
			return nil, err
		}
		for rel, sha := range t.Files {
			rel = strings.TrimPrefix(rel, "meta/")
			if rel == id+".meta" && safeRel(rel) {
				out = append(out, fileCopy{dst: filepath.Join(u.o.MetaDir, filepath.FromSlash(rel)), sha: sha})
			}
		}
	}
	if ce, ok := snap.Components[ComponentOverlay]; ok && ce.Tree != "" {
		t, err := ReadTree(ctx, u.o.Repo, ce.Tree)
		if err != nil {
			return nil, err
		}
		for rel, sha := range t.Files {
			if !safeRel(rel) {
				continue
			}
			segs := strings.Split(rel, "/")
			root := u.o.OverlayDir
			switch segs[0] {
			case "overlay":
				segs = segs[1:]
			case "volume-order-backup":
				root, segs = u.o.VolumeOrderDir, segs[1:]
			}
			if root == "" || len(segs) == 0 {
				continue
			}
			first := strings.TrimSuffix(segs[0], path.Ext(segs[0]))
			if segs[0] != id && first != id {
				continue
			}
			out = append(out, fileCopy{dst: filepath.Join(append([]string{root}, segs...)...), sha: sha})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].dst < out[j].dst })
	return out, nil
}

// safeRel reports whether a tree path stays inside its folder.
func safeRel(rel string) bool {
	if rel == "" || strings.HasPrefix(rel, "/") || strings.Contains(rel, `\`) {
		return false
	}
	for _, s := range strings.Split(rel, "/") {
		if s == ".." || s == "" {
			return false
		}
	}
	return true
}

// copyBlob writes blob sha to dst atomically unless dst already exists: a
// file that is there now is newer than the snapshot and is kept.
func (u *Undelete) copyBlob(ctx context.Context, f fileCopy) (written bool, err error) {
	if _, err := os.Lstat(f.dst); err == nil {
		return false, nil
	}
	rc, err := u.o.Repo.OpenBlob(ctx, f.sha)
	if err != nil {
		return false, err
	}
	defer rc.Close()
	if err := os.MkdirAll(filepath.Dir(f.dst), 0o755); err != nil {
		return false, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(f.dst), ".restore-*.tmp")
	if err != nil {
		return false, err
	}
	_, cerr := io.Copy(tmp, rc)
	if err := errors.Join(cerr, tmp.Close()); err != nil {
		_ = os.Remove(tmp.Name())
		return false, err
	}
	if err := os.Chmod(tmp.Name(), 0o644); err != nil {
		_ = os.Remove(tmp.Name())
		return false, err
	}
	if err := os.Rename(tmp.Name(), f.dst); err != nil {
		_ = os.Remove(tmp.Name())
		return false, err
	}
	return true, nil
}

// Restore brings the removed entry key back from a snapshot. It refuses,
// returning ErrEntryExists and a response that names the live entry, when an
// entry with the same id or name exists now. With DryRun it only plans.
func (u *Undelete) Restore(ctx context.Context, key string, req RestoreRemovedRequest) (RestoreRemovedResponse, error) {
	u.restoreMu.Lock()
	defer u.restoreMu.Unlock()
	resp := RestoreRemovedResponse{Key: key}

	snap, err := u.pickSnapshot(ctx, key, req.SnapshotID)
	if err != nil {
		return resp, err
	}
	db, err := u.stage(ctx, snap)
	if err != nil {
		return resp, err
	}
	defer db.close()
	entry, err := db.Get(key)
	if err != nil || entry == nil {
		return resp, fmt.Errorf("%w: entry %s is not in snapshot %s", ErrNotFound, key, snap.ID)
	}
	folder := entry.GetFolder()

	liveIDs, liveNames, err := u.liveSets()
	if err != nil {
		return resp, err
	}
	if name, ok := liveIDs[key]; ok {
		resp.Existing = name
		resp.Message = "an entry with this id is already in the database"
		return resp, ErrEntryExists
	}
	if id, ok := liveNames[folder]; ok {
		resp.Existing = folder
		resp.Message = fmt.Sprintf("an entry named %q (id %s) is already in the database", folder, id)
		return resp, ErrEntryExists
	}

	item, _ := db.GetEntryItem(folder)
	var health *storage.EntryHealth
	if h, herr := db.GetEntryHealth(folder); herr == nil {
		health = h
	} else if h, herr := db.GetEntryHealth(entry.Name); herr == nil {
		health = h
	}
	files, err := u.planFiles(ctx, snap, key)
	if err != nil {
		return resp, err
	}
	for _, f := range files {
		if ok, err := u.o.Repo.HasBlob(ctx, f.sha); err != nil || !ok {
			return resp, fmt.Errorf("%w: a file of this entry is missing from the backup", ErrNotFound)
		}
	}

	plan := u.planLinks(ctx, snap, key, folder, entry, item)
	if plan != nil {
		resp.Links = plan
	}
	if req.DryRun {
		resp.Message = fmt.Sprintf("would restore %q with %d file(s) and %d link(s)", folder, len(files), linkCreates(plan))
		return resp, nil
	}

	// 1. records, through the typed saves.
	if err := u.o.Store.AddOrUpdate(entry); err != nil {
		return resp, fmt.Errorf("restore entry: %w", err)
	}
	if item != nil {
		if err := u.o.Store.UpdateItem(item); err != nil {
			return resp, fmt.Errorf("restore items: %w", err)
		}
	}
	if health != nil {
		if err := u.o.Store.SaveEntryHealth(health); err != nil {
			return resp, fmt.Errorf("restore repair state: %w", err)
		}
	}
	// 2. .meta and overlay files.
	written := 0
	for _, f := range files {
		ok, err := u.copyBlob(ctx, f)
		if err != nil {
			return resp, fmt.Errorf("restore %s: %w", filepath.Base(f.dst), err)
		}
		if ok {
			written++
		}
	}
	// 3. links, create rows only.
	if plan != nil && len(plan.Rows) > 0 {
		res := ApplyLinkPlan(ctx, u.o.FS, *plan, LinkApplyOptions{Rescan: req.Rescan, Rescanner: u.o.Rescanner})
		resp.LinkResult = &res
	}
	resp.Applied = true
	resp.Message = fmt.Sprintf("restored %q from snapshot %s: %d file(s) written", folder, snap.ID, written)
	if resp.LinkResult != nil {
		resp.Message += fmt.Sprintf(", %d link(s) created", resp.LinkResult.Created)
	}
	return resp, nil
}

func linkCreates(p *LinkPlan) int {
	if p == nil {
		return 0
	}
	return p.Counts[PlanCreate]
}

func (u *Undelete) pickSnapshot(ctx context.Context, key, id string) (*Snapshot, error) {
	if id != "" {
		s, err := u.o.Repo.ReadSnapshot(ctx, id)
		if err != nil {
			return nil, err
		}
		sum, err := u.summarise(ctx, s)
		if err != nil {
			return nil, err
		}
		if _, ok := sum.entries[key]; !ok {
			return nil, fmt.Errorf("%w: entry %s is not in snapshot %s", ErrNotFound, key, id)
		}
		return s, nil
	}
	snaps, err := u.dbSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	for i := len(snaps) - 1; i >= 0; i-- {
		sum, err := u.summarise(ctx, snaps[i])
		if err != nil {
			return nil, err
		}
		if _, ok := sum.entries[key]; ok {
			return snaps[i], nil
		}
	}
	return nil, fmt.Errorf("%w: no snapshot holds entry %s", ErrNotFound, key)
}

// planLinks builds the create-only plan for the entry's links in snap. It
// returns nil when the snapshot has no link manifest.
func (u *Undelete) planLinks(ctx context.Context, snap *Snapshot, key, folder string, entry *storage.Entry, item *storage.EntryItem) *LinkPlan {
	m := u.readManifest(ctx, snap)
	if m == nil {
		return nil
	}
	var rows []LinkRow
	for _, r := range m.Rows {
		if r.EntryID == key || (r.EntryID == "" && r.Entry == folder) {
			rows = append(rows, r)
		}
	}
	files := map[string]bool{}
	if item != nil {
		for name, f := range item.Files {
			if f != nil && !f.Deleted {
				files[name] = true
			}
		}
	}
	for name, f := range entry.Files {
		if f != nil && !f.Deleted {
			files[name] = true
		}
	}
	plan := BuildLinkPlan(LinkPlanInput{
		FS:         u.o.FS,
		Lookup:     restoringLookup{base: u.o.Lookup, id: key, folder: folder, files: files},
		MountRoot:  u.o.MountRoot,
		Rows:       rows,
		SnapshotID: snap.ID,
		Now:        time.Now().UTC(),
	})
	// Applied with Retarget and Dangling off, so only create rows act:
	// retarget, conflict and unresolved rows are shown, never changed.
	return &plan
}
