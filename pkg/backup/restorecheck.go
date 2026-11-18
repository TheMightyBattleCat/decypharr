package backup

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// RestoreCheckStore is one store's count in a restore check.
type RestoreCheckStore struct {
	Name     string
	Snapshot int // records the snapshot holds
	Restored int // records in the store once the restored database is reopened
}

// RestoreCheckReport is what a restore dry run found.
type RestoreCheckReport struct {
	SnapshotID string
	// Naming is the folder naming the database was indexed under;
	// NamingFromSnapshot says it came from the snapshot's config.json.
	Naming             config.WebDavFolderNaming
	NamingFromSnapshot bool
	Stores             []RestoreCheckStore
	// Recorded is the total the snapshot's manifest recorded (0 when
	// unknown); Read is the sum of the stores' records.
	Recorded, Read int
	Import         storage.ImportReport
	// EntriesWithoutFolderItem counts entries whose folder under Naming has
	// no item record after the restore.
	EntriesWithoutFolderItem int
	// FolderItemsMissed is how many of those had an item in the snapshot
	// under another name: a mismatch when Naming came from the snapshot, a
	// note when the snapshot has no config to say which naming it used.
	FolderItemsMissed int
	Mismatches        []string
}

// OK reports whether the restore kept everything the snapshot holds.
func (r RestoreCheckReport) OK() bool { return len(r.Mismatches) == 0 }

// CheckRestore restores snapshotID's database into scratch (an empty folder
// the caller owns and removes), indexed under the snapshot's own folder
// naming, reopens it and compares every store with what the snapshot holds.
// Nothing outside scratch is written. A mismatch is in the report, not the
// error; the error is for a restore that could not be tried.
func CheckRestore(ctx context.Context, repo Repo, snapshotID, scratch string) (RestoreCheckReport, error) {
	rep := RestoreCheckReport{SnapshotID: snapshotID}
	snap, err := repo.ReadSnapshot(ctx, snapshotID)
	if err != nil {
		return rep, err
	}
	rep.SnapshotID = snap.ID
	entry, ok := snap.Components[ComponentDB]
	if !ok || len(entry.StoreBlobs) == 0 {
		return rep, fmt.Errorf("%w: snapshot %s has no database", ErrInvalid, snap.ID)
	}
	if snap.Decypharr.DBFormat > storage.DBFormat {
		return rep, fmt.Errorf("%w: snapshot %s was written by a newer build (%s, database format %d); this build reads format %d",
			ErrInvalid, snap.ID, snap.Decypharr.Commit, snap.Decypharr.DBFormat, storage.DBFormat)
	}
	if n, ok := snapshotFolderNaming(snap, repo); ok {
		rep.Naming, rep.NamingFromSnapshot = n, true
	} else {
		rep.Naming = config.Get().FolderNaming
	}

	dir := filepath.Join(scratch, "db")
	imp, err := storage.ImportStoresNamed(dir, rep.Naming, func(store string) (io.ReadCloser, error) {
		sha, ok := entry.StoreBlobs[store]
		if !ok {
			return nil, nil
		}
		return repo.OpenBlob(ctx, sha)
	})
	if err != nil {
		return rep, err
	}
	rep.Import = *imp
	st, err := storage.NewStorage(dir)
	if err != nil {
		return rep, fmt.Errorf("restored database does not open: %w", err)
	}
	counts := st.StoreCounts()
	items := st.GetEntryItems()
	err = st.ForEachMeta(func(m *storage.EntryMetaInfo) error {
		// Special keys and undecodable entries carry no folder name.
		if m.Name == "" || strings.HasPrefix(m.InfoHash, "__") {
			return nil
		}
		if _, ok := items[m.Name]; !ok {
			rep.EntriesWithoutFolderItem++
		}
		return nil
	})
	_ = st.Close()
	if err != nil {
		return rep, fmt.Errorf("read restored entries: %w", err)
	}

	for _, name := range storage.StoreNames() {
		s := RestoreCheckStore{Name: name, Snapshot: imp.Read[name], Restored: counts[name]}
		rep.Stores = append(rep.Stores, s)
		rep.Read += s.Snapshot
		if s.Restored != s.Snapshot {
			rep.Mismatches = append(rep.Mismatches, fmt.Sprintf("%s: %d of %d kept", storeLabel(name), s.Restored, s.Snapshot))
		}
	}
	rep.Recorded = entry.Records
	if rep.Recorded != 0 && rep.Read != rep.Recorded {
		rep.Mismatches = append(rep.Mismatches, fmt.Sprintf("the snapshot recorded %d records but holds %d", rep.Recorded, rep.Read))
	}
	// An entry indexed under a folder whose item was kept under another
	// name: the counts match, but the folder opens blank. Entries that had
	// no item in the snapshot are not the restore's doing. Without the
	// snapshot's config the naming is a guess, and a database-only restore
	// runs under the install's own naming, so that is only a note.
	if n := rep.EntriesWithoutFolderItem - imp.EntriesWithoutItems; n > 0 {
		rep.FolderItemsMissed = n
		if rep.NamingFromSnapshot {
			rep.Mismatches = append(rep.Mismatches, fmt.Sprintf("%d entries are indexed under a folder with no item record", n))
		}
	}
	return rep, nil
}

// storeLabel names a store the way the user knows it.
func storeLabel(name string) string {
	if name == "repair_state" {
		return "health records"
	}
	return name
}

// printRestoreCheck writes the report for the command line.
func printRestoreCheck(w io.Writer, r RestoreCheckReport) {
	from := "from the snapshot's config"
	if !r.NamingFromSnapshot {
		from = "the snapshot has no config.json, so the default"
	}
	fmt.Fprintf(w, "restore check of snapshot %s (folder_naming %q, %s)\n", r.SnapshotID, namingLabel(r.Naming), from)
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "STORE\tIN SNAPSHOT\tAFTER RESTORE")
	for _, s := range r.Stores {
		fmt.Fprintf(tw, "%s\t%d\t%d\n", storeLabel(s.Name), s.Snapshot, s.Restored)
	}
	_ = tw.Flush()
	if r.Recorded != 0 {
		fmt.Fprintf(w, "records: %d held, %d recorded by the snapshot\n", r.Read, r.Recorded)
	}
	var notes []string
	if n := r.Import.OrphanItems; n > 0 {
		notes = append(notes, fmt.Sprintf("dropped %d item records whose entry is not in the snapshot", n))
	}
	if n := r.Import.OrphanRepairState; n > 0 {
		notes = append(notes, fmt.Sprintf("dropped %d health records whose entry is not in the snapshot", n))
	}
	if n := r.Import.Undecodable; n > 0 {
		notes = append(notes, fmt.Sprintf("%d records could not be decoded and were kept as they are", n))
	}
	if n := r.FolderItemsMissed; n > 0 && !r.NamingFromSnapshot {
		notes = append(notes, fmt.Sprintf("%d entries would open blank under folder_naming %q; the snapshot has no config.json to say which naming it used, so restore it on an install that uses the naming it was taken under",
			n, namingLabel(r.Naming)))
	}
	if n := r.Import.EntriesWithoutItems; n > 0 {
		notes = append(notes, fmt.Sprintf("%d entries have no item record in the snapshot", n))
	}
	sort.Strings(notes)
	for _, n := range notes {
		fmt.Fprintln(w, "note:", n)
	}
	if r.OK() {
		fmt.Fprintln(w, "result: ok")
		return
	}
	for _, m := range r.Mismatches {
		fmt.Fprintln(w, "mismatch:", m)
	}
	fmt.Fprintln(w, "result: MISMATCH")
}
