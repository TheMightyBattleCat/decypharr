package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Marker files in the data folder. WP4 reads PostRestoreHoldFile.
const (
	// PendingRestoreFile says a staged database waits to be swapped in.
	PendingRestoreFile = "db.pending-restore.json"
	// PostRestoreHoldFile says the first start after a swap must run in the
	// post-restore hold until the user releases it.
	PostRestoreHoldFile = "post-restore-hold.json"
)

// PendingRestore is the content of PendingRestoreFile.
type PendingRestore struct {
	SnapshotID string    `json:"snapshot_id"`
	StagedDir  string    `json:"staged_dir"` // folder name inside the data folder
	StagedAt   time.Time `json:"staged_at"`
}

// PostRestoreHold is the content of PostRestoreHoldFile.
type PostRestoreHold struct {
	SnapshotID string    `json:"snapshot_id"`
	SwappedAt  time.Time `json:"swapped_at"`
	// PreviousDir is the folder name the old database was moved to.
	PreviousDir string `json:"previous_dir"`
}

// PendingRestoreExists reports whether a staged restore is waiting.
func PendingRestoreExists(mainPath string) bool {
	_, err := os.Stat(filepath.Join(mainPath, PendingRestoreFile))
	return err == nil
}

// PostRestoreHoldActive reports whether the post-restore hold marker exists.
func PostRestoreHoldActive(mainPath string) bool {
	_, err := os.Stat(filepath.Join(mainPath, PostRestoreHoldFile))
	return err == nil
}

// ReleasePostRestoreHold removes the hold marker.
func ReleasePostRestoreHold(mainPath string) error {
	err := os.Remove(filepath.Join(mainPath, PostRestoreHoldFile))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func writeJSONFile(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// StageDBRestore writes the snapshot's database into db.restore-<ts>/ under
// mainPath, checks that it opens and that the record counts match the
// snapshot, and records a pending swap. Nothing live is touched: the swap
// happens at the next start (ApplyPendingRestore).
func StageDBRestore(repo Repo, snapshotID, mainPath string) (RestoreDBResponse, error) {
	ctx := context.Background()
	snap, err := repo.ReadSnapshot(ctx, snapshotID)
	if err != nil {
		return RestoreDBResponse{}, err
	}
	entry, ok := snap.Components[ComponentDB]
	if !ok || len(entry.StoreBlobs) == 0 {
		return RestoreDBResponse{}, fmt.Errorf("%w: snapshot %s has no database", ErrInvalid, snapshotID)
	}
	return stageDB(snapshotID, snap.Decypharr, entry.Records, mainPath, func(store string) (io.ReadCloser, error) {
		sha, ok := entry.StoreBlobs[store]
		if !ok {
			return nil, nil
		}
		return repo.OpenBlob(ctx, sha)
	})
}

// stageDB is the shared core of StageDBRestore and the archive restore:
// open returns the exported stream of a store, or (nil, nil) when there is
// none. records is the total the source recorded, or 0 when unknown.
func stageDB(snapshotID string, build BuildInfo, records int, mainPath string, open func(store string) (io.ReadCloser, error)) (RestoreDBResponse, error) {
	if err := checkDBStageable(snapshotID, build, mainPath); err != nil {
		return RestoreDBResponse{}, err
	}

	now := time.Now()
	stagedName := "db.restore-" + now.Format("20060102-150405")
	staged := filepath.Join(mainPath, stagedName)
	if err := os.MkdirAll(mainPath, 0o755); err != nil {
		return RestoreDBResponse{}, err
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.RemoveAll(staged)
		}
	}()
	if err := os.Mkdir(staged, 0o755); err != nil {
		return RestoreDBResponse{}, err
	}

	rep, err := storage.ImportStores(staged, open)
	if err != nil {
		return RestoreDBResponse{}, err
	}

	total := 0
	for _, n := range rep.Read {
		total += n
	}
	if records != 0 && total != records {
		return RestoreDBResponse{}, fmt.Errorf("restored database has %d records, the snapshot recorded %d", total, records)
	}

	// Reopen the staged folder the way startup will and compare counts.
	st, err := storage.NewStorage(staged)
	if err != nil {
		return RestoreDBResponse{}, fmt.Errorf("staged database does not open: %w", err)
	}
	counts := st.StoreCounts()
	_ = st.Close()
	for name, want := range rep.Imported {
		if counts[name] != want {
			return RestoreDBResponse{}, fmt.Errorf("staged %s has %d records, expected %d", name, counts[name], want)
		}
	}

	if err := writeJSONFile(filepath.Join(mainPath, PendingRestoreFile), PendingRestore{
		SnapshotID: snapshotID, StagedDir: stagedName, StagedAt: now,
	}); err != nil {
		return RestoreDBResponse{}, err
	}
	keep = true

	resp := RestoreDBResponse{StagedDir: staged, RestartRequired: true, Counts: counts}
	if rep.OrphanItems > 0 || rep.OrphanRepairState > 0 {
		resp.Warnings = append(resp.Warnings, fmt.Sprintf("dropped %d item records and %d repair states whose entry is not in the snapshot", rep.OrphanItems, rep.OrphanRepairState))
	}
	if rep.EntriesWithoutItems > 0 {
		resp.Warnings = append(resp.Warnings, fmt.Sprintf("%d entries have no item record and were kept", rep.EntriesWithoutItems))
	}
	return resp, nil
}

// ApplyPendingRestore swaps in a staged database restore before storage
// opens; mainPath is the data folder. With no pending restore it does
// nothing. The current db/ is renamed to db.before-restore-<ts>/ (kept, never
// deleted) and the staged folder becomes db/. It then writes the
// post-restore hold marker first. The pending marker is removed once the
// swap has succeeded, or at once when the marker or stage is itself broken
// (so that cannot block every start); a failed swap leaves it for a retry.
func ApplyPendingRestore(mainPath string) error {
	markerPath := filepath.Join(mainPath, PendingRestoreFile)
	data, err := os.ReadFile(markerPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	var pr PendingRestore
	if err := json.Unmarshal(data, &pr); err != nil {
		_ = os.Remove(markerPath)
		return fmt.Errorf("pending database restore marker is unreadable: %w", err)
	}
	if pr.StagedDir == "" || filepath.Base(pr.StagedDir) != pr.StagedDir {
		_ = os.Remove(markerPath)
		return fmt.Errorf("pending database restore names an invalid folder %q", pr.StagedDir)
	}
	staged := filepath.Join(mainPath, pr.StagedDir)
	if st, err := os.Stat(staged); err != nil || !st.IsDir() {
		_ = os.Remove(markerPath)
		return fmt.Errorf("staged database %s is missing; restore was not applied", pr.StagedDir)
	}

	live := filepath.Join(mainPath, "db")
	previous := ""
	if _, err := os.Stat(live); err == nil {
		previous = "db.before-restore-" + time.Now().Format("20060102-150405")
	}
	// The hold marker goes first: if it cannot be written nothing has moved
	// and the pending marker stays, so the next start tries again.
	holdPath := filepath.Join(mainPath, PostRestoreHoldFile)
	if err := writeJSONFile(holdPath, PostRestoreHold{
		SnapshotID: pr.SnapshotID, SwappedAt: time.Now(), PreviousDir: previous,
	}); err != nil {
		return fmt.Errorf("write post-restore hold marker: %w", err)
	}
	if previous != "" {
		if err := os.Rename(live, filepath.Join(mainPath, previous)); err != nil {
			_ = os.Remove(holdPath)
			return fmt.Errorf("move current database aside: %w", err)
		}
	}
	if err := os.Rename(staged, live); err != nil {
		if previous != "" {
			_ = os.Rename(filepath.Join(mainPath, previous), live)
		}
		_ = os.Remove(holdPath)
		return fmt.Errorf("move staged database into place: %w", err)
	}
	// Both renames succeeded; only now is the restore consumed.
	return os.Remove(markerPath)
}

// checkDBStageable is the part of staging a database that needs no data: the
// build must not be newer than this one and no restore may be staged yet.
func checkDBStageable(snapshotID string, build BuildInfo, mainPath string) error {
	if build.DBFormat > storage.DBFormat {
		return fmt.Errorf("%w: snapshot %s was written by a newer build (%s, database format %d); this build reads format %d. Install that build or a later one to restore it",
			ErrInvalid, snapshotID, build.Commit, build.DBFormat, storage.DBFormat)
	}
	if PendingRestoreExists(mainPath) {
		return fmt.Errorf("%w: a database restore is already staged; restart to apply it first", ErrBusy)
	}
	return nil
}
