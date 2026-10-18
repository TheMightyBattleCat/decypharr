// Package backup is Decypharr's backup feature: a content-addressed
// repository of snapshots (database export, settings, NZB records, overlay,
// library symlink manifest), their restore, and the Library Links checks.
//
// This file holds the contracts the other packages build against: the
// repository and service interfaces and the request and response types the
// API serialises. Work packages implement them in their own files and do not
// change this one.
package backup

import (
	"context"
	"errors"
	"io"
	"time"
)

// Errors the API maps to status codes.
var (
	// ErrNotImplemented is returned by every Service method until the
	// feature is wired up; the API answers 501.
	ErrNotImplemented = errors.New("backup: not implemented")
	// ErrNotFound is a missing snapshot, plan, entry or history version (404).
	ErrNotFound = errors.New("backup: not found")
	// ErrInvalid is a request the caller can correct (400).
	ErrInvalid = errors.New("backup: invalid request")
	// ErrBusy is returned when another backup is already running (409).
	ErrBusy = errors.New("backup: another backup is already running")
	// ErrNotConfigured is returned when no backup folder is set (409).
	ErrNotConfigured = errors.New("backup: no backup folder is configured")
)

// Component names one part of a snapshot.
type Component string

const (
	ComponentDB      Component = "db"
	ComponentConfig  Component = "config"
	ComponentState   Component = "state"
	ComponentNZB     Component = "nzb"
	ComponentOverlay Component = "overlay"
	ComponentLinks   Component = "links"
	ComponentHearsay Component = "hearsay"
)

// AllComponents lists every component in a stable order.
func AllComponents() []Component {
	return []Component{ComponentDB, ComponentConfig, ComponentState, ComponentNZB, ComponentOverlay, ComponentLinks, ComponentHearsay}
}

// Valid reports whether c is a known component.
func (c Component) Valid() bool {
	for _, k := range AllComponents() {
		if c == k {
			return true
		}
	}
	return false
}

// RepoFormatVersion is the value written to repo.json. Format 2 added xz
// blobs; opening a format 1 repository upgrades its repo.json so an older
// build refuses it rather than missing the xz blobs.
const RepoFormatVersion = 2

// BlobRef names one stored blob.
type BlobRef struct {
	SHA256 string `json:"sha256"`
	// Size is the uncompressed size in bytes.
	Size int64 `json:"size"`
	// Stored is the size on disk (after zstd, or equal to Size for .raw).
	Stored int64 `json:"stored,omitempty"`
}

// BuildInfo records the build that wrote a snapshot.
type BuildInfo struct {
	Version  string `json:"version"`
	Commit   string `json:"commit"`
	DBFormat int    `json:"db_format"`
}

// ComponentEntry is one component's record inside a snapshot manifest. Which
// fields are set depends on the component.
type ComponentEntry struct {
	// db: store name -> blob sha, and the record count.
	StoreBlobs map[string]string `json:"store_blobs,omitempty"`
	Records    int               `json:"records,omitempty"`
	// config and state: file name -> blob sha.
	Files map[string]string `json:"files,omitempty"`
	// nzb and overlay: sha of the tree blob (relative path -> sha), with the
	// file and byte totals.
	Tree      string `json:"tree,omitempty"`
	FileCount int    `json:"file_count,omitempty"`
	Bytes     int64  `json:"bytes,omitempty"`
	// links: sha of the manifest blob, row count and the roots walked.
	Manifest string   `json:"manifest,omitempty"`
	Rows     int      `json:"rows,omitempty"`
	Roots    []string `json:"roots,omitempty"`
}

// SnapshotStats is what a run wrote.
type SnapshotStats struct {
	NewBlobs   int   `json:"new_blobs"`
	NewBytes   int64 `json:"new_bytes"`
	DurationMS int64 `json:"duration_ms"`
}

// Snapshot is the manifest stored in snapshots/<timestamp>-<short id>.json.zst.
type Snapshot struct {
	ID         string                       `json:"id"`
	CreatedAt  time.Time                    `json:"created_at"`
	Trigger    string                       `json:"trigger"` // "schedule:<job>", "manual:<job>", "pre:<action>"
	Decypharr  BuildInfo                    `json:"decypharr"`
	Components map[Component]ComponentEntry `json:"components"`
	Stats      SnapshotStats                `json:"stats"`
	// Result is ResultOK, or ResultPartial for a full run that saved its
	// small components but did not finish its walks (it was stopped, failed
	// or interrupted). Failed runs leave no other record here; their history
	// comes from the job log.
	Result string `json:"result,omitempty"`
}

const (
	ResultOK = "ok"
	// ResultPartial snapshots hold everything but the NZB records and the
	// overlay. They are kept until a complete snapshot of the same job is
	// newer, never count as the job's last success, and never count towards
	// a retention policy's numbers.
	ResultPartial = "partial"
)

// SnapshotInfo is a Snapshot summarised for lists.
type SnapshotInfo struct {
	ID         string      `json:"id"`
	CreatedAt  time.Time   `json:"created_at"`
	Trigger    string      `json:"trigger"`
	Components []Component `json:"components"`
	NewBytes   int64       `json:"new_bytes"`
	DurationMS int64       `json:"duration_ms"`
	Commit     string      `json:"commit,omitempty"`
	DBFormat   int         `json:"db_format,omitempty"`
	Result     string      `json:"result,omitempty"`
}

// Info summarises s.
func (s *Snapshot) Info() SnapshotInfo {
	info := SnapshotInfo{
		ID: s.ID, CreatedAt: s.CreatedAt, Trigger: s.Trigger,
		NewBytes: s.Stats.NewBytes, DurationMS: s.Stats.DurationMS,
		Commit: s.Decypharr.Commit, DBFormat: s.Decypharr.DBFormat, Result: s.Result,
	}
	for _, c := range AllComponents() {
		if _, ok := s.Components[c]; ok {
			info.Components = append(info.Components, c)
		}
	}
	return info
}

// RetentionPolicy is grandfather-father-son: keep the last N snapshots, plus
// one per day for D days, per week for W weeks and per month for M months.
type RetentionPolicy struct {
	Last    int `json:"last"`
	Daily   int `json:"daily"`
	Weekly  int `json:"weekly"`
	Monthly int `json:"monthly"`
	// TriggerPrefix limits the policy to snapshots whose trigger starts with
	// it (for example "schedule:quick"). Empty means every snapshot.
	TriggerPrefix string `json:"trigger_prefix,omitempty"`
	// MaxAgeDays drops snapshots older than this on top of the counts; 0
	// means no limit. Pre-action snapshots use it (keep 20, at most 14 days).
	MaxAgeDays int `json:"max_age_days,omitempty"`
}

// PrunePlan is what a prune would delete.
type PrunePlan struct {
	Policy         RetentionPolicy `json:"policy"`
	DeleteSnapshot []SnapshotInfo  `json:"delete_snapshots"`
	KeepSnapshots  int             `json:"keep_snapshots"`
	DeleteBlobs    []string        `json:"-"` // shas; not sent to the browser
	BlobCount      int             `json:"blob_count"`
	FreedBytes     int64           `json:"freed_bytes"`
}

// PruneResult is what a prune did.
type PruneResult struct {
	DeletedSnapshots int   `json:"deleted_snapshots"`
	DeletedBlobs     int   `json:"deleted_blobs"`
	FreedBytes       int64 `json:"freed_bytes"`
}

// Progress is called as work advances. It may be nil.
type Progress func(ProgressUpdate)

// ProgressUpdate is one progress step.
type ProgressUpdate struct {
	Component    Component `json:"component,omitempty"`
	Phase        string    `json:"phase,omitempty"`
	FilesDone    int       `json:"files_done"`
	FilesTotal   int       `json:"files_total"`
	BytesWritten int64     `json:"bytes_written"`
	// BytesDone and BytesTotal are the source bytes a walk has gone through
	// (files taken from the cache included) and will go through.
	BytesDone  int64 `json:"bytes_done,omitempty"`
	BytesTotal int64 `json:"bytes_total,omitempty"`
	// RateBPS is the walk's speed while it was not paused, and ETASeconds
	// the time left at that speed.
	RateBPS    int64 `json:"rate_bps,omitempty"`
	ETASeconds int64 `json:"eta_seconds,omitempty"`
	// Paused says why the walk is waiting, and PausedSince since when. A
	// paused walk keeps reporting, so UpdatedAt stays fresh.
	Paused      string     `json:"paused,omitempty"`
	PausedSince *time.Time `json:"paused_since,omitempty"`
	// UpdatedAt is when the update was reported and RunStarted when the run
	// began; the service fills both in.
	UpdatedAt  *time.Time `json:"updated_at,omitempty"`
	RunStarted *time.Time `json:"run_started,omitempty"`
}

// Report calls p when it is not nil.
func (p Progress) Report(u ProgressUpdate) {
	if p != nil {
		p(u)
	}
}

// VerifyOptions selects how deep Verify goes.
type VerifyOptions struct {
	// SnapshotID limits the check to one snapshot; empty checks all.
	SnapshotID string `json:"snapshot_id,omitempty"`
	// Full reads and re-hashes every blob; otherwise blob presence and size
	// are checked.
	Full bool `json:"full,omitempty"`
	// BytesPerSec limits a full check's reading, and while Pause returns
	// true it waits between blobs. The scheduled check sets both from the
	// backup settings; they are not part of the API.
	BytesPerSec int64       `json:"-"`
	Pause       func() bool `json:"-"`
}

// VerifyReport is the result of Verify.
type VerifyReport struct {
	Snapshots    int       `json:"snapshots"`
	BlobsChecked int       `json:"blobs_checked"`
	Missing      []string  `json:"missing,omitempty"`
	Corrupt      []string  `json:"corrupt,omitempty"`
	OK           bool      `json:"ok"`
	CheckedAt    time.Time `json:"checked_at"`
}

// BlobStore is the content-addressed half of a Repo.
type BlobStore interface {
	// PutBlob stores r (compressing it unless that saves under 3%) and
	// returns its reference. Storing an existing blob is a no-op that
	// reports the existing reference.
	PutBlob(ctx context.Context, r io.Reader, level CompressionLevel) (BlobRef, bool, error)
	// OpenBlob returns the decompressed contents of the blob.
	OpenBlob(ctx context.Context, sha string) (io.ReadCloser, error)
	HasBlob(ctx context.Context, sha string) (bool, error)
}

// CompressionLevel picks how a blob is compressed. Default and Best use xz,
// the smallest of the codecs measured on NZB records; Fast uses zstd for
// snapshots taken while the user waits.
type CompressionLevel int

const (
	CompressDefault CompressionLevel = iota
	CompressBest
	CompressFast
)

// Repo is one backup repository (a destination folder).
type Repo interface {
	BlobStore
	// Lock takes the single-writer lock and returns the release function.
	Lock(ctx context.Context) (unlock func(), err error)
	WriteSnapshot(ctx context.Context, s *Snapshot) error
	ListSnapshots(ctx context.Context) ([]SnapshotInfo, error)
	ReadSnapshot(ctx context.Context, id string) (*Snapshot, error)
	// DeleteSnapshot removes the manifest only; Prune removes the blobs.
	DeleteSnapshot(ctx context.Context, id string) error
	PrunePlan(ctx context.Context, policy RetentionPolicy) (PrunePlan, error)
	Prune(ctx context.Context, plan PrunePlan) (PruneResult, error)
	Verify(ctx context.Context, opts VerifyOptions, p Progress) (VerifyReport, error)
}

// LinkState is the health of one library symlink.
type LinkState string

const (
	LinkLive       LinkState = "live"
	LinkTargetGone LinkState = "target_gone"
	LinkEntryGone  LinkState = "entry_gone"
	LinkExternal   LinkState = "external"
)

// LinkRow is one symlink in the manifest.
type LinkRow struct {
	Path      string    `json:"path"`
	Target    string    `json:"target"` // raw readlink string
	MountRoot string    `json:"mount_root,omitempty"`
	Entry     string    `json:"entry,omitempty"`
	File      string    `json:"file,omitempty"`
	EntryID   string    `json:"entry_id,omitempty"`
	Arr       string    `json:"arr,omitempty"`
	ArrItemID int       `json:"arr_item_id,omitempty"`
	ArrFileID int       `json:"arr_file_id,omitempty"`
	Mode      uint32    `json:"mode,omitempty"`
	MTime     time.Time `json:"mtime,omitempty"`
	State     LinkState `json:"state"`
	Root      string    `json:"root,omitempty"`
	// ArrPath and LocalPath are the two forms when an Arr uses a path map.
	ArrPath string `json:"arr_path,omitempty"`
}

// PlanClass classifies a row of a link-restore plan.
type PlanClass string

const (
	PlanCreate     PlanClass = "create"
	PlanOK         PlanClass = "ok"
	PlanRetarget   PlanClass = "retarget"
	PlanConflict   PlanClass = "conflict"
	PlanUnresolved PlanClass = "unresolved"
)

// LinkPlanRow is one row of a restore plan.
type LinkPlanRow struct {
	Path      string    `json:"path"`
	Class     PlanClass `json:"class"`
	Target    string    `json:"target"`             // what restore would write
	Current   string    `json:"current,omitempty"`  // for retarget: what is there now
	Recorded  string    `json:"recorded,omitempty"` // the snapshot's raw target
	EntryID   string    `json:"entry_id,omitempty"`
	Arr       string    `json:"arr,omitempty"`
	ArrItemID int       `json:"arr_item_id,omitempty"`
	Note      string    `json:"note,omitempty"`
	// MTime is the recorded modification time of the link, applied when
	// restore creates or retargets it. Zero means leave the time alone.
	MTime time.Time `json:"mtime,omitempty"`
}

// LinkPlan is a dry-run result.
type LinkPlan struct {
	PlanID     string            `json:"plan_id"`
	SnapshotID string            `json:"snapshot_id"`
	Scope      LinkScope         `json:"scope"`
	Rows       []LinkPlanRow     `json:"rows"`
	Counts     map[PlanClass]int `json:"counts"`
	CreatedAt  time.Time         `json:"created_at"`
}

// LinkScope narrows a link restore or cleanup. All empty means everything.
type LinkScope struct {
	Root   string `json:"root,omitempty"`
	Series string `json:"series,omitempty"` // series or movie title
	Prefix string `json:"prefix,omitempty"` // path prefix
}

// ---- Service ----

// Service is what the API calls. The real implementation is built by
// Manager.Backup; until then every method returns ErrNotImplemented.
type Service interface {
	Status(ctx context.Context) (StatusResponse, error)
	RunJob(ctx context.Context, name string) (RunJobResponse, error)

	ListSnapshots(ctx context.Context) ([]SnapshotInfo, error)
	GetSnapshot(ctx context.Context, id string) (*Snapshot, error)
	DeleteSnapshot(ctx context.Context, id string) error
	VerifySnapshot(ctx context.Context, id string, full bool) (VerifyReport, error)
	// Download streams a component of a snapshot; the caller closes Body.
	Download(ctx context.Context, id string, component Component) (DownloadResult, error)

	RestoreDB(ctx context.Context, id string) (RestoreDBResponse, error)
	RestoreNZB(ctx context.Context, id string, req RestoreNZBRequest) (RestoreNZBResponse, error)
	PlanLinksRestore(ctx context.Context, id string, req LinksPlanRequest) (LinkPlan, error)
	ApplyLinksRestore(ctx context.Context, id string, req LinksApplyRequest) (LinksApplyResponse, error)

	LinksStatus(ctx context.Context) (LinksStatusResponse, error)
	LinksCheck(ctx context.Context) (LinksStatusResponse, error)
	ListLinks(ctx context.Context, q LinksQuery) (LinksPage, error)
	LinksMissing(ctx context.Context) ([]MissingLink, error)
	PlanLinksCleanup(ctx context.Context, req LinksCleanupPlanRequest) (CleanupPlan, error)
	ApplyLinksCleanup(ctx context.Context, req LinksCleanupApplyRequest) (CleanupResult, error)

	ListRemoved(ctx context.Context, q RemovedQuery) ([]RemovedEntry, error)
	RestoreRemoved(ctx context.Context, key string, req RestoreRemovedRequest) (RestoreRemovedResponse, error)

	ListConfigHistory(ctx context.Context) ([]ConfigVersion, error)
	DiffConfigHistory(ctx context.Context, id, against string) (ConfigDiff, error)
	RevertConfig(ctx context.Context, id string) (RevertResponse, error)

	RetentionPreview(ctx context.Context, req RetentionPreviewRequest) (PrunePlan, error)

	// PreAction takes a Quick snapshot with trigger "pre:<action>" before a
	// risky action and returns its id ("" when none was taken).
	PreAction(ctx context.Context, action string) (snapshotID string, err error)
}

// HistoryService is the settings history, built by WP6.
type HistoryService interface {
	List(ctx context.Context) ([]ConfigVersion, error)
	Diff(ctx context.Context, id, against string) (ConfigDiff, error)
	Revert(ctx context.Context, id string) (RevertResponse, error)
}

// UndeleteService is the recently-removed list and entry restore, built by WP6.
type UndeleteService interface {
	ListRemoved(ctx context.Context, q RemovedQuery) ([]RemovedEntry, error)
	Restore(ctx context.Context, key string, req RestoreRemovedRequest) (RestoreRemovedResponse, error)
}

// LinkChecker runs one Library Links check (WP3) and returns the summary.
type LinkChecker func(ctx context.Context, p Progress) (LinksStatusResponse, error)

// DBExporter streams the database stores into the blob store (WP2) and
// returns the db component entry for the manifest.
type DBExporter func(ctx context.Context, blobs BlobStore, p Progress) (ComponentEntry, error)

// ---- Requests and responses ----

// JobStatus is one job's state on the status cards.
type JobStatus struct {
	Name        string      `json:"name"`
	Enabled     bool        `json:"enabled"`
	Schedule    string      `json:"schedule"`
	Components  []Component `json:"components"`
	LastSuccess *time.Time  `json:"last_success,omitempty"`
	LastError   string      `json:"last_error,omitempty"`
	NextRun     *time.Time  `json:"next_run,omitempty"`
	Running     bool        `json:"running"`
	// Queued is why the job is waiting for the run slot ("skipped" while
	// another backup ran, "interrupted" by a restart), or empty.
	Queued string `json:"queued,omitempty"`
}

// StatusResponse is GET /api/backups/status.
type StatusResponse struct {
	Configured   bool            `json:"configured"`
	Dir          string          `json:"dir,omitempty"`
	FreeBytes    int64           `json:"free_bytes,omitempty"`
	SameDisk     bool            `json:"same_disk,omitempty"`
	RepoBytes    int64           `json:"repo_bytes"`
	Snapshots    int             `json:"snapshots"`
	LastNewBytes int64           `json:"last_new_bytes"`
	Jobs         []JobStatus     `json:"jobs"`
	Running      *ProgressUpdate `json:"running,omitempty"`
	// Check is the scheduled check; nil until backups are set up.
	Check *CheckStatus `json:"check,omitempty"`
	// Offsite is the copy to an rclone remote; nil when none is set.
	Offsite *OffsiteStatus `json:"offsite,omitempty"`
	// Alerts says whether backup failure and stale alerts can reach anyone;
	// nil when neither alert is turned on.
	Alerts *AlertState `json:"alerts,omitempty"`
	// Now is the server's clock, so the page can age progress timestamps
	// without trusting the browser's clock.
	Now time.Time `json:"now"`
	// ServerTZ is the server's time zone; job schedules are read in it.
	ServerTZ    string `json:"server_tz,omitempty"`
	PostRestore bool   `json:"post_restore_hold"`
	PendingSwap bool   `json:"pending_db_restore"`
	// LinkRestoreApplied is true once a manual link restore has created or
	// retargeted at least one link (see LinkRestoreApplied).
	LinkRestoreApplied bool `json:"link_restore_applied"`
}

// CheckStatus is the scheduled check's line on the Backups page.
type CheckStatus struct {
	Enabled  bool         `json:"enabled"`
	Schedule string       `json:"schedule,omitempty"`
	NextRun  *time.Time   `json:"next_run,omitempty"`
	Running  bool         `json:"running"`
	Queued   string       `json:"queued,omitempty"`
	Last     *CheckResult `json:"last,omitempty"`
}

// AlertState is whether a backup alert would be delivered, and if not, why.
type AlertState struct {
	Deliverable bool   `json:"deliverable"`
	Reason      string `json:"reason,omitempty"`
}

// RunJobResponse is POST /api/backups/jobs/{name}/run. The run continues in
// the background; progress is on the status response.
type RunJobResponse struct {
	Job     string `json:"job"`
	Started bool   `json:"started"`
}

// DownloadResult is a streamed download.
type DownloadResult struct {
	Filename    string        `json:"filename"`
	ContentType string        `json:"content_type"`
	Body        io.ReadCloser `json:"-"`
}

// RestoreDBResponse is POST .../restore/db: the database is staged and the
// swap happens at the next start.
type RestoreDBResponse struct {
	StagedDir       string         `json:"staged_dir"`
	RestartRequired bool           `json:"restart_required"`
	Counts          map[string]int `json:"counts,omitempty"`
	// RecordsWithoutEntry counts NZB/overlay files newer than the snapshot
	// that were kept.
	RecordsWithoutEntry int      `json:"records_without_entry"`
	Warnings            []string `json:"warnings,omitempty"`
}

// RestoreNZBRequest restores one entry's NZB record (or exports its .nzb).
type RestoreNZBRequest struct {
	Entry  string `json:"entry"`
	Export bool   `json:"export,omitempty"`
}

// RestoreNZBResponse is the result of RestoreNZB.
type RestoreNZBResponse struct {
	Entry    string `json:"entry"`
	Restored int    `json:"restored_files"`
	Message  string `json:"message,omitempty"`
}

// LinksPlanRequest is POST .../restore/links/plan.
type LinksPlanRequest struct {
	Scope LinkScope `json:"scope"`
	// IncludeUnresolved lists dangling rows as restorable.
	IncludeUnresolved bool `json:"include_unresolved,omitempty"`
}

// LinksApplyRequest is POST .../restore/links/apply.
type LinksApplyRequest struct {
	PlanID   string `json:"plan_id"`
	Retarget bool   `json:"retarget"`
	Rescan   bool   `json:"rescan"`
	// Dangling also creates the unresolved rows as dangling links.
	Dangling bool `json:"dangling,omitempty"`
}

// LinksApplyResponse is the report of an apply.
type LinksApplyResponse struct {
	Created  int      `json:"created"`
	Retarget int      `json:"retargeted"`
	Skipped  int      `json:"skipped"`
	Failed   int      `json:"failed"`
	Rescans  int      `json:"rescans"`
	Errors   []string `json:"errors,omitempty"`
	// Warnings are problems that did not stop a link being made, such as a
	// modification time that could not be set.
	Warnings []string `json:"warnings,omitempty"`
}

// RootSummary is the Library Links summary of one root folder.
type RootSummary struct {
	Root     string `json:"root"`
	Total    int    `json:"total"`
	Live     int    `json:"live"`
	Target   int    `json:"target_gone"`
	Entry    int    `json:"entry_gone"`
	External int    `json:"external"`
	// Unused is set when no series or movie in any Arr uses the root.
	Unused bool `json:"unused,omitempty"`
}

// TrendPoint is one check in the trend line.
type TrendPoint struct {
	At   time.Time `json:"at"`
	Live int       `json:"live"`
	Dead int       `json:"dead"`
}

// AutoRecreated is a link the automatic re-creation put back.
type AutoRecreated struct {
	Path       string    `json:"path"`
	SnapshotID string    `json:"snapshot_id"`
	At         time.Time `json:"at"`
}

// LinksStatusResponse is GET /api/links/status.
type LinksStatusResponse struct {
	Roots     []RootSummary   `json:"roots"`
	LastCheck *time.Time      `json:"last_check,omitempty"`
	Running   bool            `json:"running"`
	Trend     []TrendPoint    `json:"trend,omitempty"`
	Missing   int             `json:"missing"`
	Recreated []AutoRecreated `json:"recreated,omitempty"`
}

// LinksQuery is the query of GET /api/links.
type LinksQuery struct {
	State LinkState `json:"state,omitempty"`
	Root  string    `json:"root,omitempty"`
	Q     string    `json:"q,omitempty"`
	Page  int       `json:"page,omitempty"`
	// PageSize defaults to 100.
	PageSize int `json:"page_size,omitempty"`
}

// LinksPage is one page of link rows.
type LinksPage struct {
	Rows     []LinkRow `json:"rows"`
	Total    int       `json:"total"`
	Page     int       `json:"page"`
	PageSize int       `json:"page_size"`
}

// MissingLink is a file an Arr lists with no symlink on disk.
type MissingLink struct {
	Path      string `json:"path"`
	Arr       string `json:"arr"`
	ArrItemID int    `json:"arr_item_id"`
	Title     string `json:"title,omitempty"`
	// SnapshotID is the newest snapshot that has the link, when one does.
	SnapshotID string `json:"snapshot_id,omitempty"`
	Restorable bool   `json:"restorable"`
}

// LinksCleanupFilter selects dead links.
type LinksCleanupFilter struct {
	Root   string      `json:"root,omitempty"`
	States []LinkState `json:"states,omitempty"`
	Series string      `json:"series,omitempty"`
	// Q keeps links whose path or entry contains it, case-insensitively,
	// the way the link list's search box does.
	Q string `json:"q,omitempty"`
}

// LinksCleanupPlanRequest is POST /api/links/cleanup/plan.
type LinksCleanupPlanRequest struct {
	Filter LinksCleanupFilter `json:"filter"`
}

// CleanupGroup is a group of dead links that needs its own tick.
type CleanupGroup struct {
	Key   string    `json:"key"`
	Root  string    `json:"root"`
	State LinkState `json:"state"`
	Count int       `json:"count"`
	Paths []string  `json:"paths,omitempty"`
}

// CleanupPlan is the dry run of a cleanup.
type CleanupPlan struct {
	PlanID    string         `json:"plan_id"`
	Groups    []CleanupGroup `json:"groups"`
	Total     int            `json:"total"`
	CreatedAt time.Time      `json:"created_at"`
}

// LinksCleanupApplyRequest is POST /api/links/cleanup/apply.
type LinksCleanupApplyRequest struct {
	PlanID string `json:"plan_id"`
	// Groups are the keys the user ticked; none means nothing is removed.
	Groups []string `json:"groups,omitempty"`
}

// CleanupResult is the result of a cleanup.
type CleanupResult struct {
	Removed    int      `json:"removed"`
	Failed     int      `json:"failed"`
	SnapshotID string   `json:"snapshot_id,omitempty"` // the pre-action snapshot
	Errors     []string `json:"errors,omitempty"`
	// RemovedPaths are the links that were deleted; the service drops them
	// from the stored check. Not part of the API.
	RemovedPaths []string `json:"-"`
}

// RemovedQuery filters the recently-removed list.
type RemovedQuery struct {
	Name     string    `json:"name,omitempty"`
	Category string    `json:"category,omitempty"`
	Since    time.Time `json:"since,omitempty"`
	Until    time.Time `json:"until,omitempty"`
}

// RemovedEntry is an entry present in an older snapshot but not in the live
// database.
type RemovedEntry struct {
	Key            string    `json:"key"`
	Name           string    `json:"name"`
	Category       string    `json:"category,omitempty"`
	LastSnapshotID string    `json:"last_snapshot_id"`
	LastSeen       time.Time `json:"last_seen"`
	FirstMissing   time.Time `json:"first_missing,omitempty"`
	Size           int64     `json:"size"`
	LinkCount      int       `json:"link_count"`
}

// RestoreRemovedRequest is POST /api/backups/removed/{key}/restore.
type RestoreRemovedRequest struct {
	SnapshotID string `json:"snapshot_id,omitempty"`
	// DryRun returns the plan without changing anything.
	DryRun bool `json:"dry_run,omitempty"`
	Rescan bool `json:"rescan,omitempty"`
}

// RestoreRemovedResponse is the plan or result of an entry restore.
type RestoreRemovedResponse struct {
	Key        string              `json:"key"`
	Applied    bool                `json:"applied"`
	Links      *LinkPlan           `json:"links,omitempty"`
	LinkResult *LinksApplyResponse `json:"link_result,omitempty"`
	Message    string              `json:"message,omitempty"`
	// Existing names the live entry that blocked the restore, if any.
	Existing string `json:"existing,omitempty"`
}

// ConfigVersion is one entry of the settings history.
type ConfigVersion struct {
	ID     string    `json:"id"`
	Time   time.Time `json:"time"`
	Source string    `json:"source"` // endpoint that saved it
	SHA256 string    `json:"sha256"`
}

// ConfigDiffLine is one changed setting. Secrets carry no values.
type ConfigDiffLine struct {
	Path    string `json:"path"`
	Old     string `json:"old,omitempty"`
	New     string `json:"new,omitempty"`
	Secret  bool   `json:"secret,omitempty"`
	Changed bool   `json:"changed"`
}

// ConfigDiff compares a version with another ("previous", "current" or an id).
type ConfigDiff struct {
	ID      string           `json:"id"`
	Against string           `json:"against"`
	Lines   []ConfigDiffLine `json:"lines"`
}

// RevertResponse is the result of a revert.
type RevertResponse struct {
	NewID           string `json:"new_id"`
	RestartRequired bool   `json:"restart_required"`
}

// RetentionPreviewRequest is POST /api/backups/retention/preview. With
// SnapshotID set it previews deleting that one snapshot instead.
type RetentionPreviewRequest struct {
	Job        string          `json:"job,omitempty"`
	Policy     RetentionPolicy `json:"policy"`
	SnapshotID string          `json:"snapshot_id,omitempty"`
}
