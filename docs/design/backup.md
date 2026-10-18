# Backups: design

Status: proposal, not implemented. Written 2026-10-17 against `usenet-improvements` @ daacf4a5.

## What it is for

1. Back up everything Decypharr cannot rebuild by itself: the database, the parsed NZB records, the
   repair overlay, the settings and the library symlinks Sonarr and Radarr point at.
2. Keep each backup as small as possible, so regular backups are cheap enough to keep many of.
3. Run on editable schedules, from the GUI, with a history and a "run now" button.
4. Recreate every Sonarr/Radarr library symlink at its exact path and file name, from any kept
   backup, without touching the files that already exist.

## Measured starting point

Measured on a production install of about 17,000 parsed NZBs and 20,000 library links:

| Data | Live size | Compressed | Notes |
|---|---|---|---|
| Database (`db/*.db`, 9 appendstore logs) | ~52 MB | ~11% (xz), ~15% (gzip) | changes all day |
| Parsed NZB records (`usenet/meta/*.meta`) | ~9.3 GB | ~75% | dense binary; one file per NZB, written once, rarely rewritten |
| Legacy raw NZBs (`usenet/nzbs/*.nzb`) | ~17 GB | ~31% | left from before raw NZBs were deleted on completion; the `.meta` record is canonical |
| Repair overlay (`usenet/overlay/`) | ~280 MB | ~100% | repaired segment data, incompressible |
| Volume-order backups (`usenet/volume-order-backup/`) | ~16 MB | n/a | |
| Library symlinks, as a text manifest | ~20,000 rows | ~0.5 MB | a tar of the links themselves is ~50 MB, ~1 MB compressed |

Two conclusions shape the whole design:

- **Compression alone can't make this small.** The meta records barely compress, so a compressed full copy is
  still about 7 GB every run. The size win comes from storing each file **once** and letting later backups
  refer to it (content-addressed incrementals). After the first run, a daily backup is the new and changed
  records only: tens of megabytes.
- **The database can't be copied as files.** appendstore has no snapshot call, compaction swaps the log file
  underneath a reader, and each store holds an exclusive OS lock. The backup exports records through
  `Store.ForEach`, which is safe while Decypharr runs and produces an already-compacted copy.

## What goes in a backup

| Component | Source | How it is read | Default |
|---|---|---|---|
| `db` | the 9 stores in `storage.storeNames` | `ForEach` export, see below | on |
| `config` | `config.json`, `auth.json` | file copy | on |
| `state` | `arr_library_paths.json`, `plex_reap_jobs.json`, `usenet_bandwidth.json` | file copy | on |
| `nzb` | `usenet/meta/*.meta` | incremental blob store | on |
| `overlay` | `usenet/overlay/**`, `usenet/volume-order-backup/**` | incremental blob store | on |
| `links` | every symlink under the Sonarr/Radarr root folders | manifest, see below | on |
| `hearsay` | `hearsay/identity.key` and Hearsay settings | file copy | on (the key cannot be regenerated) |
| `legacy-nzb` | `usenet/nzbs/*.nzb` | none | excluded; cleanup is a separate task |

Never included: `*.lock`, `*.v3.bak`, `*.before-*`, `.processed`/`.processing`/`.failed` markers, logs,
binaries and caches (`cache/`, `usenet/streams/`, `hearsay/feeds`, `hearsay/swarm`).

## Repository layout

One backup repository per destination:

```
<backup dir>/
  repo.json                      format version, created-at, chunking/compression parameters
  blobs/<aa>/<sha256>.zst        content-addressed, zstd-compressed (or .raw when zstd saves < 3%)
  snapshots/<UTC timestamp>-<short id>.json.zst
  index/filecache.json.zst       path -> (size, mtime, sha256): skips re-hashing unchanged files
  lock                           one writer at a time
```

A **snapshot** is a small manifest:

```json
{
  "id": "20261017T030000Z-7f3a",
  "created_at": "2026-10-17T03:00:00Z",
  "trigger": "schedule:nightly",
  "decypharr": {"version": "...", "commit": "daacf4a5", "db_format": 5},
  "components": {
    "db":     {"store_blobs": {"entries": "<sha>", "items": "<sha>", "...": "..."}, "records": 41234},
    "config": {"files": {"config.json": "<sha>", "auth.json": "<sha>"}},
    "nzb":    {"tree": "<sha>", "files": 16809, "bytes": 9987654321},
    "links":  {"manifest": "<sha>", "rows": 20212, "roots": ["..."]}
  },
  "stats": {"new_blobs": 37, "new_bytes": 24117248, "duration_ms": 41210}
}
```

A `tree` blob lists `relative path -> sha256` for a directory component. Unchanged files point at
blobs already stored, so a snapshot of 17,000 meta files whose contents haven't changed costs one tree blob
(~1 MB compressed).

**Compression**: `github.com/klauspost/compress/zstd` (already a dependency). Database exports and
manifests use the best level; meta and overlay files use the default level because they barely compress. A
blob is stored uncompressed when compression saves less than 3%. No new dependencies.

**Retention** is per schedule, grandfather-father-son: keep the last N, plus D daily, W weekly and M
monthly snapshots. Deleting a snapshot only removes its manifest. A prune pass then marks every blob
referenced by a kept snapshot and deletes the rest. Prune runs after each successful backup and holds the
repository lock.

> restic or borg would give the same blob store and pruning for free. This design builds a small native
> one instead so that the GUI, the snapshot contents (db export, link manifest) and restore can be
> Decypharr-aware and need nothing extra installed. The format is deliberately simple enough to restore by
> hand with `zstd -d` and `jq` if Decypharr itself won't start.

## Database export

- For each of the 9 stores, stream `key, value` records through `ForEach` into one zstd frame per store, with
  a length-prefixed record format and a trailing record count and checksum.
- Indexed attributes are **not** exported. They are all derivable from the value:
  - `entries` and `queue` attributes come from `entryPutOptions(entry)` (`pkg/storage/metadata.go`), and
    startup's `MigrateMetadata` already rebuilds any entry whose attributes are missing;
  - `repair_state` indexes only `status`, read from the JSON value.
  Restore re-puts entries, queue and repair_state through the typed save paths so indexes are rebuilt.
  Keys starting `__` (e.g. `__migration_status__`) are re-put raw.
- **Consistency limit:** each record is read whole, but the 9 stores are not captured at one instant. An
  import or delete that lands mid-export can leave `entries` and `items` one record out of step, and no store
  order prevents both cases. The export notes each store's record count before and after its pass; if
  `entries` changed during the run, `entries` and `items` are exported once more (they are the small,
  fast ones). Restore still tolerates what is left: an item or repair state without an entry is dropped,
  an entry without items is flagged, and both are counted in the restore report.
- **Restore can't happen while Decypharr runs** (each store holds an exclusive OS lock). A DB restore
  writes a fresh `db.restore-<ts>/`, verifies it opens and the counts match the snapshot, records a pending
  swap, and asks for a restart. At startup, before `storage.NewStorage` (`pkg/manager/manager.go`), the
  current `db/` is renamed to
  `db.before-restore-<ts>/` and the staged directory becomes `db/`.
- **A DB restore must not cost data it doesn't cover.** An older DB next to today's `usenet/meta/` makes
  every NZB added since the snapshot look like an orphan to stale-NZB cleanup (`pkg/manager/stale_nzb.go`),
  and overlay reap, Plex reap and repair would act on the older state. So:
  - a DB restore restores `nzb` and `overlay` from the **same snapshot** by default, and never deletes meta
    or overlay files that are newer than it: they are kept and listed in the restore report as "records
    without an entry", with an option to re-import them;
  - the first start after the swap runs in a **post-restore hold**: overlay reap, Plex reap, repair
    auto-fix/delete, stale-NZB cleanup and webhook-driven deletes are paused, and a banner asks the user
    to review the restore report and release the hold.
- A snapshot records the build commit and db format version. Restoring into a build with an older format is
  refused with a message naming the build to install, and a newer build migrates as it does today.

## NZB records and the overlay (incremental)

- Walk `usenet/meta/`. For each `.meta`, compare `(size, mtime)` with the file cache. Unchanged files reuse
  their recorded hash without being read. Changed or new files are hashed and, if the hash is new, compressed
  into `blobs/`.
- `.meta` files are written atomically (temp file + rename in `pkg/usenet/storage.go`), so a copy taken while
  Decypharr runs is always a whole record.
- The overlay and the volume-order backups use the same walk.
- **First run** reads ~9.6 GB once. It is throttled (default 20 MB/s, configurable) and pauses while any
  stream is open (the same "viewer active" signal precache bursts use), so it never competes with playback
  for disk (see the 2026-10-08 stall investigation). Later runs read only changed files.
- Restore of a single entry's NZB: the GUI can restore one `.meta` by entry name, or export it as a
  standard `.nzb` rebuilt from the record (the record holds the posted files in upload order and every
  segment's message ID). That makes the legacy raw `.nzb` folder unnecessary.

## Library symlinks

### What Decypharr knows today, and why it isn't enough

`arr_library_paths.json` records the symlink Decypharr wrote in the download folder and the mount path it
points at. Sonarr and Radarr then **move and rename** that link into the library on import, so the library
path is not recorded anywhere in Decypharr. The current workaround, a nightly `find -type l | tar` over a
hard-coded list of folders, misses any root folder added later. On the measured install, 3 of the 8 Arr
root folders were covered. The other folders held about 1,260 links, but only 18 of them were live: 14 into
current Decypharr entries and 4 into another tool's mount. The rest were dead links, left in a root folder
no series used any more, pointing at old mount paths or at entries Decypharr no longer has. The manifest
should therefore record each link's state (live, entry gone, target path gone) so dead links are reported,
not restored by default.

### Building the manifest

1. For each configured Arr, call `GET /api/v3/rootfolder` (new in `pkg/arr`). Union with any extra folders
   set in Settings. Deduplicate the roots across Arr instances that share them. Each Arr can carry an
   optional path map (Arr prefix → local prefix) for an Arr running in a container, whose paths are in its
   own namespace; the manifest stores both forms so restore writes local paths and rescans with Arr ones.
2. Walk each root with `filepath.WalkDir`, `Lstat` and `os.Readlink` only. **Never follow a link or stat its
   target**: the target is on the FUSE mount, and a walk of 20,000 links must not turn into 20,000 mount
   lookups.
3. For every symlink, write one row:

| Field | Example | Use |
|---|---|---|
| `path` | `<root>/Show/Season 01/Show - S01E01 - Title WEBDL-1080p.mkv` | exact location and name to recreate |
| `target` | `<mount>/__all__/<entry folder>/<file>` | the raw `readlink` string, unchanged |
| `mount_root` | `<mount>` | lets restore rewrite the prefix if the mount path changes |
| `entry` / `file` | parsed from `__all__/<entry>/<file>` | re-resolve against the DB |
| `entry_id` | infohash / nzb id from the DB, when the entry exists | follows a replaced or renamed entry |
| `arr`, `arr_item_id`, `arr_file_id` | from `moviefile` / `episodefile` | rescan and drift checks |
| `mode`, `mtime` | | restored as recorded |

Links whose target is outside the Decypharr mount (e.g. another tool's mount) are recorded as-is and
restored verbatim; only `__all__` targets are re-resolved.

4. Cross-check with the Arr: each `episodefile` / `moviefile` path that Arr knows about but that has no
   symlink on disk is listed in the snapshot as **already missing**. That catches links lost before the
   backup ran, which a disk walk alone never sees.

The manifest is a zstd-compressed TSV (~0.5 MB for 20,000 rows) and is written on every snapshot.

### Restoring links

Restore is a two-step wizard, defaulting to a dry run:

1. **Plan.** Choose a snapshot and a scope (everything, one root, one series or movie, or a path prefix).
   Each row is classified:
   - `create`: nothing at `path`;
   - `ok`: a symlink with the same target is already there;
   - `retarget`: a symlink with a different target is there (shown with both targets);
   - `conflict`: a regular file or directory is there, which is **never overwritten**;
   - `unresolved`: the entry is no longer in the DB. It can be restored as a dangling link on request, or
     the entry restored first.
   Targets are validated against the DB (entry exists, file exists in the entry), not by touching the mount.
   If the entry was replaced, `entry_id` finds the new folder name and the row shows the new target.
2. **Apply.** Creates parent folders with the recorded modes, creates links atomically (temp link + rename),
   applies `retarget` only when ticked, and optionally sends `RescanSeries` / `RescanMovie` for the affected
   Arr items so Sonarr and Radarr re-read them. The result report is kept with the snapshot history.

## Schedules

- Each **job** has its own schedule and retention. Defaults:

| Job | Components | Schedule | Retention |
|---|---|---|---|
| Quick | db, config, state, links, hearsay | every 6 hours | last 8, 14 daily, 8 weekly, 6 monthly |
| Full | quick + nzb + overlay | daily 04:00 | last 3, 14 daily, 8 weekly, 12 monthly |

- A schedule is either a clock time (`04:00`) or a cron expression (`0 */6 * * *`). It is parsed with
  `utils.ConvertToClockOrCronJobDef` and registered on the manager's gocron scheduler, as Wanted Search does
  (`pkg/manager/wanted_search.go`), so saving Settings reschedules without a restart.
- One backup runs at a time. A scheduled run that finds another in progress is skipped and logged, not
  queued.
- Optional quiet hours: a job due inside the window waits until it ends.
- **Run now** runs a job immediately with the same settings.

## Destinations

- **Local** (default `<data dir>/backups`; configurable). A warning shows when it is on the same filesystem
  as `<data dir>`, because one failed disk would take both.
- **rclone remote** (optional): after a local snapshot succeeds, new blobs and the snapshot manifest are
  copied to a remote such as an existing `crypt:` remote. Use rc `sync/copy` through the rclone client
  already in `internal/rclone` (its generic `Do`) when an rc daemon with that remote is configured, otherwise
  run the `rclone` binary with the user's rclone config. Only new blobs travel, so a daily copy is as small
  as the daily snapshot. Retention prunes the remote too.
- Archives and the repository are created `0700`/`0600`. `config.json` holds provider and Arr credentials,
  so they are never logged, and download endpoints require an authenticated session.

## Verification

- Every blob is named by its hash, so `Verify` re-reads blobs and checks them: a quick check reads the
  latest snapshot's manifests, and a full check reads every blob. The full check runs weekly by default,
  throttled like the first run.
- A DB test restore can be run into a scratch directory, opened read-only, and the record counts compared
  with the snapshot, without touching the live DB.

## GUI

**Settings → Backups tab** (new `data-tab="backups"` in `config.html`):
- destination folder (shown with free space and the same-disk warning) and an optional rclone remote;
- jobs: name, components (checkboxes), schedule (clock or cron, validated live with the next 3 run times
  shown), retention (last / daily / weekly / monthly), enabled;
- extra symlink folders (beyond the Arr root folders, which are listed read-only);
- I/O limit (MB/s), pause-while-streaming toggle, quiet hours;
- notifications: on failure, and when no successful backup has run for N hours.

**Backups page** (new nav entry, `/backups`):
- status cards: last success per job, next run, repository size, and new data in the last run;
- **Run now** per job, with a live progress line (component, files done / total, MB written);
- snapshot history table: time, trigger, components, size added, duration, result. A row opens a detail
  view with:
  - download: a `.tar.zst` of the db export and config, or the link manifest as `.tsv`;
  - restore DB (stages, then prompts for a restart);
  - restore NZB record / export `.nzb` for one entry;
  - restore links (the plan / apply wizard above);
  - verify.

**Library Links page** (new nav entry, `/links`): link health as a standing view, not just a by-product of
backups. It uses the same root discovery and `Lstat`/`Readlink`-only walk as the manifest, and checks targets
against the DB and the Arr APIs, never the mount, so it is cheap enough to run often.
- **Summary per root folder**: total links and how many are
  - `live` (the target's entry and file are in Decypharr),
  - `target path gone` (points at a folder that isn't on the mount, e.g. an old mount layout),
  - `entry gone` (names an entry Decypharr no longer has),
  - `external` (points at another tool's mount; recorded, not judged).
- **Missing from disk**: files Sonarr/Radarr list (`episodefile` / `moviefile`) with no symlink at their path.
  Each row shows the newest backup that has the link, with a one-click restore through the link-restore plan.
- **Unused root folders**: Arr root folders that no series or movie uses, and folders holding only dead links.
- **Dead links**: a filterable list (root, state, series/movie) with **Clean up**. It shows a dry-run list,
  needs an explicit tick per group, and records what was removed in the next snapshot so it can be undone.
- **Check now**, plus its own schedule (default hourly) and a trend line of live/dead counts over time, so a
  sudden drop (a mount path change, a mass delete) stands out.

**API** (under the existing authenticated `/api` router):

```
GET    /api/backups/status
GET    /api/backups/snapshots
GET    /api/backups/snapshots/{id}
POST   /api/backups/jobs/{name}/run
POST   /api/backups/snapshots/{id}/verify
GET    /api/backups/snapshots/{id}/download?component=db|config|links
POST   /api/backups/snapshots/{id}/restore/db            -> stages; restart required
POST   /api/backups/snapshots/{id}/restore/nzb           {entry}
POST   /api/backups/snapshots/{id}/restore/links/plan    {scope}
POST   /api/backups/snapshots/{id}/restore/links/apply   {plan_id, retarget: bool, rescan: bool}
DELETE /api/backups/snapshots/{id}

GET    /api/links/status                     summary per root + last check + trend
POST   /api/links/check                      run a check now
GET    /api/links?state=&root=&q=&page=      link rows
GET    /api/links/missing                    Arr files with no link on disk
POST   /api/links/cleanup/plan               {filter} -> dry-run list
POST   /api/links/cleanup/apply              {plan_id}
```

## Notifications

New `config.NotificationEvent`s through `pkg/notifications`: `backup_failed`, `backup_stale` (no successful
run of a job for its threshold), `backup_links_missing` (the Arr cross-check found library files with no
symlink), `links_dropped` (the live link count fell by more than a set share since the last check).

## Quality-of-life features

### Automatic snapshot before risky actions

A Quick snapshot (db, config, state, links: ~7 MB, seconds) is taken, with `trigger: "pre:<action>"`, before:
- Clear state, Clear decode verification and Clear repair runs/history;
- stale-NZB cleanup (`CleanupStaleNZBs`), Replace on unverified entries, and Library Links cleanup;
- single and bulk entry deletes from the GUI and API (`DELETE /api/torrents…`, `/api/browse/torrents…`);
- a database format upgrade. appendstore migrates a store inside `Open` and already keeps the old log as a
  backup file, reported through `OnMigrate` (`pkg/storage/storage.go`). The `pre:upgrade` snapshot stores
  those backup files as blobs with the old format version and build, so a rollback to the older build is
  a normal restore. Config and state are captured as usual.

Rules:
- Bulk calls get one snapshot per request, not one per item. Repeated actions within 10 minutes reuse the
  last pre-action snapshot if nothing else has changed.
- If the snapshot fails, the action still runs; the result says no snapshot was taken and the failure is
  logged and notified. Settings can switch this to "refuse the action", per action.
- Pre-action snapshots have their own retention (default: keep 20, at most 14 days). The confirm dialog of
  each action names the snapshot it will create, so "undo" is a visible next step.

This replaces the hand-made `.before-*` / `.rollback-*` copies of `config.json` and `db/`.

### Recently removed (undelete an entry)

Snapshots already hold each entry's record, its items and repair state, its NZB record and its library
links, so a removed entry can be brought back as a unit.
- **Recently removed** list on the Backups page: entries present in an older snapshot but not in the live DB,
  with name, removed between (last snapshot that had it / first that didn't), size and link count. Filter by
  name, category and date.
- **Restore entry** runs as one plan:
  1. re-put the entry, items and repair state through the typed save paths;
  2. restore its `.meta` and overlay files from the same snapshot;
  3. re-create its library links through the link-restore plan (same create/ok/retarget/conflict rules);
  4. optionally rescan the affected Sonarr/Radarr items.
  It refuses if an entry with the same name or ID now exists, and shows that entry instead.
- Restoring an entry is live: unlike a full DB restore, it needs no restart, because it adds records rather
  than replacing stores.

### Settings history

- `Config.Save()` (`internal/config/config.go`) is the single path every settings save takes (`POST
  /api/config` and the per-area `PUT …/config` endpoints). After a successful save it stores the new
  `config.json` as a blob and appends `{time, source endpoint, sha}` to `<backup dir>/config-history.json`.
  An unchanged save adds nothing. History lives in the same blob store, so it costs a few KB per change.
- **Settings → History**: a list of versions with a side-by-side diff against the previous one and against
  the current one. Secrets (API keys, passwords, tokens) are masked in the diff and shown only as
  "changed" / "unchanged".
- **Revert** writes the chosen version through `Config.Save()` (which itself becomes a new history entry) and
  applies it as a normal save does. Settings that need a restart say so.
- Retention: the last 200 versions, and versions referenced by a kept snapshot are never pruned.

### Retention preview

When retention settings are edited, the Settings tab shows, before saving, the backups that would be deleted
under the new rules ("6 snapshots, 3 daily and 3 weekly, from 2026-09-01 to 2026-09-20") and the space the
prune would free (computed from blobs no longer referenced by any kept snapshot). Saving asks for
confirmation when anything would be deleted. The same preview backs **Delete** on a single snapshot.

### Restore without the GUI

For a new or rebuilt machine, where Decypharr isn't running yet or won't start:
- **Command line**, checked in `main.go` before the normal flag parsing:

  ```
  decypharr backup list    --repo <dir>
  decypharr backup verify  --repo <dir> [--snapshot <id>] [--full]
  decypharr backup restore --repo <dir> --snapshot <id|latest> --config <data dir>
                           [--only db,config,state,nzb,overlay,hearsay,links] [--links-plan|--links-apply]
  ```

  `restore` writes into a data folder that Decypharr is not running on (it refuses while the store locks are
  held). Links default to writing a plan file. `--links-apply` creates them with the same safety rules as the
  GUI wizard.
- **Setup wizard**: the first-run `/setup` page gains "Restore from a backup". It asks for the backup folder
  (or an uploaded `.tar.zst` export), lists snapshots, restores config, state and db into the data folder,
  then continues to the normal start. NZB records and the overlay restore in the background after start,
  with the post-restore hold active until they finish.

### Automatic link re-creation (opt-in)

Off by default, and only offered once the manual link restore has been used. When a Library Links check finds
a file that Sonarr/Radarr list but that has no symlink at its path, and the newest snapshot holding that link
has a `live` target (entry and file present in the DB):
- the link is re-created through the link-restore plan, `create` rows only, never `retarget` or `conflict`;
- each re-creation is logged and listed on the Library Links page as "re-created automatically", with the
  snapshot it came from;
- a limit per check (default 25) stops a mass event (a mount path change, a moved library) from being
  "fixed" link by link. Over the limit, nothing is re-created and a `links_dropped` notification is sent
  instead;
- Arr items are not rescanned automatically unless that is ticked separately.

## Code layout

```
pkg/backup/
  repo.go         repository: blobs, snapshots, lock, prune, verify
  dbexport.go     ForEach export and staged restore (uses pkg/storage)
  tree.go         incremental directory walker + file cache
  links.go        root discovery, manifest build, plan/apply
  linkcheck.go    Library Links checks, state classification, trend history, cleanup, auto re-create
  preaction.go    pre-action snapshots and their retention
  undelete.go     recently removed list and entry restore
  confighist.go   settings history, diff with secret masking, revert
  cli.go          `decypharr backup list|verify|restore`
  scheduler.go    jobs on the manager's gocron scheduler
internal/config/backup.go    settings + defaults + validation
pkg/arr/library.go           + RootFolders(), + rescan commands
pkg/server/api_backup.go     handlers
pkg/server/templates/backups.html, assets/js/backups.js
```

`pkg/storage` gains `ExportStores(w)` and `ImportStores(dir, r)` so the export lives next to the stores it
reads. Manager startup gains the pending-restore swap before storage opens.

## Size estimate (measured install)

| | Size |
|---|---|
| First full snapshot | ~7.5 GB (meta ~7 GB, overlay ~0.3 GB, db ~6 MB, links ~0.5 MB) |
| Each quick snapshot after that | ~7 MB |
| Each full snapshot after that | quick + the day's new NZB records (~0.4 MB each), typically tens of MB |
| 30 days at the default schedules | ~8.5 GB in total |

For comparison, without deduplication: ~7.5 GB per full snapshot, or ~100 GB for 14 dailies.

## Phases

1. `pkg/backup` repository, db export, config/state, the Quick job with a local destination, the Backups
   page with history and Run now. Measure real zstd ratios against the xz figures above.
2. Link manifest with Arr root discovery and the cross-check, then the Library Links page (summary, missing,
   unused roots, dead-link cleanup, check schedule), then the restore wizard and rescan. Run it side by side
   with the existing cron script for a week, compare manifests, then retire the script.
3. Incremental meta/overlay (the Full job), the throttle and pause-while-streaming, prune with the retention
   preview, verify.
4. Staged DB restore with the startup swap and post-restore hold, single-NZB restore/export, the command-line
   restore and the setup-wizard restore, notifications. (The rclone destination waits for a destination to be
   chosen.)
5. Pre-action snapshots, settings history, recently removed / restore entry.
6. Automatic link re-creation, once the manual link restore has been used in practice.

## Decisions

1. **Legacy raw `.nzb` files (~17 GB):** excluded from backups. They need cleaning up, but that is a separate
   task, out of scope here.
2. **Off-box destination:** deferred until a destination is chosen. Phase 4 builds the rclone destination
   only once one is picked; until then backups are local only.
3. **Hearsay:** back up the identity key and settings only. The feeds and swarm caches are not backed up.
