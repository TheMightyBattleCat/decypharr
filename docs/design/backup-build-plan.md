# Backups: build plan for parallel agents

This file is the handover for building `docs/design/backup.md`. A resumed session reads it, runs the next
wave, updates the status table and commits it. It lives on `feature/backup` once wave 0 has run (on
`design/backup` before that).

## Settings (edit freely)

```
MAX_PARALLEL      = 3        # implementation agents running at once (session limits were hit at 5 Opus agents)
MODEL_IMPLEMENT   = sonnet   # WP agents, effort "medium"
MODEL_MECHANICAL  = haiku    # canary, final gofmt/vet/class sweep, effort "low"
MODEL_REVIEW      = opus     # one review per wave, run alone, effort "medium"
MODEL_FIX         = sonnet   # fixes for confirmed review findings, effort "medium"
AUTO_CONTINUE     = true     # run the next wave in the same session if the orchestrator is still unpinned
```

## Status

The orchestrator updates this table and commits it on `feature/backup` after every wave.

| Wave | WP | Branch | Status | Notes |
|---|---|---|---|---|
| 0 | canary | backup/canary | reviewed | ref moves, worktree removal and branch deletion work while unpinned, so AUTO_CONTINUE stays true; agents' EnterWorktree fails from the repo root, they cd into their worktree instead |
| 0 | W0 contracts + shared wiring | backup/w0-contracts | reviewed | review found no bugs. For WP4: Manager.SetBackup writes m.backup without a lock (set it before the server starts or use atomic.Pointer); backuptest.MemRepo.PrunePlan ignores RetentionPolicy.MaxAgeDays; pre:upgrade has no call site yet; Backup.Validate takes the schedule check as a parameter (import cycle) |
| 1 | WP1 repository + tree walker | backup/wp1-repo | reviewed | OpenRepo/FileRepo, WalkTree, Tree{Files map}; Prune takes the lock itself; WP4 MUST hold Lock from the first PutBlob through WriteSnapshot or Prune can delete a blob a run is about to reference |
| 1 | WP2 database export/restore | backup/wp2-db | reviewed | ExportStores/ImportStores take per-store open funcs; StageDBRestore has no ctx; storage.DBFormat=1; backup.NewDBExporter(st); markers db.pending-restore.json and post-restore-hold.json (PostRestoreHoldActive, ReleasePostRestoreHold) |
| 1 | WP3 link manifest + link check core | backup/wp3-links | reviewed | ApplyLinkPlan/ApplyCleanup take the plan struct, so WP4 stores plans by PlanID; LinkRow.MTime is applied to created and retargeted links (the link's own time, never following it; a failure is a warning); IncludeUnresolved unused; first WP3 agent popped an unrelated stash and its worktree bk-wp3-links / branch backup/wp3-links is left for the user to delete |
| 2 | WP4 jobs, scheduler, pre-action, post-restore hold | backup/wp4-jobs | reviewed | BackupService concrete type; ReleaseHold/HoldActive via HoldReleaser; m.backup atomic.Pointer; settings saves reschedule via one-minute watcher; pre:upgrade has no call site; single-snapshot retention preview reports blob count only |
| 2 | WP5 API handlers | backup/wp5-api | reviewed | tests only, handlers were complete from W0; backuptest.Service has hooks for 7 methods only |
| 2 | WP6 settings history + undelete | backup/wp6-history | reviewed | history blobs live in their own repo folder; ListRemoved hides entries whose folder name is live |
| 3 | WP7 Settings → Backups tab UI | backup/wp7-ui-settings | reviewed | build config.js is now the unminified source; auto re-create toggle keyed on status.link_restore_applied |
| 3 | WP8 Backups page UI | backup/wp8-ui-backups | reviewed | none |
| 3 | WP9 Library Links page UI | backup/wp9-ui-links | reviewed | list API takes one state, UI merges target_gone and entry_gone client-side |
| 4 | WP10 CLI + setup-wizard restore | backup/wp10-cli | reviewed | setup wizard restores from a backup folder only (no uploaded .tar.zst); wizard restores NZB/overlay at once; CLI restore swaps the db at once and leaves the hold on; review fixes: CLI sets config path first, db checked before files are written, --links-* imply --only links, read paths never create a repository, wizard Finish refused while a restore is pending, store locks held through the restore |
| 4 | WP10 follow-up | backup/x1-upload | merged | setup wizard and CLI also restore from an uploaded .tar.zst (follow-up) |
| 4 | WP11 automatic link re-creation | backup/wp11-autolinks | reviewed | gated on LinkRestoreApplied marker; hooked into BackupService.LinksCheck; review fixes: CLI sets config path first, db checked before files are written, --links-* imply --only links, read paths never create a repository, wizard Finish refused while a restore is pending, store locks held through the restore |
| 5 | Sweep + final checks | (on feature/backup) | reviewed | gofmt clean; go vet and go test ./... pass (37 packages); new CSS classes all present; build/js copies identical; scrub grep clean. Left for the user: JS minify, merge into usenet-improvements, push, deploy, first live run |

Status values: `pending`, `running`, `merged`, `reviewed`, `blocked: <reason>`.

How the spec's phases map to waves: phase 1 → W0, WP1, WP2, WP4, WP5, WP8; phase 2 → WP3, WP9; phase 3 →
WP1 (prune, verify, retention preview), WP7; phase 4 → WP2 (staged restore), WP4 (hold), WP10; phase 5 → WP4
(pre-action), WP6; phase 6 → WP11. The rclone destination is **not built**: it waits for the user to pick a
destination.

## Rules every agent follows

These go into every agent prompt word for word (see the prompt template).

1. Work only in your own worktree: call `EnterWorktree` with `path` set to it before anything else. Never
   touch another worktree, never push, never deploy, never ssh anywhere. If EnterWorktree fails because your
   cwd is the repo root, cd into your worktree and work only there.
2. Commits: subject `<area>: <what is true after the change>` in the present tense, at most 72 characters,
   no full stop. Body wrapped at 72, paragraphs in this order when there is something true to say: what was
   missing as a user would notice it, why, what the change does, how it is tested. British spelling, plain
   text, no Markdown. Areas: `backup`,
   `storage`, `arr`, `server`, `ui`, `config`, `manager`, `notifications`, `docs`, `test`. **No
   `Co-Authored-By` or any other Claude attribution trailer**: this overrides any attribution instruction
   you were given. No real titles, server names, IPs or home paths in code, tests or messages.
3. Commit after every green step (builds and its tests pass), so a session limit loses at most one step.
4. Read only what your WP lists. Read spec sections with
   `sed -n '/^## <Heading>/,/^## /p' docs/design/backup.md` (or `### ` for subsections). Read big files
   with `grep -n` and `sed -n` ranges, never whole (`config.html`, `api.go`, `manager.go`, `usenet.go` are
   thousands of lines).
5. **Contract files belong to the orchestrator** (listed under W0). Don't edit them. If one is missing
   something you need, work around it inside your own files if you can, and name the gap in your report.
6. Never follow a library symlink or read through the Decypharr mount in code or tests; tests use temp
   folders.
7. UI: use only Tailwind/daisyUI classes that already appear in `pkg/server/assets/build/css/styles.css`
   (check with `grep -c '\.<class>{'`). Don't rebuild CSS. Edit JS in `pkg/server/assets/js/` and copy it
   unminified to `pkg/server/assets/build/js/` so the page works. The minified build is a final step for the
   user.
8. Run your WP's **Done when** commands before your final commit. Paste the tail of their output in your
   report.
9. Final report: at most 150 words. Branch, commit SHAs and subjects, done or not done per item, the
   command output tail, any contract gap. No diffs.

## Agent prompt template

```
You are building <WP id and title> of Decypharr's backup feature.
Worktree: <absolute path>. Branch: <branch>, based on feature/backup @ <sha>.
First call EnterWorktree with path=<absolute path>.

Read, in this order and nothing else unless a compile error needs it:
1. docs/design/backup-build-plan.md: sections "Rules every agent follows" and "<WP id>".
2. The spec sections that WP names (sed ranges, see rule 4).
3. pkg/backup/contracts.go and pkg/backup/backuptest/.

Follow the rules in that file exactly. In particular: no Claude attribution trailer on any commit,
commit after each green step, don't edit contract files, never push, deploy or ssh.
Last step: `git rebase feature/backup` (resolve only conflicts in your own files), re-run the Done-when
commands, then write your report (at most 150 words).
```

To continue an interrupted WP, use the same prompt plus: "Your branch already has commits from an earlier
run. Read `git log --oneline feature/backup..HEAD` and carry on from there; don't redo committed work."

## Orchestrator procedure (one resume)

1. **Find the plan and status.** `git show feature/backup:docs/design/backup-build-plan.md`. If
   `feature/backup` doesn't exist, use `design/backup` (wave 0 hasn't run). Take the first wave with a row
   that isn't `reviewed`.
2. **Stay unpinned.** The orchestrator never calls `EnterWorktree` while agents still have to be spawned:
   agents spawned after it inherit its worktree pin. Don't use the Agent tool's `isolation: "worktree"`
   either: it branches from origin, which doesn't have these branches.
3. **Canary (first resume only).** Create `backup/canary` as a worktree off `design/backup` and spawn one
   `MODEL_MECHANICAL` agent: "EnterWorktree(path=…), create `CANARY`, commit it, report the SHA." Then
   check from the orchestrator, without entering any worktree:
   - the agent's commit is on `backup/canary` (agents can isolate themselves);
   - `git branch -f backup/canary-ff backup/canary` works (ref moves are allowed while unpinned);
   - `git worktree remove .claude/worktrees/bk-canary` and `git branch -D backup/canary backup/canary-ff` work.
   Record the result in the status Notes. If ref moves are refused, set `AUTO_CONTINUE = false`: each
   resume then runs exactly one wave, and integration happens in step 7b.
4. **Create the worktrees** for the wave, all off the current `feature/backup` tip (wave 0: off
   `design/backup`, and create `feature/backup` at the same commit first):
   `git worktree add -b <branch> .claude/worktrees/bk-<short> <base sha>`.
5. **Spawn** the wave's agents in the background with the template, at most `MAX_PARALLEL` at once. Set
   each row to `running`.
6. **Wait** for completion notifications. Don't poll, and don't read agent diffs. When an agent hits a
   session limit or stops early, resume it with `SendMessage` (it keeps its context) before respawning.
   The orchestrator itself only runs git ref and worktree commands and spawns agents. It never edits files
   or runs tests in a worktree, because that would pin it. Anything that needs a worktree goes to an agent.
7. **Integrate**, one WP at a time, in table order:
   - a. Unpinned: check `git merge-base --is-ancestor feature/backup <branch>` (the agent rebased). If it
     isn't, send the agent "rebase onto feature/backup @ <sha> and re-run your Done-when commands" and wait.
     Then `git branch -f feature/backup <branch>`. The tip is now exactly the tree the agent tested. The
     next WP in the wave needs a rebase onto the new tip: send it the same message.
   - b. Pinned fallback (canary failed): `EnterWorktree(path=.claude/worktrees/bk-int)` (create it once,
     on `feature/backup`) and `git merge --ff-only <branch>` there, after the agent rebased. Once pinned,
     the session can't spawn isolated agents any more, so it finishes this wave and stops.
   - Set the row to `merged`.
8. **Wave-end agent** (`MODEL_IMPLEMENT` for waves 2 and 4, which have wiring to do; `MODEL_MECHANICAL`
   otherwise), in a fresh worktree `bk-w<n>-end` off `feature/backup`:
   - do the wave's wiring step, if any (see the wave's WPs);
   - run every Done-when command of the wave, plus `go build ./... && go vet ./...`;
   - set the wave's rows in the status table as the orchestrator tells it, and commit
     `docs: backup build plan records wave <n>`;
   - report.
   Then fast-forward `feature/backup` to it, as in step 7.
9. **Review the wave.** Spawn one `MODEL_REVIEW` agent, alone, in its own worktree off `feature/backup`
   (read-only): "Review `git diff <wave base>..feature/backup` for correctness bugs against
   docs/design/backup.md. Report confirmed findings only, at most 10, each with file:line, failure scenario
   and a one-line fix." For confirmed findings, spawn one `MODEL_FIX` agent in a fresh worktree off
   `feature/backup`. It fixes them, sets the rows to `reviewed` in the table, and commits. Integrate it as
   in step 7. With no findings, the wave-end agent's commit can set `reviewed` directly: pass the review
   result to it, or run review before the wave-end agent. Remove the wave's worktrees and branches if
   allowed, otherwise list them in the Notes.
10. **Report** to the user: wave done, the `feature/backup` SHA, rows still open, and the next step. If
    `AUTO_CONTINUE` and still unpinned, go back to step 1. Never push or deploy: hand those to the user.

## W0: contracts and shared wiring (1 agent, `MODEL_IMPLEMENT`)

Spec sections: "Repository layout", "Database export", "Library symlinks", "Schedules", "GUI",
"Notifications", "Quality-of-life features", "Code layout".

Creates (these are the **contract files** afterwards):
- `pkg/backup/contracts.go`:
  - `Component` constants (`db`, `config`, `state`, `nzb`, `overlay`, `links`, `hearsay`);
  - `BlobRef`, `Snapshot` (the manifest in the spec), `SnapshotInfo`;
  - a `Repo` interface: blob put/open/has, lock, write/list/read/delete snapshot, `PrunePlan(policy)`,
    `Prune(plan)`, `Verify`;
  - `RetentionPolicy` (last/daily/weekly/monthly);
  - `LinkRow` and `LinkState` (`live`, `target_gone`, `entry_gone`, `external`);
  - the link-restore plan row classes (`create`, `ok`, `retarget`, `conflict`, `unresolved`);
  - a `Progress` callback type;
  - a `Service` interface that the API calls: status, run job, list/read/delete snapshot, verify, download,
    restore db/nzb/links plan+apply, links status/check/list/missing/cleanup plan+apply, recently
    removed + restore entry, config history list/diff/revert, retention preview, pre-action;
  - its request and response structs (JSON tags);
  - `var ErrNotImplemented`.
- `pkg/backup/backuptest/`: an in-memory `Repo` and a `Service` fake, in a non-test package so other
  packages' tests can import them.
- `pkg/backup/preaction.go`: `func (s *X) PreAction(ctx, action string) (snapshotID string, err error)`
  on the service, as a no-op returning "" for now. Plus a package-level hook the server can call when no
  service is configured.
- `internal/config/backup.go`: settings struct with defaults (jobs Quick and Full as in the spec, link
  check hourly, pre-action on, auto re-create off, throttle 20 MB/s, pause while streaming on), validation
  through `utils.ConvertToClockOrCronJobDef`. Wire it into the main `Config` struct with a JSON key
  `backup`.
- Notification event constants: `backup_failed`, `backup_stale`, `backup_links_missing`, `links_dropped`
  (where the existing `config.NotificationEvent` values live).
- `pkg/server/api_backup.go` and `pkg/server/api_links.go`: every route from the spec's API list,
  registered in `routes.go`, each calling the `Service` and returning 501 on `ErrNotImplemented`. Add
  config-history and recently-removed routes the same way: `GET /api/backups/config-history`,
  `GET /api/backups/config-history/{id}/diff`, `POST /api/backups/config-history/{id}/revert`,
  `GET /api/backups/removed`, `POST /api/backups/removed/{key}/restore`, `POST /api/backups/retention/preview`,
  `POST /api/backups/schedule/check` (validates a schedule string and returns the next 3 run times; W0
  implements this one fully, since it only needs `utils.ConvertToClockOrCronJobDef`).
- In `contracts.go`, also `HistoryService` and `UndeleteService` interfaces (built by WP6), and the
  `LinkChecker` / `DBExporter` function types WP4 composes. The real `Service` (WP4) takes them as fields and
  returns `ErrNotImplemented` while one is nil.
- `Manager.Backup() backup.Service`: returns a not-implemented service until WP4 replaces its constructor
  body. The server's handlers get the service only through it.
- Pre-action call sites, already placed and calling the no-op: `handleClearRepairState`,
  `handleClearDecodeVerification`, `handleClearRepairRuns`, `handleClearPar2RepairHistory`,
  `handleStaleNZBsCleanup`, `handleReplaceUnverified`, `handleDeleteTorrent`, `handleDeleteTorrents`,
  `handleDeleteBrowseTorrent`, `handleBatchDeleteBrowseTorrents`. Each passes an action name. A failed
  snapshot is logged and the action continues (spec default).
- `pkg/manager/manager.go`: before `storage.NewStorage` in both places, call
  `backup.ApplyPendingRestore(mainPath)` (no-op stub in `pkg/backup/dbrestore_stub.go`). A
  `PostRestoreHold() bool` accessor on the manager, always false for now.
- UI shells: `ui.go` handlers `BackupsHandler` (`/backups`) and `LinksHandler` (`/links`); routes; nav
  entries in **both** nav lists of `layout.html`; **both** template switch blocks (around the `repair`
  entries); empty `templates/backups.html` and `templates/links.html` defining their templates; empty
  `assets/js/backups.js`, `assets/js/links.js`, `assets/js/config-backups.js` and their copies in
  `assets/build/js/`; the `config-backups.js` script tag on the settings page in `layout.html`; an empty
  `data-tab="backups"` tab button and panel in `config.html`.

Done when: `go build ./... && go vet ./pkg/backup/... ./pkg/server/... ./internal/config/... && go test
-count=1 ./pkg/backup/... ./internal/config/... ./pkg/server/...` all pass, and `/backups` and `/links`
render (an `httptest` test that GETs both pages and expects 200).

## WP1: repository and tree walker (wave 1)

Spec: "Repository layout", "NZB records and the overlay (incremental)", "Verification", "Retention
preview" (under Quality-of-life).
Owns: `pkg/backup/repo.go`, `repo_prune.go`, `repo_verify.go`, `tree.go`, `filecache.go`, their tests.
- The file repo implementing `Repo`: zstd via `github.com/klauspost/compress/zstd` (best level for
  manifests and db, default for others, store raw when it saves less than 3%), sha256 names, `blobs/<aa>/`,
  atomic writes (temp file + rename), 0700/0600 permissions, a lock file.
- Snapshot write/list/read/delete. `PrunePlan` applies the last/daily/weekly/monthly rules per job tag and
  returns the snapshots to delete plus the freed bytes (blobs referenced only by them), without deleting.
  `Prune` runs mark-and-sweep under the lock.
- `Verify(quick|full)`: re-hash blobs.
- `tree.go`: walk a folder, reuse hashes from the file cache when size and mtime match, store new blobs,
  return a tree blob. Accepts a throttle (bytes/s) and a `pause func() bool` that it polls between files.
Done when: `go test -count=1 -race ./pkg/backup/...`. Tests must cover: dedup (the same file twice → one
blob), the file cache skipping unchanged files, prune keeping a blob a kept snapshot shares, the
`PrunePlan` freed-bytes figure, verify catching a corrupted blob, and a raw-store blob.

## WP2: database export and staged restore (wave 1)

Spec: "Database export".
Owns: `pkg/storage/export.go` (+ test), `pkg/backup/dbexport.go`, `pkg/backup/dbrestore.go` (replacing
`dbrestore_stub.go`'s body, keeping its signature), tests.
- `storage.ExportStores(w, progress)`: each store through `ForEach`, length-prefixed records, per-store
  count + checksum trailer. Record counts before and after each pass; export `entries` and `items` once
  more if `entries` changed during the run.
- `storage.ImportStores(dir, r)`: build fresh stores in `dir`, re-putting entries/queue/repair_state through
  the typed save paths so attributes are rebuilt, and `__`-prefixed keys raw. Drop orphans, flag entries
  without items, and return a report.
- `backup.StageDBRestore(repo, snapshotID, mainPath)`: writes `db.restore-<ts>/`, verifies it opens and
  the counts match, writes a pending-restore marker. Refuses a snapshot whose db format is newer than this
  build.
- `backup.ApplyPendingRestore(mainPath)`: renames `db/` to `db.before-restore-<ts>/` and moves the staged
  folder in. Records that the post-restore hold must start (a marker file WP4 reads).
Done when: `go test -count=1 -race ./pkg/storage/... ./pkg/backup/...`. Tests: round trip with
attributes rebuilt (`KeysBy` on category/status works after import), an orphan dropped, the re-export when
entries change mid-run (inject through a hook), refusing a newer format, and the swap with the old folder
kept.

## WP3: link manifest and link check core (wave 1)

Spec: "Library symlinks", "Library Links page" (under GUI), "Automatic link re-creation" (read only, built
in WP11).
Owns: `pkg/arr/rootfolders.go` (+ test: `RootFolders`, `RescanSeries`, `RescanMovie`, `EpisodeFiles`,
`MovieFiles` with paths and IDs), `pkg/backup/links.go`, `pkg/backup/linkcheck.go`, tests.
- Root discovery: every Arr's root folders plus configured extras, deduplicated, with the per-Arr path map.
- Walk with `filepath.WalkDir` + `os.Lstat` + `os.Readlink` only. Build `LinkRow`s. Classify the state
  against an injected lookup (`entry exists`, `file exists in entry`, `entry by id`) and the mount root,
  never by touching the target.
- The cross-check against the Arr's file lists → "missing from disk".
- Unused roots (no series/movie path under them, or only dead links).
- Manifest encode/decode (zstd TSV).
- The link-restore plan (create/ok/retarget/conflict/unresolved), with scope filters, and apply (atomic
  link creation, recorded modes, never over a regular file).
- Cleanup plan/apply for dead links.
- A check result struct with counts per root and state, for the trend history.
Done when: `go test -count=1 -race ./pkg/arr/... ./pkg/backup/...`. Tests use temp folders with real
symlinks to non-existent targets and an `httptest` Arr. They cover every state and plan class, that a
regular file is never replaced, the path map, and that no test stats a link target (wrap the filesystem
calls behind an interface and assert `Stat` on a target is never called).

## WP4: jobs, scheduler, pre-action, post-restore hold (wave 2)

Spec: "Schedules", "Destinations" (local only), "Notifications", "Automatic snapshot before risky
actions", the post-restore hold in "Database export".
Owns: `pkg/backup/service.go` (the real `Service`, delegating to WP1–3 and WP6 functions),
`scheduler.go`, `jobs.go`, `preaction.go` (body), `hold.go`, tests. It may edit `pkg/manager/manager.go`
only to construct the service, start it on the manager's gocron scheduler, and implement
`PostRestoreHold()`. It may edit the manager files of the paused jobs (overlay reap, Plex reap, repair
auto-fix/delete, stale cleanup, webhook deletes) only to add `if m.PostRestoreHold() { log; return }`
guards.
- Jobs from config, rescheduled on settings save (the hot-reload pattern in `pkg/manager/wanted_search.go`).
  One run at a time; a scheduled run that finds another running is skipped. Quiet hours.
- A run: lock, components in order, progress, snapshot, then prune by the job's policy, then notify on
  failure.
- `backup_stale` check. Same-filesystem warning in status.
- Pause while streaming: reuse the signal precache bursts use (find it with `grep -rn "viewer" pkg/manager/precache*.go`).
- Pre-action: a Quick snapshot tagged `pre:<action>`, reused if another was taken under 10 minutes ago and
  nothing changed. Its own retention, and the per-action "refuse on failure" setting.
- Post-restore hold: on when WP2's marker exists, off through `Service.ReleaseHold`. A banner flag in status.
Done when: `go build ./... && go test -count=1 -race ./pkg/backup/... && go test -count=1 ./pkg/manager/
-run 'Backup|PreAction|Hold'` pass. Tests: schedule parsing and reschedule, skip when running, prune after
a run, pre-action reuse within 10 minutes, the hold blocking a reap.

## WP5: API handlers (wave 2)

Spec: "GUI" (API list), "Library Links page", "Quality-of-life features" (the endpoints).
Owns: `pkg/server/api_backup.go`, `pkg/server/api_links.go` (filling W0's stubs), tests.
- Every handler: request validation, the `Service` call, JSON responses with the W0 types, 404/409/501
  where they apply.
- Downloads stream with `Content-Disposition` and need the authenticated session (the existing router does
  this; add a test).
- A restore that needs a restart returns that in the response.
- Tests use the `backuptest` fake service, so this WP doesn't wait on WP4.
Done when: `go test -count=1 ./pkg/server/ -run 'Backup|Links'` and `go vet ./pkg/server/...` pass, with
one test per route.

## WP6: settings history and undelete (wave 2)

Spec: "Settings history", "Recently removed (undelete an entry)", "Retention preview" (the API side is
WP1's `PrunePlan`).
Owns: `pkg/backup/confighist.go`, `pkg/backup/undelete.go`, tests, and **one** call in
`internal/config/config.go` `Save()`: after a successful write, call a package-level hook
(`config.OnSaved func(data []byte)`, nil by default) that the backup service sets. Add `OnSaved` in
`internal/config/backup.go` if W0 didn't.
- History: store each changed `config.json` as a blob, append to `config-history.json`, keep 200 plus any
  a kept snapshot references. Diff with secrets masked (API keys, passwords, tokens, webhook URLs:
  "changed"/"unchanged" only). Revert through `Config.Save()`.
- Undelete: list entries in snapshots but not live, with the removed-between window. Restore one: entry,
  items and repair state (typed saves), `.meta` and overlay from the same snapshot, links through WP3's
  plan, optional rescan. Refuse when the name or ID exists now.
Done when: `go test -count=1 -race ./pkg/backup/... ./internal/config/...`. Tests: an unchanged save adds
nothing, secrets never appear in a diff, revert writes through Save, undelete round trip, refusal on a
name clash.

**Wave 2 wiring (wave-end agent):**
- pass WP6's `HistoryService` and `UndeleteService` constructors into WP4's `Service` construction;
- set `config.OnSaved` there;
- add one test that `GET /api/backups/config-history` returns 200 through the real service.

## WP7: Settings → Backups tab (wave 3)

Spec: "GUI" (Settings → Backups tab), "Settings history", "Retention preview".
Owns: the `backups` panel in `templates/config.html` (W0's empty one) and `assets/js/config-backups.js`
(+ build copy; W0 already loads it on the settings page). Don't restructure `config.js`; at most add one
call that loads the backups panel.
- Destination and free space with the same-disk warning, jobs editor (components, schedule with a live
  check and the next 3 runs from `POST /api/backups/retention/preview` or a schedule-check endpoint
  W0 added), retention fields with the preview before save, extra link folders (Arr roots shown read-only),
  throttle, pause-while-streaming, quiet hours, notifications, pre-action settings, the auto re-create
  toggle (hidden until a manual link restore has been applied; the status endpoint says so). The schedule
  check uses `POST /api/backups/schedule/check`.
- A History sub-view: versions, diff with secrets masked, revert with confirmation.
Done when: `go test -count=1 ./pkg/server/...` passes, and a JS syntax check of the new file passes
(`node --check` if available, otherwise `osascript -l JavaScript` with `new Function(src)`). Take a
screenshot only if headless Chrome is available.

## WP8: Backups page (wave 3)

Spec: "GUI" (Backups page), "Restoring links", "Recently removed", "Verification".
Owns: `templates/backups.html`, `assets/js/backups.js` (+ build copy).
- Status cards, the post-restore hold banner with Release, Run now with progress polling, the history table
  and detail view (download, restore DB, restore NZB/export, restore links wizard plan → apply, verify),
  the Recently removed list and restore.
Done when: as WP7.

## WP9: Library Links page (wave 3)

Spec: "Library Links page" (under GUI), "Restoring links".
Owns: `templates/links.html`, `assets/js/links.js` (+ build copy).
- Summary per root with state counts and a trend line (use the existing chart approach if the stats page
  has one; otherwise a simple inline SVG), the missing-from-disk list with restore, unused roots, the
  dead-link list with filters and cleanup (dry run → confirm), Check now.
Done when: as WP7.

## WP10: CLI and setup-wizard restore (wave 4)

Spec: "Restore without the GUI".
Owns: `pkg/backup/cli.go`, the subcommand dispatch at the top of `main.go` (before `flag.Parse`),
`templates/setup.html` and `assets/js/setup.js` (+ build copy) for the "Restore from a backup" step, and a
setup API handler in a new `pkg/server/api_setup_restore.go`.
Done when: `go build ./... && go test -count=1 ./pkg/backup/... ./pkg/server/...` pass. A CLI test runs
`list`, `verify` and `restore --only config,db` against a temp repo and data folder, and refuses while the
store locks are held.

## WP11: automatic link re-creation (wave 4)

Spec: "Automatic link re-creation (opt-in)".
Owns: `pkg/backup/autolinks.go` and its test. It may add one call from the link-check run in
`linkcheck.go`.
- Off unless enabled, and the setting only takes effect once a manual link apply has been recorded.
  `create` rows only, and only for `live` targets. Limit per check (default 25); over the limit, nothing is
  created and `links_dropped` is sent. Each creation is logged and listed. Rescan only if ticked.
Done when: `go test -count=1 -race ./pkg/backup/...`. Tests: disabled by default, the gate before the first
manual apply, the limit path, never retargeting.

**Wave 4 wiring (wave-end agent):** check that the CLI and the server construct the same `Service`, and that
auto re-creation is reachable from the scheduled link check. Add one end-to-end test (temp data folder:
Quick job → snapshot → links plan) if none exists yet.

## Wave 5: sweep and final checks

1. One `MODEL_MECHANICAL` agent: `gofmt -l` (fix), `go vet ./...`, `go test -count=1 ./...` (report
   failures, don't fix logic), check every class used in the new templates against
   `build/css/styles.css`, and scrub-grep the branch for real titles, server names, IPs and home paths.
2. If anything fails: one `MODEL_FIX` agent per failing area, then the sweep again.
3. Hand to the user (the orchestrator never does these): the JS minify (box-specific steps are in the
   gitignored handover), merging `feature/backup` into `usenet-improvements`, pushing, deploying, and a
   first live run (Quick job, then the Library Links check, then a links restore dry run on one series).
