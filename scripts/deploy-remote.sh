#!/usr/bin/env bash
#
# Deploy a decypharr branch to the decypharr_beta service on a remote server.
#
# Run this from your local checkout (not on the server). It never touches
# GitHub - it archives the requested commit straight out of the local git
# checkout, ships it to the remote server over SSH, builds it there, and swaps it
# into the live service with a binary backup + automatic rollback on any
# failed health check.
#
# Usage:
#   scripts/deploy-remote.sh [branch] [expected-commit]
#
#   branch           Branch to deploy. Defaults to the current branch.
#   expected-commit  Short or full commit hash the branch tip must match
#                    before anything is touched (safety pin). Optional -
#                    if omitted, whatever the branch currently resolves to
#                    is deployed, unpinned.
#
# Example (this run):
#   scripts/deploy-remote.sh feature/padding-par2-overlay a03cda1
#
# Rollback is binary-swap only: it restores the pre-deploy decypharr binary
# and restarts the service. It does NOT revert config.json - this script
# never writes config.json, so there is nothing there to revert.
#
# Hard rule: PrecacheReadAhead ("Fast read-ahead") MAY be enabled - this
# script never touches config.json either way - but unbounded read-ahead is
# bandwidth/cache-aggressive, so this script refuses to leave a deploy
# standing if read-ahead is on with PrecacheMaxBytes unset/0 or above the
# 256GiB ceiling (see verify_precache_bounded).

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BRANCH="${1:-$(git -C "$REPO_DIR" rev-parse --abbrev-ref HEAD)}"
EXPECTED_COMMIT="${2:-}"

# Host and folders come from scripts/deploy.env (not committed); see
# scripts/deploy.env.example.
[ -f "$(dirname "$0")/deploy.env" ] && . "$(dirname "$0")/deploy.env"
REMOTE_HOST="${DEPLOY_HOST:?set DEPLOY_HOST in scripts/deploy.env}"
REMOTE_BUILD_DIR="${DEPLOY_BUILD_DIR:?set DEPLOY_BUILD_DIR in scripts/deploy.env}"
REMOTE_DEPLOY_DIR="${DEPLOY_DIR:?set DEPLOY_DIR in scripts/deploy.env}"
REMOTE_BIN="${REMOTE_DEPLOY_DIR}/decypharr"
SERVICE="${DEPLOY_SERVICE:-decypharr_beta}"
MOUNT_PATH="${DEPLOY_MOUNT_PATH:?set DEPLOY_MOUNT_PATH in scripts/deploy.env}"
HEALTH_PORT="8686"
KEEP_BACKUPS=5
# 256 GiB - must match internal/config/precache.go's precacheMaxBytesCeiling
PRECACHE_MAX_BYTES_CEILING=274877906944

SSH="ssh -o BatchMode=yes -o ConnectTimeout=10 ${REMOTE_HOST}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log()  { printf '[deploy] %s %s\n' "$(date '+%H:%M:%S')" "$*"; }
fail() { printf '[deploy] ABORT: %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Step 1 - pre-flight
# ---------------------------------------------------------------------------

preflight() {
    log "Pre-flight checks..."

    local current_branch current_commit
    current_branch="$(git -C "$REPO_DIR" rev-parse --abbrev-ref HEAD)"
    [ "$current_branch" = "$BRANCH" ] || fail "main checkout is on '$current_branch', expected '$BRANCH'"

    current_commit="$(git -C "$REPO_DIR" rev-parse HEAD)"
    if [ -n "$EXPECTED_COMMIT" ]; then
        # Deploy exactly the pinned commit's tree, not necessarily whatever
        # HEAD has moved on to since (e.g. this very script's own commit) -
        # but refuse to ship a hash that isn't actually part of this branch's
        # history.
        git -C "$REPO_DIR" rev-parse --verify "${EXPECTED_COMMIT}^{commit}" >/dev/null 2>&1 \
            || fail "expected commit $EXPECTED_COMMIT does not exist in the main checkout"
        git -C "$REPO_DIR" merge-base --is-ancestor "$EXPECTED_COMMIT" HEAD \
            || fail "expected commit $EXPECTED_COMMIT is not an ancestor of $BRANCH's current tip ($current_commit)"
        TARGET_COMMIT="$(git -C "$REPO_DIR" rev-parse "$EXPECTED_COMMIT")"
    else
        TARGET_COMMIT="$current_commit"
    fi
    log "Deploying $BRANCH @ ${TARGET_COMMIT:0:12} (checkout HEAD is ${current_commit:0:12})"

    [ -z "$(git -C "$REPO_DIR" status --porcelain)" ] || fail "main checkout has uncommitted changes"

    local worktree_count
    worktree_count="$(git -C "$REPO_DIR" worktree list | wc -l | tr -d ' ')"
    [ "$worktree_count" -eq 1 ] || log "WARNING: $((worktree_count - 1)) extra worktree(s) still present (expected only the main checkout) - continuing anyway, this is not fatal to the deploy"

    $SSH true || fail "cannot SSH to $REMOTE_HOST"

    local svc_state
    svc_state="$($SSH "systemctl is-active $SERVICE" || true)"
    [ "$svc_state" = "active" ] || fail "$SERVICE is not active on $REMOTE_HOST (state: $svc_state) - refusing to deploy on top of an already-broken service"

    # swap_and_restart (step 5) runs `sudo systemctl stop/start $SERVICE` over
    # a non-interactive SSH heredoc (no pty allocated), so it can never answer
    # a password prompt - if the sudoers NOPASSWD rule is missing or wrong,
    # that stop just hangs/fails with the service already down and nothing
    # left to start it back up. Prove passwordless sudo actually works for
    # this service BEFORE the service is touched, not after: `-n` makes sudo
    # fail immediately instead of prompting, so this is safe to run without
    # side effects (is-active needs the same NOPASSWD grant stop/start does,
    # but never changes service state itself).
    local sudo_check_output
    if ! sudo_check_output="$($SSH "sudo -n systemctl is-active $SERVICE" 2>&1)"; then
        fail "passwordless sudo is not working for systemctl on $REMOTE_HOST ('sudo -n systemctl is-active $SERVICE' failed: $sudo_check_output) - fix the sudoers NOPASSWD rule for $SERVICE before deploying; a missing rule takes the service down mid-deploy when step 5's non-interactive stop/start can't answer a password prompt"
    fi
    log "Passwordless sudo for $SERVICE confirmed working"

    OLD_BINARY_HASH="$($SSH "sha256sum $REMOTE_BIN" | cut -d' ' -f1)"
    log "Current running binary hash: $OLD_BINARY_HASH"

    PRECACHE_BEFORE="$($SSH "python3 -c \"import json; print(json.load(open('${REMOTE_DEPLOY_DIR}/config.json')).get('repair',{}).get('precache_read_ahead_enabled', False))\"" 2>/dev/null || echo "False")"
    log "PrecacheReadAhead before deploy: $PRECACHE_BEFORE"

    log "Pre-flight OK"
}

# ---------------------------------------------------------------------------
# Step 2 - sync branch content + rebuild frontend assets
# ---------------------------------------------------------------------------

sync_and_build_assets() {
    log "Archiving $BRANCH @ ${TARGET_COMMIT:0:12} to $REMOTE_HOST:$REMOTE_BUILD_DIR ..."
    $SSH "mkdir -p $REMOTE_BUILD_DIR"
    git -C "$REPO_DIR" archive "$TARGET_COMMIT" | $SSH "tar -x -C $REMOTE_BUILD_DIR"

    log "Rebuilding frontend assets (npm, Node v20) - always rebuilt, never trusted stale..."
    $SSH bash -s <<REMOTE
set -euo pipefail
export NVM_DIR="\$HOME/.nvm"
# shellcheck disable=SC1091
source "\$NVM_DIR/nvm.sh"
nvm use 20 >/dev/null
cd "$REMOTE_BUILD_DIR"
npm ci
npm run build
REMOTE
    log "Frontend assets rebuilt"
}

# ---------------------------------------------------------------------------
# Step 3 - go build && go vet (abort before the running service is touched)
# ---------------------------------------------------------------------------

build_binary() {
    log "go build && go vet on $REMOTE_HOST ..."
    $SSH bash -s <<REMOTE
set -euo pipefail
export GOROOT=/usr/local/go
export GOPATH="\$HOME/go"
export PATH="\$GOPATH/bin:\$GOROOT/bin:\$PATH"
cd "$REMOTE_BUILD_DIR"
rm -f decypharr_new
go build -o decypharr_new .
go vet ./...
REMOTE
    log "Build + vet OK - decypharr_new ready in $REMOTE_BUILD_DIR (service not yet touched)"
}

# ---------------------------------------------------------------------------
# Step 4 - backup current binary
# ---------------------------------------------------------------------------

backup_binary() {
    log "Backing up current binary..."
    BACKUP_NAME="$($SSH bash -s <<REMOTE
set -euo pipefail
cd "$REMOTE_DEPLOY_DIR"
ts=\$(date +%s)
name="decypharr.bak.\${ts}"
cp decypharr "\$name"
# Keep the last $KEEP_BACKUPS backups, prune older.
ls -t decypharr.bak.* | tail -n +$((KEEP_BACKUPS + 1)) | xargs -r rm -f
echo "\$name"
REMOTE
)"
    [ -n "$BACKUP_NAME" ] || fail "backup step produced no backup filename"
    log "Backed up to $REMOTE_DEPLOY_DIR/$BACKUP_NAME"
}

# ---------------------------------------------------------------------------
# Step 5 - stop, swap, start
# ---------------------------------------------------------------------------

swap_and_restart() {
    log "Stopping $SERVICE, swapping binary, starting $SERVICE..."
    $SSH bash -s <<REMOTE
set -euo pipefail
sudo systemctl stop $SERVICE
mv "$REMOTE_BUILD_DIR/decypharr_new" "$REMOTE_BIN"
chmod +x "$REMOTE_BIN"
sudo systemctl start $SERVICE
sleep 3
systemctl is-active $SERVICE
REMOTE
    log "Service restarted with new binary"
}

# ---------------------------------------------------------------------------
# Step 6 - health checks
# ---------------------------------------------------------------------------

run_healthchecks() {
    log "Running health checks..."
    HEALTH_OUTPUT="$($SSH bash -s <<REMOTE
set -uo pipefail
PORT="$HEALTH_PORT"
MOUNT="$MOUNT_PATH"
DEPLOY_DIR="$REMOTE_DEPLOY_DIR"

TOKEN=\$(python3 -c "import json; print(json.load(open('\${DEPLOY_DIR}/auth.json'))['api_token'])" 2>/dev/null || true)
AUTH_HEADER=()
[ -n "\$TOKEN" ] && AUTH_HEADER=(-H "Authorization: Bearer \$TOKEN")

# The http_code %{http_code} placeholder already prints 000 on a refused or
# failed connection, but the old "curl ... or echo 000" appended a SECOND
# 000, giving "000000" - which both misread as healthy: "000000" != "000"
# broke the warmup loop on its first iteration, and "000000" -lt 500 passed
# every HTTP check with the listener down. That is what rolled back an
# otherwise-fine deploy. Capture the code cleanly and treat 000 as down.
http_code() {
    local c
    c=\$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "\$@" 2>/dev/null) || true
    [ -n "\$c" ] || c=000
    printf '%s' "\$c"
}
http_ok() {
    # reachable and not 5xx; 000 means the listener is down
    [ -n "\${1:-}" ] || return 1
    [ "\$1" = 000 ] && return 1
    [ "\$1" -ge 200 ] 2>/dev/null && [ "\$1" -lt 500 ] 2>/dev/null
}

# Wait for the HTTP listener to actually bind - on a busy restart it can lag
# the process start by 10s+. Retry on 000, never treat it as up.
for i in \$(seq 1 20); do
    code=\$(http_code "http://127.0.0.1:\${PORT}/")
    [ "\$code" != "000" ] && break
    sleep 1
done

# mount - retry briefly; the FUSE mount can lag the "DFS started" log by a beat
mnt=fail
for i in \$(seq 1 5); do
    if mountpoint -q "\$MOUNT"; then
        mnt=ok
        break
    fi
    sleep 2
done
if [ "\$mnt" = ok ]; then
    echo "PASS mount: \$MOUNT is mounted"
else
    echo "FAIL mount: \$MOUNT is NOT a mountpoint"
fi

# HTTP root
code=\$(http_code "http://127.0.0.1:\${PORT}/")
if http_ok "\$code"; then
    echo "PASS http_root: HTTP \$code"
else
    echo "FAIL http_root: HTTP \$code"
fi

# /repair
code=\$(http_code "http://127.0.0.1:\${PORT}/repair")
if http_ok "\$code"; then
    echo "PASS repair_page: HTTP \$code"
else
    echo "FAIL repair_page: HTTP \$code"
fi

# /api/overlay/disk-usage - must respond AND report a sane (not GB-scale) figure.
# This endpoint walks every overlay entry (~15k) and serializes a large JSON
# body, so a freshly-restarted service needs well over 5s to answer it - a
# flat --max-time 5 here false-fails every deploy and triggers a needless
# rollback. Retry with a generous per-attempt timeout (same shape as the
# http_root warmup loop above): up to 3 attempts, 5s apart.
bytes=-1
body=""
for i in \$(seq 1 3); do
    body=\$(curl -s --max-time 20 "\${AUTH_HEADER[@]}" "http://127.0.0.1:\${PORT}/api/overlay/disk-usage" 2>/dev/null || true)
    bytes=\$(echo "\$body" | python3 -c "import json,sys; print(json.load(sys.stdin).get('total_overlay_disk_bytes',-1))" 2>/dev/null || echo -1)
    [ "\$bytes" -ge 0 ] 2>/dev/null && break
    sleep 5
done
if [ "\$bytes" -ge 0 ] 2>/dev/null && [ "\$bytes" -lt 1000000000 ]; then
    echo "PASS overlay_disk_usage: total_overlay_disk_bytes=\$bytes (< 1GB)"
else
    echo "FAIL overlay_disk_usage: total_overlay_disk_bytes=\$bytes (expected 0 <= x < 1e9; response: \$body)"
fi

# /api/overlay/repair-progress - just needs to respond (not connection failure/5xx)
code=\$(http_code "\${AUTH_HEADER[@]}" "http://127.0.0.1:\${PORT}/api/overlay/repair-progress?entry=healthcheck&file=healthcheck")
if http_ok "\$code"; then
    echo "PASS overlay_repair_progress: HTTP \$code"
else
    echo "FAIL overlay_repair_progress: HTTP \$code"
fi

# /api/precache/status - must respond and report read-ahead + its bound.
# Does not assert read_ahead_enabled=False anymore: read-ahead may
# legitimately be on now. The bound is enforced by verify_precache_bounded
# (step 8), not here - this check just needs the endpoint to answer sanely.
body=\$(curl -s --max-time 5 "\${AUTH_HEADER[@]}" "http://127.0.0.1:\${PORT}/api/precache/status" 2>/dev/null || true)
enabled=\$(echo "\$body" | python3 -c "import json,sys; print(json.load(sys.stdin).get('read_ahead_enabled', 'MISSING'))" 2>/dev/null || echo "PARSE_ERROR")
max_bytes=\$(echo "\$body" | python3 -c "import json,sys; print(json.load(sys.stdin).get('max_bytes', 'MISSING'))" 2>/dev/null || echo "PARSE_ERROR")
if [ "\$enabled" != "PARSE_ERROR" ] && [ "\$enabled" != "MISSING" ]; then
    echo "PASS precache_status: read_ahead_enabled=\$enabled max_bytes=\$max_bytes"
else
    echo "FAIL precache_status: could not parse read_ahead_enabled/max_bytes (response: \$body)"
fi
REMOTE
)"
    echo "$HEALTH_OUTPUT"
}

# ---------------------------------------------------------------------------
# Step 7 - rollback
# ---------------------------------------------------------------------------

rollback() {
    local failing_checks="$1"
    log "Rolling back to $BACKUP_NAME ..."
    $SSH bash -s <<REMOTE
set -euo pipefail
sudo systemctl stop $SERVICE || true
cp "${REMOTE_DEPLOY_DIR}/${BACKUP_NAME}" "$REMOTE_BIN"
chmod +x "$REMOTE_BIN"
sudo systemctl start $SERVICE
sleep 2
systemctl is-active $SERVICE
REMOTE
    fail "health check(s) failed, rolled back to pre-deploy binary ($OLD_BINARY_HASH). Failing checks:
$failing_checks"
}

# ---------------------------------------------------------------------------
# Step 8 - verify the hard rule (PrecacheReadAhead, if on, stays bounded)
# ---------------------------------------------------------------------------

verify_precache_bounded() {
    local after max_bytes
    after="$($SSH "python3 -c \"import json; print(json.load(open('${REMOTE_DEPLOY_DIR}/config.json')).get('repair',{}).get('precache_read_ahead_enabled', False))\"" 2>/dev/null || echo "False")"

    if [ "$after" != "True" ]; then
        PRECACHE_SUMMARY="off (was $PRECACHE_BEFORE before)"
        log "PrecacheReadAhead is off after deploy (config.json untouched by this script)"
        return
    fi

    max_bytes="$($SSH "python3 -c \"import json; print(json.load(open('${REMOTE_DEPLOY_DIR}/config.json')).get('precache',{}).get('precache_max_bytes'))\"" 2>/dev/null || echo "None")"

    if [ "$max_bytes" = "None" ] || [ "$max_bytes" = "0" ]; then
        rollback "hard_rule_violation: PrecacheReadAhead is enabled after deploy (was $PRECACHE_BEFORE before) but precache_max_bytes is $max_bytes (unset/0 = unbounded) - read-ahead must be bounded by a sane PrecacheMaxBytes. Rolled back."
    fi
    if [ "$max_bytes" -gt "$PRECACHE_MAX_BYTES_CEILING" ] 2>/dev/null; then
        rollback "hard_rule_violation: PrecacheReadAhead is enabled after deploy with precache_max_bytes=$max_bytes bytes, exceeding the 256GiB ceiling ($PRECACHE_MAX_BYTES_CEILING). Rolled back."
    fi

    PRECACHE_SUMMARY="on, bounded by precache_max_bytes=${max_bytes} bytes (<= 256GiB ceiling)"
    log "PrecacheReadAhead is on after deploy, bounded by precache_max_bytes=$max_bytes bytes"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
    preflight
    sync_and_build_assets
    build_binary
    backup_binary
    swap_and_restart

    local health_output failing
    health_output="$(run_healthchecks)"
    echo "$health_output"
    failing="$(echo "$health_output" | grep '^FAIL' || true)"

    if [ -n "$failing" ]; then
        rollback "$failing"
    fi

    verify_precache_bounded

    local passed
    passed="$(echo "$health_output" | grep '^PASS' | sed 's/^PASS //')"

    echo ""
    echo "=== Deploy successful ==="
    echo "Branch:        $BRANCH"
    echo "New tip:       $TARGET_COMMIT"
    echo "Old binary:    $OLD_BINARY_HASH"
    echo "Backup:        ${REMOTE_DEPLOY_DIR}/${BACKUP_NAME}"
    echo "Checks passed:"
    echo "$passed" | sed 's/^/  - /'
    echo "PrecacheReadAhead: $PRECACHE_SUMMARY"
}

main "$@"
