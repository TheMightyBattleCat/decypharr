#!/usr/bin/env bash
#
# Deploy a decypharr branch to thedradis's decypharr_beta service.
#
# Run this FROM the Mac checkout (not on thedradis). It never touches
# GitHub - it archives the requested commit straight out of the local git
# checkout, ships it to thedradis over SSH, builds it there, and swaps it
# into the live service with a binary backup + automatic rollback on any
# failed health check.
#
# Usage:
#   scripts/deploy-thedradis.sh [branch] [expected-commit]
#
#   branch           Branch to deploy. Defaults to the current branch.
#   expected-commit  Short or full commit hash the branch tip must match
#                    before anything is touched (safety pin). Optional -
#                    if omitted, whatever the branch currently resolves to
#                    is deployed, unpinned.
#
# Example (this run):
#   scripts/deploy-thedradis.sh feature/padding-par2-overlay a03cda1
#
# Rollback is binary-swap only: it restores the pre-deploy decypharr binary
# and restarts the service. It does NOT revert config.json - this script
# never writes config.json, so there is nothing there to revert.
#
# Hard rule: this script must never enable PrecacheReadAhead ("Fast
# read-ahead"). It never touches config.json at all, and it verifies that
# invariant held after every run (see verify_precache_still_off).

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BRANCH="${1:-$(git -C "$REPO_DIR" rev-parse --abbrev-ref HEAD)}"
EXPECTED_COMMIT="${2:-}"

REMOTE_HOST="thedradis"
REMOTE_BUILD_DIR="/home/battlecat/decypharr_consolidate_build"
REMOTE_DEPLOY_DIR="/home/battlecat/decypharr"
REMOTE_BIN="${REMOTE_DEPLOY_DIR}/decypharr"
SERVICE="decypharr_beta"
MOUNT_PATH="/home/battlecat/realdebrid"
HEALTH_PORT="8686"
KEEP_BACKUPS=5

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

# Give the HTTP listener a few seconds to actually bind after start.
for i in \$(seq 1 10); do
    code=\$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:\${PORT}/" 2>/dev/null || echo 000)
    [ "\$code" != "000" ] && break
    sleep 1
done

# mount
if mountpoint -q "\$MOUNT"; then
    echo "PASS mount: \$MOUNT is mounted"
else
    echo "FAIL mount: \$MOUNT is NOT a mountpoint"
fi

# HTTP root
code=\$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:\${PORT}/" 2>/dev/null || echo 000)
if [ "\$code" != "000" ] && [ "\$code" -lt 500 ]; then
    echo "PASS http_root: HTTP \$code"
else
    echo "FAIL http_root: HTTP \$code"
fi

# /repair
code=\$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:\${PORT}/repair" 2>/dev/null || echo 000)
if [ "\$code" != "000" ] && [ "\$code" -lt 500 ]; then
    echo "PASS repair_page: HTTP \$code"
else
    echo "FAIL repair_page: HTTP \$code"
fi

# /api/overlay/disk-usage - must respond AND report a sane (not GB-scale) figure
body=\$(curl -s --max-time 5 "\${AUTH_HEADER[@]}" "http://127.0.0.1:\${PORT}/api/overlay/disk-usage" 2>/dev/null || true)
bytes=\$(echo "\$body" | python3 -c "import json,sys; print(json.load(sys.stdin).get('total_overlay_disk_bytes',-1))" 2>/dev/null || echo -1)
if [ "\$bytes" -ge 0 ] 2>/dev/null && [ "\$bytes" -lt 1000000000 ]; then
    echo "PASS overlay_disk_usage: total_overlay_disk_bytes=\$bytes (< 1GB)"
else
    echo "FAIL overlay_disk_usage: total_overlay_disk_bytes=\$bytes (expected 0 <= x < 1e9; response: \$body)"
fi

# /api/overlay/repair-progress - just needs to respond (200 or a real JSON/text body, not connection failure/5xx)
code=\$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "\${AUTH_HEADER[@]}" "http://127.0.0.1:\${PORT}/api/overlay/repair-progress?entry=healthcheck&file=healthcheck" 2>/dev/null || echo 000)
if [ "\$code" != "000" ] && [ "\$code" -lt 500 ]; then
    echo "PASS overlay_repair_progress: HTTP \$code"
else
    echo "FAIL overlay_repair_progress: HTTP \$code"
fi

# /api/precache/status - must respond AND report the feature disabled
body=\$(curl -s --max-time 5 "\${AUTH_HEADER[@]}" "http://127.0.0.1:\${PORT}/api/precache/status" 2>/dev/null || true)
enabled=\$(echo "\$body" | python3 -c "import json,sys; print(json.load(sys.stdin).get('read_ahead_enabled', 'MISSING'))" 2>/dev/null || echo "PARSE_ERROR")
if [ "\$enabled" = "False" ]; then
    echo "PASS precache_status: read_ahead_enabled=False"
else
    echo "FAIL precache_status: read_ahead_enabled=\$enabled (response: \$body)"
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
# Step 8 - verify the hard rule (PrecacheReadAhead never enabled by this script)
# ---------------------------------------------------------------------------

verify_precache_still_off() {
    local after
    after="$($SSH "python3 -c \"import json; print(json.load(open('${REMOTE_DEPLOY_DIR}/config.json')).get('repair',{}).get('precache_read_ahead_enabled', False))\"" 2>/dev/null || echo "False")"
    if [ "$after" = "True" ]; then
        rollback "hard_rule_violation: PrecacheReadAhead is enabled after deploy (was $PRECACHE_BEFORE before) - this script never enables it, something else did. Rolled back."
    fi
    log "PrecacheReadAhead confirmed still off after deploy (config.json untouched by this script)"
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

    verify_precache_still_off

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
    echo "PrecacheReadAhead: off (untouched)"
}

main "$@"
