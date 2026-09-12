#!/usr/bin/env bash
#
# Clean up torrent entries no configured debrid can serve, on a remote server.
# Run from your local checkout; every call goes over SSH to the server's local API.
#
# Usage:
#   scripts/debrid-gone.sh list [--limit N]
#   scripts/debrid-gone.sh fix [--delete] [--limit N] [--delete-limit N] [--batches N] [--wait-max MIN]
#
# list  Dry run. Counts what a fix would do and names some of each:
#         orphaned  no Arr points at the entry: deleted by fix --delete
#         regrab    an Arr points at it: marked broken, re-grabbed by Fix broken
#         kept      only a skip_repair Arr points at it: left alone
#       Lists every Arr's library first, so it takes about a minute.
#
# fix   Runs up to --batches batches (default 1). Each batch deletes up to
#       --delete-limit orphans (default 10, only with --delete) and marks up
#       to --limit entries broken (default 50), which starts a Fix broken run.
#       Waits up to --wait-max minutes (default 20) for any active repair run
#       before each batch. Stops on an error, a failed delete, or when nothing
#       is left.
#
# Deletes touch decypharr's store only: nothing is removed from a debrid
# account, and no Arr file is touched.

set -euo pipefail

[ -f "$(dirname "$0")/deploy.env" ] && . "$(dirname "$0")/deploy.env"
HOST="${DEPLOY_HOST:?set DEPLOY_HOST in scripts/deploy.env}"

usage() {
	sed -n '6,8p' "$0" | sed 's/^# \{0,1\}//'
	exit 2
}

# api METHOD PATH: prints the body, then the HTTP status on the last line.
# ssh hands its arguments to the remote shell as one string, so they are
# quoted for it: an unquoted & in a query would end the command there.
api() {
	# shellcheck disable=SC2029
	ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST" "bash -s -- $(printf '%q %q' "$1" "$2")" <<'REMOTE'
set -euo pipefail
T=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["api_token"])' "${DECYPHARR_DIR:-$HOME/decypharr}/auth.json")
curl -sS -X "$1" -H "Authorization: Bearer $T" -w '\n%{http_code}' "http://127.0.0.1:8686$2"
REMOTE
}

# summary: reads a debrid-gone JSON result on stdin and prints it.
summary() {
	python3 -c '
import json, sys
d = json.load(sys.stdin)
print("remaining: total %d | orphaned %d | regrab %d | already_broken %d | kept %d | invalid %d" % (
    d["total"], d["orphaned"], d["regrab"], d["already_broken"], d["kept"], d["invalid"]))
print("by reason:", ", ".join("%s %d" % kv for kv in sorted(d["by_reason"].items())))
for key, label in (("orphans", "orphans"), ("entries", "re-grab")):
    rows = d.get(key) or []
    if rows:
        print("%s (%d shown):" % (label, len(rows)))
        for e in rows:
            print("  %-12s %-22s %3d files  %s" % (e["provider"] or "-", e["reason"], e["files"], e["name"]))
if "deleted" in d and (d["deleted"] or d["delete_failed"] or d["marked"] or d.get("run")):
    run = d.get("run") or {}
    print("this batch: deleted %d | delete_failed %d | marked broken %d | run %s" % (
        d["deleted"], d["delete_failed"], d["marked"], run.get("id", "-")))
if d.get("error"):
    print("re-grab did not start:", d["error"])
'
}

# field NAME: prints one top-level field of the JSON on stdin.
field() {
	python3 -c 'import json,sys; v=json.load(sys.stdin).get(sys.argv[1]); print(v if v is not None else "")' "$1"
}

# wait_idle: waits up to $wait_max minutes for the active repair run to end.
wait_idle() {
	local deadline=$(($(date +%s) + wait_max * 60))
	while :; do
		local out code body active
		out=$(api GET /api/repair/status)
		code=${out##*$'\n'}
		body=${out%$'\n'*}
		[[ $code == 200 ]] || { echo "repair status: HTTP $code: $body" >&2; exit 1; }
		active=$(python3 -c 'import json,sys; r=json.load(sys.stdin).get("active_run") or {}; print(r.get("id",""), r.get("source",""), r.get("stage",""))' <<<"$body")
		[[ -z ${active// } ]] && return
		if (($(date +%s) >= deadline)); then
			echo "gave up after ${wait_max} min waiting for repair run: $active" >&2
			echo "stop it (Repair page, or POST /api/repair/stop) or pass a larger --wait-max" >&2
			exit 1
		fi
		echo "$(date +%T) waiting for repair run: $active"
		sleep 30
	done
}

cmd="${1:-}"
[[ -n $cmd ]] || usage
shift

limit=""
delete_limit=10
delete=0
batches=1
wait_max=20
while [[ $# -gt 0 ]]; do
	case "$1" in
		--limit) limit="$2"; shift 2 ;;
		--delete-limit) delete_limit="$2"; shift 2 ;;
		--delete) delete=1; shift ;;
		--batches) batches="$2"; shift 2 ;;
		--wait-max) wait_max="$2"; shift 2 ;;
		*) usage ;;
	esac
done

case "$cmd" in
	list)
		echo "listing (lists every Arr's library first, about a minute)..."
		out=$(api GET "/api/repair/debrid-gone?limit=${limit:-20}")
		code=${out##*$'\n'}
		body=${out%$'\n'*}
		[[ $code == 200 ]] || { echo "HTTP $code: $body" >&2; exit 1; }
		summary <<<"$body"
		;;
	fix)
		query="delete=${delete}&delete_limit=${delete_limit}"
		[[ -n $limit ]] && query+="&limit=${limit}"
		for ((b = 1; b <= batches; b++)); do
			wait_idle
			echo "$(date +%T) batch $b/$batches: POST fix?$query (lists every Arr's library first)..."
			out=$(api POST "/api/repair/debrid-gone/fix?$query")
			code=${out##*$'\n'}
			body=${out%$'\n'*}
			if [[ $code != 200 ]]; then
				if [[ $body == *"no unservable debrid entries to delete or re-grab"* ]]; then
					echo "nothing left to delete or re-grab."
					exit 0
				fi
				echo "HTTP $code: $body" >&2
				exit 1
			fi
			summary <<<"$body"
			if [[ $(field delete_failed <<<"$body") != 0 || -n $(field error <<<"$body") ]]; then
				echo "stopping: see the delete failures or error above" >&2
				exit 1
			fi
			if [[ $(field deleted <<<"$body") == 0 && $(field marked <<<"$body") == 0 ]]; then
				echo "batch did nothing; stopping."
				exit 0
			fi
		done
		;;
	*) usage ;;
esac
