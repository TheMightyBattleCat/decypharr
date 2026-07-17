#!/usr/bin/env bash
set -euo pipefail

# --- Guard: must be run from the repo, on the feature branch ---
ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$ROOT" ]; then echo "ERROR: run this from inside the decypharr clone."; exit 1; fi
cd "$ROOT"
BR="$(git rev-parse --abbrev-ref HEAD)"
if [ "$BR" != "feature/usenet-bandwidth-monitor" ]; then
  echo "ERROR: you are on branch '$BR'."
  echo "Run:  git checkout feature/usenet-bandwidth-monitor   (commit/stash any local changes first)"
  exit 1
fi
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "ERROR: working tree has uncommitted changes. Commit or stash them, then re-run."
  exit 1
fi

if [ ! -f internal/nntp/bandwidth.go ]; then
  echo "ERROR: internal/nntp/bandwidth.go not found - are you on the feature branch with the initial commit?"; exit 1
fi
if grep -q "Reserve string" internal/config/usenet.go 2>/dev/null; then
  echo "Looks already applied (usenet.go already has the Reserve field). Nothing to do."; exit 0
fi

BW_B64='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'
CFG_B64='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'
export BW_B64 CFG_B64

echo ">> Decoding full-replacement files..."
python3 - "$ROOT" <<'PYDEC'
import base64, gzip, sys, os
root = sys.argv[1]
BW_B64 = os.environ["BW_B64"]
CFG_B64 = os.environ["CFG_B64"]
def write(rel, b64):
    data = gzip.decompress(base64.b64decode(b64))
    path = os.path.join(root, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)
    print("   wrote", rel, len(data), "bytes")
write("internal/nntp/bandwidth.go", BW_B64)
write("pkg/server/assets/build/js/config.js", CFG_B64)
PYDEC

echo ">> Applying in-place source edits..."
python3 - "$ROOT" <<'PYEDIT'
import sys, os
root = sys.argv[1]

def read(rel):
    with open(os.path.join(root, rel), "r") as f:
        return f.read()

def write(rel, s):
    with open(os.path.join(root, rel), "w") as f:
        f.write(s)

def replace_once(s, old, new, label):
    n = s.count(old)
    assert n == 1, "expected 1 match for %s, got %d" % (label, n)
    return s.replace(old, new, 1)

def region_replace(s, start, end, new, label):
    i = s.find(start)
    assert i != -1, "start marker not found for %s" % label
    assert s.count(start) == 1, "start marker not unique for %s" % label
    j = s.find(end, i)
    assert j != -1, "end marker not found for %s" % label
    j += len(end)
    return s[:i] + new + s[j:]

# ---- internal/config/usenet.go : add Reserve field ----
f = "internal/config/usenet.go"
s = read(f)
old = (
    "\t// QuotaResetHour is the hour of day (0..23, server local time) the period\n"
    "\t// rolls over. Default 0 (midnight).\n"
    "\tQuotaResetHour int `json:\"quota_reset_hour,omitempty\"`\n"
    "}"
)
new = (
    "\t// QuotaResetHour is the hour of day (0..23, server local time) the period\n"
    "\t// rolls over. Default 0 (midnight).\n"
    "\tQuotaResetHour int `json:\"quota_reset_hour,omitempty\"`\n"
    "\t// Reserve is a tail of the Quota held back for fills only, as a human size\n"
    "\t// (e.g. \"500GB\"). Once (Quota - Reserve) is used the provider stops leading\n"
    "\t// bulk and drops to a fill/backup role for the rest of the period, so its\n"
    "\t// reserve is spent only completing segments the other primaries can't\n"
    "\t// provide. Blank defaults to 10% of Quota.\n"
    "\tReserve string `json:\"reserve,omitempty\"`\n"
    "}"
)
write(f, replace_once(s, old, new, "usenet.go Reserve field"))
print("   usenet.go: Reserve field added")

# ---- internal/nntp/client.go : rewrite getAnyAvailableConnection tier logic ----
f = "internal/nntp/client.go"
s = read(f)
new_block = (
"\t// Effective serving tiers (recomputed each call from live quota state):\n"
"\t//   lead  = primary below its soft threshold \xe2\x80\x94 carries bulk load\n"
"\t//   fill  = configured backup, OR a primary in its reserve band \xe2\x80\x94 used only\n"
"\t//           to fill segments the leads can't provide (article-not-found /\n"
"\t//           connection error), drawing a capped primary's held-back reserve\n"
"\t//   blocked = at/over hard quota \xe2\x80\x94 never used\n"
"\t//\n"
"\t// Bulk draws from the lead tier. The fill tier is consulted only when leads\n"
"\t// EXIST but are all excluded for this segment. If no lead exists at all\n"
"\t// (every primary at/over its soft cap), bulk fails here rather than spilling\n"
"\t// onto reserves or metered backups \xe2\x80\x94 reserves are a cushion for the normal\n"
"\t// case (one server capped, others still leading), not a way to keep\n"
"\t// streaming when every primary is maxed.\n"
"\tleadExists := false\n"
"\tleadUsable := false\n"
"\tfor _, p := range c.providers {\n"
"\t\tif c.providerTier(p) != tierLead {\n"
"\t\t\tcontinue\n"
"\t\t}\n"
"\t\tleadExists = true\n"
"\t\tif !exclusions.excludes(p) {\n"
"\t\t\tleadUsable = true\n"
"\t\t\tbreak\n"
"\t\t}\n"
"\t}\n"
"\n"
"\ttarget := tierLead\n"
"\tif !leadUsable {\n"
"\t\tif !leadExists {\n"
"\t\t\treturn nil, config.UsenetProvider{}, errors.New(\"no eligible providers available\")\n"
"\t\t}\n"
"\t\ttarget = tierFill\n"
"\t}\n"
"\n"
"\t// Phase 1: Non-blocking scan - try to get a free slot from any provider\n"
"\t// within the target tier.\n"
"\teligibleCount := 0\n"
"\tfor _, provider := range c.providers {\n"
"\t\tif c.providerTier(provider) != target || exclusions.excludes(provider) {\n"
"\t\t\tcontinue\n"
"\t\t}\n"
"\t\teligibleCount++\n"
"\t\tpp := c.pools[provider.Host]\n"
"\n"
"\t\tselect {\n"
"\t\tcase pp.slots <- struct{}{}:\n"
"\t\t\t// Got a slot - try to get or create connection\n"
"\t\t\tconn, err := c.getOrCreateFromPool(ctx, pp, provider)\n"
"\t\t\tif err != nil {\n"
"\t\t\t\t<-pp.slots // Release slot on error\n"
"\t\t\t\tcontinue   // Try next provider\n"
"\t\t\t}\n"
"\t\t\treturn conn, provider, nil\n"
"\t\tdefault:\n"
"\t\t\t// Pool at capacity, try next provider\n"
"\t\t\tcontinue\n"
"\t\t}\n"
"\t}\n"
"\n"
"\tif eligibleCount == 0 {\n"
"\t\treturn nil, config.UsenetProvider{}, errors.New(\"no eligible providers available\")\n"
"\t}\n"
"\n"
"\t// Phase 2: All providers in the target tier busy - race for first available\n"
"\t// slot in the tier. When the lead tier is in use this is the wait that lets\n"
"\t// the fill tier remain idle rather than getting roped in.\n"
"\teligible := make([]config.UsenetProvider, 0, eligibleCount)\n"
"\tfor _, provider := range c.providers {\n"
"\t\tif c.providerTier(provider) == target && !exclusions.excludes(provider) {\n"
"\t\t\teligible = append(eligible, provider)\n"
"\t\t}\n"
"\t}\n"
"\treturn c.raceForConnection(ctx, eligible)"
)
s = region_replace(s, "\tuseBackups := true", "\treturn c.raceForConnection(ctx, eligible)", new_block, "client.go getAnyAvailableConnection")

# Stats() bandwidth block enrich
old_stats = (
"\t\t// Add bandwidth usage / quota state\n"
"\t\tif c.bw != nil {\n"
"\t\t\tif bw, ok := c.bw.Snapshot(p.Host); ok {\n"
"\t\t\t\tproviderInfo[\"bytes_used\"] = bw.BytesUsed\n"
"\t\t\t\tproviderInfo[\"quota_bytes\"] = bw.QuotaBytes\n"
"\t\t\t\tproviderInfo[\"quota_period\"] = bw.Period\n"
"\t\t\t\tproviderInfo[\"quota_exceeded\"] = bw.Exceeded\n"
"\t\t\t\tif !bw.ResetAt.IsZero() {\n"
"\t\t\t\t\tproviderInfo[\"quota_reset_at\"] = bw.ResetAt.Format(\"2006-01-02T15:04:05Z07:00\")\n"
"\t\t\t\t}\n"
"\t\t\t}\n"
"\t\t}"
)
new_stats = (
"\t\t// Add bandwidth usage / quota state\n"
"\t\tif c.bw != nil {\n"
"\t\t\tif bw, ok := c.bw.Snapshot(p.Host); ok {\n"
"\t\t\t\tproviderInfo[\"bytes_used\"] = bw.BytesUsed\n"
"\t\t\t\tproviderInfo[\"quota_bytes\"] = bw.QuotaBytes\n"
"\t\t\t\tproviderInfo[\"reserve_bytes\"] = bw.ReserveBytes\n"
"\t\t\t\tproviderInfo[\"soft_threshold\"] = bw.SoftThreshold\n"
"\t\t\t\tproviderInfo[\"quota_period\"] = bw.Period\n"
"\t\t\t\tproviderInfo[\"quota_exceeded\"] = bw.Exceeded\n"
"\t\t\t\tproviderInfo[\"fill_only\"] = bw.FillOnly\n"
"\t\t\t\tif !bw.ResetAt.IsZero() {\n"
"\t\t\t\t\tproviderInfo[\"quota_reset_at\"] = bw.ResetAt.Format(\"2006-01-02T15:04:05Z07:00\")\n"
"\t\t\t\t}\n"
"\t\t\t}\n"
"\t\t}"
)
s = replace_once(s, old_stats, new_stats, "client.go Stats bandwidth block")
write(f, s)
print("   client.go: getAnyAvailableConnection + Stats updated")

# ---- pkg/server/assets/js/config.js : 3 source edits (reserve input, getter, collect) ----
f = "pkg/server/assets/js/config.js"
s = read(f)
tpl_old = (
'                        <span class="text-sm opacity-70">Data cap per period. Empty or 0 = unlimited.</span>\n'
'                    </div>'
)
tpl_new = (
'                        <span class="text-sm opacity-70">Data cap per period. Empty or 0 = unlimited.</span>\n'
'                    </div>\n'
'                    <div>\n'
'                        <label class="label" for="usenet_provider_${index}_reserve">\n'
'                            <span class="font-medium">Reserve (fills)</span>\n'
'                        </label>\n'
'                        <input type="text" class="input w-full"\n'
'                               name="usenet.providers[${index}].reserve"\n'
'                               id="usenet_provider_${index}_reserve"\n'
'                               placeholder="blank = 10% of cap">\n'
'                        <span class="text-sm opacity-70">Held back for fills only. Once you\'ve used the cap minus this reserve, the server drops to a fill/backup role for the rest of the period; blank defaults to 10% of the cap.</span>\n'
'                    </div>'
)
s = replace_once(s, tpl_old, tpl_new, "config.js reserve input")
s = replace_once(
    s,
    "            const quotaResetHourInput = getField('quota_reset_hour');",
    "            const quotaResetHourInput = getField('quota_reset_hour');\n            const reserveInput = getField('reserve');",
    "config.js reserve getter",
)
col_old = (
    "                quota_reset_hour: quotaResetHourInput ? (parseInt(quotaResetHourInput.value) || 0) : 0\n"
    "            };"
)
col_new = (
    "                quota_reset_hour: quotaResetHourInput ? (parseInt(quotaResetHourInput.value) || 0) : 0,\n"
    "                // Reserve held back for fills only; blank = 10% of cap (server-side default).\n"
    "                reserve: reserveInput ? reserveInput.value.trim() : ''\n"
    "            };"
)
s = replace_once(s, col_old, col_new, "config.js reserve collect")
write(f, s)
print("   config.js (source): reserve input + getter + collect")

# ---- pkg/server/templates/stats.html : replace the bandwidth IIFE ----
f = "pkg/server/templates/stats.html"
s = read(f)
new_iife = r'''${(() => {
                                    const used = provider.bytes_used || 0;
                                    const q = provider.quota_bytes || 0;
                                    if (!q && !used) return '';
                                    if (!q) {
                                        return `<div class="mt-1 text-xs text-base-content/60">${window.decypharrUtils.formatBytes(used)} downloaded this period</div>`;
                                    }
                                    const reserve = provider.reserve_bytes || 0;
                                    const soft = provider.soft_threshold || (q - reserve);
                                    const pct = Math.min(100, (used / q) * 100);
                                    const softPct = Math.min(100, (soft / q) * 100);
                                    const exceeded = provider.quota_exceeded;
                                    const fillOnly = provider.fill_only;
                                    const barColor = exceeded ? 'bg-error' : (fillOnly ? 'bg-info' : (pct >= 80 ? 'bg-warning' : 'bg-success'));
                                    let badge = '';
                                    if (exceeded) badge = ' <span class="badge badge-error badge-xs ml-1">Limit reached</span>';
                                    else if (fillOnly) badge = ' <span class="badge badge-info badge-xs ml-1">Fills only</span>';
                                    let resetStr = '';
                                    if (provider.quota_reset_at) {
                                        const ms = new Date(provider.quota_reset_at) - new Date();
                                        if (ms > 0) {
                                            const h = Math.floor(ms / 3600000);
                                            const d = Math.floor(h / 24);
                                            resetStr = d >= 1 ? `resets in ${d}d ${h % 24}h` : `resets in ${h}h`;
                                        }
                                    }
                                    const textColor = exceeded ? 'text-error font-medium' : (fillOnly ? 'text-info font-medium' : 'text-base-content/70');
                                    let sub = '';
                                    if (reserve > 0) {
                                        sub = fillOnly
                                            ? `In reserve \u2014 fills only, ${window.decypharrUtils.formatBytes(Math.max(0, q - used))} of reserve left`
                                            : `${window.decypharrUtils.formatBytes(Math.max(0, soft - used))} until fills-only \u00b7 reserve ${window.decypharrUtils.formatBytes(reserve)}`;
                                    }
                                    return `
                                        <div class="mt-1">
                                            <div class="flex justify-between items-center text-xs">
                                                <span class="${textColor}">${window.decypharrUtils.formatBytes(used)} / ${window.decypharrUtils.formatBytes(q)}${badge}</span>
                                                <span class="text-base-content/50">${resetStr}</span>
                                            </div>
                                            <div class="relative w-full bg-base-300 rounded-full h-1.5 mt-1 overflow-hidden">
                                                <div class="h-full ${barColor} transition-all duration-300" style="width: ${pct}%"></div>
                                                ${reserve > 0 ? `<div class="absolute top-0 bottom-0 w-px bg-base-content/40" style="left: ${softPct}%" title="Fills-only threshold"></div>` : ''}
                                            </div>
                                            ${sub ? `<div class="text-xs text-base-content/50 mt-0.5">${sub}</div>` : ''}
                                        </div>`;
                                })()}'''
s = region_replace(s, "${(() => {\n                                    const used = provider.bytes_used || 0;", "})()}", new_iife, "stats.html bandwidth IIFE")
write(f, s)
print("   stats.html: bandwidth IIFE replaced")
PYEDIT


echo ">> Formatting, building, vetting..."
if command -v gofmt >/dev/null; then gofmt -w internal/nntp/bandwidth.go internal/nntp/client.go internal/config/usenet.go; fi
if command -v node   >/dev/null; then node -c pkg/server/assets/js/config.js && echo "   config.js source: syntax OK"; fi
if command -v go >/dev/null; then
  go build ./... && go vet ./... && echo "   go build & vet: clean"
else
  echo "   WARNING: 'go' not found - skipping build/vet. Verify before pushing."
fi

echo ">> Amending into one clean commit..."
git add -A
git commit --amend -F- <<'MSG'
Add per-provider Usenet bandwidth monitoring, quotas, and fill reserves

Some Usenet providers ban you for exceeding a fair-use cap over a week or
month, and there was no way to see per-provider usage or stop using one before
it tripped that limit.

This meters bytes downloaded per provider at the socket (the actual transfer
volume the provider bills) and shows usage under each server on the stats page.
Each provider can have an optional cap with a daily, weekly, or monthly reset
anchored to a configurable day and hour, plus a reserve that defaults to ten
percent of the cap. Providers are tried in strict priority order: while a
provider is below its cap minus its reserve it leads bulk downloads; once it
crosses that point it stops taking bulk and drops to a fill role for the rest
of the period, so its reserve is spent only completing segments the remaining
primaries cannot provide. At the hard cap it goes idle, and when no provider is
leading bulk, downloads fail rather than spilling onto backup providers that
may be metered. Usage, the reserve, and a fills-only state are shown per
provider, and everything resets with the quota period. Usage persists across
restarts.
MSG

echo ""
echo ">> Done. One clean commit on feature/usenet-bandwidth-monitor. Review it:"
echo "     git show --stat HEAD"
echo ">> Then push (rewrites your own branch; force-with-lease is the safe form):"
echo "     git push --force-with-lease"
