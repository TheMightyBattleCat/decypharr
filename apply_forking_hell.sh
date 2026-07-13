#!/usr/bin/env bash
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$ROOT" ]; then echo "ERROR: run from inside the decypharr clone."; exit 1; fi
cd "$ROOT"
BR="$(git rev-parse --abbrev-ref HEAD)"
if [ "$BR" != "forking-hell" ]; then
  echo "ERROR: you are on branch '$BR'. Switch first:  git checkout forking-hell"; exit 1
fi
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "ERROR: working tree has uncommitted changes (finish 'git merge --abort' / 'git reset --hard origin/forking-hell' first)."; exit 1
fi
git fetch origin forking-hell >/dev/null 2>&1 || true
if [ "$(git rev-parse HEAD)" != "$(git rev-parse origin/forking-hell 2>/dev/null || echo x)" ]; then
  echo "ERROR: forking-hell is not at origin/forking-hell. Run:  git reset --hard origin/forking-hell"; exit 1
fi
if grep -q "Reserve string" internal/config/usenet.go 2>/dev/null; then
  echo "Looks already applied (usenet.go already has Reserve). Nothing to do."; exit 0
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
if command -v go >/dev/null; then go build ./... && go vet ./... && echo "   go build & vet: clean"; else echo "   WARNING: 'go' not found - verify before pushing."; fi

echo ">> Committing reserve update onto forking-hell..."
git add -A
git commit -F- <<'MSG'
Add fill reserves to the Usenet bandwidth monitor

Brings forking-hell in line with the refined feature: each provider now keeps a
reserve (defaulting to 10% of its cap). While below the cap minus its reserve a
provider leads bulk; once it crosses that point it drops to a fill role for the
rest of the period, spending its reserve only to complete segments the other
primaries can't provide; at the hard cap it goes idle. When no provider is
leading bulk, downloads fail rather than spilling onto metered backups. The
reserve and a fills-only state are shown per provider on the stats page.
MSG

echo ""
echo ">> Done. forking-hell now has repair + quota + reserve in one new commit."
echo "     git show --stat HEAD"
echo ">> Push (fast-forwards; no force needed):"
echo "     git push origin forking-hell"
