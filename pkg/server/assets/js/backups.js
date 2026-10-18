// Backups page: status cards, run now, history, snapshot detail and the
// restore tools, and the recently removed list.
(function () {
    'use strict';

    const root = document.getElementById('backups-page');
    if (!root) return;

    const api = `${window.urlBase || '/'}api/backups`;
    const utils = window.decypharrUtils;
    const esc = (s) => utils.escapeHtml(String(s == null ? '' : s));
    const toast = (m, t) => utils.createToast(m, t || 'success');
    const $ = (id) => document.getElementById(id);

    const state = {status: null, snapshots: [], timer: null, wasRunning: false, current: null};

    async function call(method, path, body) {
        const opts = {method, headers: {}};
        if (body !== undefined) {
            opts.headers['Content-Type'] = 'application/json';
            opts.body = JSON.stringify(body);
        }
        const res = await fetch(`${api}${path}`, opts);
        if (res.status === 204) return null;
        let data = null;
        try {
            data = await res.json();
        } catch (e) { /* no body */ }
        if (!res.ok) throw new Error((data && data.error) || `Request failed (${res.status})`);
        return data;
    }

    function fmtBytes(n) {
        return n ? utils.formatBytes(n) : '0 B';
    }

    function fmtTime(t) {
        if (!t || String(t).startsWith('0001')) return '-';
        const d = new Date(t);
        return isNaN(d) ? '-' : d.toLocaleString();
    }

    function fmtDuration(ms) {
        if (!ms) return '-';
        if (ms < 1000) return `${ms} ms`;
        const s = Math.round(ms / 1000);
        if (s < 60) return `${s} s`;
        if (s < 3600) return `${Math.floor(s / 60)} min ${s % 60} s`;
        return `${Math.floor(s / 3600)} h ${Math.floor((s % 3600) / 60)} min`;
    }

    // msBetween is b - a for two server timestamps, 0 when either is missing.
    function msBetween(a, b) {
        const ta = Date.parse(a || ''), tb = Date.parse(b || '');
        return isNaN(ta) || isNaN(tb) ? 0 : Math.max(0, tb - ta);
    }

    // progressHtml renders a running job's progress line and bar. Ages are
    // measured against the server's clock (st.now).
    function progressHtml(p, now) {
        const parts = [`${esc(p.component || '')} ${esc(p.phase || 'working')}`];
        const total = p.files_total || 0;
        if (total) parts.push(`${p.files_done || 0} / ${total} files`);
        if (p.bytes_total) parts.push(`${fmtBytes(p.bytes_done)} of ${fmtBytes(p.bytes_total)} read`);
        parts.push(`${fmtBytes(p.bytes_written)} written`);
        if (!p.paused && p.rate_bps) parts.push(`${fmtBytes(p.rate_bps)}/s`);
        if (!p.paused && p.eta_seconds) parts.push(`about ${fmtDuration(p.eta_seconds * 1000)} left`);

        const meta = [];
        const ran = msBetween(p.run_started, now);
        if (ran) meta.push(`running for ${fmtDuration(ran)}`);
        // Long single steps (the database export, the Arr lookups) report
        // nothing for a while, so only say so once it is noticeable.
        const quiet = msBetween(p.updated_at, now);
        if (quiet > 15000) meta.push(`last update ${fmtDuration(quiet)} ago`);

        let paused = '';
        if (p.paused) {
            const since = msBetween(p.paused_since, now);
            paused = `<div class="text-xs mt-1"><span class="badge badge-warning badge-sm mr-1">Paused</span>` +
                `${esc(p.paused)}${since ? ` · for ${fmtDuration(since)}` : ''} · ` +
                `<a href="${window.urlBase || '/'}settings#tab-backups" class="link link-hover">turn off pausing in Settings</a>` +
                ` to carry on now</div>`;
        }

        const done = p.bytes_total ? (p.bytes_done || 0) / p.bytes_total : (total ? (p.files_done || 0) / total : -1);
        const cls = p.paused ? 'progress-warning' : 'progress-primary';
        const bar = done >= 0
            ? `<progress class="progress ${cls} w-full" value="${Math.min(100, Math.round(100 * done))}" max="100"></progress>`
            : `<progress class="progress ${cls} w-full"></progress>`;
        return `<div class="mt-2"><div class="text-xs opacity-70">${parts.join(' · ')}</div>` + paused +
            (meta.length ? `<div class="text-xs opacity-50">${meta.join(' · ')}</div>` : '') + bar + '</div>';
    }

    // ---- confirm dialog ----

    function confirmDialog(title, bodyHtml, okLabel, danger) {
        return new Promise((resolve) => {
            const dlg = $('bkConfirmModal');
            $('bkConfirmTitle').textContent = title;
            $('bkConfirmBody').innerHTML = bodyHtml;
            const ok = $('bkConfirmOk');
            const cancel = $('bkConfirmCancel');
            ok.textContent = okLabel || 'OK';
            ok.className = `btn btn-sm ${danger ? 'btn-error' : 'btn-primary'}`;
            cancel.classList.remove('hidden');
            const done = (v) => {
                ok.onclick = null;
                cancel.onclick = null;
                dlg.onclose = null;
                dlg.close();
                resolve(v);
            };
            ok.onclick = () => done(true);
            cancel.onclick = () => done(false);
            dlg.onclose = () => resolve(false);
            dlg.showModal();
        });
    }

    function infoDialog(title, bodyHtml) {
        return new Promise((resolve) => {
            const dlg = $('bkConfirmModal');
            $('bkConfirmTitle').textContent = title;
            $('bkConfirmBody').innerHTML = bodyHtml;
            const ok = $('bkConfirmOk');
            const cancel = $('bkConfirmCancel');
            ok.textContent = 'Close';
            ok.className = 'btn btn-sm btn-primary';
            cancel.classList.add('hidden');
            ok.onclick = () => {
                dlg.close();
                resolve();
            };
            dlg.onclose = () => resolve();
            dlg.showModal();
        });
    }

    // ---- status ----

    function renderStatus(st) {
        state.status = st;
        $('bkHoldBanner').classList.toggle('hidden', !st.post_restore_hold);
        $('bkSwapBanner').classList.toggle('hidden', !st.pending_db_restore);
        $('bkNotConfigured').classList.toggle('hidden', !!st.configured);

        const line = st.configured
            ? `${esc(st.dir)}${st.free_bytes ? ` · ${fmtBytes(st.free_bytes)} free` : ''}`
            : 'Not configured';
        $('bkStatusLine').innerHTML = line +
            (st.same_disk ? ' <span class="badge badge-warning badge-sm ml-1">same disk as the database</span>' : '');

        const cards = [
            ['Repository size', fmtBytes(st.repo_bytes)],
            ['Snapshots', st.snapshots || 0],
            ['New data in the last run', fmtBytes(st.last_new_bytes)],
            ['Free space', st.free_bytes ? fmtBytes(st.free_bytes) : '-'],
        ];
        $('bkSummary').innerHTML = cards.map(([k, v]) =>
            `<div class="stat bg-base-200 rounded-box p-3"><div class="stat-title text-xs">${esc(k)}</div>` +
            `<div class="stat-value text-lg">${esc(v)}</div></div>`).join('');

        const run = st.running;
        $('bkJobs').innerHTML = (st.jobs || []).map((j) => {
            const comps = (j.components || []).join(', ');
            let progress = '';
            if (j.running) {
                progress = progressHtml(run || {}, st.now);
            }
            return `<div class="card bg-base-200"><div class="card-body p-4">` +
                `<div class="flex items-center justify-between gap-2">` +
                `<div><div class="font-semibold">${esc(j.name)}` +
                `${j.enabled ? '' : ' <span class="badge badge-ghost badge-sm">disabled</span>'}</div>` +
                `<div class="text-xs opacity-70">${esc(comps)} · ${esc(j.schedule || 'manual only')}</div></div>` +
                (j.running
                    ? `<button class="btn btn-sm btn-error btn-outline" data-stop="1"><i class="bi bi-stop-fill mr-1"></i>Stop</button></div>`
                    : `<button class="btn btn-sm btn-primary" data-run="${esc(j.name)}"` +
                      `${!st.configured ? ' disabled' : ''}><i class="bi bi-play-fill mr-1"></i>Run now</button></div>`) +
                `<div class="text-xs mt-2">Last success: ${esc(fmtTime(j.last_success))} · ` +
                `Next run: ${esc(fmtTime(j.next_run))}</div>` +
                (j.last_error ? `<div class="text-xs text-error mt-1 break-all">${esc(j.last_error)}</div>` : '') +
                progress + '</div></div>';
        }).join('') || '<div class="text-sm opacity-70">No jobs are set up.</div>';
    }

    function anyRunning(st) {
        return !!(st && ((st.jobs || []).some((j) => j.running) || st.running));
    }

    async function refreshStatus() {
        try {
            const st = await call('GET', '/status');
            const running = anyRunning(st);
            renderStatus(st);
            if (state.wasRunning && !running) {
                toast('Backup finished');
                await loadSnapshots();
                loadRemoved();
            }
            state.wasRunning = running;
            schedulePoll(running ? 2000 : 30000);
        } catch (e) {
            $('bkStatusLine').textContent = e.message;
            schedulePoll(30000);
        }
    }

    function schedulePoll(ms) {
        clearTimeout(state.timer);
        state.timer = setTimeout(refreshStatus, ms);
    }

    async function runJob(name) {
        try {
            await call('POST', `/jobs/${encodeURIComponent(name)}/run`);
            toast(`Started ${name}`);
            state.wasRunning = true;
            refreshStatus();
        } catch (e) {
            toast(esc(e.message), 'error');
        }
    }

    async function stopRun() {
        const ok = await confirmDialog('Stop the backup?',
            '<p>This run\'s snapshot is not saved. What it has already stored is reused by the next run.</p>',
            'Stop', true);
        if (!ok) return;
        try {
            await call('POST', '/running/stop');
            toast('Stopping the backup');
            refreshStatus();
        } catch (e) {
            toast(esc(e.message), 'error');
        }
    }

    async function releaseHold() {
        const ok = await confirmDialog('Release the hold?',
            '<p>Held background work will start again.</p>', 'Release');
        if (!ok) return;
        try {
            await call('POST', '/hold/release');
            toast('Hold released');
            refreshStatus();
        } catch (e) {
            toast(esc(e.message), 'error');
        }
    }

    // ---- history ----

    async function loadSnapshots() {
        try {
            const list = (await call('GET', '/snapshots')) || [];
            list.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
            state.snapshots = list;
            renderHistory();
        } catch (e) {
            $('bkHistoryBody').innerHTML =
                `<tr><td colspan="7" class="text-error">${esc(e.message)}</td></tr>`;
        }
    }

    function renderHistory() {
        const body = $('bkHistoryBody');
        if (!state.snapshots.length) {
            body.innerHTML = '<tr><td colspan="7" class="opacity-70">No snapshots yet.</td></tr>';
            return;
        }
        body.innerHTML = state.snapshots.map((s) =>
            `<tr><td class="whitespace-nowrap">${esc(fmtTime(s.created_at))}</td>` +
            `<td>${esc(s.trigger)}</td>` +
            `<td>${(s.components || []).map((c) => `<span class="badge badge-outline badge-sm mr-1">${esc(c)}</span>`).join('')}</td>` +
            `<td>${esc(fmtBytes(s.new_bytes))}</td><td>${esc(fmtDuration(s.duration_ms))}</td>` +
            `<td><span class="badge badge-success badge-sm">ok</span></td>` +
            `<td><button class="btn btn-xs btn-outline" data-open="${esc(s.id)}">Open</button></td></tr>`).join('');
    }

    // ---- snapshot detail ----

    async function openSnapshot(id) {
        state.current = id;
        const info = state.snapshots.find((s) => s.id === id) || {};
        $('bkDetailTitle').textContent = `Snapshot ${fmtTime(info.created_at)}`;
        $('bkDetailMeta').textContent = `${id} · ${info.trigger || ''}`;
        const comps = new Set(info.components || []);
        const dl = (c, label) => comps.has(c) || c === 'config'
            ? `<a class="btn btn-sm btn-outline" href="${api}/snapshots/${encodeURIComponent(id)}/download?component=${c}">` +
            `<i class="bi bi-download mr-1"></i>${label}</a>` : '';
        $('bkDetailBody').innerHTML =
            `<div><h4 class="font-semibold mb-2">Download</h4><div class="flex flex-wrap gap-2">` +
            (comps.has('db') ? dl('db', 'Database and settings (.tar.zst)') : dl('config', 'Settings (.tar.zst)')) +
            (comps.has('links') ? dl('links', 'Link manifest (.tsv)') : '') + `</div></div>` +

            (comps.has('db') ? `<div><h4 class="font-semibold mb-2">Restore the database</h4>` +
                `<p class="text-xs opacity-70 mb-2">Stages the database from this snapshot. It is swapped in at the next start.</p>` +
                `<button class="btn btn-sm btn-warning" id="bkRestoreDb"><i class="bi bi-database-down mr-1"></i>Restore database</button></div>` : '') +

            (comps.has('nzb') ? `<div><h4 class="font-semibold mb-2">Restore one NZB record</h4>` +
                `<div class="flex flex-wrap gap-2 items-center"><input id="bkNzbEntry" class="input input-sm" placeholder="Entry name">` +
                `<label class="label cursor-pointer gap-2"><input id="bkNzbExport" type="checkbox" class="checkbox checkbox-sm">` +
                `<span class="label-text">Export as .nzb instead</span></label>` +
                `<button class="btn btn-sm btn-outline" id="bkRestoreNzb">Restore</button></div>` +
                `<div id="bkNzbResult" class="text-xs mt-2"></div></div>` : '') +

            (comps.has('links') ? `<div><h4 class="font-semibold mb-2">Restore links</h4>` +
                `<p class="text-xs opacity-70 mb-2">Step 1 plans what would change. Nothing is touched until you apply it, and files that are in the way are never overwritten.</p>` +
                `<div class="flex flex-wrap gap-2 items-center">` +
                `<input id="bkScopeRoot" class="input input-sm" placeholder="Root folder">` +
                `<input id="bkScopeSeries" class="input input-sm" placeholder="Series or movie">` +
                `<input id="bkScopePrefix" class="input input-sm" placeholder="Path prefix">` +
                `<label class="label cursor-pointer gap-2"><input id="bkScopeUnresolved" type="checkbox" class="checkbox checkbox-sm">` +
                `<span class="label-text">List unresolved rows</span></label>` +
                `<button class="btn btn-sm btn-outline" id="bkPlanLinks">Plan</button></div>` +
                `<div id="bkLinksPlan" class="mt-3"></div></div>` : '') +

            `<div><h4 class="font-semibold mb-2">Verify</h4><div class="flex flex-wrap gap-2 items-center">` +
            `<button class="btn btn-sm btn-outline" data-verify="quick">Quick check</button>` +
            `<button class="btn btn-sm btn-outline" data-verify="full">Full check (re-reads every blob)</button></div>` +
            `<div id="bkVerifyResult" class="text-sm mt-2"></div></div>` +

            `<div><h4 class="font-semibold mb-2">Delete</h4>` +
            `<button class="btn btn-sm btn-error btn-outline" id="bkDeleteSnap"><i class="bi bi-trash mr-1"></i>Delete this snapshot</button></div>`;
        $('bkDetailModal').showModal();
    }

    async function verify(full) {
        const out = $('bkVerifyResult');
        out.textContent = 'Checking…';
        try {
            const r = await call('POST', `/snapshots/${encodeURIComponent(state.current)}/verify${full ? '?full=true' : ''}`);
            const bad = (r.missing || []).length + (r.corrupt || []).length;
            out.innerHTML = r.ok
                ? `<span class="text-success">All good: ${r.blobs_checked} blobs checked.</span>`
                : `<span class="text-error">${(r.missing || []).length} missing and ${(r.corrupt || []).length} corrupt of ${r.blobs_checked} blobs checked (${bad} problems).</span>` +
                `<div class="font-mono text-xs break-all mt-1">${esc([...(r.missing || []), ...(r.corrupt || [])].slice(0, 20).join(' '))}</div>`;
        } catch (e) {
            out.innerHTML = `<span class="text-error">${esc(e.message)}</span>`;
        }
    }

    async function restoreDb() {
        const ok = await confirmDialog('Restore the database?',
            '<p>This stages the database from this snapshot. The live database is replaced the next time Decypharr starts, and background work is held afterwards until you release it.</p>',
            'Stage restore', true);
        if (!ok) return;
        try {
            const r = await call('POST', `/snapshots/${encodeURIComponent(state.current)}/restore/db`);
            const counts = Object.entries(r.counts || {}).map(([k, v]) => `${esc(k)}: ${v}`).join(', ');
            await infoDialog('Restore staged',
                `<p>${r.restart_required ? 'Restart Decypharr to finish the restore.' : 'Staged.'}</p>` +
                (counts ? `<p class="text-xs">${counts}</p>` : '') +
                (r.records_without_entry ? `<p class="text-xs">${r.records_without_entry} NZB or overlay files newer than the snapshot were kept.</p>` : '') +
                (r.warnings || []).map((w) => `<p class="text-xs text-warning">${esc(w)}</p>`).join(''));
            refreshStatus();
        } catch (e) {
            toast(esc(e.message), 'error');
        }
    }

    async function restoreNzb() {
        const entry = $('bkNzbEntry').value.trim();
        if (!entry) {
            toast('Enter an entry name', 'warning');
            return;
        }
        const out = $('bkNzbResult');
        try {
            const r = await call('POST', `/snapshots/${encodeURIComponent(state.current)}/restore/nzb`,
                {entry, export: $('bkNzbExport').checked});
            out.innerHTML = `<span class="text-success">${esc(r.message || `Restored ${r.restored_files} file(s) for ${r.entry}`)}</span>`;
        } catch (e) {
            out.innerHTML = `<span class="text-error">${esc(e.message)}</span>`;
        }
    }

    // ---- link restore wizard ----

    let linkPlan = null;

    async function planLinks() {
        const out = $('bkLinksPlan');
        out.textContent = 'Planning…';
        try {
            linkPlan = await call('POST', `/snapshots/${encodeURIComponent(state.current)}/restore/links/plan`, {
                scope: {
                    root: $('bkScopeRoot').value.trim(),
                    series: $('bkScopeSeries').value.trim(),
                    prefix: $('bkScopePrefix').value.trim(),
                },
                include_unresolved: $('bkScopeUnresolved').checked,
            });
            renderPlan(out, linkPlan);
        } catch (e) {
            out.innerHTML = `<span class="text-error">${esc(e.message)}</span>`;
        }
    }

    const classBadge = {
        create: 'badge-success', ok: 'badge-ghost', retarget: 'badge-warning',
        conflict: 'badge-error', unresolved: 'badge-info',
    };

    function renderPlan(out, plan) {
        const counts = plan.counts || {};
        const rows = plan.rows || [];
        const shown = rows.filter((r) => r.class !== 'ok').slice(0, 200);
        out.innerHTML =
            `<div class="flex flex-wrap gap-2 mb-2">` +
            ['create', 'ok', 'retarget', 'conflict', 'unresolved'].map((c) =>
                `<span class="badge ${classBadge[c]}">${c}: ${counts[c] || 0}</span>`).join('') + `</div>` +
            (shown.length ? `<div class="overflow-x-auto max-h-64 overflow-y-auto"><table class="table table-sm">` +
                `<thead><tr><th>Class</th><th>Path</th><th>Target</th></tr></thead><tbody>` +
                shown.map((r) => `<tr><td><span class="badge badge-sm ${classBadge[r.class] || ''}">${esc(r.class)}</span></td>` +
                    `<td class="break-all text-xs">${esc(r.path)}</td>` +
                    `<td class="break-all text-xs">${r.class === 'retarget' ? `${esc(r.current)} → ` : ''}${esc(r.target || r.recorded)}` +
                    `${r.note ? `<div class="opacity-70">${esc(r.note)}</div>` : ''}</td></tr>`).join('') +
                `</tbody></table></div>` +
                (rows.length > 200 ? `<div class="text-xs opacity-70 mt-1">Showing the first 200 changes.</div>` : '') :
                '<div class="text-sm opacity-70">Nothing needs changing.</div>') +
            `<div class="flex flex-wrap gap-4 items-center mt-3">` +
            `<label class="label cursor-pointer gap-2"><input id="bkApplyRetarget" type="checkbox" class="checkbox checkbox-sm"><span class="label-text">Apply retargets</span></label>` +
            `<label class="label cursor-pointer gap-2"><input id="bkApplyDangling" type="checkbox" class="checkbox checkbox-sm"><span class="label-text">Create unresolved rows as dangling links</span></label>` +
            `<label class="label cursor-pointer gap-2"><input id="bkApplyRescan" type="checkbox" class="checkbox checkbox-sm"><span class="label-text">Rescan in Sonarr and Radarr</span></label>` +
            `<button class="btn btn-sm btn-primary" id="bkApplyLinks"${counts.create || counts.retarget || counts.unresolved ? '' : ' disabled'}>Apply plan</button></div>` +
            `<div id="bkApplyResult" class="text-sm mt-2"></div>`;
    }

    function applyReport(r) {
        return `<span class="text-success">Created ${r.created}, retargeted ${r.retargeted}, skipped ${r.skipped}, failed ${r.failed}` +
            `${r.rescans ? `, ${r.rescans} rescans sent` : ''}.</span>` +
            ((r.errors || []).length ? `<div class="text-error text-xs break-all mt-1">${esc(r.errors.slice(0, 10).join(' | '))}</div>` : '') +
            ((r.warnings || []).length ? `<div class="text-warning text-xs break-all mt-1">${esc(r.warnings.slice(0, 10).join(' | '))}${r.warnings.length > 10 ? ` (+${r.warnings.length - 10} more)` : ''}</div>` : '');
    }

    function applyMessage() {
        const c = linkPlan.counts || {};
        let m = `<p>This creates ${c.create || 0} link(s) on disk. Existing files are never overwritten.</p>`;
        if ($('bkApplyRetarget').checked) {
            m += `<p>It also retargets ${c.retarget || 0} existing link(s): a retarget replaces the link that is there now.</p>`;
        }
        if ($('bkApplyDangling').checked) {
            m += `<p>It also creates ${c.unresolved || 0} unresolved row(s) as dangling links.</p>`;
        }
        return m;
    }

    async function applyLinks() {
        if (!linkPlan) return;
        const out = $('bkApplyResult');
        const ok = await confirmDialog('Apply the link plan?',
            applyMessage(),
            'Apply');
        if (!ok) return;
        try {
            const r = await call('POST', `/snapshots/${encodeURIComponent(state.current)}/restore/links/apply`, {
                plan_id: linkPlan.plan_id,
                retarget: $('bkApplyRetarget').checked,
                rescan: $('bkApplyRescan').checked,
                dangling: $('bkApplyDangling').checked,
            });
            out.innerHTML = applyReport(r);
            toast('Links restored');
        } catch (e) {
            out.innerHTML = `<span class="text-error">${esc(e.message)}</span>`;
        }
    }

    async function deleteSnapshot() {
        let extra = '';
        try {
            const p = await call('POST', '/retention/preview', {snapshot_id: state.current, policy: {}});
            extra = `<p class="text-xs">Frees about ${esc(fmtBytes(p.freed_bytes))} (${p.blob_count || 0} blobs) once the repository is pruned.</p>`;
        } catch (e) { /* preview is optional */ }
        const ok = await confirmDialog('Delete this snapshot?',
            '<p>The snapshot is removed and cannot be restored from.</p>' + extra, 'Delete', true);
        if (!ok) return;
        try {
            await call('DELETE', `/snapshots/${encodeURIComponent(state.current)}`);
            $('bkDetailModal').close();
            toast('Snapshot deleted');
            loadSnapshots();
            refreshStatus();
        } catch (e) {
            toast(esc(e.message), 'error');
        }
    }

    // ---- recently removed ----

    async function loadRemoved() {
        const q = new URLSearchParams();
        const name = $('bkRemovedName').value.trim();
        const cat = $('bkRemovedCategory').value.trim();
        const since = $('bkRemovedSince').value;
        if (name) q.set('name', name);
        if (cat) q.set('category', cat);
        if (since) q.set('since', new Date(since).toISOString());
        const body = $('bkRemovedBody');
        try {
            const list = (await call('GET', `/removed${q.toString() ? `?${q}` : ''}`)) || [];
            if (!list.length) {
                body.innerHTML = '<tr><td colspan="6" class="opacity-70">Nothing has been removed since the last snapshots.</td></tr>';
                return;
            }
            body.innerHTML = list.map((r) =>
                `<tr><td class="break-all">${esc(r.name)}</td><td>${esc(r.category || '-')}</td>` +
                `<td class="whitespace-nowrap text-xs">${esc(fmtTime(r.last_seen))} to ${esc(fmtTime(r.first_missing))}</td>` +
                `<td>${esc(fmtBytes(r.size))}</td><td>${r.link_count || 0}</td>` +
                `<td><button class="btn btn-xs btn-outline" data-restore-removed="${esc(r.key)}" data-name="${esc(r.name)}">Restore</button></td></tr>`).join('');
        } catch (e) {
            body.innerHTML = `<tr><td colspan="6" class="text-error">${esc(e.message)}</td></tr>`;
        }
    }

    async function restoreRemoved(key, name) {
        const path = `/removed/${encodeURIComponent(key)}/restore`;
        try {
            const plan = await call('POST', path, {dry_run: true});
            if (plan.existing) {
                await infoDialog('Cannot restore', `<p>An entry with the same name or ID already exists: <b>${esc(plan.existing)}</b>.</p>`);
                return;
            }
            const counts = plan.links && plan.links.counts || {};
            const ok = await confirmDialog(`Restore ${name}?`,
                `<p>${esc(plan.message || 'The entry, its items and its NZB record are put back.')}</p>` +
                `<p class="text-xs">Links: ${counts.create || 0} to create, ${counts.ok || 0} already in place, ${counts.conflict || 0} in conflict.</p>` +
                `<label class="label cursor-pointer justify-start gap-2"><input id="bkRemovedRescan" type="checkbox" class="checkbox checkbox-sm">` +
                `<span class="label-text">Rescan in Sonarr and Radarr</span></label>`, 'Restore');
            if (!ok) return;
            const rescan = !!($('bkRemovedRescan') || {}).checked;
            const res = await call('POST', path, {dry_run: false, rescan});
            await infoDialog('Restored', `<p>${esc(res.message || 'Entry restored.')}</p>` +
                (res.link_result ? `<p class="text-sm">${applyReport(res.link_result)}</p>` : ''));
            loadRemoved();
        } catch (e) {
            toast(esc(e.message), 'error');
        }
    }

    // ---- wiring ----

    root.addEventListener('click', (ev) => {
        const t = ev.target.closest('button, a');
        if (!t) return;
        if (t.dataset.run) runJob(t.dataset.run);
        else if (t.dataset.stop) stopRun();
        else if (t.dataset.open) openSnapshot(t.dataset.open);
        else if (t.dataset.verify) verify(t.dataset.verify === 'full');
        else if (t.dataset.restoreRemoved) restoreRemoved(t.dataset.restoreRemoved, t.dataset.name);
        else if (t.id === 'bkReleaseHoldBtn') releaseHold();
        else if (t.id === 'bkRestoreDb') restoreDb();
        else if (t.id === 'bkRestoreNzb') restoreNzb();
        else if (t.id === 'bkPlanLinks') planLinks();
        else if (t.id === 'bkApplyLinks') applyLinks();
        else if (t.id === 'bkDeleteSnap') deleteSnapshot();
        else if (t.id === 'bkRemovedSearch') loadRemoved();
    });

    refreshStatus();
    loadSnapshots();
    loadRemoved();
})();
