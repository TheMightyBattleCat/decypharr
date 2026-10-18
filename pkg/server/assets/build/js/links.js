// Library Links page: link health per root, missing links with restore,
// unused roots, dead links with cleanup (dry run, then confirm), Check now.
(function () {
    const api = `${window.urlBase || '/'}api`;
    const STATE_LABELS = {
        live: 'Live',
        target_gone: 'Target path gone',
        entry_gone: 'Entry gone',
        external: 'External',
    };
    const STATE_BADGES = {
        live: 'badge-success',
        target_gone: 'badge-warning',
        entry_gone: 'badge-error',
        external: 'badge-ghost',
    };
    const PAGE_SIZE = 50;

    const state = {
        status: null,
        missing: [],
        dead: {rows: [], total: 0, page: 1},
        filters: {root: '', state: '', q: ''},
        pollTimer: null,
    };

    const esc = (v) => window.decypharrUtils.escapeHtml(String(v ?? ''));
    const toast = (msg, type) => window.decypharrUtils.createToast(msg, type || 'success');
    const root = () => document.getElementById('links-page');

    async function call(path, opts) {
        const res = await fetch(`${api}${path}`, opts);
        let body = null;
        try {
            body = await res.json();
        } catch (e) { /* empty body */ }
        if (!res.ok) {
            const msg = body && body.error ? body.error : `Request failed (${res.status})`;
            const err = new Error(msg);
            err.status = res.status;
            throw err;
        }
        return body;
    }

    function post(path, data) {
        return call(path, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(data || {}),
        });
    }

    function fmtTime(t) {
        if (!t) return 'never';
        const d = new Date(t);
        return isNaN(d) ? 'never' : d.toLocaleString();
    }

    // ---- rendering ----

    function skeleton() {
        root().innerHTML = `
        <div class="flex items-center justify-between flex-wrap gap-2">
            <div>
                <h1 class="text-2xl font-bold">Library Links</h1>
                <p class="text-sm" id="links-last-check"></p>
            </div>
            <button class="btn btn-primary btn-sm" id="links-check-now">Check now</button>
        </div>
        <div id="links-error"></div>
        <div class="card bg-base-100 shadow-xl"><div class="card-body" id="links-roots"></div></div>
        <div class="card bg-base-100 shadow-xl"><div class="card-body" id="links-trend"></div></div>
        <div class="card bg-base-100 shadow-xl"><div class="card-body" id="links-missing"></div></div>
        <div class="card bg-base-100 shadow-xl"><div class="card-body" id="links-unused"></div></div>
        <div class="card bg-base-100 shadow-xl"><div class="card-body" id="links-dead"></div></div>
        <div class="card bg-base-100 shadow-xl hidden" id="links-recreated-card"><div class="card-body" id="links-recreated"></div></div>
        <div id="links-modal"></div>`;
        document.getElementById('links-check-now').addEventListener('click', checkNow);
    }

    function showError(msg) {
        const el = document.getElementById('links-error');
        if (!el) return;
        el.innerHTML = msg ? `<div class="alert alert-error"><span>${esc(msg)}</span></div>` : '';
    }

    function renderStatus() {
        const st = state.status;
        if (!st) return;
        const last = document.getElementById('links-last-check');
        last.textContent = st.running ? 'Checking now...' : `Last check: ${fmtTime(st.last_check)}`;
        const btn = document.getElementById('links-check-now');
        btn.disabled = !!st.running;
        btn.textContent = st.running ? 'Checking...' : 'Check now';

        const roots = st.roots || [];
        document.getElementById('links-roots').innerHTML = `
            <h2 class="card-title">Root folders</h2>
            ${roots.length === 0 ? '<p class="text-sm">No link folders found yet. Run a check.</p>' : `
            <div class="overflow-x-auto"><table class="table table-sm">
                <thead><tr><th>Root</th><th class="text-right">Total</th><th class="text-right">Live</th>
                <th class="text-right">Target path gone</th><th class="text-right">Entry gone</th>
                <th class="text-right">External</th></tr></thead>
                <tbody>${roots.map((r) => `<tr>
                    <td class="font-mono break-all">${esc(r.root)} ${r.unused ? '<span class="badge badge-sm badge-ghost">unused</span>' : ''}</td>
                    <td class="text-right">${r.total}</td>
                    <td class="text-right text-success">${r.live}</td>
                    <td class="text-right ${r.target_gone ? 'text-warning' : ''}">${r.target_gone}</td>
                    <td class="text-right ${r.entry_gone ? 'text-error' : ''}">${r.entry_gone}</td>
                    <td class="text-right">${r.external}</td></tr>`).join('')}
                </tbody></table></div>`}`;

        renderTrend(st.trend || []);
        renderUnused(roots);
        renderRecreated(st.recreated || []);
        renderRootFilter(roots);
    }

    function renderTrend(points) {
        const el = document.getElementById('links-trend');
        let body;
        if (points.length < 2) {
            body = '<p class="text-sm">The trend line appears after two checks.</p>';
        } else {
            const W = 600, H = 120, P = 8;
            const max = Math.max(1, ...points.map((p) => Math.max(p.live, p.dead)));
            const x = (i) => P + (i * (W - 2 * P)) / (points.length - 1);
            const y = (v) => H - P - (v * (H - 2 * P)) / max;
            const line = (key) => points.map((p, i) => `${i ? 'L' : 'M'}${x(i).toFixed(1)},${y(p[key]).toFixed(1)}`).join(' ');
            const first = points[0], last = points[points.length - 1];
            body = `
            <svg viewBox="0 0 ${W} ${H}" class="w-full" role="img" aria-label="Live and dead links over time">
                <path d="${line('live')}" fill="none" stroke="currentColor" stroke-width="2" class="text-success"/>
                <path d="${line('dead')}" fill="none" stroke="currentColor" stroke-width="2" class="text-error"/>
            </svg>
            <div class="flex justify-between text-xs">
                <span>${esc(fmtTime(first.at))}</span>
                <span><span class="text-success">Live ${last.live}</span> / <span class="text-error">Dead ${last.dead}</span></span>
                <span>${esc(fmtTime(last.at))}</span>
            </div>`;
        }
        el.innerHTML = `<h2 class="card-title">Trend</h2>${body}`;
    }

    function renderUnused(roots) {
        // A root nothing uses, or one holding only dead links.
        const rows = roots.filter((r) => r.unused || (r.total > 0 && r.live === 0 && r.external === 0));
        document.getElementById('links-unused').innerHTML = `
            <h2 class="card-title">Unused root folders</h2>
            ${rows.length === 0 ? '<p class="text-sm">Every root folder is in use.</p>' : `
            <ul class="space-y-2">${rows.map((r) => `<li class="flex items-center gap-2 flex-wrap">
                <span class="font-mono break-all">${esc(r.root)}</span>
                <span class="badge badge-sm ${r.unused ? 'badge-ghost' : 'badge-warning'}">${r.unused ? 'no series or movie uses it' : 'only dead links'}</span>
                <span class="text-sm">${r.total} link${r.total === 1 ? '' : 's'}</span></li>`).join('')}</ul>`}`;
    }

    function renderRecreated(items) {
        document.getElementById('links-recreated-card').classList.toggle('hidden', items.length === 0);
        document.getElementById('links-recreated').innerHTML = `
            <h2 class="card-title">Re-created automatically</h2>
            <div class="overflow-x-auto"><table class="table table-sm"><thead><tr><th>Path</th><th>Snapshot</th><th>When</th></tr></thead>
            <tbody>${items.map((i) => `<tr><td class="font-mono break-all">${esc(i.path)}</td>
                <td class="font-mono">${esc(i.snapshot_id)}</td><td>${esc(fmtTime(i.at))}</td></tr>`).join('')}</tbody></table></div>`;
    }

    function renderMissing() {
        const rows = state.missing;
        document.getElementById('links-missing').innerHTML = `
            <h2 class="card-title">Missing from disk</h2>
            <p class="text-sm">Files Sonarr or Radarr list that have no link at their path.</p>
            ${rows.length === 0 ? '<p class="text-sm">Nothing is missing.</p>' : `
            <div class="overflow-x-auto"><table class="table table-sm">
                <thead><tr><th>Title</th><th>Path</th><th>Arr</th><th>Newest backup</th><th></th></tr></thead>
                <tbody>${rows.map((m, i) => `<tr>
                    <td>${esc(m.title)}</td>
                    <td class="font-mono break-all">${esc(m.path)}</td>
                    <td>${esc(m.arr)}</td>
                    <td class="font-mono">${m.snapshot_id ? esc(m.snapshot_id) : '<span class="text-sm">none</span>'}</td>
                    <td class="text-right">${m.restorable
                        ? `<button class="btn btn-xs btn-primary" data-restore="${i}">Restore</button>`
                        : '<span class="text-sm">not restorable</span>'}</td></tr>`).join('')}
                </tbody></table></div>`}`;
        document.querySelectorAll('[data-restore]').forEach((b) =>
            b.addEventListener('click', () => restoreMissing(state.missing[Number(b.dataset.restore)])));
    }

    function renderRootFilter(roots) {
        const sel = document.getElementById('links-filter-root');
        if (!sel) return;
        const cur = state.filters.root;
        sel.innerHTML = `<option value="">All roots</option>` +
            roots.map((r) => `<option value="${esc(r.root)}" ${r.root === cur ? 'selected' : ''}>${esc(r.root)}</option>`).join('');
    }

    function renderDeadShell() {
        const f = state.filters;
        document.getElementById('links-dead').innerHTML = `
            <div class="flex items-center justify-between flex-wrap gap-2">
                <h2 class="card-title">Dead links</h2>
                <button class="btn btn-sm btn-error" id="links-cleanup">Clean up</button>
            </div>
            <div class="flex flex-wrap gap-2">
                <select class="select select-sm" id="links-filter-root"></select>
                <select class="select select-sm" id="links-filter-state">
                    <option value="">Any dead state</option>
                    <option value="target_gone" ${f.state === 'target_gone' ? 'selected' : ''}>Target path gone</option>
                    <option value="entry_gone" ${f.state === 'entry_gone' ? 'selected' : ''}>Entry gone</option>
                </select>
                <input class="input input-sm" id="links-filter-q" placeholder="Series, movie or path" value="${esc(f.q)}">
            </div>
            <div id="links-dead-list"></div>`;
        renderRootFilter((state.status && state.status.roots) || []);
        document.getElementById('links-filter-root').addEventListener('change', (e) => {
            state.filters.root = e.target.value; loadDead(1);
        });
        document.getElementById('links-filter-state').addEventListener('change', (e) => {
            state.filters.state = e.target.value; loadDead(1);
        });
        document.getElementById('links-filter-q').addEventListener('input',
            window.decypharrUtils.debounce((e) => {
                state.filters.q = e.target.value.trim(); loadDead(1);
            }, 400));
        document.getElementById('links-cleanup').addEventListener('click', planCleanup);
    }

    function renderDeadList() {
        const d = state.dead;
        const pages = Math.max(1, Math.ceil(d.total / PAGE_SIZE));
        document.getElementById('links-dead-list').innerHTML = d.rows.length === 0
            ? '<p class="text-sm">No dead links match.</p>'
            : `<div class="overflow-x-auto"><table class="table table-sm">
                <thead><tr><th>Path</th><th>Points at</th><th>State</th></tr></thead>
                <tbody>${d.rows.map((r) => `<tr>
                    <td class="font-mono break-all">${esc(r.path)}</td>
                    <td class="font-mono break-all">${esc(r.target)}</td>
                    <td><span class="badge badge-sm ${STATE_BADGES[r.state] || 'badge-ghost'}">${esc(STATE_LABELS[r.state] || r.state)}</span></td>
                </tr>`).join('')}</tbody></table></div>
              <div class="flex items-center justify-between text-sm">
                <span>${d.total} dead link${d.total === 1 ? '' : 's'}</span>
                <div class="join">
                    <button class="btn btn-xs join-item" id="links-prev" ${d.page <= 1 ? 'disabled' : ''}>Previous</button>
                    <span class="btn btn-xs join-item">Page ${d.page} of ${pages}</span>
                    <button class="btn btn-xs join-item" id="links-next" ${d.page >= pages ? 'disabled' : ''}>Next</button>
                </div></div>`;
        const prev = document.getElementById('links-prev');
        const next = document.getElementById('links-next');
        if (prev) prev.addEventListener('click', () => loadDead(d.page - 1));
        if (next) next.addEventListener('click', () => loadDead(d.page + 1));
    }

    // ---- modal ----

    function openModal(html) {
        document.getElementById('links-modal').innerHTML = `
            <div class="modal modal-open"><div class="modal-box max-w-3xl">${html}</div></div>`;
    }

    function closeModal() {
        document.getElementById('links-modal').innerHTML = '';
    }

    // ---- data ----

    async function loadStatus() {
        try {
            state.status = await call('/links/status');
            showError('');
            renderStatus();
            schedulePoll();
        } catch (e) {
            showError(e.status === 501 ? 'Backups are not available in this build.' : e.message);
        }
    }

    function schedulePoll() {
        clearTimeout(state.pollTimer);
        if (state.status && state.status.running) {
            state.pollTimer = setTimeout(async () => {
                await loadStatus();
                if (!state.status.running) refreshLists();
            }, 3000);
        }
    }

    async function loadMissing() {
        try {
            state.missing = (await call('/links/missing')) || [];
        } catch (e) {
            state.missing = [];
            showError(e.message);
        }
        renderMissing();
    }

    async function loadDead(page) {
        const f = state.filters;
        const qs = new URLSearchParams({page: String(page), page_size: String(PAGE_SIZE)});
        if (f.root) qs.set('root', f.root);
        if (f.q) qs.set('q', f.q);
        // The list endpoint takes one state; without one, fetch both dead states.
        const states = f.state ? [f.state] : ['target_gone', 'entry_gone'];
        try {
            if (states.length === 1) {
                qs.set('state', states[0]);
                const res = await call(`/links?${qs}`);
                state.dead = {rows: res.rows || [], total: res.total || 0, page: res.page || page};
            } else {
                // Two states cannot be paged as one list, so page them in order.
                const counts = await Promise.all(states.map(async (s) => {
                    const q = new URLSearchParams(qs); q.set('state', s); q.set('page', '1'); q.set('page_size', '1');
                    return (await call(`/links?${q}`)).total || 0;
                }));
                const total = counts[0] + counts[1];
                const start = (page - 1) * PAGE_SIZE;
                let rows = [];
                for (let i = 0; i < 2; i++) {
                    const before = i === 0 ? 0 : counts[0];
                    const lo = Math.max(start, before), hi = Math.min(start + PAGE_SIZE, before + counts[i]);
                    if (hi <= lo) continue;
                    // Row offsets are not page-aligned across states, so fetch whole pages and slice.
                    const firstPage = Math.floor((lo - before) / PAGE_SIZE) + 1;
                    const lastPage = Math.floor((hi - 1 - before) / PAGE_SIZE) + 1;
                    for (let p = firstPage; p <= lastPage; p++) {
                        const q = new URLSearchParams(qs); q.set('state', states[i]); q.set('page', String(p));
                        const res = await call(`/links?${q}`);
                        const off = before + (p - 1) * PAGE_SIZE;
                        (res.rows || []).forEach((r, k) => {
                            const abs = off + k;
                            if (abs >= lo && abs < hi) rows.push(r);
                        });
                    }
                }
                state.dead = {rows, total, page};
            }
        } catch (e) {
            state.dead = {rows: [], total: 0, page: 1};
            showError(e.message);
        }
        renderDeadList();
    }

    function refreshLists() {
        loadMissing();
        loadDead(1);
    }

    // ---- actions ----

    async function checkNow() {
        const btn = document.getElementById('links-check-now');
        btn.disabled = true;
        try {
            state.status = await call('/links/check', {method: 'POST'});
            renderStatus();
            schedulePoll();
            if (!state.status.running) refreshLists();
        } catch (e) {
            toast(esc(e.message), 'error');
            btn.disabled = false;
        }
    }

    async function restoreMissing(m) {
        if (!m || !m.snapshot_id) return;
        let plan;
        try {
            plan = await post(`/backups/snapshots/${encodeURIComponent(m.snapshot_id)}/restore/links/plan`,
                {scope: {prefix: m.path}});
        } catch (e) {
            toast(esc(e.message), 'error');
            return;
        }
        const c = plan.counts || {};
        const create = c.create || 0;
        openModal(`
            <h3 class="font-bold text-lg">Restore link</h3>
            <p class="font-mono break-all text-sm">${esc(m.path)}</p>
            <p class="text-sm">Dry run against snapshot <span class="font-mono">${esc(m.snapshot_id)}</span>:
               ${create} to create, ${c.ok || 0} already in place, ${c.retarget || 0} to retarget,
               ${c.conflict || 0} conflict (never overwritten), ${c.unresolved || 0} unresolved.</p>
            <div class="overflow-x-auto"><table class="table table-sm"><tbody>
                ${(plan.rows || []).map((r) => `<tr><td>${esc(r.class)}</td><td class="font-mono break-all">${esc(r.path)}</td>
                    <td class="font-mono break-all">${esc(r.target)}</td></tr>`).join('')}
            </tbody></table></div>
            <label class="flex items-center gap-2 mt-2"><input type="checkbox" class="checkbox checkbox-sm" id="links-rescan" checked>
                <span class="text-sm">Ask Sonarr or Radarr to rescan afterwards</span></label>
            <div class="modal-action">
                <button class="btn btn-sm" id="links-modal-cancel">Cancel</button>
                <button class="btn btn-sm btn-primary" id="links-modal-apply" ${create === 0 ? 'disabled' : ''}>Restore ${create} link${create === 1 ? '' : 's'}</button>
            </div>`);
        document.getElementById('links-modal-cancel').addEventListener('click', closeModal);
        document.getElementById('links-modal-apply').addEventListener('click', async (e) => {
            e.target.disabled = true;
            try {
                const res = await post(`/backups/snapshots/${encodeURIComponent(m.snapshot_id)}/restore/links/apply`,
                    {plan_id: plan.plan_id, retarget: false, rescan: document.getElementById('links-rescan').checked});
                closeModal();
                const warned = (res.warnings || []).length;
                toast(`Created ${res.created}, skipped ${res.skipped}, failed ${res.failed}` +
                    (warned ? `; ${warned} warning${warned === 1 ? '' : 's'}: ${esc((res.warnings[0] || '').slice(0, 200))}` : ''),
                    (res.failed || warned) ? 'warning' : 'success');
                refreshLists();
                loadStatus();
            } catch (err) {
                toast(esc(err.message), 'error');
                e.target.disabled = false;
            }
        });
    }

    async function planCleanup() {
        const f = state.filters;
        const filter = {};
        if (f.root) filter.root = f.root;
        if (f.q) filter.q = f.q;
        filter.states = f.state ? [f.state] : ['target_gone', 'entry_gone'];
        let plan;
        try {
            plan = await post('/links/cleanup/plan', {filter});
        } catch (e) {
            toast(esc(e.message), 'error');
            return;
        }
        const groups = plan.groups || [];
        openModal(`
            <h3 class="font-bold text-lg">Clean up dead links</h3>
            <p class="text-sm">Dry run: ${plan.total} link${plan.total === 1 ? '' : 's'} would be removed. Tick each group to remove it.
               A backup is taken first, so the removal can be undone.</p>
            ${groups.length === 0 ? '<p class="text-sm">Nothing to clean up.</p>' : `
            <div class="space-y-2">${groups.map((g, i) => `<div>
                <label class="flex items-center gap-2"><input type="checkbox" class="checkbox checkbox-sm" data-group="${esc(g.key)}">
                    <span class="font-mono break-all">${esc(g.root)}</span>
                    <span class="badge badge-sm ${STATE_BADGES[g.state] || 'badge-ghost'}">${esc(STATE_LABELS[g.state] || g.state)}</span>
                    <span class="text-sm">${g.count} link${g.count === 1 ? '' : 's'}</span></label>
                ${(g.paths || []).length ? `<details class="ml-4"><summary class="text-sm">Show paths</summary>
                    <ul class="font-mono text-xs break-all">${g.paths.map((p) => `<li>${esc(p)}</li>`).join('')}</ul></details>` : ''}
                </div>`).join('')}</div>`}
            <div class="modal-action">
                <button class="btn btn-sm" id="links-modal-cancel">Cancel</button>
                <button class="btn btn-sm btn-error" id="links-modal-apply" disabled>Remove ticked groups</button>
            </div>`);
        const applyBtn = document.getElementById('links-modal-apply');
        const ticked = () => Array.from(document.querySelectorAll('[data-group]:checked')).map((c) => c.dataset.group);
        document.querySelectorAll('[data-group]').forEach((c) =>
            c.addEventListener('change', () => { applyBtn.disabled = ticked().length === 0; }));
        document.getElementById('links-modal-cancel').addEventListener('click', closeModal);
        applyBtn.addEventListener('click', async () => {
            applyBtn.disabled = true;
            try {
                const res = await post('/links/cleanup/apply', {plan_id: plan.plan_id, groups: ticked()});
                closeModal();
                if (!res.removed && !res.failed) {
                    toast('Nothing to remove (already gone)', 'info');
                } else {
                    toast(`Removed ${res.removed}, failed ${res.failed}`, res.failed ? 'warning' : 'success');
                }
                refreshLists();
                loadStatus();
            } catch (err) {
                toast(esc(err.message), 'error');
                applyBtn.disabled = false;
            }
        });
    }

    document.addEventListener('DOMContentLoaded', async () => {
        if (!root()) return;
        skeleton();
        renderDeadShell();
        await loadStatus();
        refreshLists();
    });
})();
