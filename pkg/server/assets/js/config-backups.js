// Settings -> Backups tab: destination, jobs, retention preview, throttling,
// notifications, pre-action snapshots, and the settings history.
//
// config.js calls window.configBackups.populate(config.backup) when the page
// loads and adds window.configBackups.collect() to the saved config. Nothing
// here saves on its own except revert, which has its own endpoint.
(function () {
    'use strict';

    const COMPONENTS = [
        ['db', 'Database'], ['config', 'Settings'], ['state', 'State'], ['nzb', 'NZB records'],
        ['overlay', 'Overlay'], ['links', 'Links'], ['hearsay', 'Hearsay'],
    ];

    const u = () => window.decypharrUtils;
    const esc = (s) => u().escapeHtml(String(s ?? ''));
    const toast = (m, t) => u().createToast(m, t || 'success');
    const bytes = (n) => u().formatBytes(n || 0);
    const when = (t) => (t ? new Date(t).toLocaleString() : '');

    async function api(path, opts) {
        const res = await u().fetcher(path, opts);
        let body = null;
        try { body = await res.json(); } catch (_) { /* no body */ }
        if (!res.ok) {
            throw new Error((body && body.error) || `Request failed (${res.status})`);
        }
        return body;
    }

    const post = (path, data) => api(path, {method: 'POST', body: JSON.stringify(data || {})});

    const cb = {
        cfg: null,          // the backup settings as loaded; unknown keys survive a save
        loaded: false,
        status: null,
        roots: [],
        view: 'settings',
        versions: null,
        loadedRetention: {}, // job name -> "last/daily/weekly/monthly" at load time
        retentionConfirmed: false,
        timers: {},

        el(id) { return document.getElementById(id); },
        root() { return this.el('backups-settings'); },

        populate(backup) {
            this.cfg = JSON.parse(JSON.stringify(backup || {}));
            this.cfg.jobs = this.cfg.jobs || [];
            this.cfg.extra_link_folders = this.cfg.extra_link_folders || [];
            this.loadedRetention = {};
            this.cfg.jobs.forEach(j => { this.loadedRetention[j.name] = this.retKey(j.retention); });
            this.loaded = true;
            this.retentionConfirmed = false;
            this.render();
            this.refreshStatus();
        },

        retKey(r) {
            r = r || {};
            return [r.last || 0, r.daily || 0, r.weekly || 0, r.monthly || 0].join('/');
        },

        async refreshStatus() {
            try { this.status = await api('/api/backups/status'); } catch (_) { this.status = null; }
            try {
                const l = await api('/api/links/status');
                this.roots = (l.roots || []).map(r => r.root);
            } catch (_) { this.roots = []; }
            this.renderDestination();
            this.renderHold();
            this.renderRoots();
            this.renderAutoRecreate();
        },

        // ---- rendering ----

        render() {
            const root = this.root();
            if (!root) return;
            root.innerHTML = `
            <div class="space-y-6">
                <div class="flex gap-2 border-b border-base-300">
                    <button type="button" class="btn btn-sm btn-ghost" data-bk-view="settings">Settings</button>
                    <button type="button" class="btn btn-sm btn-ghost" data-bk-view="history">History</button>
                </div>
                <div id="bk-hold"></div>
                <div id="bk-view-settings" class="space-y-6">${this.settingsHTML()}</div>
                <div id="bk-view-history" class="hidden space-y-4"></div>
            </div>`;
            this.fill();
            this.renderJobs();
            this.bind();
            this.showView(this.view);
        },

        settingsHTML() {
            return `
            <div class="space-y-3">
                <h3 class="text-lg font-semibold">Destination</h3>
                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                        <label class="label" for="bk-dir"><span class="font-medium">Backup folder</span></label>
                        <input type="text" class="input w-full" id="bk-dir" placeholder="/path/to/backups">
                        <span class="text-sm opacity-70">Backups stay off until a folder is set. It must be on a different disk from the Decypharr data folder.</span>
                    </div>
                    <div>
                        <label class="label" for="bk-rclone"><span class="font-medium">Rclone remote (optional)</span></label>
                        <input type="text" class="input w-full" id="bk-rclone" placeholder="remote:path">
                    </div>
                </div>
                <div id="bk-destination"></div>
            </div>

            <div class="space-y-3">
                <div class="flex items-center justify-between">
                    <h3 class="text-lg font-semibold">Jobs</h3>
                    <button type="button" class="btn btn-sm btn-outline" id="bk-add-job"><i class="bi bi-plus"></i> Add job</button>
                </div>
                <div id="bk-jobs" class="space-y-4"></div>
            </div>

            <div class="space-y-3">
                <h3 class="text-lg font-semibold">Symlink folders</h3>
                <div>
                    <span class="font-medium text-sm">Arr root folders (read-only)</span>
                    <div id="bk-roots" class="text-sm opacity-70 font-mono"></div>
                </div>
                <div>
                    <label class="label" for="bk-extra"><span class="font-medium">Extra folders to walk for symlinks</span></label>
                    <textarea class="textarea w-full font-mono text-sm" id="bk-extra" rows="3" placeholder="One folder per line"></textarea>
                </div>
            </div>

            <div class="space-y-3">
                <h3 class="text-lg font-semibold">Load</h3>
                <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div>
                        <label class="label" for="bk-throttle"><span class="font-medium">I/O limit (MB/s, 0 = none)</span></label>
                        <input type="number" min="0" class="input w-full" id="bk-throttle">
                    </div>
                    <div>
                        <label class="label" for="bk-quiet-start"><span class="font-medium">Quiet hours start</span></label>
                        <input type="time" class="input w-full" id="bk-quiet-start">
                    </div>
                    <div>
                        <label class="label" for="bk-quiet-end"><span class="font-medium">Quiet hours end</span></label>
                        <input type="time" class="input w-full" id="bk-quiet-end">
                    </div>
                </div>
                <span class="text-sm opacity-70">Scheduled jobs are held back inside the quiet window. Leave both empty for none.</span>
                <label class="label cursor-pointer justify-start gap-3">
                    <input type="checkbox" class="checkbox checkbox-primary checkbox-sm" id="bk-pause-streaming">
                    <span class="label-text font-medium">Pause while something is streaming</span>
                </label>
                <span class="text-sm opacity-70">On: scheduled and manual backups wait while anyone plays something through Decypharr, so playback keeps the disks to itself. Off: backups carry on during playback, using some disk and CPU while people watch. Changing this also applies to a backup that is already running.</span>
            </div>

            <div class="space-y-3">
                <h3 class="text-lg font-semibold">Notifications</h3>
                <label class="label cursor-pointer justify-start gap-3">
                    <input type="checkbox" class="checkbox checkbox-primary checkbox-sm" id="bk-notify-failure">
                    <span class="label-text font-medium">Notify when a backup fails</span>
                </label>
                <div class="w-full max-w-md">
                    <label class="label" for="bk-stale"><span class="font-medium">Notify when no backup has succeeded for (hours, 0 = off)</span></label>
                    <input type="number" min="0" class="input w-full" id="bk-stale">
                </div>
            </div>

            <div class="space-y-3">
                <h3 class="text-lg font-semibold">Snapshot before risky actions</h3>
                <label class="label cursor-pointer justify-start gap-3">
                    <input type="checkbox" class="checkbox checkbox-primary checkbox-sm" id="bk-pre-enabled">
                    <span class="label-text font-medium">Take a quick snapshot before bulk deletes, upgrades and similar actions</span>
                </label>
                <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div>
                        <label class="label" for="bk-pre-keep"><span class="font-medium">Keep at most</span></label>
                        <input type="number" min="1" class="input w-full" id="bk-pre-keep">
                    </div>
                    <div>
                        <label class="label" for="bk-pre-age"><span class="font-medium">Delete after (days)</span></label>
                        <input type="number" min="1" class="input w-full" id="bk-pre-age">
                    </div>
                    <div>
                        <label class="label" for="bk-pre-refuse"><span class="font-medium">Refuse the action if the snapshot fails</span></label>
                        <input type="text" class="input w-full" id="bk-pre-refuse" placeholder="action names, comma separated">
                    </div>
                </div>
            </div>

            <div class="space-y-3 hidden" id="bk-auto-wrap">
                <h3 class="text-lg font-semibold">Automatic link re-creation</h3>
                <label class="label cursor-pointer justify-start gap-3">
                    <input type="checkbox" class="checkbox checkbox-primary checkbox-sm" id="bk-auto-enabled">
                    <span class="label-text font-medium">Re-create links that have gone missing when a link check finds them</span>
                </label>
                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                        <label class="label" for="bk-auto-limit"><span class="font-medium">Most links one check re-creates</span></label>
                        <input type="number" min="1" class="input w-full" id="bk-auto-limit">
                        <span class="text-sm opacity-70">Over this number nothing is re-created.</span>
                    </div>
                    <label class="label cursor-pointer justify-start gap-3">
                        <input type="checkbox" class="checkbox checkbox-primary checkbox-sm" id="bk-auto-rescan">
                        <span class="label-text font-medium">Ask the Arr to rescan afterwards</span>
                    </label>
                </div>
            </div>`;
        },

        fill() {
            const c = this.cfg;
            const set = (id, v) => { const e = this.el(id); if (e) e.value = v ?? ''; };
            const chk = (id, v) => { const e = this.el(id); if (e) e.checked = !!v; };
            set('bk-dir', c.dir);
            set('bk-rclone', c.rclone_remote);
            set('bk-extra', (c.extra_link_folders || []).join('\n'));
            set('bk-throttle', c.throttle_mbps || 0);
            set('bk-quiet-start', (c.quiet_hours || {}).start);
            set('bk-quiet-end', (c.quiet_hours || {}).end);
            chk('bk-pause-streaming', c.pause_while_streaming);
            chk('bk-notify-failure', (c.notify || {}).on_failure);
            set('bk-stale', (c.notify || {}).stale_hours || 0);
            const p = c.pre_action || {};
            chk('bk-pre-enabled', p.enabled);
            set('bk-pre-keep', p.keep);
            set('bk-pre-age', p.max_age_days);
            set('bk-pre-refuse', (p.refuse || []).join(', '));
            const a = c.auto_recreate || {};
            chk('bk-auto-enabled', a.enabled);
            set('bk-auto-limit', a.limit);
            chk('bk-auto-rescan', a.rescan);
        },

        renderDestination() {
            const box = this.el('bk-destination');
            if (!box) return;
            const s = this.status;
            if (!s || !s.configured) {
                box.innerHTML = s ? '<div class="text-sm opacity-70">Not set up yet.</div>' : '';
                return;
            }
            let html = `<div class="text-sm">Free space: <b>${esc(bytes(s.free_bytes))}</b> &middot; repository: <b>${esc(bytes(s.repo_bytes))}</b> &middot; ${esc(s.snapshots)} snapshots</div>`;
            if (s.same_disk) {
                html += `<div class="alert alert-warning mt-2"><i class="bi bi-exclamation-triangle"></i>
                    <span class="text-sm">The backup folder is on the same disk as the Decypharr data folder. A disk failure would take both.</span></div>`;
            }
            box.innerHTML = html;
        },

        renderHold() {
            const box = this.el('bk-hold');
            if (!box) return;
            const s = this.status || {};
            let html = '';
            if (s.post_restore_hold) {
                html += `<div class="alert alert-warning"><i class="bi bi-pause-circle"></i>
                    <span class="text-sm flex-1">A restore has just been applied and scheduled backups are on hold, so they do not overwrite the older copies.</span>
                    <button type="button" class="btn btn-sm" id="bk-release-hold">Release hold</button></div>`;
            }
            if (s.pending_db_restore) {
                html += `<div class="alert alert-info"><i class="bi bi-info-circle"></i>
                    <span class="text-sm">A database restore is staged and is applied at the next start.</span></div>`;
            }
            box.innerHTML = html;
            const btn = this.el('bk-release-hold');
            if (btn) btn.addEventListener('click', async () => {
                try {
                    await post('/api/backups/hold/release');
                    toast('Hold released');
                    this.refreshStatus();
                } catch (e) { toast(esc(e.message), 'error'); }
            });
        },

        renderRoots() {
            const box = this.el('bk-roots');
            if (!box) return;
            box.innerHTML = this.roots.length
                ? this.roots.map(r => `<div>${esc(r)}</div>`).join('')
                : '<div>None found yet.</div>';
        },

        renderAutoRecreate() {
            const wrap = this.el('bk-auto-wrap');
            if (!wrap) return;
            // Hidden until a manual link restore has been applied. The status
            // reply has no such field yet, so also show it once it is enabled.
            const s = this.status || {};
            const seen = s.link_restore_applied || s.links_restored || (this.cfg.auto_recreate || {}).enabled;
            wrap.classList.toggle('hidden', !seen);
        },

        renderJobs() {
            const box = this.el('bk-jobs');
            if (!box) return;
            box.innerHTML = this.cfg.jobs.map((j, i) => this.jobHTML(j, i)).join('') ||
                '<div class="text-sm opacity-70">No jobs. Add one to back up on a schedule.</div>';
            this.cfg.jobs.forEach((j, i) => this.checkSchedule(i));
        },

        jobHTML(j, i) {
            const r = j.retention || {};
            const comps = COMPONENTS.map(([k, label]) => `
                <label class="label cursor-pointer justify-start gap-2">
                    <input type="checkbox" class="checkbox checkbox-sm" data-bk-comp="${k}" ${(j.components || []).includes(k) ? 'checked' : ''}>
                    <span class="label-text text-sm">${label}</span>
                </label>`).join('');
            const ret = (key, label) => `
                <div>
                    <label class="label"><span class="font-medium text-sm">${label}</span></label>
                    <input type="number" min="0" class="input input-sm w-full" data-bk-ret="${key}" value="${esc(r[key] || 0)}">
                </div>`;
            return `
            <div class="p-4 rounded-lg bg-base-200 space-y-3" data-bk-job="${i}">
                <div class="flex items-center justify-between gap-3 flex-wrap">
                    <div class="flex items-center gap-3 flex-wrap">
                        <input type="text" class="input input-sm font-semibold" data-bk-name value="${esc(j.name)}" aria-label="Job name">
                        <label class="label cursor-pointer gap-2">
                            <input type="checkbox" class="toggle toggle-primary" data-bk-enabled ${j.enabled ? 'checked' : ''}>
                            <span class="label-text text-sm">Enabled</span>
                        </label>
                    </div>
                    <button type="button" class="btn btn-xs btn-error btn-outline" data-bk-remove="${i}">Remove</button>
                </div>
                <div class="flex flex-wrap gap-x-4">${comps}</div>
                <div>
                    <label class="label"><span class="font-medium text-sm">Schedule (clock time such as 04:00, or a cron expression)</span></label>
                    <input type="text" class="input input-sm w-full max-w-md font-mono" data-bk-schedule value="${esc(j.schedule)}">
                    <div class="text-sm mt-1" data-bk-next></div>
                </div>
                <div>
                    <span class="font-medium text-sm">Keep</span>
                    <div class="grid grid-cols-2 md:grid-cols-4 gap-3">
                        ${ret('last', 'Last')}${ret('daily', 'Daily')}${ret('weekly', 'Weekly')}${ret('monthly', 'Monthly')}
                    </div>
                    <div class="flex items-center gap-3 mt-2">
                        <button type="button" class="btn btn-xs btn-outline" data-bk-preview="${i}">Preview what retention would delete</button>
                    </div>
                    <div class="text-sm mt-2" data-bk-preview-out></div>
                </div>
            </div>`;
        },

        // ---- events ----

        bind() {
            const root = this.root();
            if (this.rootBound) return this.bindSubmit();
            this.rootBound = true;
            root.addEventListener('click', (e) => {
                const t = e.target.closest('button');
                if (!t) return;
                if (t.dataset.bkView) this.showView(t.dataset.bkView);
                else if (t.id === 'bk-add-job') this.addJob();
                else if (t.dataset.bkRemove !== undefined) this.removeJob(+t.dataset.bkRemove);
                else if (t.dataset.bkPreview !== undefined) this.preview(+t.dataset.bkPreview);
                else if (t.dataset.bkDiff) this.showDiff(t.dataset.bkDiff);
                else if (t.dataset.bkRevert) this.revert(t.dataset.bkRevert);
            });
            root.addEventListener('input', (e) => {
                const card = e.target.closest('[data-bk-job]');
                if (!card) return;
                const i = +card.dataset.bkJob;
                if (e.target.matches('[data-bk-schedule]')) {
                    clearTimeout(this.timers[i]);
                    this.timers[i] = setTimeout(() => this.checkSchedule(i), 400);
                } else if (e.target.matches('[data-bk-ret]')) {
                    this.retentionConfirmed = false;
                    const out = card.querySelector('[data-bk-preview-out]');
                    if (out) out.textContent = '';
                }
            });
            this.bindSubmit();
        },

        bindSubmit() {
            if (!this.submitBound) {
                const form = document.getElementById('configForm');
                if (form) form.addEventListener('submit', (e) => this.onSubmit(e), true);
                this.submitBound = true;
            }
        },

        showView(v) {
            this.view = v;
            this.el('bk-view-settings')?.classList.toggle('hidden', v !== 'settings');
            this.el('bk-view-history')?.classList.toggle('hidden', v !== 'history');
            this.root()?.querySelectorAll('[data-bk-view]').forEach(b => {
                b.classList.toggle('btn-active', b.dataset.bkView === v);
            });
            if (v === 'history') this.loadHistory();
        },

        syncJobs() {
            const cards = this.root().querySelectorAll('[data-bk-job]');
            cards.forEach((card, i) => {
                const j = this.cfg.jobs[i];
                if (!j) return;
                j.name = card.querySelector('[data-bk-name]').value.trim();
                j.enabled = card.querySelector('[data-bk-enabled]').checked;
                j.schedule = card.querySelector('[data-bk-schedule]').value.trim();
                j.components = [...card.querySelectorAll('[data-bk-comp]')].filter(x => x.checked).map(x => x.dataset.bkComp);
                const ret = {};
                card.querySelectorAll('[data-bk-ret]').forEach(x => { ret[x.dataset.bkRet] = parseInt(x.value, 10) || 0; });
                j.retention = Object.assign({}, j.retention, ret);
            });
        },

        addJob() {
            this.syncJobs();
            let n = this.cfg.jobs.length + 1;
            const names = this.cfg.jobs.map(j => j.name);
            while (names.includes('job-' + n)) n++;
            this.cfg.jobs.push({
                name: 'job-' + n, enabled: true, components: ['db', 'config'], schedule: '04:00',
                retention: {last: 3, daily: 7, weekly: 4, monthly: 3},
            });
            this.renderJobs();
        },

        removeJob(i) {
            this.syncJobs();
            const j = this.cfg.jobs[i];
            if (!j || !confirm(`Remove the job "${j.name}"? Snapshots it already made are kept until retention removes them.`)) return;
            this.cfg.jobs.splice(i, 1);
            this.renderJobs();
        },

        async checkSchedule(i) {
            const card = this.root()?.querySelector(`[data-bk-job="${i}"]`);
            if (!card) return;
            const out = card.querySelector('[data-bk-next]');
            const sched = card.querySelector('[data-bk-schedule]').value.trim();
            if (!sched) { out.innerHTML = '<span class="text-error">Enter a schedule.</span>'; return; }
            try {
                const r = await post('/api/backups/schedule/check', {schedule: sched});
                if (r.valid) {
                    out.innerHTML = '<span class="opacity-70">Next runs: ' + (r.next || []).map(t => esc(when(t))).join(' &middot; ') + '</span>';
                } else {
                    out.innerHTML = `<span class="text-error">${esc(r.error || 'Not a valid schedule')}</span>`;
                }
            } catch (e) {
                out.innerHTML = `<span class="text-error">${esc(e.message)}</span>`;
            }
        },

        policyOf(card) {
            const p = {};
            card.querySelectorAll('[data-bk-ret]').forEach(x => { p[x.dataset.bkRet] = parseInt(x.value, 10) || 0; });
            return p;
        },

        async previewData(i) {
            const card = this.root().querySelector(`[data-bk-job="${i}"]`);
            const name = card.querySelector('[data-bk-name]').value.trim();
            const plan = await post('/api/backups/retention/preview', {
                policy: Object.assign({trigger_prefix: 'schedule:' + name}, this.policyOf(card)),
            });
            return {card, plan};
        },

        summarise(plan) {
            const del = plan.delete_snapshots || [];
            if (!del.length) return `Nothing would be deleted (${plan.keep_snapshots || 0} kept).`;
            const times = del.map(s => new Date(s.created_at).getTime()).sort((a, b) => a - b);
            const from = new Date(times[0]).toLocaleDateString();
            const to = new Date(times[times.length - 1]).toLocaleDateString();
            return `${del.length} snapshot${del.length === 1 ? '' : 's'} would be deleted, from ${from} to ${to}, freeing ${bytes(plan.freed_bytes)}. ${plan.keep_snapshots || 0} kept.`;
        },

        async preview(i) {
            this.syncJobs();
            try {
                const {card, plan} = await this.previewData(i);
                const out = card.querySelector('[data-bk-preview-out]');
                const del = (plan.delete_snapshots || []).length;
                out.innerHTML = `<span class="${del ? 'text-warning' : 'opacity-70'}">${esc(this.summarise(plan))}</span>`;
            } catch (e) { toast(esc(e.message), 'error'); }
        },

        // The main Save button submits #configForm. When a job's retention
        // has been tightened, ask first if the new rules would delete
        // anything; then submit again.
        onSubmit(e) {
            if (!this.loaded || this.retentionConfirmed) return;
            this.syncJobs();
            const changed = [];
            this.cfg.jobs.forEach((j, i) => {
                if (this.loadedRetention[j.name] !== undefined && this.loadedRetention[j.name] !== this.retKey(j.retention)) changed.push(i);
            });
            if (!changed.length) return;
            e.preventDefault();
            e.stopImmediatePropagation();
            (async () => {
                const lines = [];
                try {
                    for (const i of changed) {
                        const {plan} = await this.previewData(i);
                        if ((plan.delete_snapshots || []).length) lines.push(`${this.cfg.jobs[i].name}: ${this.summarise(plan)}`);
                    }
                } catch (err) {
                    // The preview is advisory: if it fails, save anyway.
                }
                if (lines.length && !confirm('Saving these retention settings deletes backups:\n\n' + lines.join('\n') + '\n\nSave anyway?')) return;
                this.retentionConfirmed = true;
                document.getElementById('configForm').requestSubmit();
            })();
        },

        // ---- save ----

        collect() {
            if (!this.loaded || !this.root() || !this.el('bk-dir')) return undefined;
            this.syncJobs();
            const num = (id, d) => { const v = parseInt(this.el(id).value, 10); return Number.isFinite(v) ? v : d; };
            const c = JSON.parse(JSON.stringify(this.cfg));
            c.dir = this.el('bk-dir').value.trim();
            c.rclone_remote = this.el('bk-rclone').value.trim();
            c.extra_link_folders = this.el('bk-extra').value.split('\n').map(s => s.trim()).filter(Boolean);
            c.throttle_mbps = num('bk-throttle', 0);
            c.quiet_hours = {start: this.el('bk-quiet-start').value, end: this.el('bk-quiet-end').value};
            c.pause_while_streaming = this.el('bk-pause-streaming').checked;
            c.notify = Object.assign({}, c.notify, {
                on_failure: this.el('bk-notify-failure').checked,
                stale_hours: num('bk-stale', 0),
            });
            c.pre_action = Object.assign({}, c.pre_action, {
                enabled: this.el('bk-pre-enabled').checked,
                keep: num('bk-pre-keep', 0),
                max_age_days: num('bk-pre-age', 0),
                refuse: this.el('bk-pre-refuse').value.split(',').map(s => s.trim()).filter(Boolean),
            });
            c.auto_recreate = Object.assign({}, c.auto_recreate, {
                enabled: this.el('bk-auto-enabled').checked,
                limit: num('bk-auto-limit', 0),
                rescan: this.el('bk-auto-rescan').checked,
            });
            return c;
        },

        // ---- history ----

        async loadHistory() {
            const box = this.el('bk-view-history');
            box.innerHTML = '<div class="text-sm opacity-70">Loading...</div>';
            try {
                this.versions = await api('/api/backups/config-history');
            } catch (e) {
                box.innerHTML = `<div class="alert alert-error text-sm">${esc(e.message)}</div>`;
                return;
            }
            const v = this.versions || [];
            if (!v.length) {
                box.innerHTML = '<div class="text-sm opacity-70">No saved versions yet. Every settings save adds one.</div>';
                return;
            }
            box.innerHTML = `
                <p class="text-sm opacity-70">Every settings save is kept. Secrets are never shown in a comparison, only whether they changed. Reverting saves the old version as a new one.</p>
                <div class="overflow-x-auto"><table class="table table-xs w-full">
                    <thead><tr><th>Saved</th><th>From</th><th>Compare with</th><th></th></tr></thead>
                    <tbody>${v.map((x, i) => `
                        <tr>
                            <td>${esc(when(x.time))}${i === 0 ? ' <span class="badge badge-sm badge-info">newest</span>' : ''}</td>
                            <td class="font-mono">${esc(x.source)}</td>
                            <td><select class="select select-sm" data-bk-against="${esc(x.id)}">
                                <option value="previous">previous version</option>
                                <option value="current">current settings</option>
                            </select></td>
                            <td class="flex gap-2">
                                <button type="button" class="btn btn-xs btn-outline" data-bk-diff="${esc(x.id)}">Diff</button>
                                ${i === 0 ? '' : `<button type="button" class="btn btn-xs btn-warning btn-outline" data-bk-revert="${esc(x.id)}">Revert</button>`}
                            </td>
                        </tr>`).join('')}</tbody>
                </table></div>
                <div id="bk-diff"></div>`;
        },

        async showDiff(id) {
            const sel = [...this.root().querySelectorAll('[data-bk-against]')].find(s => s.dataset.bkAgainst === id);
            const against = sel ? sel.value : 'previous';
            const out = this.el('bk-diff');
            out.innerHTML = '<div class="text-sm opacity-70">Loading...</div>';
            try {
                const d = await api(`/api/backups/config-history/${encodeURIComponent(id)}/diff?against=${encodeURIComponent(against)}`);
                const lines = (d.lines || []).filter(l => l.changed);
                if (!lines.length) {
                    out.innerHTML = `<div class="text-sm opacity-70">No differences against the ${esc(against)} version.</div>`;
                    return;
                }
                out.innerHTML = `<div class="overflow-x-auto max-h-96 overflow-y-auto"><table class="table table-xs w-full">
                    <thead><tr><th>Setting</th><th>${esc(against)}</th><th>This version</th></tr></thead>
                    <tbody>${lines.map(l => l.secret
                        ? `<tr><td class="font-mono">${esc(l.path)}</td><td colspan="2" class="opacity-70">secret, changed</td></tr>`
                        : `<tr><td class="font-mono">${esc(l.path)}</td><td class="font-mono break-all text-error">${esc(l.old)}</td><td class="font-mono break-all text-success">${esc(l.new)}</td></tr>`
                    ).join('')}</tbody></table></div>`;
            } catch (e) {
                out.innerHTML = `<div class="alert alert-error text-sm">${esc(e.message)}</div>`;
            }
        },

        async revert(id) {
            const v = (this.versions || []).find(x => x.id === id);
            if (!confirm(`Revert settings to the version saved ${v ? when(v.time) : id}?\n\nThe current settings are kept in the history as well. Settings that need a restart restart Decypharr.`)) return;
            try {
                const r = await post(`/api/backups/config-history/${encodeURIComponent(id)}/revert`);
                if (r.restart_required) {
                    toast('Settings reverted. Decypharr is restarting...');
                    setTimeout(() => window.location.reload(), 3000);
                } else {
                    toast('Settings reverted.');
                    window.location.reload();
                }
            } catch (e) { toast(esc(e.message), 'error'); }
        },
    };

    window.configBackups = cb;
})();
