// Repair v2 — health checker dashboard.
//
// Settings live in the global Settings page; this controller only handles
// status, run/stop, and history. Polls /api/repair/status while a run is
// active so the UI reflects live progress.
const PRECACHE_GIB = 1024 * 1024 * 1024;
// PlexConfig.SessionCacheTTL is a Go time.Duration - it round-trips through
// JSON as plain nanoseconds (no custom marshaller), while the "Session cache
// TTL" input is in seconds for a human to read.
const NS_PER_SECOND = 1e9;
const PLEX_TOKEN_PLACEHOLDER = '********';
// The only unverified reason Replace acts on, and how many entries one Replace
// re-grabs (manager.reasonTailTruncated, defaultReplaceUnverifiedLimit).
const REASON_TAIL_TRUNCATED = 'import_tail_truncated';
const REPLACE_UNVERIFIED_BATCH = 25;
const UNVERIFIED_REASON_LABELS = {
    import_tail_truncated: 'Tail truncated',
    no_seek_index: 'No seek index',
    decode_timeout: 'Timed out',
    read_budget_spent: 'Read budget spent',
    decoded_with_errors: 'Decoded with errors',
    decode_inconclusive: 'Inconclusive',
};

class RepairManager {
    constructor() {
        this.api = (window.API || '/api').replace(/\/$/, '');
        this.statusTimer = null;
        this.activeRunId = null;
        this.brokenState = {items: [], page: 1, pageSize: 25};
        // reason filters the list ('all' or one reason); runId, when set, keeps
        // only entries last recorded by that run (opened from Run History).
        this.unverifiedState = {items: [], page: 1, pageSize: 25, reason: 'all', runId: '', runLabel: ''};
        this.repairConfig = {};
        this.repairConfigDefaults = {};
        this.precacheConfig = {};
        this.plexConfig = {};
        // Stale Plex version reaper: last /plex/reap/status snapshot, the
        // backlog rows ticked for removal (by rating key), and the list filter.
        this.plexReap = {status: null, selected: new Set(), filter: 'reapable', timer: null};
        this.latestStatus = {};
        this.overlayFiles = [];
        this.overlaySelected = new Set();
        this.overlayDiskUsage = {};
        this.overlayHistory = [];
        this.overlayHistoryTotalCount = 0;
        // Server has no pagination on this endpoint yet (see loadOverlayHistory) -
        // this caps how many rows we ever hold/render client-side.
        this.overlayHistoryCap = 500;
        this.overlayOrphanCount = 0;
        this.overlayFilesSearch = '';
        this.overlayFilesVerdictFilter = 'all';
        this.overlayFilesRepairFilter = 'all';
        this.overlayHistorySearch = '';
        this.overlayHistoryOutcomeFilter = 'all';
        this.precacheReadiness = [];
        this.precachePaused = false;
        this.precacheSearch = '';
        this.precacheSort = 'ready_at';
        // Per-row live-progress poll timers, keyed by overlayKey() - see
        // pollOverlayProgress. Cleared and rebuilt on every renderOverlayFiles
        // pass so a stale row never keeps polling after the table's rebuilt.
        this.overlayProgressTimers = new Map();
        this.bind();
        this.loadAll();
    }

    bind() {
        const $ = (id) => document.getElementById(id);
        $('runNowBtn')?.addEventListener('click', () => this.openRunModal());
        $('stopRunBtn')?.addEventListener('click', () => this.stopRun());
        this.bindOverlayConfirmButton(
            $('fixBrokenBtn'),
            () => Array.from({length: this.latestStatus?.health_counts?.broken || 0}),
            'delete & re-search',
            () => this.fixBroken(),
        );
        $('clearStateBtn')?.addEventListener('click', () => this.openClearStateModal());
        $('viewBrokenBtn')?.addEventListener('click', () => this.openBrokenModal());
        $('viewUnverifiedBtn')?.addEventListener('click', () => this.openUnverifiedModal());
        $('refreshUnverifiedBtn')?.addEventListener('click', () => this.loadUnverified());
        $('unverifiedReasonFilter')?.addEventListener('change', (e) => {
            this.unverifiedState.reason = e.target.value;
            this.unverifiedState.page = 1;
            this.renderUnverifiedPage();
        });
        $('clearUnverifiedRunFilterBtn')?.addEventListener('click', () => {
            this.unverifiedState.runId = '';
            this.unverifiedState.page = 1;
            this.renderUnverified();
        });
        this.bindOverlayConfirmButton(
            $('replaceUnverifiedBtn'),
            () => this.replaceUnverifiedBatch(),
            'replace',
            (names) => this.replaceUnverified(names),
        );
        $('refreshHistoryBtn')?.addEventListener('click', () => this.loadHistory());
        $('refreshBrokenBtn')?.addEventListener('click', () => this.loadBroken());
        $('clearSupersededBtn')?.addEventListener('click', () => this.clearSuperseded());
        $('clearHistoryBtn')?.addEventListener('click', () => this.clearHistory());
        $('runRepairForm')?.addEventListener('submit', (e) => {
            e.preventDefault();
            this.runNow();
        });
        $('clearStateForm')?.addEventListener('submit', (e) => {
            e.preventDefault();
            this.clearState();
        });
        $('recheckMediaForm')?.addEventListener('submit', (e) => {
            e.preventDefault();
            this.recheckMedia();
        });

        // Overlay / Padding
        $('overlayRefreshBtn')?.addEventListener('click', () => this.loadOverlayAll());
        $('overlayFilesRefreshBtn')?.addEventListener('click', () => this.loadOverlayFiles());
        $('overlayRefreshHistoryBtn')?.addEventListener('click', () => this.loadOverlayHistory());
        $('overlayClearHistoryBtn')?.addEventListener('click', () => this.clearOverlayHistory());
        $('overlayConfigForm')?.addEventListener('submit', (e) => {
            e.preventDefault();
            this.saveOverlayConfig();
        });
        $('precacheConfigForm')?.addEventListener('submit', (e) => {
            e.preventDefault();
            this.savePrecacheConfig();
        });
        $('plexConfigForm')?.addEventListener('submit', (e) => {
            e.preventDefault();
            this.savePlexConfig();
        });
        $('plexTestBtn')?.addEventListener('click', () => this.testPlexConnection());
        $('plexReapRefreshBtn')?.addEventListener('click', () => this.loadPlexReap());
        $('plexReapScanBtn')?.addEventListener('click', () => this.startPlexReapScan());
        $('plexReapApplyBtn')?.addEventListener('click', () => this.applyPlexReap());
        $('plexReapFilter')?.addEventListener('change', (e) => {
            this.plexReap.filter = e.target.value;
            this.plexReap.selected.clear();
            this.renderPlexReap();
        });
        $('plexReapSelectAll')?.addEventListener('change', (e) => this.togglePlexReapSelectAll(e.target.checked));
        $('plexReapTableBody')?.addEventListener('change', (e) => {
            const key = e.target?.dataset?.ratingKey;
            if (!key) return;
            if (e.target.checked) this.plexReap.selected.add(key);
            else this.plexReap.selected.delete(key);
            this.updatePlexReapApplyBtn();
        });
        $('overlaySelectAllCheckbox')?.addEventListener('change', (e) => this.toggleOverlaySelectAll(e.target.checked));
        $('overlayClearSelectionBtn')?.addEventListener('click', () => this.clearOverlaySelection());
        $('overlayGCOrphansBtn')?.addEventListener('click', () => this.handleOverlayGCOrphans());
        this.bindDebouncedInput($('overlayFilesSearchInput'), (v) => {
            this.overlayFilesSearch = v.trim().toLowerCase();
            this.renderOverlayFiles();
        });
        $('overlayFilesVerdictFilter')?.addEventListener('change', (e) => {
            this.overlayFilesVerdictFilter = e.target.value;
            this.renderOverlayFiles();
        });
        $('overlayFilesRepairFilter')?.addEventListener('change', (e) => {
            this.overlayFilesRepairFilter = e.target.value;
            this.renderOverlayFiles();
        });
        this.bindDebouncedInput($('overlayHistorySearchInput'), (v) => {
            this.overlayHistorySearch = v.trim().toLowerCase();
            this.renderOverlayHistory();
        });
        $('overlayHistoryOutcomeFilter')?.addEventListener('change', (e) => {
            this.overlayHistoryOutcomeFilter = e.target.value;
            this.renderOverlayHistory();
        });
        this.bindDebouncedInput($('precacheSearchInput'), (v) => {
            this.precacheSearch = v.trim().toLowerCase();
            this.renderPrecacheReadiness();
        });
        $('precacheSortSelect')?.addEventListener('change', (e) => {
            this.precacheSort = e.target.value;
            this.renderPrecacheReadiness();
        });
        $('precacheRefreshBtn')?.addEventListener('click', () => this.loadPrecacheStatus(true));
        $('precachePauseBtn')?.addEventListener('click', () => this.handleTogglePrecachePaused());
        $('precachePurgeIncompleteBtn')?.addEventListener('click', () => this.handlePurgeIncompletePrecache());
        this.bindOverlayConfirmButton(
            $('overlayBulkDeleteResearchBtn'),
            () => this.overlaySelectedFiles(),
            'delete & re-search',
            (items) => this.runOverlayBulkDeleteResearch(items),
        );
        this.bindOverlayConfirmButton(
            $('overlayBulkResearchBtn'),
            () => this.overlaySelectedFiles().filter((f) => f.verdict === 'failed'),
            'research',
            (items) => this.runOverlayBulkResearch(items),
        );
        this.bindOverlayConfirmButton(
            $('overlayBulkReclaimBtn'),
            () => this.overlaySelectedFiles().filter((f) => f.verdict === 'clean'),
            'reclaim',
            (items) => this.runOverlayBulkReclaim(items),
        );
    }

    async loadAll() {
        await Promise.all([this.loadRepairConfig(), this.loadPrecacheConfig(), this.loadPlexConfig(), this.loadStatus(), this.loadHistory(), this.loadArrs(), this.loadOverlayAll(), this.loadPrecacheStatus(), this.loadPlexReap()]);
        this.populateOverlayConfigForm();
        this.populatePrecacheConfigForm();
        this.populatePlexConfigForm();
    }

    async loadRepairConfig() {
        try {
            const data = await this.fetchJSON(`${this.api}/repair/config`) || {};
            this.repairConfig = data.repair || {};
            this.repairConfigDefaults = data.defaults || {};
        } catch (e) {
            console.error('Failed to load repair config', e);
            this.repairConfig = {};
            this.repairConfigDefaults = {};
        }
    }

    async loadPrecacheConfig() {
        try {
            this.precacheConfig = await this.fetchJSON(`${this.api}/precache/config`) || {};
        } catch (e) {
            console.error('Failed to load precache config', e);
            this.precacheConfig = {};
        }
    }

    async loadPlexConfig() {
        try {
            this.plexConfig = await this.fetchJSON(`${this.api}/plex/config`) || {};
        } catch (e) {
            console.error('Failed to load plex config', e);
            this.plexConfig = {};
        }
    }

    openRunModal() {
        const modal = document.getElementById('runRepairModal');
        if (!modal) return;
        const ignore = document.getElementById('runIgnoreLastChecked');
        const autoRepair = document.getElementById('runAutoRepair');
        const unrestrictLink = document.getElementById('runUnrestrictLink');
        if (ignore) ignore.checked = false;
        if (autoRepair) autoRepair.checked = !!this.repairConfig.auto_repair;
        if (unrestrictLink) unrestrictLink.checked = false;
        const defaultProtocol = this.repairConfig.skip_nzb_repair ? 'torrent' : 'all';
        const protocol = document.querySelector(`input[name="runProtocol"][value="${defaultProtocol}"]`)
            || document.getElementById('runProtocolAll');
        if (protocol) protocol.checked = true;
        if (typeof modal.showModal === 'function') {
            modal.showModal();
        } else {
            modal.setAttribute('open', '');
        }
    }

    openClearStateModal() {
        const modal = document.getElementById('clearStateModal');
        if (!modal) return;
        document.getElementById('clearStateError')?.classList.add('hidden');
        document.querySelectorAll('input[name="repair_state"]').forEach((input) => {
            input.checked = false;
        });
        this.updateClearStateCounts(this.latestStatus || {});
        if (typeof modal.showModal === 'function') {
            modal.showModal();
        } else {
            modal.setAttribute('open', '');
        }
    }

    openBrokenModal() {
        const modal = document.getElementById('brokenModal');
        if (!modal) return;
        // Fetch fresh data on every open.
        this.loadBroken();
        if (typeof modal.showModal === 'function') {
            modal.showModal();
        } else {
            modal.setAttribute('open', '');
        }
    }

    isBrokenModalOpen() {
        const modal = document.getElementById('brokenModal');
        return !!(modal && modal.open);
    }

    updateBrokenCount(n) {
        const badge = document.getElementById('brokenCountBadge');
        if (badge) {
            badge.textContent = n;
            badge.classList.toggle('hidden', n === 0);
        }
        const modalCount = document.getElementById('brokenModalCount');
        if (modalCount) modalCount.textContent = n;
    }

    async loadArrs() {
        try {
            const arrs = await this.fetchJSON(`${this.api}/arrs`);
            const sel = document.getElementById('recheckArr');
            if (!sel) return;
            const placeholder = sel.querySelector('option[value=""]');
            sel.innerHTML = '';
            if (placeholder) sel.appendChild(placeholder);
            for (const a of arrs || []) {
                if (!a || !a.name) continue;
                const opt = document.createElement('option');
                opt.value = a.name;
                opt.textContent = a.name;
                sel.appendChild(opt);
            }
        } catch (e) {
            console.error('Failed to load arrs', e);
        }
    }

    async recheckMedia() {
        const $ = (id) => document.getElementById(id);
        const mediaId = $('recheckMediaId').value.trim();
        if (!mediaId) {
            this.toast('Media id is required', 'warning');
            return;
        }
        const body = {
            arr: $('recheckArr').value,
            media_id: mediaId,
            fix: $('recheckFix').checked,
            force_decode: $('recheckForceDecode').checked,
        };
        const btn = $('recheckMediaBtn');
        const out = $('recheckMediaResult');
        btn.disabled = true;
        out.classList.add('hidden');
        out.textContent = '';
        try {
            const res = await fetch(`${this.api}/repair/recheck/media`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(body),
            });
            const text = await res.text();
            let data = null;
            try {
                data = text ? JSON.parse(text) : null;
            } catch { /* leave null */
            }
            if (!res.ok) {
                const msg = (data && (data.error || data.message)) || text || `HTTP ${res.status}`;
                throw new Error(msg);
            }
            // Server kicks the recheck off in the background and returns a
            // run record immediately. Reload so the dashboard reflects the
            // new active run; status polling takes it from there.
            this.toast('Recheck started', 'success');
            window.location.reload();
        } catch (e) {
            out.classList.remove('hidden');
            out.innerHTML = `<span class="text-error">Recheck failed: ${this.escape(e.message)}</span>`;
            btn.disabled = false;
        }
    }

    renderRecheckResult(container, run) {
        if (!container) return;
        if (!run) {
            container.classList.add('hidden');
            return;
        }
        container.classList.remove('hidden');
        const stats = run.stats || {};
        const status = run.status || 'unknown';
        const cls = {
            running: 'badge-info',
            completed: 'badge-success',
            failed: 'badge-error',
            cancelled: 'badge-warning',
        }[status] || 'badge-ghost';
        container.innerHTML = `
            <div class="flex flex-wrap gap-3 items-center">
                <span class="badge ${cls}">${this.escape(status)}</span>
                <span class="font-mono text-xs">${this.escape(run.id || '')}</span>
                ${run.source ? `<span class="opacity-70 text-xs">${this.escape(run.source)}</span>` : ''}
            </div>
            <div class="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-6 gap-2 mt-3 text-xs">
                <div>Candidates: <strong>${stats.candidates ?? 0}</strong></div>
                <div>Probed: <strong>${stats.probed ?? 0}</strong></div>
                <div class="${stats.broken ? 'text-error' : ''}">Broken: <strong>${stats.broken ?? 0}</strong></div>
                <div class="${stats.healthy ? 'text-success' : ''}">Healthy: <strong>${stats.healthy ?? 0}</strong></div>
                <div class="${stats.repaired ? 'text-success' : ''}">Repaired: <strong>${stats.repaired ?? 0}</strong></div>
                <div class="${stats.repair_failed ? 'text-error' : ''}">Repair fail: <strong>${stats.repair_failed ?? 0}</strong></div>
                <div class="${stats.decode_skipped ? 'text-warning' : ''}" title="Entries only shallow-checked because their decode fingerprint still matched. Re-run with 'Force decode verification' to deep-check these.">Decode skipped: <strong>${stats.decode_skipped ?? 0}</strong></div>
            </div>
            ${run.error ? `<div class="mt-2 text-error text-xs">${this.escape(run.error)}</div>` : ''}
        `;
    }

    escape(s) {
        const div = document.createElement('div');
        div.textContent = s == null ? '' : String(s);
        return div.innerHTML;
    }

    async runNow() {
        const btn = document.getElementById('runRepairSubmitBtn');
        if (btn) btn.disabled = true;
        try {
            const ignoreLastChecked = !!document.getElementById('runIgnoreLastChecked')?.checked;
            const autoRepair = !!document.getElementById('runAutoRepair')?.checked;
            const unrestrictLink = !!document.getElementById('runUnrestrictLink')?.checked;
            const protocol = document.querySelector('input[name="runProtocol"]:checked')?.value || 'all';
            const res = await fetch(`${this.api}/repair/run`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    ignore_last_checked: ignoreLastChecked,
                    auto_repair: autoRepair,
                    unrestrict_link: unrestrictLink,
                    protocol,
                }),
            });
            if (!res.ok) {
                const txt = await res.text();
                throw new Error(txt || `HTTP ${res.status}`);
            }
            document.getElementById('runRepairModal')?.close?.();
            this.toast(ignoreLastChecked ? 'Sweep started, including freshly checked entries' : 'Sweep started', 'success');
            await this.loadStatus();
        } catch (e) {
            this.toast(`Run failed: ${e.message}`, 'error');
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    async clearSuperseded() {
        const btn = document.getElementById('clearSupersededBtn');
        if (btn) btn.disabled = true;
        try {
            const res = await fetch(`${this.api}/repair/clear-superseded`, {method: 'POST'});
            const text = await res.text();
            let data = null;
            try {
                data = text ? JSON.parse(text) : null;
            } catch { /* leave null */
            }
            if (!res.ok) throw new Error((data && (data.error || data.message)) || text || `HTTP ${res.status}`);
            const cleared = data?.cleared_entries ?? 0;
            const stillBroken = data?.still_broken ?? 0;
            this.toast(`Cleared ${cleared} replaced entr${cleared === 1 ? 'y' : 'ies'}; ${stillBroken} still broken`, 'success');
            await Promise.all([this.loadStatus(), this.loadBroken()]);
        } catch (e) {
            this.toast(`Clear replaced failed: ${e.message}`, 'error');
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    // fixBroken sends delete + re-search for every broken entry to its Arr.
    // The click is confirmed inline (bindOverlayConfirmButton), not with
    // window.confirm(), which a browser or extension can suppress silently.
    // The page is not reloaded, so the toast stays readable; the status poll
    // picks up the run and refreshes history when it ends.
    async fixBroken() {
        const btn = document.getElementById('fixBrokenBtn');
        if (btn) btn.disabled = true;
        try {
            const res = await fetch(`${this.api}/repair/fix`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({}),
            });
            const text = await res.text();
            if (!res.ok) throw new Error(text.trim() || `HTTP ${res.status}`);
            this.toast('Fix started: deleting and re-searching broken entries through their Arr', 'success');
            await Promise.all([this.loadStatus(), this.loadHistory()]);
        } catch (e) {
            this.toast(`Fix failed: ${e.message}`, 'error');
            if (btn) btn.disabled = false;
        }
    }

    async clearState() {
        const selected = [...document.querySelectorAll('input[name="repair_state"]:checked')]
            .map((input) => input.value);
        const clearDecode = document.getElementById('clearDecodeVerification')?.checked;
        const error = document.getElementById('clearStateError');
        if (!selected.length && !clearDecode) {
            if (error) {
                error.textContent = 'Select at least one state.';
                error.classList.remove('hidden');
            }
            return;
        }

        const btn = document.getElementById('clearStateSubmitBtn');
        if (btn) btn.disabled = true;
        try {
            if (selected.length) {
                const res = await fetch(`${this.api}/repair/clear-state`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({statuses: selected}),
                });
                const text = await res.text();
                let data = null;
                try {
                    data = text ? JSON.parse(text) : null;
                } catch { /* leave null */
                }
                if (!res.ok) throw new Error((data && (data.error || data.message)) || text || `HTTP ${res.status}`);
                if (data?.cleared > 0) {
                    this.toast(`Cleared ${data.cleared} repair state entr${data.cleared === 1 ? 'y' : 'ies'}`, 'success');
                }
            }
            if (clearDecode) {
                try {
                    const dres = await fetch(`${this.api}/repair/clear-decode-verification`, {method: 'POST'});
                    const dtxt = await dres.text();
                    let ddata = null;
                    try {
                        ddata = dtxt ? JSON.parse(dtxt) : null;
                    } catch { /* leave null */
                    }
                    if (!dres.ok) throw new Error((ddata && (ddata.error || ddata.message)) || dtxt || `HTTP ${dres.status}`);
                    this.toast(`Cleared decode markers on ${ddata?.cleared ?? 0} entr${ddata?.cleared === 1 ? 'y' : 'ies'}`, 'success');
                } catch (de) {
                    this.toast(`Decode clear failed: ${de.message}`, 'error');
                }
            }
            document.getElementById('clearStateModal')?.close?.();
            await Promise.all([this.loadStatus(), this.loadBroken()]);
        } catch (e) {
            this.toast(`Clear failed: ${e.message}`, 'error');
            if (error) {
                error.textContent = e.message;
                error.classList.remove('hidden');
            }
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    async stopRun() {
        try {
            const res = await fetch(`${this.api}/repair/stop`, {method: 'POST'});
            if (!res.ok) {
                const txt = await res.text();
                throw new Error(txt || `HTTP ${res.status}`);
            }
            this.toast('Stop requested', 'info');
            await this.loadStatus();
        } catch (e) {
            this.toast(`Stop failed: ${e.message}`, 'error');
        }
    }

    async clearHistory() {
        if (!confirm('Clear all run history?')) return;
        try {
            const res = await fetch(`${this.api}/repair/runs`, {method: 'DELETE'});
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            await this.loadHistory();
        } catch (e) {
            this.toast(`Clear failed: ${e.message}`, 'error');
        }
    }

    async loadStatus() {
        try {
            const status = await this.fetchJSON(`${this.api}/repair/status`);
            this.renderStatus(status || {});
            this.scheduleStatusPoll(status);
        } catch (e) {
            console.error('Failed to load status', e);
        }
    }

    scheduleStatusPoll(status) {
        const isRunning = !!(status && status.active_run);
        const wasRunning = this.wasRunning === true;
        this.wasRunning = isRunning;
        // Run ended → refresh history. Only refetch the broken modal contents
        // when it's actually open; the count badge is already updated from
        // status.health_counts on every poll.
        if (wasRunning && !isRunning) {
            this.loadHistory();
            if (this.isBrokenModalOpen()) this.loadBroken();
            if (this.isUnverifiedModalOpen()) this.loadUnverified();
        }
        if (this.statusTimer) {
            clearTimeout(this.statusTimer);
            this.statusTimer = null;
        }
        const delay = isRunning ? 2000 : 15000;
        this.statusTimer = setTimeout(() => this.loadStatus(), delay);
    }

    renderStatus(status) {
        this.latestStatus = status || {};
        const line = document.getElementById('repairStatusLine');
        const stop = document.getElementById('stopRunBtn');
        const run = document.getElementById('runNowBtn');
        const panel = document.getElementById('activeRunPanel');
        const grid = document.getElementById('healthCountsGrid');

        if (!status.enabled) {
            line.textContent = 'Repair is disabled. Enable it in Settings → Repair, or click "Run now" for a one-off check.';
        } else {
            const next = status.next_run_at ? new Date(status.next_run_at).toLocaleString() : 'unknown';
            line.textContent = `Repair enabled · next scheduled run: ${next}`;
        }

        const brokenCount = (status.health_counts || {}).broken || 0;
        this.updateBrokenCount(brokenCount);
        const fix = document.getElementById('fixBrokenBtn');
        if (fix) fix.disabled = !!status.active_run || brokenCount === 0;
        const clear = document.getElementById('clearStateBtn');
        if (clear) clear.disabled = !!status.active_run;
        const view = document.getElementById('viewBrokenBtn');
        if (view) view.disabled = brokenCount === 0;
        const unverifiedCount = status.unverified_count || 0;
        this.updateUnverifiedCount(unverifiedCount);
        const viewUnverified = document.getElementById('viewUnverifiedBtn');
        if (viewUnverified) viewUnverified.disabled = unverifiedCount === 0;
        this.updateReplaceUnverifiedButton();
        this.updateClearStateCounts(status || {});

        if (status.active_run) {
            stop.disabled = false;
            run.disabled = true;
            panel.classList.remove('hidden');
            this.activeRunId = status.active_run.id;
            document.getElementById('activeRunStage').textContent = status.active_run.stage || 'running';
            document.getElementById('activeRunIdText').textContent = status.active_run.id || '-';
            document.getElementById('activeRunStarted').textContent = status.active_run.started_at
                ? new Date(status.active_run.started_at).toLocaleString()
                : '-';
            this.renderRunStats(document.getElementById('activeRunStats'), status.active_run.stats || {});
        } else {
            stop.disabled = true;
            run.disabled = false;
            panel.classList.add('hidden');
            this.activeRunId = null;
            document.getElementById('activeRunStage').textContent = '-';
            document.getElementById('activeRunIdText').textContent = '-';
            document.getElementById('activeRunStarted').textContent = '-';
        }

        const counts = status.health_counts || {};
        const order = ['healthy', 'broken', 'repairing', 'stale', 'unknown', 'unsupported'];
        grid.innerHTML = '';
        for (const key of order) {
            const n = counts[key] || 0;
            const card = document.createElement('div');
            card.className = 'stat bg-base-200 rounded-box p-3';
            card.innerHTML = `
                <div class="stat-title text-xs capitalize">${key}</div>
                <div class="stat-value text-lg ${this.healthColor(key)}">${n}</div>
            `;
            grid.appendChild(card);
        }
        // Unverified entries are healthy, so they are not in health_counts.
        const tile = document.createElement('div');
        tile.className = 'stat bg-base-200 rounded-box p-3 cursor-pointer hover:bg-base-300 transition-colors';
        tile.setAttribute('role', 'button');
        tile.tabIndex = 0;
        tile.title = 'Healthy entries with a file the last check could not verify';
        tile.innerHTML = `
            <div class="stat-title text-xs">Unverified</div>
            <div class="stat-value text-lg ${unverifiedCount ? 'text-warning' : ''}">${unverifiedCount}</div>
        `;
        tile.addEventListener('click', () => this.openUnverifiedModal());
        tile.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                this.openUnverifiedModal();
            }
        });
        grid.appendChild(tile);
    }

    updateClearStateCounts(counts) {
        document.querySelectorAll('[data-clear-state-count]').forEach((el) => {
            const status = el.getAttribute('data-clear-state-count');
            el.textContent = counts?.health_counts?.[status] || 0;
        });
        // Decode-verified count lives outside health_counts
        const dvEl = document.querySelector('[data-clear-state-count="decode_verified"]');
        if (dvEl) dvEl.textContent = counts.decode_verified_count ?? 0;
        // Of those, markers stamped from a bounded head scan: the file has no
        // usable seek index, so only its first few GB were decoded.
        const partialEl = document.querySelector('[data-clear-state-count="decode_verified_partial"]');
        if (partialEl) {
            const partial = counts.decode_verified_partial_count ?? 0;
            partialEl.textContent = `${partial} partial`;
            partialEl.classList.toggle('hidden', partial === 0);
        }
    }

    renderRunStats(container, stats) {
        if (!container) return;
        const fields = [
            ['candidates', 'Candidates'],
            ['skipped_fresh', 'Skipped'],
            ['probed', 'Probed'],
            ['healthy', 'Healthy'],
            ['broken', 'Broken'],
            ['repaired', 'Repaired'],
            ['cleared', 'Cleared'],
            ['repair_failed', 'Repair fail'],
        ];
        container.innerHTML = '';
        for (const [k, label] of fields) {
            const el = document.createElement('div');
            el.className = 'bg-base-100 rounded p-2';
            el.innerHTML = `<div class="text-[10px] opacity-60 uppercase">${label}</div><div class="font-mono">${stats[k] || 0}</div>`;
            container.appendChild(el);
        }
    }

    healthColor(status) {
        switch (status) {
            case 'healthy':
                return 'text-success';
            case 'broken':
                return 'text-error';
            case 'repairing':
                return 'text-info';
            case 'stale':
                return 'text-warning';
            case 'unsupported':
                return 'text-base-content/60';
            default:
                return '';
        }
    }

    async loadHistory() {
        try {
            const runs = await this.fetchJSON(`${this.api}/repair/runs`);
            this.renderHistory(runs || []);
        } catch (e) {
            console.error('Failed to load history', e);
        }
    }

    async loadBroken() {
        try {
            const list = await this.fetchJSON(`${this.api}/repair/health?status=broken`);
            this.renderBroken(list || []);
        } catch (e) {
            console.error('Failed to load broken entries', e);
        }
    }

    renderBroken(entries) {
        this.updateBrokenCount(entries.length);

        // Sort: most recently failed first, then by name.
        entries.sort((a, b) => {
            const ta = a.last_failed_at ? new Date(a.last_failed_at).getTime() : 0;
            const tb = b.last_failed_at ? new Date(b.last_failed_at).getTime() : 0;
            if (ta !== tb) return tb - ta;
            return (a.entry_name || '').localeCompare(b.entry_name || '');
        });

        this.brokenState.items = entries;
        // Clamp current page so a shrinking list doesn't strand the user on an empty page.
        const totalPages = Math.max(1, Math.ceil(entries.length / this.brokenState.pageSize));
        if (this.brokenState.page > totalPages) this.brokenState.page = totalPages;
        if (this.brokenState.page < 1) this.brokenState.page = 1;
        this.renderBrokenPage();
    }

    renderBrokenPage() {
        const tbody = document.getElementById('brokenTableBody');
        const empty = document.getElementById('noBrokenMessage');
        if (!tbody) return;
        tbody.innerHTML = '';

        const {items, page, pageSize} = this.brokenState;
        if (!items.length) {
            empty?.classList.remove('hidden');
            this.renderBrokenPagination();
            return;
        }
        empty?.classList.add('hidden');

        const start = (page - 1) * pageSize;
        const slice = items.slice(start, start + pageSize);

        for (const h of slice) {
            const rowId = `broken-row-${this.slug(h.entry_name)}`;
            const fileCount = h.file_count ?? 0;
            const brokenCount = h.broken_count ?? (h.broken_files?.length ?? 0);
            const lastChecked = h.last_checked_at ? new Date(h.last_checked_at).toLocaleString() : '-';
            const lastRepair = h.last_repair_at ? new Date(h.last_repair_at).toLocaleString() : '-';
            const reason = h.failure_reason || '-';

            const tr = document.createElement('tr');
            tr.className = 'cursor-pointer hover:bg-base-200';
            tr.innerHTML = `
                <td class="w-8">
                    <i class="bi bi-chevron-right transition-transform" id="${rowId}-caret"></i>
                </td>
                <td class="font-mono text-sm break-all">${this.escape(h.entry_name)}</td>
                <td><span class="badge badge-ghost badge-sm">${this.escape(h.protocol || 'unknown')}</span></td>
                <td>${fileCount}</td>
                <td class="text-error font-medium">${brokenCount}</td>
                <td class="text-xs">${this.escape(reason)}</td>
                <td class="text-xs">${lastChecked}</td>
                <td class="text-xs">${lastRepair}</td>
                <td class="text-right whitespace-nowrap">
                    <button class="btn btn-xs btn-outline" data-action="recheck" data-name="${this.escapeAttr(h.entry_name)}" aria-label="Recheck ${this.escape(h.entry_name)}">
                        <i class="bi bi-search-heart"></i>
                    </button>
                    <button class="btn btn-xs btn-error btn-outline" data-action="fix" data-name="${this.escapeAttr(h.entry_name)}" aria-label="Fix ${this.escape(h.entry_name)}">
                        <i class="bi bi-bandaid"></i>
                    </button>
                </td>
            `;
            tbody.appendChild(tr);

            const detail = document.createElement('tr');
            detail.id = rowId;
            detail.className = 'hidden';
            detail.innerHTML = `
                <td colspan="9" class="bg-base-200/40 p-0">
                    <div class="p-4 space-y-2">
                        ${this.renderBrokenFiles(h.broken_files || [])}
                    </div>
                </td>
            `;
            tbody.appendChild(detail);

            tr.addEventListener('click', (ev) => {
                if (ev.target.closest('[data-action]')) return;
                const hidden = detail.classList.toggle('hidden');
                const caret = document.getElementById(`${rowId}-caret`);
                if (caret) caret.style.transform = hidden ? '' : 'rotate(90deg)';
            });
            tr.querySelector('[data-action="recheck"]')?.addEventListener('click', (ev) => {
                ev.stopPropagation();
                this.recheckOne(h.entry_name);
            });
            this.bindOverlayConfirmButton(
                tr.querySelector('[data-action="fix"]'),
                () => [h.entry_name],
                'delete & re-search',
                () => this.fixOne(h.entry_name),
            );
        }
        this.renderBrokenPagination();
    }

    renderBrokenPagination() {
        const bar = document.getElementById('brokenPaginationBar');
        const info = document.getElementById('brokenPaginationInfo');
        const controls = document.getElementById('brokenPaginationControls');
        if (!bar || !info || !controls) return;

        const {items, page, pageSize} = this.brokenState;
        const total = items.length;
        if (total === 0) {
            bar.classList.add('hidden');
            return;
        }
        bar.classList.remove('hidden');

        const totalPages = Math.max(1, Math.ceil(total / pageSize));
        const start = (page - 1) * pageSize + 1;
        const end = Math.min(start + pageSize - 1, total);
        info.textContent = `Showing ${start}-${end} of ${total}`;

        if (totalPages <= 1) {
            controls.innerHTML = '';
            return;
        }

        let html = `<button class="join-item btn btn-sm ${page === 1 ? 'btn-disabled' : ''}"
                            onclick="window.repairManager.goToBrokenPage(${page - 1})">«</button>`;
        for (let i = 1; i <= totalPages; i++) {
            if (i === 1 || i === totalPages || (i >= page - 2 && i <= page + 2)) {
                html += `<button class="join-item btn btn-sm ${i === page ? 'btn-active' : ''}"
                                onclick="window.repairManager.goToBrokenPage(${i})">${i}</button>`;
            } else if (i === page - 3 || i === page + 3) {
                html += `<button class="join-item btn btn-sm btn-disabled">…</button>`;
            }
        }
        html += `<button class="join-item btn btn-sm ${page === totalPages ? 'btn-disabled' : ''}"
                         onclick="window.repairManager.goToBrokenPage(${page + 1})">»</button>`;
        controls.innerHTML = html;
    }

    goToBrokenPage(p) {
        const totalPages = Math.max(1, Math.ceil(this.brokenState.items.length / this.brokenState.pageSize));
        if (p < 1 || p > totalPages || p === this.brokenState.page) return;
        this.brokenState.page = p;
        this.renderBrokenPage();
    }

    renderBrokenFiles(files) {
        if (!files.length) {
            return `<div class="text-sm opacity-60">No broken file details.</div>`;
        }
        const rows = files.map(f => {
            const arr = f.arr_name ? `${this.escape(f.arr_name)}${f.arr_kind ? ` (${this.escape(f.arr_kind)})` : ''}` : '<span class="opacity-50">—</span>';
            const ids = [];
            if (f.media_id) ids.push(`media:${f.media_id}`);
            if (f.episode_id) ids.push(`ep:${f.episode_id}`);
            if (f.arr_file_id) ids.push(`file:${f.arr_file_id}`);
            const idStr = ids.length ? `<span class="font-mono text-[10px] opacity-70">${ids.join(' · ')}</span>` : '';
            const size = f.size ? this.formatBytes(f.size) : '-';
            return `
                <tr>
                    <td class="font-mono text-xs break-all">${this.escape(f.file_name || '')}</td>
                    <td class="text-xs">${this.escape(f.reason || '-')}</td>
                    <td class="text-xs">${size}</td>
                    <td class="text-xs">${arr}</td>
                    <td>${idStr}</td>
                </tr>
            `;
        }).join('');
        return `
            <div class="overflow-x-auto">
                <table class="table table-xs">
                    <thead><tr><th>File</th><th>Reason</th><th>Size</th><th>Arr</th><th>Ids</th></tr></thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        `;
    }

    // ---- Unverified entries ------------------------------------------------

    // openUnverifiedModal shows the Unverified list, optionally narrowed to the
    // entries last recorded by one run ({runId, runLabel}).
    openUnverifiedModal({runId = '', runLabel = ''} = {}) {
        const modal = document.getElementById('unverifiedModal');
        if (!modal) return;
        Object.assign(this.unverifiedState, {runId, runLabel, page: 1});
        this.loadUnverified();
        if (typeof modal.showModal === 'function') {
            if (!modal.open) modal.showModal();
        } else {
            modal.setAttribute('open', '');
        }
    }

    isUnverifiedModalOpen() {
        const modal = document.getElementById('unverifiedModal');
        return !!(modal && modal.open);
    }

    updateUnverifiedCount(n) {
        const badge = document.getElementById('unverifiedCountBadge');
        if (badge) {
            badge.textContent = n;
            badge.classList.toggle('hidden', n === 0);
        }
    }

    async loadUnverified() {
        try {
            const list = await this.fetchJSON(`${this.api}/repair/unverified`);
            this.unverifiedState.items = (list || []).sort((a, b) => (a.entry_name || '').localeCompare(b.entry_name || ''));
            this.updateUnverifiedCount(this.unverifiedState.items.length);
            this.renderUnverified();
        } catch (e) {
            console.error('Failed to load unverified entries', e);
        }
    }

    unverifiedReasonLabel(reason) {
        return UNVERIFIED_REASON_LABELS[reason] || reason || '-';
    }

    // unverifiedInRun is the list before the reason filter: every entry, or
    // only those the selected run recorded.
    unverifiedInRun() {
        const {items, runId} = this.unverifiedState;
        return runId ? items.filter((h) => h.unverified_run_id === runId) : items;
    }

    filteredUnverified() {
        const {reason} = this.unverifiedState;
        const items = this.unverifiedInRun();
        if (reason === 'all') return items;
        return items.filter((h) => (h.unverified_files || []).some((f) => f.reason === reason));
    }

    // sortReasons puts the reason Replace acts on first, then the rest by
    // count (most first) when counts are given.
    sortReasons(reasons, counts = {}) {
        return reasons.sort((a, b) => {
            if (a === REASON_TAIL_TRUNCATED) return -1;
            if (b === REASON_TAIL_TRUNCATED) return 1;
            return (counts[b] || 0) - (counts[a] || 0);
        });
    }

    // renderUnverified rebuilds the reason filter (with a count per reason, so
    // a small Replace-eligible count reads as such) and the run filter bar,
    // then the current page.
    renderUnverified() {
        const st = this.unverifiedState;
        const items = this.unverifiedInRun();
        const counts = {};
        for (const h of items) {
            for (const r of new Set((h.unverified_files || []).map((f) => f.reason))) {
                counts[r] = (counts[r] || 0) + 1;
            }
        }
        const select = document.getElementById('unverifiedReasonFilter');
        if (select) {
            if (st.reason !== 'all' && !counts[st.reason]) st.reason = 'all';
            const options = [`<option value="all">All reasons (${items.length})</option>`];
            for (const r of this.sortReasons(Object.keys(counts), counts)) {
                options.push(`<option value="${this.escapeAttr(r)}">${this.escape(this.unverifiedReasonLabel(r))} (${counts[r]})</option>`);
            }
            select.innerHTML = options.join('');
            select.value = st.reason;
        }
        const bar = document.getElementById('unverifiedRunFilterBar');
        const text = document.getElementById('unverifiedRunFilterText');
        if (bar && text) {
            bar.classList.toggle('hidden', !st.runId);
            text.textContent = st.runId
                ? `Entries still unverified from the run started ${st.runLabel}. An entry checked again since is listed under its latest run.`
                : '';
        }
        this.renderUnverifiedPage();
    }

    renderUnverifiedPage() {
        const st = this.unverifiedState;
        const tbody = document.getElementById('unverifiedTableBody');
        const empty = document.getElementById('noUnverifiedMessage');
        if (!tbody) return;
        tbody.innerHTML = '';

        const items = this.filteredUnverified();
        const count = document.getElementById('unverifiedModalCount');
        if (count) count.textContent = items.length;
        document.getElementById('unverifiedReplaceWarning')?.classList.toggle('hidden', st.reason !== REASON_TAIL_TRUNCATED);
        this.updateReplaceUnverifiedButton();

        const totalPages = Math.max(1, Math.ceil(items.length / st.pageSize));
        st.page = Math.min(Math.max(1, st.page), totalPages);
        if (!items.length) {
            const text = document.getElementById('noUnverifiedText');
            if (text) text.textContent = st.runId ? 'No entry is still unverified from this run.' : 'No unverified entries.';
            empty?.classList.remove('hidden');
            this.renderUnverifiedPagination(0);
            return;
        }
        empty?.classList.add('hidden');

        const start = (st.page - 1) * st.pageSize;
        for (const h of items.slice(start, start + st.pageSize)) {
            const files = h.unverified_files || [];
            const rowId = `unverified-row-${this.slug(h.entry_name)}`;
            const replaceable = files.some((f) => f.reason === REASON_TAIL_TRUNCATED);
            const reasons = this.sortReasons([...new Set(files.map((f) => f.reason))]);
            const reasonText = this.unverifiedReasonLabel(reasons[0]) + (reasons.length > 1 ? ` +${reasons.length - 1}` : '');
            const shortBy = Math.max(0, ...files.map((f) => f.short_bytes || 0));
            const lastChecked = h.last_checked_at ? new Date(h.last_checked_at).toLocaleString() : '-';

            const tr = document.createElement('tr');
            tr.className = 'cursor-pointer hover:bg-base-200';
            tr.innerHTML = `
                <td class="w-8">
                    <i class="bi bi-chevron-right transition-transform" id="${rowId}-caret"></i>
                </td>
                <td class="font-mono text-sm break-all">${this.escape(h.entry_name)}</td>
                <td>${h.file_count ?? 0}</td>
                <td class="text-warning font-medium">${files.length}</td>
                <td class="text-xs">${this.escape(reasonText)}</td>
                <td class="text-xs">${shortBy ? this.formatBytes(shortBy) : '-'}</td>
                <td class="text-xs">${lastChecked}</td>
                <td class="text-right whitespace-nowrap">
                    <button class="btn btn-xs btn-outline" data-action="recheck" aria-label="Recheck ${this.escapeAttr(h.entry_name)}" title="Check this entry again">
                        <i class="bi bi-search-heart"></i>
                    </button>
                    ${replaceable ? `<button class="btn btn-xs btn-warning btn-outline" data-action="replace" aria-label="Replace ${this.escapeAttr(h.entry_name)}" title="Delete and re-search the tail-truncated files, keeping the release">
                        <i class="bi bi-arrow-repeat"></i>
                    </button>` : ''}
                </td>
            `;
            tbody.appendChild(tr);

            const detail = document.createElement('tr');
            detail.id = rowId;
            detail.className = 'hidden';
            detail.innerHTML = `
                <td colspan="8" class="bg-base-200/40 p-0">
                    <div class="p-4 space-y-2">${this.renderUnverifiedFiles(files)}</div>
                </td>
            `;
            tbody.appendChild(detail);

            tr.addEventListener('click', (ev) => {
                if (ev.target.closest('[data-action]')) return;
                const hidden = detail.classList.toggle('hidden');
                const caret = document.getElementById(`${rowId}-caret`);
                if (caret) caret.style.transform = hidden ? '' : 'rotate(90deg)';
            });
            tr.querySelector('[data-action="recheck"]')?.addEventListener('click', (ev) => {
                ev.stopPropagation();
                this.recheckOne(h.entry_name);
            });
            this.bindOverlayConfirmButton(
                tr.querySelector('[data-action="replace"]'),
                () => [h.entry_name],
                'replace',
                (names) => this.replaceUnverified(names),
            );
        }
        this.renderUnverifiedPagination(items.length);
    }

    renderUnverifiedPagination(total) {
        const bar = document.getElementById('unverifiedPaginationBar');
        const info = document.getElementById('unverifiedPaginationInfo');
        const controls = document.getElementById('unverifiedPaginationControls');
        if (!bar || !info || !controls) return;
        if (total === 0) {
            bar.classList.add('hidden');
            return;
        }
        bar.classList.remove('hidden');
        const {page, pageSize} = this.unverifiedState;
        const totalPages = Math.max(1, Math.ceil(total / pageSize));
        const start = (page - 1) * pageSize + 1;
        info.textContent = `Showing ${start}-${Math.min(start + pageSize - 1, total)} of ${total}`;
        if (totalPages <= 1) {
            controls.innerHTML = '';
            return;
        }
        let html = `<button class="join-item btn btn-sm ${page === 1 ? 'btn-disabled' : ''}"
                            onclick="window.repairManager.goToUnverifiedPage(${page - 1})">«</button>`;
        for (let i = 1; i <= totalPages; i++) {
            if (i === 1 || i === totalPages || (i >= page - 2 && i <= page + 2)) {
                html += `<button class="join-item btn btn-sm ${i === page ? 'btn-active' : ''}"
                                onclick="window.repairManager.goToUnverifiedPage(${i})">${i}</button>`;
            } else if (i === page - 3 || i === page + 3) {
                html += `<button class="join-item btn btn-sm btn-disabled">…</button>`;
            }
        }
        html += `<button class="join-item btn btn-sm ${page === totalPages ? 'btn-disabled' : ''}"
                         onclick="window.repairManager.goToUnverifiedPage(${page + 1})">»</button>`;
        controls.innerHTML = html;
    }

    goToUnverifiedPage(p) {
        const totalPages = Math.max(1, Math.ceil(this.filteredUnverified().length / this.unverifiedState.pageSize));
        if (p < 1 || p > totalPages || p === this.unverifiedState.page) return;
        this.unverifiedState.page = p;
        this.renderUnverifiedPage();
    }

    renderUnverifiedFiles(files) {
        if (!files.length) {
            return `<div class="text-sm opacity-60">No file details.</div>`;
        }
        const rows = files.map((f) => `
            <tr>
                <td class="font-mono text-xs break-all">${this.escape(f.file_name || '')}</td>
                <td class="text-xs">${this.escape(this.unverifiedReasonLabel(f.reason))}</td>
                <td class="text-xs">${f.short_bytes ? this.formatBytes(f.short_bytes) : '-'}</td>
                <td class="text-xs">${f.size ? this.formatBytes(f.size) : '-'}</td>
            </tr>
        `).join('');
        return `
            <div class="overflow-x-auto">
                <table class="table table-xs">
                    <thead><tr><th>File</th><th>Reason</th><th>Short by</th><th>Size</th></tr></thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        `;
    }

    // replaceUnverifiedBatch is what the bulk Replace acts on: the first batch
    // of listed entries with a tail-truncated file, in list order. Empty
    // unless the Tail truncated reason is selected.
    replaceUnverifiedBatch() {
        if (this.unverifiedState.reason !== REASON_TAIL_TRUNCATED) return [];
        return this.filteredUnverified().slice(0, REPLACE_UNVERIFIED_BATCH).map((h) => h.entry_name);
    }

    // updateReplaceUnverifiedButton enables the bulk Replace only for the Tail
    // truncated reason with no run active, and never labels a capped batch
    // "all".
    updateReplaceUnverifiedButton() {
        const btn = document.getElementById('replaceUnverifiedBtn');
        const label = document.getElementById('replaceUnverifiedLabel');
        if (!btn || !label || btn.dataset.confirming === 'true') return;
        const tail = this.unverifiedState.reason === REASON_TAIL_TRUNCATED;
        const total = tail ? this.filteredUnverified().length : 0;
        const running = !!this.latestStatus?.active_run;
        btn.disabled = !tail || total === 0 || running;
        btn.title = !tail ? 'Pick the Tail truncated reason to replace those files'
            : running ? 'Wait for the current repair run to finish' : '';
        label.textContent = total > REPLACE_UNVERIFIED_BATCH
            ? `Replace next ${REPLACE_UNVERIFIED_BATCH} of ${total}`
            : `Replace all${total ? ` (${total})` : ''}`;
    }

    async replaceUnverified(names) {
        const btn = document.getElementById('replaceUnverifiedBtn');
        if (btn) btn.disabled = true;
        try {
            const res = await fetch(`${this.api}/repair/unverified/replace`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({names, limit: names.length}),
            });
            const text = await res.text();
            if (!res.ok) throw new Error(text.trim() || `HTTP ${res.status}`);
            const data = text ? JSON.parse(text) : {};
            const queued = data.queued?.length ?? 0;
            this.toast(`Replacing ${queued} entr${queued === 1 ? 'y' : 'ies'}: deleting and re-searching through their Arr, releases kept`, 'success');
            await Promise.all([this.loadStatus(), this.loadHistory(), this.loadUnverified()]);
        } catch (e) {
            this.toast(`Replace failed: ${e.message}`, 'error');
        } finally {
            this.updateReplaceUnverifiedButton();
        }
    }

    async recheckOne(name) {
        try {
            const res = await fetch(`${this.api}/repair/health/${encodeURIComponent(name)}/check`, {method: 'POST'});
            if (!res.ok) throw new Error(await res.text() || `HTTP ${res.status}`);
            this.toast(`Recheck started for ${name}`, 'success');
            // Recheck flips the entry to repairing; refresh shortly so the row updates.
            setTimeout(() => {
                this.loadBroken();
                if (this.isUnverifiedModalOpen()) this.loadUnverified();
                this.loadStatus();
            }, 800);
        } catch (e) {
            this.toast(`Recheck failed: ${e.message}`, 'error');
        }
    }

    // fixOne is fixBroken for a single row of the broken-entries modal.
    async fixOne(name) {
        try {
            const res = await fetch(`${this.api}/repair/fix`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({names: [name]}),
            });
            if (!res.ok) throw new Error((await res.text()).trim() || `HTTP ${res.status}`);
            this.toast(`Fix started for ${name}`, 'success');
            await Promise.all([this.loadStatus(), this.loadHistory()]);
        } catch (e) {
            this.toast(`Fix failed: ${e.message}`, 'error');
        }
    }

    slug(s) {
        return String(s || '').replace(/[^a-zA-Z0-9_-]+/g, '_');
    }

    // bindDebouncedInput wires a live-search box: onChange fires `delay`ms
    // after the user stops typing, not on every keystroke.
    bindDebouncedInput(el, onChange, delay = 200) {
        if (!el) return;
        let timer = null;
        el.addEventListener('input', (e) => {
            clearTimeout(timer);
            const value = e.target.value;
            timer = setTimeout(() => onChange(value), delay);
        });
    }

    escapeAttr(s) {
        return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    formatBytes(n) {
        if (!n || n < 0) return '-';
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        let i = 0;
        let v = n;
        while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
        return `${v.toFixed(v >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
    }

    renderHistory(runs) {
        const tbody = document.getElementById('runsTableBody');
        const empty = document.getElementById('noRunsMessage');
        tbody.innerHTML = '';
        if (!runs.length) {
            empty.classList.remove('hidden');
            return;
        }
        empty.classList.add('hidden');
        for (const run of runs) {
            const tr = document.createElement('tr');
            const start = run.started_at ? new Date(run.started_at) : null;
            const end = run.completed_at ? new Date(run.completed_at) : null;
            const duration = start && end ? this.formatDuration(end - start) : (start ? 'running' : '-');
            tr.innerHTML = `
                <td class="font-mono text-sm">${start ? start.toLocaleString() : '-'}</td>
                <td>${run.trigger || '-'}</td>
                <td>${this.statusBadge(run.status)}</td>
                <td>${run.stats?.probed ?? 0}</td>
                <td class="${run.stats?.broken ? 'text-error font-medium' : ''}">${run.stats?.broken ?? 0}</td>
                <td class="${run.stats?.repaired ? 'text-success font-medium' : ''}">${run.stats?.repaired ?? 0}</td>
                <td class="${run.stats?.cleared ? 'text-warning font-medium' : ''}">${run.stats?.cleared ?? 0}</td>
                <td>${run.stats?.unverified
                    ? `<button type="button" class="link link-hover text-warning font-medium" data-unverified-run title="List the entries still unverified from this run">${run.stats.unverified}</button>`
                    : 0}</td>
                <td>${duration}</td>
                <td class="text-xs text-error">${run.error || ''}</td>
            `;
            tr.querySelector('[data-unverified-run]')?.addEventListener('click', () => {
                this.openUnverifiedModal({runId: run.id, runLabel: start ? start.toLocaleString() : run.id});
            });
            tbody.appendChild(tr);
        }
    }

    statusBadge(status) {
        const cls = {
            running: 'badge-info',
            completed: 'badge-success',
            failed: 'badge-error',
            cancelled: 'badge-warning',
        }[status] || 'badge-ghost';
        return `<span class="badge ${cls}">${status || 'unknown'}</span>`;
    }

    formatDuration(ms) {
        if (!ms || ms < 0) return '-';
        const s = Math.round(ms / 1000);
        if (s < 60) return `${s}s`;
        const m = Math.floor(s / 60);
        const r = s % 60;
        return `${m}m ${r}s`;
    }

    // === Pre-cache summary (read-ahead + Sonarr next-episode) ===

    async loadPrecacheStatus(rescan = false) {
        if (rescan) {
            try {
                await fetch(`${this.api}/precache/rescan`, {method: 'POST'});
            } catch (e) {
                console.error('Failed to rescan precache cache', e);
            }
        }
        try {
            const status = await this.fetchJSON(`${this.api}/precache/status`);
            this.renderPrecache(status || {});
        } catch (e) {
            console.error('Failed to load precache status', e);
        } finally {
            if (this.precacheTimer) clearTimeout(this.precacheTimer);
            this.precacheTimer = null;
            // Keep polling only while something is still in flight - idle/
            // complete rows are static until the user hits Refresh.
            if (this.hasInFlightPrecacheEntries()) {
                this.precacheTimer = setTimeout(() => this.loadPrecacheStatus(), 15000);
            }
        }
    }

    // hasInFlightPrecacheEntries reports whether any loaded readiness row is
    // still short of full cache coverage - see loadPrecacheStatus's polling
    // gate above.
    hasInFlightPrecacheEntries() {
        return (this.precacheReadiness || []).some((r) => (r.cache_coverage || 0) < 1);
    }

    // handlePurgeIncompletePrecache dry-runs the delete first so the confirm
    // dialog can show an accurate count/size, then re-runs for real on
    // confirmation. Entries still being burst-downloaded into are always
    // skipped server-side (see Precache.InflightHas), so the preview matches
    // what the real pass will do.
    async handlePurgeIncompletePrecache() {
        const btn = document.getElementById('precachePurgeIncompleteBtn');
        if (btn) btn.disabled = true;
        try {
            const previewRes = await fetch(`${this.api}/precache/purge-incomplete?execute=false`, {method: 'POST'});
            const preview = await this.parseJSONSafe(previewRes);
            if (!previewRes.ok) throw new Error((preview && (preview.error || preview.message)) || `HTTP ${previewRes.status}`);
            const deleted = preview?.deleted || [];
            const skipped = preview?.skipped_inflight || [];
            if (!deleted.length) {
                window.createToast('No incomplete precache entries to delete', 'info');
                return;
            }
            const skippedNote = skipped.length ? `\n\n${skipped.length} still downloading will be skipped.` : '';
            if (!confirm(`Delete ${deleted.length} incomplete precache entr${deleted.length === 1 ? 'y' : 'ies'} (~${this.formatBytes(preview?.freed_bytes || 0)})?${skippedNote}`)) return;

            const res = await fetch(`${this.api}/precache/purge-incomplete?execute=true`, {method: 'POST'});
            const data = await this.parseJSONSafe(res);
            if (!res.ok) throw new Error((data && (data.error || data.message)) || `HTTP ${res.status}`);
            const deletedCount = (data?.deleted || []).length;
            const skippedCount = (data?.skipped_inflight || []).length;
            const failedCount = (data?.failed || []).length;
            let msg = `Deleted ${deletedCount} (freed ${this.formatBytes(data?.freed_bytes || 0)})`;
            if (skippedCount) msg += `, ${skippedCount} in-flight skipped`;
            if (failedCount) msg += `, ${failedCount} couldn't be removed`;
            window.createToast(msg, failedCount ? 'warning' : 'success');
            await this.loadPrecacheStatus();
        } catch (e) {
            window.createToast(`Delete incomplete failed: ${e.message}`, 'error');
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    // handleTogglePrecachePaused flips the global runtime pause (in-memory
    // only, clears on restart - see Precache.SetPaused). Halts new
    // read-ahead/next-episode bursts from starting; anything already
    // downloading finishes on its own.
    async handleTogglePrecachePaused() {
        const btn = document.getElementById('precachePauseBtn');
        if (btn) btn.disabled = true;
        try {
            const res = await fetch(`${this.api}/precache/pause`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({paused: !this.precachePaused}),
            });
            const data = await this.parseJSONSafe(res);
            if (!res.ok) throw new Error((data && (data.error || data.message)) || `HTTP ${res.status}`);
            await this.loadPrecacheStatus();
        } catch (e) {
            window.createToast(`Pause precache failed: ${e.message}`, 'error');
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    // handleTogglePrecacheEntryPaused flips the runtime pause for one
    // readiness row, keyed by (info_hash, filename) - see
    // Precache.SetKeyPaused.
    async handleTogglePrecacheEntryPaused(row) {
        try {
            const res = await fetch(`${this.api}/precache/entry-pause`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({info_hash: row.info_hash, filename: row.filename, paused: !row.paused}),
            });
            const data = await this.parseJSONSafe(res);
            if (!res.ok) throw new Error((data && (data.error || data.message)) || `HTTP ${res.status}`);
            await this.loadPrecacheStatus();
        } catch (e) {
            window.createToast(`Pause entry failed: ${e.message}`, 'error');
        }
    }

    renderPrecache(status) {
        const line = document.getElementById('precacheStatusLine');
        if (line) {
            if (!status.read_ahead_enabled) {
                line.textContent = 'Read-ahead pre-caching is disabled.';
            } else {
                line.textContent = `Read-ahead kicks in at ${status.threshold_percent ?? 10}% into playback, `
                    + `${status.read_ahead_concurrency ?? '-'} segments in parallel.`;
            }
        }
        const footprint = document.getElementById('precacheFootprint');
        if (footprint) {
            footprint.textContent = `${this.formatBytes(status.precached_bytes || 0)} / ${this.formatBytes(status.max_bytes || 0)}`;
        }
        const nextEpisodes = document.getElementById('precacheNextEpisodes');
        if (nextEpisodes) {
            const n = status.next_episodes || 0;
            nextEpisodes.textContent = n > 0 ? `${n} ahead${status.evict_after_watched ? ' · evict after watched' : ''}` : 'off';
        }
        this.precachePaused = !!status.paused;
        const pauseBtn = document.getElementById('precachePauseBtn');
        if (pauseBtn) {
            pauseBtn.innerHTML = this.precachePaused
                ? '<i class="bi bi-play-fill mr-1"></i>Resume precache'
                : '<i class="bi bi-pause-fill mr-1"></i>Pause precache';
            pauseBtn.classList.toggle('btn-warning', this.precachePaused);
            pauseBtn.classList.toggle('btn-outline', !this.precachePaused);
        }
        this.precacheReadiness = status.readiness || [];
        this.renderPrecacheReadiness();
    }

    // filteredSortedPrecacheReadiness applies the search box and sort
    // dropdown (see bind()) to the loaded readiness rows, client-side -
    // same idiom as filteredOverlayFiles.
    filteredSortedPrecacheReadiness() {
        const search = this.precacheSearch;
        const list = (this.precacheReadiness || []).filter((r) => {
            if (!search) return true;
            return `${r.entry_name || ''} ${r.filename || ''}`.toLowerCase().includes(search);
        });
        switch (this.precacheSort) {
            case 'cache_coverage':
                list.sort((a, b) => (b.cache_coverage || 0) - (a.cache_coverage || 0));
                break;
            case 'outcome':
                list.sort((a, b) => this.precacheOutcomeRank(a) - this.precacheOutcomeRank(b));
                break;
            case 'ready_at':
            default:
                list.sort((a, b) => new Date(b.ready_at || 0) - new Date(a.ready_at || 0));
                break;
        }
        return list;
    }

    // precacheOutcomeRank orders rows for the "Outcome" sort: still-damaged
    // first (most actionable), then repaired-ahead-of-time, then clean, then
    // unknown - mirrors the badge precedence in renderPrecacheReadiness.
    precacheOutcomeRank(r) {
        if (r.segments_pending > 0) return 0;
        if (r.segments_repaired > 0) return 1;
        if (r.clean) return 2;
        return 3;
    }

    renderPrecacheReadiness() {
        const tbody = document.getElementById('precacheReadinessBody');
        const empty = document.getElementById('noPrecacheMessage');
        const noMatch = document.getElementById('precacheNoMatch');
        if (!tbody) return;
        tbody.innerHTML = '';
        if (!(this.precacheReadiness || []).length) {
            empty?.classList.remove('hidden');
            noMatch?.classList.add('hidden');
            return;
        }
        empty?.classList.add('hidden');
        const readiness = this.filteredSortedPrecacheReadiness();
        if (!readiness.length) {
            noMatch?.classList.remove('hidden');
            return;
        }
        noMatch?.classList.add('hidden');
        for (const r of readiness) {
            const tr = document.createElement('tr');
            if (r.paused) tr.classList.add('opacity-50');
            const readyAt = r.ready_at ? new Date(r.ready_at).toLocaleString() : '-';
            let outcome;
            if (r.clean) {
                outcome = '<span class="badge badge-success">clean</span>';
            } else if (r.segments_repaired > 0) {
                outcome = `<span class="badge badge-warning">${r.segments_repaired} segment(s) repaired ahead of time</span>`;
            } else if (r.segments_pending > 0) {
                outcome = `<span class="badge badge-error">${r.segments_pending} segment(s) still damaged</span>`;
            } else {
                outcome = '<span class="badge badge-ghost">unknown</span>';
            }
            if (r.paused) {
                outcome += ' <span class="badge badge-outline">paused</span>';
            }
            tr.innerHTML = `
                <td class="font-mono text-sm whitespace-nowrap">${readyAt}</td>
                <td class="break-all">${r.entry_name || '-'}</td>
                <td class="text-xs opacity-70 break-all">${r.filename || '-'}</td>
                <td class="whitespace-nowrap">${this.renderCacheCoverageBar(r.cached_bytes || 0, r.total_bytes || 0)}</td>
                <td class="whitespace-nowrap">${outcome}</td>
                <td class="whitespace-nowrap"></td>
            `;
            const toggleCell = tr.lastElementChild;
            const toggleBtn = document.createElement('button');
            toggleBtn.className = 'btn btn-ghost btn-xs';
            toggleBtn.title = r.paused ? 'Resume this entry' : 'Pause this entry';
            toggleBtn.innerHTML = r.paused ? '<i class="bi bi-play-fill"></i>' : '<i class="bi bi-pause-fill"></i>';
            toggleBtn.addEventListener('click', () => this.handleTogglePrecacheEntryPaused(r));
            toggleCell.appendChild(toggleBtn);
            tbody.appendChild(tr);
        }
    }

    async fetchJSON(url) {
        const res = await fetch(url, {credentials: 'same-origin'});
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
    }

    toast(message, type = 'info') {
        if (typeof window.createToast === 'function') return window.createToast(message, type);
        console.log(`[${type}]`, message);
    }

    // ---- Overlay / Padding -------------------------------------------------

    async loadOverlayAll() {
        await Promise.all([this.loadOverlayFiles(), this.loadOverlayDiskUsage(), this.loadOverlayHistory(), this.loadOverlayOrphanCount()]);
    }

    overlayKey(f) {
        return `${f.entry}::${f.file}`;
    }

    async parseJSONSafe(res) {
        const text = await res.text();
        try {
            return text ? JSON.parse(text) : null;
        } catch {
            return null;
        }
    }

    async loadOverlayFiles() {
        try {
            this.overlayFiles = await this.fetchJSON(`${this.api}/overlay/files`) || [];
        } catch (e) {
            console.error('Failed to load overlay files', e);
            this.overlayFiles = [];
        }
        // Selection can only ever reference files still in the list.
        const known = new Set(this.overlayFiles.map((f) => this.overlayKey(f)));
        for (const key of [...this.overlaySelected]) {
            if (!known.has(key)) this.overlaySelected.delete(key);
        }
        this.renderOverlaySummaryCounts();
        this.renderOverlayFiles();
    }

    async loadOverlayDiskUsage() {
        try {
            this.overlayDiskUsage = await this.fetchJSON(`${this.api}/overlay/disk-usage`) || {};
        } catch (e) {
            console.error('Failed to load overlay disk usage', e);
            this.overlayDiskUsage = {};
        }
        this.renderOverlayDiskUsage();
    }

    async loadOverlayHistory() {
        try {
            // /overlay/repair-history has no server-side limit or pagination
            // yet - it returns every persisted attempt, newest first. Cap
            // what we hold client-side so this can't balloon into thousands
            // of rendered rows; server-side paginated search is a follow-up.
            const all = await this.fetchJSON(`${this.api}/overlay/repair-history`) || [];
            this.overlayHistoryTotalCount = all.length;
            this.overlayHistory = all.slice(0, this.overlayHistoryCap);
        } catch (e) {
            console.error('Failed to load par2 repair history', e);
            this.overlayHistory = [];
            this.overlayHistoryTotalCount = 0;
        }
        this.renderOverlayHistory();
    }

    renderOverlayHistoryCapNote() {
        const note = document.getElementById('overlayHistoryCapNote');
        if (!note) return;
        if (this.overlayHistoryTotalCount > this.overlayHistoryCap) {
            note.textContent = `Showing most recent ${this.overlayHistoryCap} of ${this.overlayHistoryTotalCount} attempts. Server-side paginated search is a planned follow-up.`;
            note.classList.remove('hidden');
        } else {
            note.classList.add('hidden');
        }
    }

    // filteredOverlayHistory applies the search box + outcome dropdown
    // (AND'd together) to the loaded (already capped) history, client-side.
    filteredOverlayHistory() {
        const search = this.overlayHistorySearch;
        const outcome = this.overlayHistoryOutcomeFilter;
        return (this.overlayHistory || []).filter((run) => {
            if (search) {
                const hay = `${run.entry_name || ''} ${run.nzb_id || ''}`.toLowerCase();
                if (!hay.includes(search)) return false;
            }
            if (outcome !== 'all' && run.outcome !== outcome) return false;
            return true;
        });
    }

    async loadOverlayOrphanCount() {
        try {
            const data = await this.fetchJSON(`${this.api}/overlay/gc-orphans/count`) || {};
            this.overlayOrphanCount = (data.orphan_entries || 0) + (data.orphan_files || 0);
        } catch (e) {
            console.error('Failed to load overlay orphan count', e);
            this.overlayOrphanCount = 0;
        }
        this.renderOverlayOrphanButton();
    }

    renderOverlayOrphanButton() {
        const btn = document.getElementById('overlayGCOrphansBtn');
        const count = document.getElementById('overlayOrphanCount');
        if (count) count.textContent = this.overlayOrphanCount;
        if (btn) btn.classList.toggle('hidden', this.overlayOrphanCount <= 0);
    }

    async handleOverlayGCOrphans() {
        if (!confirm(`Clean up ${this.overlayOrphanCount} orphaned overlay record(s)?\n\nThese no longer correspond to a live, Arr-owned file (entry deleted or superseded by a re-grabbed twin) and are safe to remove.`)) return;
        const btn = document.getElementById('overlayGCOrphansBtn');
        if (btn) btn.disabled = true;
        try {
            const res = await fetch(`${this.api}/overlay/gc-orphans`, {method: 'POST'});
            const data = await this.parseJSONSafe(res);
            if (!res.ok) throw new Error((data && (data.error || data.message)) || `HTTP ${res.status}`);
            const deleted = (data?.deleted_entries || 0) + (data?.deleted_files || 0);
            window.createToast(`Cleaned up ${deleted} orphaned overlay record(s)`, 'success');
            await Promise.all([this.loadOverlayFiles(), this.loadOverlayDiskUsage(), this.loadOverlayOrphanCount()]);
        } catch (e) {
            window.createToast(`Cleanup failed: ${e.message}`, 'error');
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    renderOverlaySummaryCounts() {
        const counts = {clean: 0, degraded: 0, failed: 0};
        for (const f of this.overlayFiles) {
            if (counts[f.verdict] !== undefined) counts[f.verdict]++;
        }
        const clean = document.getElementById('overlayCountClean');
        const degraded = document.getElementById('overlayCountDegraded');
        const failed = document.getElementById('overlayCountFailed');
        if (clean) clean.textContent = counts.clean;
        if (degraded) degraded.textContent = counts.degraded;
        if (failed) failed.textContent = counts.failed;
    }

    renderOverlayDiskUsage() {
        const d = this.overlayDiskUsage || {};
        const disk = document.getElementById('overlayDiskBytes');
        const protectedEl = document.getElementById('overlayProtectedBytes');
        // total_overlay_disk_bytes is the true local cost (patches + manifests
        // + retained PAR2 metadata); total_protected_release_bytes is only the
        // declared size PAR2 protects on Usenet - never a disk figure.
        if (disk) disk.textContent = this.formatBytes(d.total_overlay_disk_bytes || 0);
        if (protectedEl) protectedEl.textContent = `PAR2 covers ${this.formatBytes(d.total_protected_release_bytes || 0)}`;
    }

    renderOverlaySparkline(runs, total) {
        if (!total || total <= 0 || !runs || !runs.length) {
            return '<span class="opacity-40 text-xs">-</span>';
        }
        const colors = {dead: 'bg-error', padded: 'bg-warning', patched: 'bg-success'};
        const bars = runs.map((r) => {
            const width = Math.max(((r.end - r.start + 1) / total) * 100, 0.8);
            const left = (r.start / total) * 100;
            const cls = colors[r.status] || 'bg-base-300';
            return `<div class="absolute top-0 bottom-0 ${cls}" style="left:${left}%;width:${width}%" title="${this.escapeAttr(r.status)} ${r.start}-${r.end}"></div>`;
        }).join('');
        return `<div class="relative w-24 h-3 bg-base-300/40 rounded overflow-hidden">${bars}</div>`;
    }

    renderCacheCoverageBar(cached, total) {
        if (!total || total <= 0) {
            return '<span class="opacity-40 text-xs">-</span>';
        }
        const pct = Math.min(Math.max((cached / total) * 100, 0), 100);
        return `
            <div class="flex items-center gap-2">
                <div class="relative w-24 h-3 bg-base-300/40 rounded overflow-hidden">
                    <div class="absolute top-0 bottom-0 left-0 bg-success" style="width:${pct}%"></div>
                </div>
                <span class="text-xs opacity-70">${Math.round(pct)}%</span>
            </div>`;
    }

    overlayVerdictBadge(v) {
        const cls = {clean: 'badge-success', degraded: 'badge-warning', failed: 'badge-error'}[v] || 'badge-ghost';
        return `<span class="badge ${cls} badge-sm">${this.escape(v || 'unknown')}</span>`;
    }

    overlayRepairableBadge(f) {
        const pending = Math.max(f.dead_segments || 0, f.par2_dead_segments_discovered || 0) + (f.padded_segments || 0) > 0;
        if (!pending) return '<span class="badge badge-ghost badge-sm">n/a</span>';
        if (f.repairable) return '<span class="badge badge-success badge-sm" title="PAR2 repair is available">repairable</span>';
        const reason = f.not_repairable_reason || '';
        if (/recovery slice/i.test(reason)) {
            return `<span class="badge badge-warning badge-sm" title="${this.escapeAttr(reason)}">insufficient recovery</span>`;
        }
        if (f.backfill_eligible) {
            return `<span class="badge badge-warning badge-sm" title="PAR2 references not retained - will be rebuilt from source on repair">via backfill</span>`;
        }
        return `<span class="badge badge-ghost badge-sm" title="${this.escapeAttr(reason)}">no par2</span>`;
    }

    // overlayRepairStatusBucket maps a file's raw repair_status (+ repairable)
    // onto the coarser buckets the "Repair status" filter offers - repairable
    // covers both never-attempted (none) and transient-failed-but-still-
    // retrying (failed), since both are "will be repaired automatically".
    // Returns null for files that don't fall in any actionable bucket (e.g.
    // clean, no damage at all) - only matched by the "all" filter.
    overlayRepairStatusBucket(f) {
        if (f.repair_status === 'running' || f.repair_status === 'queued') return 'running';
        if (f.repair_status === 'unrepairable') return 'terminal-unrepairable';
        if (f.repair_status === 'unavailable') return 'unavailable';
        if (f.repair_status === 'completed') return 'patched';
        if (f.repairable) return 'repairable';
        return null;
    }

    // filteredOverlayFiles applies the search box + both status dropdowns
    // (AND'd together) to the loaded overlay files, client-side.
    filteredOverlayFiles() {
        const search = this.overlayFilesSearch;
        const verdict = this.overlayFilesVerdictFilter;
        const repairFilter = this.overlayFilesRepairFilter;
        return (this.overlayFiles || []).filter((f) => {
            if (search && !`${f.file || ''} ${f.entry || ''}`.toLowerCase().includes(search)) return false;
            if (verdict !== 'all' && f.verdict !== verdict) return false;
            if (repairFilter !== 'all' && this.overlayRepairStatusBucket(f) !== repairFilter) return false;
            return true;
        });
    }

    // renderOverlayRepairStatusCell renders the full "Repair status" cell for
    // one file: a live progress bar while running/queued (see
    // pollOverlayProgress), or a terminal/transient-aware badge with the
    // actual failure reason shown inline (never a bare "failed") once
    // resolved. Terminal (unrepairable - automatic retries have stopped,
    // manual repair-now only) is visually distinct from transient (failed,
    // still backing off and will retry automatically).
    renderOverlayRepairStatusCell(f, domId) {
        if (f.repair_status === 'running' || f.repair_status === 'queued') {
            const badge = f.repair_status === 'running'
                ? '<span class="badge badge-info badge-sm">running</span>'
                : '<span class="badge badge-ghost badge-sm">queued</span>';
            return `<div class="min-w-[12rem]">${badge}<div id="${domId}" class="mt-1">${this.renderOverlayProgressBody(null)}</div></div>`;
        }

        if (f.repair_status === 'unrepairable') {
            const reason = f.par2_last_error || f.repair_status_reason || 'retrying can\'t fix this';
            return `
                <div class="min-w-[10rem]">
                    <span class="badge badge-error badge-sm" title="Automatic retries have stopped - manual repair-now only">unrepairable</span>
                    <div class="text-[11px] text-error/90 mt-1 break-words">${this.escape(reason)}</div>
                </div>`;
        }

        if (f.repair_status === 'failed') {
            const reason = f.par2_last_error || f.repair_status_reason || 'unknown error';
            const retry = f.par2_next_retry_at ? this.formatRetryAt(f.par2_next_retry_at) : null;
            return `
                <div class="min-w-[10rem]">
                    <span class="badge badge-warning badge-sm" title="Transient failure - will back off and retry automatically">failed, retrying</span>
                    <div class="text-[11px] text-warning/90 mt-1 break-words">${this.escape(reason)}</div>
                    ${retry ? `<div class="text-[10px] opacity-60 mt-0.5">next retry ${this.escape(retry)}</div>` : ''}
                </div>`;
        }

        const cls = {
            none: 'badge-ghost',
            completed: 'badge-success',
            unavailable: 'badge-ghost',
        }[f.repair_status] || 'badge-ghost';
        const label = (f.repair_status || 'none').replace(/_/g, ' ');
        return `<span class="badge ${cls} badge-sm" title="${this.escapeAttr(f.repair_status_reason || '')}">${this.escape(label)}</span>`;
    }

    formatRetryAt(iso) {
        const t = new Date(iso);
        if (Number.isNaN(t.getTime())) return null;
        const deltaMs = t.getTime() - Date.now();
        if (deltaMs <= 0) return 'shortly';
        return `in ${this.formatDuration(deltaMs)}`;
    }

    overlayPhaseLabel(phase) {
        const labels = {
            queued: 'Queued',
            fetching_recovery: 'Fetching recovery vols',
            streaming_intact: 'Streaming intact slices',
            solving: 'Solving',
            writing: 'Writing',
            completed: 'Completed',
            failed: 'Failed',
        };
        return labels[phase] || (phase || 'Unknown');
    }

    // renderOverlayProgressBody renders the live-progress body for one job:
    // phase, slices-read/total, recovery-vols fetched/needed, bytes (cache
    // vs usenet split once either is nonzero), and elapsed time. p === null
    // is the initial "waiting for progress" placeholder shown before the
    // first poll response lands.
    renderOverlayProgressBody(p) {
        if (!p || p.status === 'no_job') {
            return '<div class="text-[11px] opacity-60">waiting for progress…</div>';
        }
        const intactTotal = p.intact_slices_total || 0;
        const intactRead = p.intact_slices_read || 0;
        const volsNeeded = p.recovery_slices_needed || 0;
        const volsFetched = p.recovery_slices_fetched || 0;
        let pct = null;
        if (intactTotal > 0) pct = Math.min(100, Math.round((intactRead / intactTotal) * 100));
        else if (volsNeeded > 0) pct = Math.min(100, Math.round((volsFetched / volsNeeded) * 100));

        const elapsed = p.started_at ? this.formatDuration(Date.now() - new Date(p.started_at).getTime()) : '-';
        const totalBytes = (p.cache_bytes || 0) + (p.usenet_bytes || 0);
        const splitLine = (p.cache_bytes || p.usenet_bytes)
            ? `<div class="opacity-70">${this.formatBytes(totalBytes)} <span class="opacity-60">(${this.formatBytes(p.cache_bytes || 0)} cache / ${this.formatBytes(p.usenet_bytes || 0)} usenet)</span></div>`
            : '';

        return `
            <div class="text-[11px] space-y-0.5">
                <div class="flex items-center gap-1 flex-wrap">
                    <span class="badge badge-info badge-xs">${this.escape(this.overlayPhaseLabel(p.phase))}</span>
                    <span class="opacity-60">${elapsed} elapsed</span>
                </div>
                <progress class="progress progress-info w-28 h-1.5" ${pct === null ? '' : `value="${pct}"`} max="100"></progress>
                ${intactTotal ? `<div class="opacity-70">slices ${intactRead}/${intactTotal}</div>` : ''}
                ${volsNeeded ? `<div class="opacity-70">recovery vols ${volsFetched}/${volsNeeded}</div>` : ''}
                ${splitLine}
                ${p.last_error ? `<div class="text-error/90">${this.escape(p.last_error)}</div>` : ''}
            </div>`;
    }

    // pollOverlayProgress polls /overlay/repair-progress for one running or
    // queued file every 2s and patches its progress cell in place (no full
    // table re-render, so selection/scroll state survives). "no_job" (the
    // worker hasn't actually started this job in-process yet - e.g. still
    // queued behind another) is NOT a stop condition, just an empty
    // placeholder - only an explicit completed/failed phase stops the poll,
    // followed by one full loadOverlayFiles() so the row picks up its final
    // repair_status/disk figures. key is overlayKey(f) (identifies the
    // timer); domId is the row's generated progress-cell id (identifies
    // where to render - see renderOverlayFiles).
    pollOverlayProgress(f, key, domId) {
        if (this.overlayProgressTimers.has(key)) return;

        const tick = async () => {
            let p = null;
            try {
                p = await this.fetchJSON(`${this.api}/overlay/repair-progress?entry=${encodeURIComponent(f.entry)}&file=${encodeURIComponent(f.file)}`);
            } catch (e) {
                console.error('Failed to poll overlay repair progress', e);
                return;
            }
            const cell = document.getElementById(domId);
            if (cell) cell.innerHTML = this.renderOverlayProgressBody(p);

            if (p && (p.phase === 'completed' || p.phase === 'failed')) {
                clearInterval(this.overlayProgressTimers.get(key));
                this.overlayProgressTimers.delete(key);
                this.loadOverlayFiles();
            }
        };

        tick();
        const id = setInterval(tick, 2000);
        this.overlayProgressTimers.set(key, id);
    }

    stopAllOverlayProgressPolling() {
        for (const id of this.overlayProgressTimers.values()) clearInterval(id);
        this.overlayProgressTimers.clear();
    }

    renderOverlayFiles() {
        const tbody = document.getElementById('overlayFilesTableBody');
        const empty = document.getElementById('overlayFilesEmpty');
        const noMatch = document.getElementById('overlayFilesNoMatch');
        if (!tbody) return;
        // Every row is about to be torn down and rebuilt - drop any live
        // progress pollers pointed at the old DOM nodes before they're gone.
        this.stopAllOverlayProgressPolling();
        tbody.innerHTML = '';

        if (!(this.overlayFiles || []).length) {
            empty?.classList.remove('hidden');
            noMatch?.classList.add('hidden');
            this.updateOverlayBulkBar();
            return;
        }
        empty?.classList.add('hidden');

        // Search box + verdict/repair-status dropdowns, AND'd together, over
        // the already-loaded rows - see filteredOverlayFiles.
        const files = this.filteredOverlayFiles();
        if (!files.length) {
            noMatch?.classList.remove('hidden');
            this.updateOverlayBulkBar();
            return;
        }
        noMatch?.classList.add('hidden');

        for (let i = 0; i < files.length; i++) {
            const f = files[i];
            const key = this.overlayKey(f);
            // A generated, always-DOM-safe id for this row's progress cell -
            // entry/file names can contain characters (spaces, brackets, ...)
            // that are unsafe to embed directly in an id attribute.
            const progressDomId = `overlayProgress-row-${i}`;
            const deadCount = Math.max(f.dead_segments || 0, f.par2_dead_segments_discovered || 0);
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td onclick="event.stopPropagation();">
                    <input type="checkbox" class="checkbox checkbox-sm checkbox-primary overlay-row-checkbox"
                           data-key="${this.escapeAttr(key)}" ${this.overlaySelected.has(key) ? 'checked' : ''}>
                </td>
                <td>
                    <div class="font-mono text-xs break-all">${this.escape(f.file)}</div>
                    <div class="text-[10px] opacity-60 break-all">${this.escape(f.entry)}</div>
                </td>
                <td>${this.overlayVerdictBadge(f.verdict)}</td>
                <td class="text-xs whitespace-nowrap">
                    <span class="text-error" title="dead (${f.dead_segments || 0} recorded, ${f.par2_dead_segments_discovered || 0} found by repair)">D:${deadCount}</span>
                    <span class="text-warning ml-1" title="padded">P:${f.padded_segments || 0}</span>
                    <span class="text-success ml-1" title="patched">F:${f.patched_segments || 0}</span>
                </td>
                <td>
                    ${this.renderOverlaySparkline(f.segment_runs, f.total_segments)}
                    <div class="text-[10px] opacity-60 mt-1">${((f.damage_byte_ratio || 0) * 100).toFixed(2)}%${f.coverage_fraction ? ' (' + (f.coverage_fraction * 100).toFixed(0) + '% verified)' : ''}</div>
                </td>
                <td>${this.overlayRepairableBadge(f)}</td>
                <td>${this.renderOverlayRepairStatusCell(f, progressDomId)}</td>
                <td class="text-right text-xs whitespace-nowrap">
                    <div title="Real bytes on disk: patch + retained PAR2 metadata">${this.formatBytes(f.overlay_disk_bytes || 0)}</div>
                    <div class="opacity-50" title="Informational only - declared size PAR2 protects on Usenet, not a disk cost">protects ${this.formatBytes(f.protected_release_bytes || 0)}</div>
                </td>
                <td class="text-right whitespace-nowrap">
                    <button class="btn btn-xs btn-outline" data-action="repair-now" ${!(f.repairable || f.backfill_eligible) ? 'disabled' : ''}
                            title="${this.escapeAttr(f.repairable ? 'Run a PAR2 repair pass now' : (f.backfill_eligible ? 'Rebuild PAR2 references from source, then repair' : (f.not_repairable_reason || 'Not repairable')))}"
                            aria-label="Repair now">
                        <i class="bi bi-tools"></i>
                    </button>
                    <button class="btn btn-xs btn-outline" data-action="verify" ${f.patched_segments ? '' : 'disabled'}
                            title="Verify patched bytes against PAR2's whole-file MD5" aria-label="Verify">
                        <i class="bi bi-shield-check"></i>
                    </button>
                    <button class="btn btn-xs btn-warning btn-outline" data-action="reclaim"
                            title="Delete overlay patches/manifest for this file" aria-label="Reclaim metadata">
                        <i class="bi bi-recycle"></i>
                    </button>
                    <button class="btn btn-xs btn-error btn-outline" data-action="research"
                            title="Delete overlay state, blocklist, and re-search via the Arr" aria-label="Delete and re-search">
                        <i class="bi bi-arrow-repeat"></i>
                    </button>
                </td>
            `;
            tbody.appendChild(tr);

            tr.querySelector('.overlay-row-checkbox')?.addEventListener('change', (e) => {
                this.toggleOverlayRowSelect(key, e.target.checked);
            });
            tr.querySelector('[data-action="repair-now"]')?.addEventListener('click', () => this.overlayRepairNow(f));
            tr.querySelector('[data-action="verify"]')?.addEventListener('click', () => this.overlayVerify(f));
            tr.querySelector('[data-action="reclaim"]')?.addEventListener('click', () => this.overlayReclaim(f));
            tr.querySelector('[data-action="research"]')?.addEventListener('click', () => this.overlayResearch(f));

            if (f.repair_status === 'running' || f.repair_status === 'queued') {
                this.pollOverlayProgress(f, key, progressDomId);
            }
        }
        this.updateOverlayBulkBar();
    }

    async overlayRepairNow(f) {
        try {
            const res = await fetch(`${this.api}/overlay/repair-now`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({entry: f.entry, file: f.file}),
            });
            const data = await this.parseJSONSafe(res);
            if (!res.ok) throw new Error((data && (data.error || data.message)) || `HTTP ${res.status}`);
            if (data && data.status === 'unavailable') {
                window.createToast(`Not repairable: ${data.reason || 'unavailable'}`, 'warning');
            } else {
                window.createToast(`PAR2 repair queued for ${f.file}`, 'success');
            }
            setTimeout(() => this.loadOverlayFiles(), 1000);
        } catch (e) {
            window.createToast(`Repair now failed: ${e.message}`, 'error');
        }
    }

    async overlayVerify(f) {
        try {
            const res = await fetch(`${this.api}/overlay/verify`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({entry: f.entry, file: f.file}),
            });
            const data = await this.parseJSONSafe(res);
            if (!res.ok) throw new Error((data && (data.error || data.message)) || `HTTP ${res.status}`);
            if (data && data.pass) {
                window.createToast(`Verify passed: ${f.file} matches PAR2's whole-file MD5`, 'success');
            } else {
                window.createToast(`Verify FAILED for ${f.file}: ${(data && data.reason) || 'MD5 mismatch'}`, 'error');
            }
        } catch (e) {
            window.createToast(`Verify failed: ${e.message}`, 'error');
        }
    }

    async overlayReclaim(f) {
        if (!confirm(`Reclaim overlay metadata for "${f.file}"?\n\nThis deletes its stored patches/manifest. Repair state can be rediscovered on the next playback or sweep.`)) return;
        try {
            const res = await fetch(`${this.api}/overlay/reclaim`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({entry: f.entry, file: f.file, nzb_id: f.nzb_id}),
            });
            const data = await this.parseJSONSafe(res);
            if (!res.ok) throw new Error((data && (data.error || data.message)) || `HTTP ${res.status}`);
            if (data && data.status === 'not_found') {
                window.createToast(`Nothing to reclaim for ${f.file}: ${data.reason || 'no overlay record found'}`, 'warning');
            } else {
                window.createToast(`Reclaimed overlay metadata for ${f.file}`, 'success');
            }
            this.overlaySelected.delete(this.overlayKey(f));
            await Promise.all([this.loadOverlayFiles(), this.loadOverlayDiskUsage()]);
        } catch (e) {
            window.createToast(`Reclaim failed: ${e.message}`, 'error');
        }
    }

    async overlayResearch(f) {
        if (!confirm(`This BLOCKLISTS "${f.entry}" and triggers a re-search via its Arr, discarding this release entirely. This cannot be undone.\n\nContinue?`)) return;
        try {
            const res = await fetch(`${this.api}/overlay/research`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({entry: f.entry, file: f.file, nzb_id: f.nzb_id}),
            });
            const data = await this.parseJSONSafe(res);
            if (!res.ok) throw new Error((data && (data.error || data.message)) || `HTTP ${res.status}`);
            window.createToast(`Blocklisted and re-searching for ${f.entry}`, 'success');
            this.overlaySelected.delete(this.overlayKey(f));
            await this.loadOverlayFiles();
        } catch (e) {
            window.createToast(`Research failed: ${e.message}`, 'error');
        }
    }

    toggleOverlaySelectAll(checked) {
        this.overlaySelected.clear();
        if (checked) {
            for (const f of this.overlayFiles) this.overlaySelected.add(this.overlayKey(f));
        }
        this.renderOverlayFiles();
    }

    toggleOverlayRowSelect(key, checked) {
        if (checked) this.overlaySelected.add(key); else this.overlaySelected.delete(key);
        const all = document.getElementById('overlaySelectAllCheckbox');
        if (all) {
            all.checked = this.overlayFiles.length > 0 && this.overlaySelected.size === this.overlayFiles.length;
            all.indeterminate = this.overlaySelected.size > 0 && this.overlaySelected.size < this.overlayFiles.length;
        }
        this.updateOverlayBulkBar();
    }

    clearOverlaySelection() {
        this.overlaySelected.clear();
        const all = document.getElementById('overlaySelectAllCheckbox');
        if (all) {
            all.checked = false;
            all.indeterminate = false;
        }
        this.renderOverlayFiles();
    }

    overlaySelectedFiles() {
        return this.overlayFiles.filter((f) => this.overlaySelected.has(this.overlayKey(f)));
    }

    updateOverlayBulkBar() {
        const bar = document.getElementById('overlayBulkBar');
        const count = document.getElementById('overlaySelectedCount');
        if (count) count.textContent = this.overlaySelected.size;
        if (bar) bar.classList.toggle('hidden', this.overlaySelected.size === 0);

        const failedSelected = this.overlaySelectedFiles().filter((f) => f.verdict === 'failed');
        const cleanSelected = this.overlaySelectedFiles().filter((f) => f.verdict === 'clean');

        const allCount = this.overlaySelected.size;
        const deleteResearchBtn = document.getElementById('overlayBulkDeleteResearchBtn');
        if (deleteResearchBtn && deleteResearchBtn.dataset.confirming !== 'true') {
            deleteResearchBtn.disabled = allCount === 0;
            deleteResearchBtn.innerHTML = `<i class="bi bi-arrow-repeat mr-1"></i>Delete &amp; re-search selected (${allCount})`;
        }

        const researchBtn = document.getElementById('overlayBulkResearchBtn');
        if (researchBtn && researchBtn.dataset.confirming !== 'true') {
            researchBtn.disabled = failedSelected.length === 0;
            researchBtn.innerHTML = `<i class="bi bi-arrow-repeat mr-1"></i>Re-search all failed (${failedSelected.length})`;
        }
        const reclaimBtn = document.getElementById('overlayBulkReclaimBtn');
        if (reclaimBtn && reclaimBtn.dataset.confirming !== 'true') {
            reclaimBtn.disabled = cleanSelected.length === 0;
            reclaimBtn.innerHTML = `<i class="bi bi-recycle mr-1"></i>Reclaim all clean-verdict metadata (${cleanSelected.length})`;
        }
    }

    // bindOverlayConfirmButton wires a cheap two-step confirm onto btn,
    // mirroring browse.js's stale-NZB delete buttons: the first click swaps
    // the label to "Really <action> N?"; a second click within the window
    // runs onConfirm. itemsFn is called fresh on each click so the count
    // always reflects the current selection.
    bindOverlayConfirmButton(btn, itemsFn, actionLabel, onConfirm) {
        if (!btn) return;
        btn.addEventListener('click', () => {
            const items = itemsFn();
            if (!items.length) {
                window.createToast('Nothing to do', 'warning');
                return;
            }
            if (btn.dataset.confirming === 'true') {
                btn.dataset.confirming = '';
                if (btn.dataset.originalHtml) btn.innerHTML = btn.dataset.originalHtml;
                onConfirm(items);
                return;
            }
            btn.dataset.confirming = 'true';
            btn.dataset.originalHtml = btn.innerHTML;
            btn.textContent = `Really ${actionLabel} ${items.length}?`;
            setTimeout(() => {
                if (btn.dataset.confirming === 'true') {
                    btn.dataset.confirming = '';
                    if (btn.dataset.originalHtml) btn.innerHTML = btn.dataset.originalHtml;
                }
            }, 4000);
        });
    }

    // runOverlayBulkDeleteResearch applies POST /overlay/research to every
    // selected file regardless of verdict - the "nuke everything selected"
    // action. bindOverlayConfirmButton already gave the inline two-click
    // confirm; this adds a hard window.confirm() (the blocklist + re-search
    // is irreversible) plus a second one if any clean-verdict files are in
    // the set, since those have no detected damage.
    // skip_repair:true tells the server to discard this copy outright - skip
    // the warm PAR2 pass and the per-entry cooldown - so the sequential loop
    // spends ~seconds, not ~minutes, per file and same-entry selections
    // action every file.
    async runOverlayBulkDeleteResearch(items) {
        if (!window.confirm(
            `This will permanently delete overlay state, blocklist the current releases, ` +
            `and trigger Arr re-searches for ${items.length} file(s).\n\n` +
            `This cannot be undone. Continue?`
        )) return;

        const cleanItems = items.filter((f) => f.verdict === 'clean');
        if (cleanItems.length > 0) {
            if (!window.confirm(
                `⚠️ ${cleanItems.length} of the ${items.length} selected file(s) have a clean verdict ` +
                `with no detected damage. These will also be deleted and re-searched.\n\n` +
                `Continue anyway?`
            )) return;
        }

        window.createToast(`Deleting & re-searching ${items.length} file(s)…`, 'info');
        let ok = 0, fail = 0;
        for (const f of items) {
            try {
                const res = await fetch(`${this.api}/overlay/research`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({entry: f.entry, file: f.file, nzb_id: f.nzb_id, skip_repair: true}),
                });
                if (!res.ok) throw new Error(await res.text() || `HTTP ${res.status}`);
                ok++;
                this.overlaySelected.delete(this.overlayKey(f));
            } catch (e) {
                fail++;
                console.error('Bulk delete & re-search failed for', f.file, e);
            }
        }
        window.createToast(`Delete & re-search: ${ok} started, ${fail} failed`, fail ? 'warning' : 'success');
        this.clearOverlaySelection();
        await this.loadOverlayFiles();
    }

    async runOverlayBulkResearch(items) {
        window.createToast(`Re-searching ${items.length} file(s)…`, 'info');
        let ok = 0, fail = 0;
        for (const f of items) {
            try {
                const res = await fetch(`${this.api}/overlay/research`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({entry: f.entry, file: f.file, nzb_id: f.nzb_id}),
                });
                if (!res.ok) throw new Error(await res.text() || `HTTP ${res.status}`);
                ok++;
                this.overlaySelected.delete(this.overlayKey(f));
            } catch (e) {
                fail++;
                console.error('Bulk research failed for', f.file, e);
            }
        }
        window.createToast(`Re-search: ${ok} started, ${fail} failed`, fail ? 'warning' : 'success');
        await this.loadOverlayFiles();
    }

    async runOverlayBulkReclaim(items) {
        window.createToast(`Reclaiming ${items.length} file(s)…`, 'info');
        let ok = 0, notFound = 0, fail = 0;
        for (const f of items) {
            try {
                const res = await fetch(`${this.api}/overlay/reclaim`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({entry: f.entry, file: f.file, nzb_id: f.nzb_id}),
                });
                const data = await this.parseJSONSafe(res);
                if (!res.ok) throw new Error((data && (data.error || data.message)) || `HTTP ${res.status}`);
                if (data && data.status === 'not_found') {
                    notFound++;
                } else {
                    ok++;
                }
                this.overlaySelected.delete(this.overlayKey(f));
            } catch (e) {
                fail++;
                console.error('Bulk reclaim failed for', f.file, e);
            }
        }
        const parts = [`${ok} done`];
        if (notFound) parts.push(`${notFound} nothing to reclaim`);
        if (fail) parts.push(`${fail} failed`);
        window.createToast(`Reclaim: ${parts.join(', ')}`, (fail || notFound) ? 'warning' : 'success');
        await Promise.all([this.loadOverlayFiles(), this.loadOverlayDiskUsage()]);
    }

    renderOverlayHistory() {
        this.renderOverlayHistoryCapNote();
        const tbody = document.getElementById('overlayHistoryTableBody');
        const empty = document.getElementById('overlayHistoryEmpty');
        const noMatch = document.getElementById('overlayHistoryNoMatch');
        if (!tbody) return;
        tbody.innerHTML = '';

        if (!(this.overlayHistory || []).length) {
            empty?.classList.remove('hidden');
            noMatch?.classList.add('hidden');
            return;
        }
        empty?.classList.add('hidden');

        // Search box + outcome dropdown, AND'd together, over the loaded
        // (capped) history - see filteredOverlayHistory.
        const runs = this.filteredOverlayHistory();
        if (!runs.length) {
            noMatch?.classList.remove('hidden');
            return;
        }
        noMatch?.classList.add('hidden');

        for (const run of runs) {
            const tr = document.createElement('tr');
            const started = run.started_at ? new Date(run.started_at) : null;
            const durationMs = (run.duration || 0) / 1e6; // Go time.Duration is nanoseconds
            tr.innerHTML = `
                <td class="font-mono text-sm">${started ? started.toLocaleString() : '-'}</td>
                <td class="text-xs break-all">${this.escape(run.entry_name || run.nzb_id || '-')}</td>
                <td>${this.overlayOutcomeBadge(run.outcome, run.crc_canary)}</td>
                <td class="text-xs">${this.formatBytes(run.read_bytes || 0)}</td>
                <td class="text-xs">${run.slices_repaired ?? '-'}</td>
                <td class="text-xs">${run.segments_patched ?? '-'}</td>
                <td class="text-xs">${this.formatDuration(durationMs)}</td>
                <td class="text-xs text-error">${this.escape(run.fail_reason || '')}</td>
            `;
            tbody.appendChild(tr);
        }
    }

    overlayOutcomeBadge(outcome, canary) {
        const cls = {completed: 'badge-success', failed: 'badge-error', unavailable: 'badge-ghost'}[outcome] || 'badge-ghost';
        const canaryTag = canary
            ? ' <span class="badge badge-error badge-xs ml-1" title="Checksum verification failed - reconstructed/intact data did not match its recorded MD5/CRC32">CRC</span>'
            : '';
        return `<span class="badge ${cls} badge-sm">${this.escape(outcome || 'unknown')}</span>${canaryTag}`;
    }

    async clearOverlayHistory() {
        if (!confirm('Clear all PAR2 repair history?')) return;
        try {
            const res = await fetch(`${this.api}/overlay/repair-history`, {method: 'DELETE'});
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            await this.loadOverlayHistory();
        } catch (e) {
            window.createToast(`Clear failed: ${e.message}`, 'error');
        }
    }

    populateOverlayConfigForm() {
        const c = this.repairConfig || {};
        const d = this.repairConfigDefaults || {};
        const $ = (id) => document.getElementById(id);
        if ($('overlayPlaybackPadding')) $('overlayPlaybackPadding').checked = c.playback_padding !== false;
        if ($('overlayPar2Repair')) $('overlayPar2Repair').checked = c.par2_repair !== false;
        if ($('overlayPadMaxRun')) {
            $('overlayPadMaxRun').placeholder = d.pad_max_run_segments ?? '';
            $('overlayPadMaxRun').value = c.pad_max_run_segments ?? '';
        }
        if ($('overlayPadMaxTotal')) {
            $('overlayPadMaxTotal').placeholder = d.pad_max_total_segments ?? '';
            $('overlayPadMaxTotal').value = c.pad_max_total_segments ?? '';
        }
        if ($('overlayPadMaxRatio')) {
            $('overlayPadMaxRatio').placeholder = d.pad_max_byte_ratio ?? '';
            $('overlayPadMaxRatio').value = c.pad_max_byte_ratio ?? '';
        }
        if ($('overlayPar2Mode')) $('overlayPar2Mode').value = c.par2_repair_mode || 'auto_all';
        if ($('overlayPar2MinSegments')) {
            $('overlayPar2MinSegments').placeholder = d.par2_repair_min_segments ?? '';
            $('overlayPar2MinSegments').value = c.par2_repair_min_segments ?? '';
        }
    }

    async saveOverlayConfig() {
        const $ = (id) => document.getElementById(id);
        const btn = $('overlayConfigSaveBtn');
        if (btn) btn.disabled = true;
        try {
            const payload = {
                ...this.repairConfig,
                playback_padding: !!$('overlayPlaybackPadding')?.checked,
                par2_repair: !!$('overlayPar2Repair')?.checked,
                pad_max_run_segments: parseInt($('overlayPadMaxRun')?.value, 10) || 0,
                pad_max_total_segments: parseInt($('overlayPadMaxTotal')?.value, 10) || 0,
                pad_max_byte_ratio: parseFloat($('overlayPadMaxRatio')?.value) || 0,
                par2_repair_mode: $('overlayPar2Mode')?.value || 'auto_all',
                par2_repair_min_segments: parseInt($('overlayPar2MinSegments')?.value, 10) || 0,
            };
            const res = await fetch(`${this.api}/repair/config`, {
                method: 'PUT',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(payload),
            });
            const text = await res.text();
            let data = null;
            try {
                data = text ? JSON.parse(text) : null;
            } catch { /* leave null */
            }
            if (!res.ok) throw new Error((data && (data.error || data.message)) || text || `HTTP ${res.status}`);
            this.repairConfig = data || payload;
            this.populateOverlayConfigForm();
            window.createToast('Overlay/PAR2 config saved', 'success');
        } catch (e) {
            window.createToast(`Save failed: ${e.message}`, 'error');
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    populatePrecacheConfigForm() {
        const r = this.repairConfig || {};
        const p = this.precacheConfig || {};
        const $ = (id) => document.getElementById(id);
        if ($('precacheReadAhead')) $('precacheReadAhead').checked = r.precache_read_ahead_enabled === true;
        if ($('precacheMaxBytesGiB')) {
            const bytes = p.precache_max_bytes ?? (10 * PRECACHE_GIB);
            $('precacheMaxBytesGiB').value = Math.round((bytes / PRECACHE_GIB) * 100) / 100;
        }
        if ($('precacheThresholdPercent')) $('precacheThresholdPercent').value = p.precache_threshold_percent || 10;
    }

    async savePrecacheConfig() {
        const $ = (id) => document.getElementById(id);
        const btn = $('precacheConfigSaveBtn');
        if (btn) btn.disabled = true;
        try {
            const giB = parseFloat($('precacheMaxBytesGiB')?.value);
            // 0/blank/negative -> 0 bytes, which the backend treats as an
            // explicit disable (see PrecacheMaxBytes's doc comment), not
            // "unset" - matches the min="0" on the input.
            const maxBytes = Number.isFinite(giB) && giB > 0 ? Math.round(giB * PRECACHE_GIB) : 0;

            const repairPayload = {
                ...this.repairConfig,
                precache_read_ahead_enabled: !!$('precacheReadAhead')?.checked,
            };
            const precachePayload = {
                ...this.precacheConfig,
                precache_threshold_percent: parseInt($('precacheThresholdPercent')?.value, 10) || 0,
                precache_max_bytes: maxBytes,
            };

            const parseResponse = async (res) => {
                const text = await res.text();
                let data = null;
                try {
                    data = text ? JSON.parse(text) : null;
                } catch { /* leave null */
                }
                return {res, data, text};
            };

            const [repairRes, precacheRes] = await Promise.all([
                fetch(`${this.api}/repair/config`, {
                    method: 'PUT',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(repairPayload),
                }).then(parseResponse),
                fetch(`${this.api}/precache/config`, {
                    method: 'PUT',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(precachePayload),
                }).then(parseResponse),
            ]);
            if (!repairRes.res.ok) {
                throw new Error((repairRes.data && (repairRes.data.error || repairRes.data.message)) || repairRes.text || `HTTP ${repairRes.res.status}`);
            }
            if (!precacheRes.res.ok) {
                throw new Error((precacheRes.data && (precacheRes.data.error || precacheRes.data.message)) || precacheRes.text || `HTTP ${precacheRes.res.status}`);
            }

            this.repairConfig = repairRes.data || repairPayload;
            this.precacheConfig = precacheRes.data || precachePayload;
            this.populatePrecacheConfigForm();
            window.createToast('Pre-cache config saved', 'success');
        } catch (e) {
            window.createToast(`Save failed: ${e.message}`, 'error');
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    populatePlexConfigForm() {
        const p = this.plexConfig || {};
        const $ = (id) => document.getElementById(id);
        if ($('plexUrl')) $('plexUrl').value = p.plex_url || '';
        if ($('plexToken')) {
            $('plexToken').value = '';
            $('plexToken').placeholder = p.plex_token ? 'unchanged (set)' : '';
        }
        if ($('plexSessionTtl')) {
            const ttlSeconds = p.plex_session_cache_ttl ? Math.round(p.plex_session_cache_ttl / NS_PER_SECOND) : '';
            $('plexSessionTtl').value = ttlSeconds;
            $('plexSessionTtl').placeholder = '10';
        }
        if ($('plexReapMode')) $('plexReapMode').value = p.plex_reap_mode || 'off';
        if ($('plexTestResult')) $('plexTestResult').textContent = '';
    }

    async savePlexConfig() {
        const $ = (id) => document.getElementById(id);
        const btn = $('plexConfigSaveBtn');
        if (btn) btn.disabled = true;
        try {
            const ttlSeconds = parseInt($('plexSessionTtl')?.value, 10);
            const payload = {
                plex_url: $('plexUrl')?.value.trim() || '',
                // Blank leaves the saved token as-is - see handleUpdatePlexConfig's
                // preserve-on-blank handling.
                plex_token: $('plexToken')?.value || '',
                plex_session_cache_ttl: Number.isFinite(ttlSeconds) && ttlSeconds > 0 ? ttlSeconds * NS_PER_SECOND : 0,
                plex_reap_mode: $('plexReapMode')?.value || 'off',
            };

            const res = await fetch(`${this.api}/plex/config`, {
                method: 'PUT',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(payload),
            });
            const text = await res.text();
            let data = null;
            try {
                data = text ? JSON.parse(text) : null;
            } catch { /* leave null */
            }
            if (!res.ok) {
                throw new Error((data && (data.error || data.message)) || text || `HTTP ${res.status}`);
            }

            this.plexConfig = data || payload;
            this.populatePlexConfigForm();
            window.createToast('Plex config saved', 'success');
            this.loadPlexReap();
        } catch (e) {
            window.createToast(`Save failed: ${e.message}`, 'error');
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    // ---- Stale Plex versions ---------------------------------------------

    async loadPlexReap() {
        try {
            this.plexReap.status = await this.fetchJSON(`${this.api}/plex/reap/status`) || {};
        } catch (e) {
            console.error('Failed to load Plex reap status', e);
            return;
        }
        this.renderPlexReap();
        if (this.plexReap.timer) clearTimeout(this.plexReap.timer);
        this.plexReap.timer = null;
        const scan = this.plexReap.status.scan || {};
        if (scan.running || scan.applying) {
            this.plexReap.timer = setTimeout(() => this.loadPlexReap(), 3000);
        }
    }

    // plexReapGroup buckets a backlog row for the filter select.
    plexReapGroup(c) {
        if (c.status === 'reapable') return 'reapable';
        if (c.reason === 'all_versions_unavailable') return 'review';
        if (c.status === 'reaped' || c.status === 'would_reap' || c.status === 'failed') return 'done';
        return 'other';
    }

    plexReapReasonLabel(reason) {
        const labels = {
            no_unavailable_version: 'No unavailable version',
            plex_not_marked_unavailable_yet: 'Plex has not marked it unavailable yet',
            old_file_not_in_item: 'Old file not in this title',
            all_versions_unavailable: 'Every version unavailable - review in Plex',
            no_live_version_on_disk: 'Working version not readable on disk',
            old_file_still_on_disk: 'Old file still on disk',
            no_arr_tracks_title: 'No Arr tracks this title',
            arr_still_references_old_file: 'Arr still references the old file',
            arr_current_file_not_live_in_plex: "Arr's current file is not a working Plex version",
            reimported_at_same_path: 'Replaced at the same path',
            playing_now: 'Playing now',
            lookup_failed: 'Lookup failed',
            plex_delete_failed: 'Plex delete failed',
            not_in_any_plex_library: 'Not in a Plex library',
            not_found_in_plex: 'Not found in Plex',
        };
        return labels[reason] || reason || '';
    }

    plexReapStatusBadge(status) {
        const cls = {
            reapable: 'badge-warning', reaped: 'badge-success', would_reap: 'badge-info',
            skipped: 'badge-ghost', waiting: 'badge-ghost', gave_up: 'badge-ghost', failed: 'badge-error',
        }[status] || 'badge-ghost';
        const label = {reapable: 'removable', would_reap: 'would remove', gave_up: 'gave up'}[status] || status;
        return `<span class="badge badge-sm ${cls}">${this.escape(label)}</span>`;
    }

    renderPlexReap() {
        const $ = (id) => document.getElementById(id);
        const st = this.plexReap.status || {};
        const scan = st.scan || {};
        const candidates = scan.candidates || [];

        const summary = $('plexReapSummary');
        if (summary) {
            const modeLabel = {off: 'off', dry_run: 'dry run', on: 'on'}[st.mode] || 'off';
            const counts = {reapable: 0, review: 0, other: 0, done: 0};
            candidates.forEach((c) => counts[this.plexReapGroup(c)]++);
            let text = `Automatic removal: <b>${this.escape(modeLabel)}</b>.`;
            if (scan.running) {
                text += ` Scanning ${this.escape(scan.section || '')}: ${scan.scanned || 0} of ${scan.total || '?'} items...`;
            } else if (scan.applying) {
                text += ' Removing selected versions...';
            } else if (scan.finished_at) {
                text += ` Last scan ${this.escape(new Date(scan.finished_at).toLocaleString())}: ${counts.reapable} removable, ${counts.review} need review, ${counts.other} not removable, ${counts.done} processed.`;
            }
            if (scan.error) text += ` <span class="text-error">Scan error: ${this.escape(scan.error)}</span>`;
            if (scan.applied) {
                const a = scan.applied;
                text += ` Last removal: ${a.reaped || 0} removed, ${a.skipped || 0} skipped, ${a.failed || 0} failed.`;
            }
            summary.innerHTML = text;
        }

        const filter = this.plexReap.filter;
        const shown = candidates.filter((c) => filter === 'all' || this.plexReapGroup(c) === filter);
        const body = $('plexReapTableBody');
        if (body) {
            body.innerHTML = shown.map((c) => {
                const selectable = c.status === 'reapable';
                const checked = this.plexReap.selected.has(c.rating_key) ? 'checked' : '';
                const staleFiles = (c.stale || []).flatMap((m) => m.files || []);
                const reason = c.reason ? `<div class="text-xs opacity-70">${this.escape(this.plexReapReasonLabel(c.reason))}</div>` : '';
                const detail = c.detail ? `<div class="text-xs opacity-50 break-all">${this.escape(c.detail)}</div>` : '';
                return `<tr>
                    <td>${selectable ? `<input type="checkbox" class="checkbox checkbox-sm" data-rating-key="${this.escape(c.rating_key)}" ${checked}>` : ''}</td>
                    <td class="text-sm">${this.escape(c.title)}</td>
                    <td class="text-xs">${this.escape(c.section_title)}</td>
                    <td class="font-mono text-xs break-all">${staleFiles.map((f) => this.escape(f.split('/').pop())).join('<br>')}</td>
                    <td>${this.plexReapStatusBadge(c.status)}${reason}${detail}</td>
                </tr>`;
            }).join('');
        }
        const empty = $('plexReapEmpty');
        if (empty) {
            empty.classList.toggle('hidden', shown.length > 0);
            empty.textContent = candidates.length === 0
                ? (scan.running ? 'Scanning...' : 'No scan yet. Scan the library to list stale versions.')
                : 'Nothing in this view.';
        }
        const selectAll = $('plexReapSelectAll');
        if (selectAll) selectAll.checked = false;
        if ($('plexReapScanBtn')) $('plexReapScanBtn').disabled = !!(scan.running || scan.applying);

        const pending = $('plexReapPending');
        if (pending) {
            const jobs = st.pending || [];
            pending.textContent = jobs.length
                ? `${jobs.length} replaced file${jobs.length === 1 ? '' : 's'} waiting: ${jobs.map((j) => `${j.title_hint || (j.stale_paths || [])[0]?.split('/').pop() || ''} (${j.source}${j.last_reason ? ', ' + this.plexReapReasonLabel(j.last_reason) : ''})`).join('; ')}`
                : 'No replaced files waiting.';
        }
        const decisions = $('plexReapDecisionsBody');
        if (decisions) {
            const rows = (st.decisions || []).slice(0, 50);
            decisions.innerHTML = rows.length ? rows.map((d) => {
                const files = (d.paths || []).map((f) => this.escape(f.split('/').pop())).join('<br>');
                const reason = d.reason ? `<div class="text-xs opacity-70">${this.escape(this.plexReapReasonLabel(d.reason))}</div>` : '';
                return `<tr>
                    <td class="text-xs whitespace-nowrap">${this.escape(new Date(d.time).toLocaleString())}</td>
                    <td class="text-xs">${this.escape(d.source)}</td>
                    <td class="text-sm">${this.escape(d.title || '')}<div class="font-mono text-xs opacity-60 break-all">${files}</div></td>
                    <td>${this.plexReapStatusBadge(d.status)}${reason}</td>
                </tr>`;
            }).join('') : '<tr><td colspan="4" class="text-center text-sm opacity-60">No decisions yet.</td></tr>';
        }
        this.updatePlexReapApplyBtn();
    }

    togglePlexReapSelectAll(checked) {
        const candidates = (this.plexReap.status?.scan?.candidates) || [];
        const filter = this.plexReap.filter;
        candidates
            .filter((c) => c.status === 'reapable' && (filter === 'all' || this.plexReapGroup(c) === filter))
            .forEach((c) => checked ? this.plexReap.selected.add(c.rating_key) : this.plexReap.selected.delete(c.rating_key));
        document.querySelectorAll('#plexReapTableBody input[data-rating-key]').forEach((el) => {
            el.checked = checked;
        });
        this.updatePlexReapApplyBtn();
    }

    updatePlexReapApplyBtn() {
        const btn = document.getElementById('plexReapApplyBtn');
        if (!btn) return;
        const scan = this.plexReap.status?.scan || {};
        const n = this.plexReap.selected.size;
        btn.disabled = n === 0 || !!(scan.running || scan.applying);
        btn.innerHTML = `<i class="bi bi-trash3 mr-1"></i>Remove selected${n ? ` (${n})` : ''}`;
    }

    async startPlexReapScan() {
        try {
            const res = await fetch(`${this.api}/plex/reap/scan`, {method: 'POST'});
            if (!res.ok) throw new Error((await res.text()) || `HTTP ${res.status}`);
            this.plexReap.selected.clear();
            this.toast('Plex scan started', 'info');
        } catch (e) {
            this.toast(`Scan failed: ${e.message}`, 'error');
        }
        this.loadPlexReap();
    }

    async applyPlexReap() {
        const keys = [...this.plexReap.selected];
        if (!keys.length) return;
        if (!confirm(`Remove the unavailable Plex version from ${keys.length} title${keys.length === 1 ? '' : 's'}? Each is re-checked first; working versions and watch history are kept.`)) return;
        try {
            const res = await fetch(`${this.api}/plex/reap/apply`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({rating_keys: keys}),
            });
            if (!res.ok) throw new Error((await res.text()) || `HTTP ${res.status}`);
            this.plexReap.selected.clear();
            this.toast('Removing selected versions', 'info');
        } catch (e) {
            this.toast(`Remove failed: ${e.message}`, 'error');
        }
        this.loadPlexReap();
    }

    async testPlexConnection() {
        const $ = (id) => document.getElementById(id);
        const btn = $('plexTestBtn');
        const result = $('plexTestResult');
        const url = $('plexUrl')?.value.trim() || '';
        // Blank token falls back to the saved one server-side - see
        // handlePlexTestConnection.
        const typedToken = $('plexToken')?.value || '';
        if (!url) {
            if (result) {
                result.textContent = 'Enter a server URL first';
                result.className = 'text-sm text-warning';
            }
            return;
        }
        if (btn) btn.disabled = true;
        if (result) {
            result.textContent = 'Testing…';
            result.className = 'text-sm opacity-70';
        }
        try {
            const res = await fetch(`${this.api}/plex/test`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({url, token: typedToken || PLEX_TOKEN_PLACEHOLDER}),
            });
            const text = await res.text();
            let data = null;
            try {
                data = text ? JSON.parse(text) : null;
            } catch { /* leave null */
            }
            if (!res.ok) {
                throw new Error((data && (data.error || data.message)) || text || `HTTP ${res.status}`);
            }
            if (result) {
                if (data && data.ok) {
                    result.textContent = 'Connected';
                    result.className = 'text-sm text-success';
                } else {
                    result.textContent = `Unreachable: ${(data && data.error) || 'unknown error'}`;
                    result.className = 'text-sm text-error';
                }
            }
        } catch (e) {
            if (result) {
                result.textContent = `Unreachable: ${e.message}`;
                result.className = 'text-sm text-error';
            }
        } finally {
            if (btn) btn.disabled = false;
        }
    }
}

window.RepairManager = RepairManager;
