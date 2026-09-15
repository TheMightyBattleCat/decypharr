// PlexReaper removes a title's stale Plex versions once the file behind them
// has been replaced - the "Unavailable" entries Plex keeps after an Arr
// upgrade or rename, or a decypharr re-grab, because the server's automatic
// "Empty trash after every scan" is (rightly, on a mount-backed library) off.
//
// It only ever acts on the specific title that changed (event jobs) or on
// items the user chose from a backlog scan, never section-wide, and only
// through evaluateReap's guards. See config.PlexConfig.ReapMode.
package manager

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	json "github.com/bytedance/sonic"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/arr"
)

// Job sources.
const (
	ReapSourceUpgrade = "upgrade"
	ReapSourceRename  = "rename"
	ReapSourceRepair  = "repair"
)

const (
	plexReapJobsFile      = "plex_reap_jobs.json"
	plexReapTick          = 15 * time.Second
	plexReapDecisionCap   = 300
	plexReapSectionTTL    = 5 * time.Minute
	plexReapScanSettle    = 20 * time.Second
	plexReapEventMaxTries = 10
	plexReapMaxWalks      = 2
	// A re-grab can take a long time to find and import a replacement.
	plexReapRepairMaxAge   = 72 * time.Hour
	plexReapRepairFirstGap = 5 * time.Minute
	plexReapRepairMaxGap   = 30 * time.Minute
)

// plexReapJob is one "these old files were replaced" notice.
type plexReapJob struct {
	ID          string    `json:"id"`
	Source      string    `json:"source"`
	StalePaths  []string  `json:"stale_paths"`
	CurrentPath string    `json:"current_path,omitempty"`
	TitleHint   string    `json:"title_hint,omitempty"`
	ArrName     string    `json:"arr_name,omitempty"`
	MediaID     int       `json:"media_id,omitempty"` // Radarr movie id / Sonarr series id
	CreatedAt   time.Time `json:"created_at"`
	NextAt      time.Time `json:"next_at"`
	Attempts    int       `json:"attempts"`
	Scanned     bool      `json:"scanned"`
	// Walks counts whole-section item walks (title search missed), capped
	// at plexReapMaxWalks per job.
	Walks      int    `json:"walks,omitempty"`
	LastStatus string `json:"last_status,omitempty"`
	LastReason string `json:"last_reason,omitempty"`
}

// PlexReapDecision is one logged outcome, shown on the repair page.
type PlexReapDecision struct {
	Time      time.Time `json:"time"`
	Source    string    `json:"source"`
	Title     string    `json:"title,omitempty"`
	RatingKey string    `json:"rating_key,omitempty"`
	Section   string    `json:"section,omitempty"`
	MediaIDs  []int64   `json:"media_ids,omitempty"`
	Paths     []string  `json:"paths,omitempty"`
	Status    string    `json:"status"` // reaped, would_reap, skipped, gave_up, failed
	Reason    string    `json:"reason,omitempty"`
	Detail    string    `json:"detail,omitempty"`
}

// PlexReapCandidate is one item found by a backlog scan.
type PlexReapCandidate struct {
	SectionKey   string          `json:"section_key"`
	SectionTitle string          `json:"section_title"`
	SectionType  string          `json:"section_type"`
	RatingKey    string          `json:"rating_key"`
	Title        string          `json:"title"`
	Stale        []PlexReapMedia `json:"stale"`
	Live         []PlexReapMedia `json:"live"`
	Status       string          `json:"status"`
	Reason       string          `json:"reason,omitempty"`
	Detail       string          `json:"detail,omitempty"`
}

// PlexReapMedia is one version and its files.
type PlexReapMedia struct {
	ID    int64    `json:"id"`
	Files []string `json:"files"`
}

// PlexReapScan is the backlog scan state.
type PlexReapScan struct {
	Running    bool                `json:"running"`
	StartedAt  time.Time           `json:"started_at,omitempty"`
	FinishedAt time.Time           `json:"finished_at,omitempty"`
	Section    string              `json:"section,omitempty"`
	Scanned    int                 `json:"scanned"`
	Total      int                 `json:"total"`
	Error      string              `json:"error,omitempty"`
	Candidates []PlexReapCandidate `json:"candidates"`
	// Applying is true while a backlog apply runs; Applied is its result.
	Applying bool                 `json:"applying"`
	Applied  *PlexReapApplyResult `json:"applied,omitempty"`
}

// PlexReapStatus is the repair page's view of the reaper.
type PlexReapStatus struct {
	Mode      config.PlexReapMode `json:"mode"`
	Pending   []plexReapJob       `json:"pending"`
	Decisions []PlexReapDecision  `json:"decisions"`
	Scan      PlexReapScan        `json:"scan"`
}

// PlexReaper is the stale-version reaper service.
type PlexReaper struct {
	logger   zerolog.Logger
	library  *plexLibrary
	resolver *reapArrResolver
	jobsPath string
	now      func() time.Time
	env      func(reapArrLookup, func() (bool, error)) reapEnv

	// actMu serialises evaluate-then-delete so the worker and a backlog
	// apply never act on the same item concurrently.
	actMu sync.Mutex

	mu        sync.Mutex
	jobs      map[string]*plexReapJob
	decisions []PlexReapDecision
	scan      PlexReapScan
	sections  []plexSection
	sectAt    time.Time

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewPlexReaper builds the reaper. arrs returns the Arrs to consult (one per
// distinct host).
func NewPlexReaper(arrs func() []*arr.Arr) *PlexReaper {
	lib := newPlexLibrary()
	r := &PlexReaper{
		logger:   logger.New("plex-reap"),
		library:  lib,
		resolver: newReapArrResolver(arrs, lib),
		jobsPath: filepath.Join(config.GetMainPath(), plexReapJobsFile),
		now:      time.Now,
		env:      defaultReapEnv,
		jobs:     make(map[string]*plexReapJob),
	}
	r.load()
	return r
}

func (r *PlexReaper) mode() config.PlexReapMode {
	return r.library.cfg().Reap()
}

// Start launches the job worker.
func (r *PlexReaper) Start(ctx context.Context) {
	r.ctx, r.cancel = context.WithCancel(ctx)
	r.wg.Add(1)
	go r.loop()
}

// Stop stops the worker and a running backlog scan.
func (r *PlexReaper) Stop() {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
}

func (r *PlexReaper) loop() {
	defer r.wg.Done()
	t := time.NewTicker(plexReapTick)
	defer t.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-t.C:
		}
		r.runDue(r.ctx)
	}
}

// --- job persistence ---

func (r *PlexReaper) load() {
	data, err := os.ReadFile(r.jobsPath)
	if err != nil {
		return
	}
	var jobs []*plexReapJob
	if err := json.Unmarshal(data, &jobs); err != nil {
		r.logger.Warn().Err(err).Msg("Plex reap: ignoring unreadable pending-jobs file")
		return
	}
	for _, j := range jobs {
		if j != nil && j.ID != "" {
			r.jobs[j.ID] = j
		}
	}
}

// saveLocked writes pending jobs atomically. Caller holds r.mu.
func (r *PlexReaper) saveLocked() {
	jobs := make([]*plexReapJob, 0, len(r.jobs))
	for _, j := range r.jobs {
		jobs = append(jobs, j)
	}
	sort.Slice(jobs, func(a, b int) bool { return jobs[a].CreatedAt.Before(jobs[b].CreatedAt) })
	data, err := json.Marshal(jobs)
	if err != nil {
		return
	}
	tmp := r.jobsPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		r.logger.Warn().Err(err).Msg("Plex reap: failed to persist pending jobs")
		return
	}
	_ = os.Rename(tmp, r.jobsPath)
}

// --- enqueue ---

func reapJobID(paths []string) string {
	sorted := make([]string, 0, len(paths))
	for _, p := range paths {
		sorted = append(sorted, filepath.Clean(p))
	}
	sort.Strings(sorted)
	h := sha1.New()
	for _, p := range sorted {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// PlexReapNotice describes replaced files, from a webhook or a re-grab.
type PlexReapNotice struct {
	Source      string
	StalePaths  []string
	CurrentPath string
	TitleHint   string
	ArrName     string
	MediaID     int
}

// Enqueue records replaced files for reaping. A no-op while the reaper is
// off. The same old files noticed twice (duplicate deliveries, or an Arr
// configured under two names) merge into one job.
func (r *PlexReaper) Enqueue(n PlexReapNotice) {
	if r == nil || r.mode() == config.PlexReapOff {
		return
	}
	var stale []string
	for _, p := range n.StalePaths {
		if p == "" {
			continue
		}
		p = filepath.Clean(p)
		if n.CurrentPath != "" && p == filepath.Clean(n.CurrentPath) {
			continue // replaced in place: Plex has nothing stale
		}
		if !slices.Contains(stale, p) {
			stale = append(stale, p)
		}
	}
	if len(stale) == 0 {
		return
	}
	now := r.now()
	id := reapJobID(stale)
	first := now.Add(plexReapScanSettle)
	if n.Source == ReapSourceRepair {
		first = now.Add(plexReapRepairFirstGap)
	}

	r.mu.Lock()
	if j, ok := r.jobs[id]; ok {
		if n.CurrentPath != "" {
			j.CurrentPath = n.CurrentPath
		}
		if n.TitleHint != "" {
			j.TitleHint = n.TitleHint
		}
		// A webhook for a pending re-grab job means the replacement landed.
		if n.Source != ReapSourceRepair && j.NextAt.After(first) {
			j.NextAt = first
		}
	} else {
		r.jobs[id] = &plexReapJob{
			ID: id, Source: n.Source, StalePaths: stale, CurrentPath: n.CurrentPath,
			TitleHint: n.TitleHint, ArrName: n.ArrName, MediaID: n.MediaID,
			CreatedAt: now, NextAt: first,
		}
	}
	r.saveLocked()
	r.mu.Unlock()
	r.logger.Info().Str("source", n.Source).Strs("old_files", stale).Str("title", n.TitleHint).
		Msg("Plex reap: queued replaced files")
}

// NudgeFolder brings forward pending re-grab jobs whose old files sit under
// dir - called when an Arr reports an import there, which is when a re-grab's
// replacement has landed.
func (r *PlexReaper) NudgeFolder(dir string) {
	if r == nil || dir == "" {
		return
	}
	soon := r.now().Add(plexReapScanSettle)
	nudged := false
	r.mu.Lock()
	for _, j := range r.jobs {
		for _, p := range j.StalePaths {
			if pathUnder(p, dir) && j.NextAt.After(soon) {
				j.NextAt = soon
				j.Scanned = false
				nudged = true
				break
			}
		}
	}
	if nudged {
		r.saveLocked()
	}
	r.mu.Unlock()
}

// --- worker ---

func (r *PlexReaper) record(d PlexReapDecision) {
	d.Time = r.now()
	r.mu.Lock()
	r.decisions = append(r.decisions, d)
	if over := len(r.decisions) - plexReapDecisionCap; over > 0 {
		r.decisions = slices.Delete(r.decisions, 0, over)
	}
	r.mu.Unlock()

	ev := r.logger.Info()
	if d.Status == "failed" {
		ev = r.logger.Warn()
	}
	ev.Str("source", d.Source).Str("title", d.Title).Str("rating_key", d.RatingKey).
		Interface("media_ids", d.MediaIDs).Strs("files", d.Paths).Str("status", d.Status).
		Str("reason", d.Reason).Str("detail", d.Detail).Msg("Plex reap: decision")
}

func (r *PlexReaper) cachedSections(ctx context.Context) ([]plexSection, error) {
	r.mu.Lock()
	if r.sections != nil && r.now().Sub(r.sectAt) < plexReapSectionTTL {
		s := r.sections
		r.mu.Unlock()
		return s, nil
	}
	r.mu.Unlock()
	s, err := r.library.Sections(ctx)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.sections, r.sectAt = s, r.now()
	r.mu.Unlock()
	return s, nil
}

func (r *PlexReaper) runDue(ctx context.Context) {
	if r.mode() == config.PlexReapOff {
		return
	}
	now := r.now()
	r.mu.Lock()
	var due []plexReapJob
	for _, j := range r.jobs {
		if !j.NextAt.After(now) {
			due = append(due, *j)
		}
	}
	r.mu.Unlock()
	sort.Slice(due, func(a, b int) bool { return due[a].NextAt.Before(due[b].NextAt) })

	for i := range due {
		if ctx.Err() != nil {
			return
		}
		job := due[i]
		done := r.process(ctx, &job)
		r.mu.Lock()
		if done {
			delete(r.jobs, job.ID)
		} else if cur, ok := r.jobs[job.ID]; ok {
			// Keep a nudge that arrived while processing.
			next := job.NextAt
			if cur.NextAt.After(now) && cur.NextAt.Before(next) {
				next = cur.NextAt
			}
			job.NextAt = next
			*cur = job
		}
		r.saveLocked()
		r.mu.Unlock()
	}
}

// retryGap is the wait before attempt n+1 of job.
func retryGap(job *plexReapJob) time.Duration {
	if job.Source == ReapSourceRepair {
		gap := plexReapRepairFirstGap << min(job.Attempts, 4)
		return min(gap, plexReapRepairMaxGap)
	}
	return plexReapScanSettle << min(job.Attempts, 7)
}

// expired reports whether job should give up.
func (r *PlexReaper) expired(job *plexReapJob) bool {
	if job.Source == ReapSourceRepair {
		return r.now().Sub(job.CreatedAt) > plexReapRepairMaxAge
	}
	return job.Attempts >= plexReapEventMaxTries
}

// process runs one attempt of job and reports whether the job is finished.
func (r *PlexReaper) process(ctx context.Context, job *plexReapJob) bool {
	job.Attempts++
	base := PlexReapDecision{Source: job.Source, Title: job.TitleHint, Paths: job.StalePaths}

	sections, err := r.cachedSections(ctx)
	if err != nil {
		return r.retry(job, base, reasonLookupFailed, err.Error())
	}
	var secs []plexSection
	for _, p := range job.StalePaths {
		for _, s := range sectionsForPath(sections, p) {
			if !slices.ContainsFunc(secs, func(x plexSection) bool { return x.Key == s.Key }) {
				secs = append(secs, s)
			}
		}
	}
	if len(secs) == 0 {
		base.Status, base.Reason = reapSkipped, "not_in_any_plex_library"
		r.record(base)
		return true
	}

	if job.TitleHint == "" && job.ArrName != "" && job.MediaID > 0 {
		job.TitleHint = r.arrTitle(ctx, job.ArrName, job.MediaID)
		base.Title = job.TitleHint
	}

	// Ask Plex to look at the folders once per round, then give the scan a
	// moment before evaluating. A re-grab's folder is rescanned each round
	// (cheap: one folder), since nothing else tells Plex the old file went.
	if !job.Scanned {
		dirs := map[string]bool{}
		for _, p := range append(slices.Clone(job.StalePaths), job.CurrentPath) {
			if p != "" {
				dirs[filepath.Dir(p)] = true
			}
		}
		for _, s := range secs {
			for d := range dirs {
				if err := r.library.RefreshPath(ctx, s.Key, d); err != nil {
					r.logger.Debug().Err(err).Str("section", s.Title).Str("dir", d).Msg("Plex reap: folder scan request failed")
				}
			}
		}
		job.Scanned = true
		job.Attempts-- // the scan request isn't an evaluation attempt
		job.NextAt = r.now().Add(plexReapScanSettle)
		return false
	}

	lookupPaths := slices.Clone(job.StalePaths)
	if job.CurrentPath != "" {
		lookupPaths = append(lookupPaths, job.CurrentPath)
	}

	var waitReason, waitDetail string
	var skips []PlexReapDecision
	found := false
	walk := job.Walks < plexReapMaxWalks
	if walk {
		job.Walks++
	}
	for _, s := range secs {
		item, err := r.library.FindItemByFile(ctx, s, job.TitleHint, lookupPaths, walk)
		if err != nil {
			waitReason, waitDetail = reasonLookupFailed, err.Error()
			continue
		}
		if item == nil {
			continue
		}
		found = true
		d := base
		d.Section, d.RatingKey, d.Title = s.Title, string(item.RatingKey), item.displayTitle()
		v := r.act(ctx, s, item, job.StalePaths, r.mode() == config.PlexReapOn, &d)
		switch v.Status {
		case reapReapable:
			// act recorded the outcome.
		case reapWaiting:
			waitReason, waitDetail = v.Reason, v.Detail
		default:
			if v.Reason == "plex_delete_failed" {
				continue // act recorded it
			}
			d.Status, d.Reason, d.Detail = reapSkipped, v.Reason, v.Detail
			skips = append(skips, d)
		}
	}

	if waitReason != "" {
		return r.retry(job, base, waitReason, waitDetail)
	}
	if !found {
		return r.retry(job, base, "not_found_in_plex", "")
	}
	for _, d := range skips {
		r.record(d)
	}
	return true
}

// retry reschedules job, or gives up once it has run out of attempts.
func (r *PlexReaper) retry(job *plexReapJob, base PlexReapDecision, reason, detail string) bool {
	job.LastStatus, job.LastReason = reapWaiting, reason
	if r.expired(job) {
		base.Status, base.Reason, base.Detail = "gave_up", reason, detail
		r.record(base)
		return true
	}
	job.NextAt = r.now().Add(retryGap(job))
	if job.Source == ReapSourceRepair && job.Attempts%3 == 0 {
		job.Scanned = false
	}
	return false
}

// act evaluates item and, when reapable, removes (remove=true) or reports
// (dry run) its stale versions, recording the outcome in d. Returns the
// verdict of the evaluation that decided.
func (r *PlexReaper) act(ctx context.Context, s plexSection, item *plexItem, targets []string, remove bool, d *PlexReapDecision) reapVerdict {
	r.actMu.Lock()
	defer r.actMu.Unlock()

	v := r.evaluate(ctx, s, item, targets)
	if v.Status != reapReapable {
		return v
	}
	d.MediaIDs, d.Paths = nil, nil
	for _, m := range v.Stale {
		d.MediaIDs = append(d.MediaIDs, int64(m.ID))
		d.Paths = append(d.Paths, mediaFiles(m)...)
	}
	if !remove {
		d.Status = "would_reap"
		r.record(*d)
		return v
	}

	// Re-read the item right before deleting: the listing we evaluated may
	// be seconds (backlog: minutes) old.
	fresh, err := r.library.Metadata(ctx, string(item.RatingKey))
	if err != nil {
		return reapVerdict{Status: reapWaiting, Reason: reasonLookupFailed, Detail: err.Error()}
	}
	v = r.evaluate(ctx, s, fresh, targets)
	if v.Status != reapReapable {
		return v
	}
	d.MediaIDs, d.Paths = nil, nil
	for _, m := range v.Stale {
		if err := r.library.DeleteMedia(ctx, string(fresh.RatingKey), int64(m.ID)); err != nil {
			d.Status, d.Reason, d.Detail = "failed", "plex_delete_failed", err.Error()
			d.MediaIDs, d.Paths = []int64{int64(m.ID)}, mediaFiles(m)
			r.record(*d)
			return reapVerdict{Status: reapSkipped, Reason: "plex_delete_failed", Detail: err.Error()}
		}
		d.MediaIDs = append(d.MediaIDs, int64(m.ID))
		d.Paths = append(d.Paths, mediaFiles(m)...)
	}
	d.Status = "reaped"
	r.record(*d)
	return v
}

func (r *PlexReaper) evaluate(ctx context.Context, s plexSection, item *plexItem, targets []string) reapVerdict {
	arrLookup := func() (bool, map[string]bool, error) {
		return r.resolver.lookup(ctx, item, s.Type)
	}
	playing := func() (bool, error) {
		keys, err := r.library.PlayingRatingKeys(ctx)
		if err != nil {
			return false, err
		}
		return keys[string(item.RatingKey)], nil
	}
	return evaluateReap(item, targets, r.env(arrLookup, playing))
}

// arrTitle looks up a movie or series title for a re-grab job.
func (r *PlexReaper) arrTitle(ctx context.Context, arrName string, mediaID int) string {
	for _, a := range r.resolver.arrs() {
		if a.Name != arrName {
			continue
		}
		var rec struct {
			Title string `json:"title"`
		}
		endpoint := "api/v3/movie/"
		if r.resolver.arrType(ctx, a) == arr.Sonarr {
			endpoint = "api/v3/series/"
		}
		if resp, err := a.RequestCtx(ctx, http.MethodGet, endpoint+strconv.Itoa(mediaID), nil, &rec); err == nil && resp.StatusCode == http.StatusOK {
			return rec.Title
		}
	}
	return ""
}

// --- backlog ---

// StartScan walks every movie/show section for items with unavailable
// versions and evaluates each (no changes made). Returns false when a scan
// is already running.
func (r *PlexReaper) StartScan() bool {
	r.mu.Lock()
	if r.scan.Running || r.scan.Applying {
		r.mu.Unlock()
		return false
	}
	r.scan = PlexReapScan{Running: true, StartedAt: r.now()}
	r.mu.Unlock()

	ctx := context.Background()
	if r.ctx != nil {
		ctx = r.ctx
	}
	go func() {
		err := r.runScan(ctx)
		r.mu.Lock()
		r.scan.Running = false
		r.scan.FinishedAt = r.now()
		r.scan.Section = ""
		if err != nil {
			r.scan.Error = err.Error()
		}
		r.mu.Unlock()
		if err != nil {
			r.logger.Warn().Err(err).Msg("Plex reap: backlog scan failed")
		}
	}()
	return true
}

func (r *PlexReaper) runScan(ctx context.Context) error {
	r.resolver.resetShowCache()
	sections, err := r.library.Sections(ctx)
	if err != nil {
		return err
	}
	playing, err := r.library.PlayingRatingKeys(ctx)
	if err != nil {
		return err
	}

	// A folder shared by two sections is scanned once per section; items
	// are separate Plex rows, so both are real.
	for _, s := range sections {
		plexType := 1
		switch s.Type {
		case "movie":
		case "show":
			plexType = 4
		default:
			continue
		}
		r.mu.Lock()
		r.scan.Section = s.Title
		r.mu.Unlock()

		for start := 0; ; start += plexScanPageSize {
			if err := ctx.Err(); err != nil {
				return err
			}
			items, total, err := r.library.ListPage(ctx, s.Key, plexType, start, plexScanPageSize)
			if err != nil {
				return err
			}
			for i := range items {
				it := &items[i]
				hasStale := false
				for _, m := range it.Media {
					if m.DeletedAt > 0 {
						hasStale = true
						break
					}
				}
				if !hasStale {
					continue
				}
				arrLookup := func() (bool, map[string]bool, error) { return r.resolver.lookup(ctx, it, s.Type) }
				isPlaying := func() (bool, error) { return playing[string(it.RatingKey)], nil }
				v := evaluateReap(it, nil, r.env(arrLookup, isPlaying))
				r.mu.Lock()
				r.scan.Candidates = append(r.scan.Candidates, candidateFrom(s, it, v))
				r.mu.Unlock()
			}
			r.mu.Lock()
			r.scan.Scanned += len(items)
			if start == 0 {
				r.scan.Total += total
			}
			r.mu.Unlock()
			if len(items) == 0 || start+len(items) >= total {
				break
			}
		}
	}
	return nil
}

func candidateFrom(s plexSection, it *plexItem, v reapVerdict) PlexReapCandidate {
	c := PlexReapCandidate{
		SectionKey: s.Key, SectionTitle: s.Title, SectionType: s.Type,
		RatingKey: string(it.RatingKey), Title: it.displayTitle(),
		Status: v.Status, Reason: v.Reason, Detail: v.Detail,
	}
	for _, m := range it.Media {
		pm := PlexReapMedia{ID: int64(m.ID), Files: mediaFiles(m)}
		if m.DeletedAt > 0 {
			c.Stale = append(c.Stale, pm)
		} else {
			c.Live = append(c.Live, pm)
		}
	}
	return c
}

// PlexReapApplyResult summarises a backlog apply.
type PlexReapApplyResult struct {
	Reaped  int                `json:"reaped"`
	Skipped int                `json:"skipped"`
	Failed  int                `json:"failed"`
	Results []PlexReapDecision `json:"results"`
}

// StartApply runs ApplyScan in the background; progress and the result show
// in Status().Scan. Returns false when a scan or apply is already running.
func (r *PlexReaper) StartApply(ratingKeys []string) bool {
	r.mu.Lock()
	if r.scan.Running || r.scan.Applying {
		r.mu.Unlock()
		return false
	}
	r.scan.Applying = true
	r.scan.Applied = nil
	r.mu.Unlock()

	ctx := context.Background()
	if r.ctx != nil {
		ctx = r.ctx
	}
	go func() {
		res := r.ApplyScan(ctx, ratingKeys)
		r.mu.Lock()
		r.scan.Applying = false
		r.scan.Applied = &res
		r.mu.Unlock()
	}()
	return true
}

// ApplyScan removes the stale versions of the chosen scanned items
// (ratingKeys; empty = every reapable one), re-checking every guard against
// fresh data first. It acts even in dry-run mode - it is an explicit user
// action - but never while Plex is unconfigured.
func (r *PlexReaper) ApplyScan(ctx context.Context, ratingKeys []string) PlexReapApplyResult {
	var res PlexReapApplyResult
	if !r.library.cfg().Enabled() {
		return res
	}
	want := make(map[string]bool, len(ratingKeys))
	for _, k := range ratingKeys {
		want[k] = true
	}
	r.mu.Lock()
	var chosen []PlexReapCandidate
	for _, c := range r.scan.Candidates {
		if len(want) > 0 && !want[c.RatingKey] {
			continue
		}
		if len(want) == 0 && c.Status != reapReapable {
			continue
		}
		chosen = append(chosen, c)
	}
	r.mu.Unlock()

	for _, c := range chosen {
		if ctx.Err() != nil {
			break
		}
		s := plexSection{Key: c.SectionKey, Title: c.SectionTitle, Type: c.SectionType}
		d := PlexReapDecision{Source: "backlog", Title: c.Title, RatingKey: c.RatingKey, Section: c.SectionTitle}
		item, err := r.library.Metadata(ctx, c.RatingKey)
		if err != nil {
			d.Status, d.Reason, d.Detail = "failed", reasonLookupFailed, err.Error()
			r.record(d)
		} else {
			v := r.act(ctx, s, item, nil, true, &d)
			if v.Status != reapReapable {
				d.Status, d.Reason, d.Detail = reapSkipped, v.Reason, v.Detail
				if d.Reason == "plex_delete_failed" {
					d.Status = "failed"
				} else {
					r.record(d)
				}
			}
		}
		switch d.Status {
		case "reaped":
			res.Reaped++
		case "failed":
			res.Failed++
		default:
			res.Skipped++
		}
		res.Results = append(res.Results, d)
		r.updateCandidate(c.RatingKey, d)
	}
	return res
}

func (r *PlexReaper) updateCandidate(ratingKey string, d PlexReapDecision) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.scan.Candidates {
		if r.scan.Candidates[i].RatingKey == ratingKey {
			r.scan.Candidates[i].Status = d.Status
			r.scan.Candidates[i].Reason = d.Reason
			r.scan.Candidates[i].Detail = d.Detail
		}
	}
}

// Status returns a snapshot for the repair page.
func (r *PlexReaper) Status() PlexReapStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := PlexReapStatus{Mode: r.library.cfg().Reap(), Scan: r.scan}
	st.Scan.Candidates = slices.Clone(r.scan.Candidates)
	for _, j := range r.jobs {
		st.Pending = append(st.Pending, *j)
	}
	sort.Slice(st.Pending, func(a, b int) bool { return st.Pending[a].NextAt.Before(st.Pending[b].NextAt) })
	st.Decisions = slices.Clone(r.decisions)
	slices.Reverse(st.Decisions)
	return st
}
