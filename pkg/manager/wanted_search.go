package manager

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
)

const (
	wantedSearchTag = "arr-wanted-search"
	// wantedSearchCoalesce is how long after a search is sent to an instance
	// another scheduled firing for it is skipped: two entries for one Sonarr
	// whose schedules land on the same minute ("12:00" and "0 12 * * *").
	wantedSearchCoalesce = 10 * time.Minute
	// wantedSearchCallTimeout bounds each call to an Arr. The shared Arr
	// client has no timeout of its own and retries, so without this an Arr
	// that is down would hold the send lock for every later firing.
	wantedSearchCallTimeout = 30 * time.Second
)

// WantedSearch asks each Sonarr/Radarr that has a wanted-search schedule
// (config.Arr.WantedSearch) to search for everything it is missing - the
// same as its Wanted -> Missing -> Search All.
//
// One search per instance: Arrs sharing an API key share one job per
// schedule, and a firing sends one search per (app, API key). A Sonarr
// listed as "sonarr" and again as the download-client category "tv" is asked
// once, while a Sonarr and a Radarr given the same key (a common docker env
// setup) are each still asked. The Arr also refuses to queue a second
// identical command while one is queued or running.
type WantedSearch struct {
	scheduler gocron.Scheduler
	lookup    func(name string) *arr.Arr // live Arr storage
	logger    zerolog.Logger

	mu        sync.Mutex // guards parentCtx and jobs
	parentCtx context.Context
	jobs      map[wantedJobKey]gocron.Job

	sendMu   sync.Mutex // serialises sends; guards lastSent
	lastSent map[string]wantedSent

	resultsMu sync.Mutex
	results   map[string]WantedSearchResult // Arr name -> its last outcome
}

// wantedJobKey identifies one scheduled job: the Arrs sharing an API key and
// a schedule string.
type wantedJobKey struct {
	token    string
	schedule string
}

type wantedSent struct {
	at  time.Time
	arr string
}

// WantedSearchResult is what one Arr's part of a firing did.
type WantedSearchResult struct {
	At      time.Time `json:"at"`
	Trigger string    `json:"trigger"` // "scheduled" | "manual"
	Arr     string    `json:"arr"`
	// App is what the Arr's system/status called itself ("Sonarr").
	App           string `json:"app,omitempty"`
	Sent          bool   `json:"sent"`
	CommandID     int    `json:"command_id,omitempty"`
	CommandStatus string `json:"command_status,omitempty"`
	// Skipped says why nothing was sent when that was expected (a duplicate,
	// not Sonarr/Radarr); Error when something failed.
	Skipped string `json:"skipped,omitempty"`
	Error   string `json:"error,omitempty"`
}

// WantedSearchArrStatus is one Arr's row in WantedSearchStatus.
type WantedSearchArrStatus struct {
	Name     string `json:"name"`
	Enabled  bool   `json:"enabled"`
	Schedule string `json:"schedule,omitempty"`
	// NextRunText is the next firing in the server's time zone, the zone the
	// schedule is written in.
	NextRun     *time.Time          `json:"next_run,omitempty"`
	NextRunText string              `json:"next_run_text,omitempty"`
	Last        *WantedSearchResult `json:"last,omitempty"`
}

// WantedSearchStatus is the Settings page's view of every Arr's schedule.
type WantedSearchStatus struct {
	TimeZone string                  `json:"time_zone"`
	Arrs     []WantedSearchArrStatus `json:"arrs"`
}

// NewWantedSearch builds the service; Start registers its jobs.
func NewWantedSearch(scheduler gocron.Scheduler, lookup func(name string) *arr.Arr) *WantedSearch {
	return &WantedSearch{
		scheduler: scheduler,
		lookup:    lookup,
		logger:    logger.New("wanted-search"),
		parentCtx: context.Background(),
		jobs:      make(map[wantedJobKey]gocron.Job),
		lastSent:  make(map[string]wantedSent),
		results:   make(map[string]WantedSearchResult),
	}
}

// Start registers a job per schedule from the live config.
func (w *WantedSearch) Start(ctx context.Context) error {
	w.mu.Lock()
	w.parentCtx = ctx
	w.mu.Unlock()
	return w.ApplyConfig()
}

// Stop unregisters every job.
func (w *WantedSearch) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.scheduler.RemoveByTags(wantedSearchTag)
	w.jobs = make(map[wantedJobKey]gocron.Job)
}

// ApplyConfig re-registers the jobs from the live config. Called after every
// settings save; a search already being sent is not interrupted. Schedules
// are clock times or cron expressions, so re-registering does not move the
// next firing.
func (w *WantedSearch) ApplyConfig() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.scheduler.RemoveByTags(wantedSearchTag)
	w.jobs = make(map[wantedJobKey]gocron.Job)

	var errs []error
	for key, names := range w.groups() {
		jd, err := utils.ConvertToClockOrCronJobDef(key.schedule)
		if err != nil {
			errs = append(errs, fmt.Errorf("wanted search for %s: %w", strings.Join(names, ", "), err))
			continue
		}
		job, err := w.scheduler.NewJob(jd,
			gocron.NewTask(func() { w.runScheduled(key) }),
			gocron.WithTags(wantedSearchTag),
		)
		if err != nil {
			errs = append(errs, fmt.Errorf("wanted search for %s: %w", strings.Join(names, ", "), err))
			continue
		}
		w.jobs[key] = job
		ev := w.logger.Info().Strs("arrs", names).Str("schedule", key.schedule)
		if len(names) > 1 {
			ev.Msg("Wanted search scheduled; these Arrs share an API key, so each is searched once")
		} else {
			ev.Msg("Wanted search scheduled")
		}
	}
	for _, err := range errs {
		w.logger.Warn().Err(err).Msg("Wanted search not scheduled")
	}
	return errors.Join(errs...)
}

// RunNow sends the wanted search to one Arr straight away, whether or not it
// has a schedule. The coalesce window does not apply: the user asked.
func (w *WantedSearch) RunNow(name string) (WantedSearchResult, error) {
	if w.arrFor(name) == nil {
		return WantedSearchResult{}, fmt.Errorf("no Arr named %q with a host and API key", name)
	}
	return w.search([]string{name}, "manual", true)[0], nil
}

// Status reports each configured Arr's schedule, next firing and last outcome.
func (w *WantedSearch) Status() WantedSearchStatus {
	zone, _ := time.Now().Zone()
	out := WantedSearchStatus{TimeZone: zone}

	w.mu.Lock()
	jobs := make(map[wantedJobKey]gocron.Job, len(w.jobs))
	for k, j := range w.jobs {
		jobs[k] = j
	}
	w.mu.Unlock()

	w.resultsMu.Lock()
	defer w.resultsMu.Unlock()
	for _, e := range config.Get().Arrs {
		st := WantedSearchArrStatus{
			Name:     e.Name,
			Enabled:  e.WantedSearch.Enabled,
			Schedule: strings.TrimSpace(e.WantedSearch.Schedule),
		}
		if a := w.arrFor(e.Name); a != nil && st.Enabled {
			if job, ok := jobs[wantedJobKey{token: a.Token, schedule: st.Schedule}]; ok {
				if next, err := job.NextRun(); err == nil && !next.IsZero() {
					st.NextRun = &next
					st.NextRunText = next.Format("Mon 2 Jan 15:04 MST")
				}
			}
		}
		if last, ok := w.results[e.Name]; ok {
			st.Last = &last
		}
		out.Arrs = append(out.Arrs, st)
	}
	return out
}

// groups maps each (API key, schedule) to the Arrs with wanted search on for
// it: manually added entries first, then auto-detected ones, then by name.
func (w *WantedSearch) groups() map[wantedJobKey][]string {
	type member struct {
		name string
		auto bool
	}
	byKey := make(map[wantedJobKey][]member)
	for _, e := range config.Get().Arrs {
		if !e.WantedSearch.Enabled {
			continue
		}
		a := w.arrFor(e.Name)
		if a == nil {
			w.logger.Warn().Str("arr", e.Name).Msg("Wanted search is on, but this Arr has no host or API key")
			continue
		}
		key := wantedJobKey{token: a.Token, schedule: strings.TrimSpace(e.WantedSearch.Schedule)}
		byKey[key] = append(byKey[key], member{name: e.Name, auto: e.Source == string(arr.SourceAuto)})
	}
	out := make(map[wantedJobKey][]string, len(byKey))
	for key, ms := range byKey {
		sort.Slice(ms, func(i, j int) bool {
			if ms[i].auto != ms[j].auto {
				return !ms[i].auto
			}
			return ms[i].name < ms[j].name
		})
		names := make([]string, len(ms))
		for i, m := range ms {
			names[i] = m.name
		}
		out[key] = names
	}
	return out
}

// arrFor returns the Arr to call for name: the live one (whose host a
// download-client handshake may have refreshed), else one built from its
// config entry. nil when neither has a host and API key.
func (w *WantedSearch) arrFor(name string) *arr.Arr {
	if w.lookup != nil {
		if a := w.lookup(name); a != nil && a.Host != "" && strings.TrimSpace(a.Token) != "" {
			return a
		}
	}
	for _, e := range config.Get().Arrs {
		if e.Name == name && e.Host != "" && strings.TrimSpace(e.Token) != "" {
			return arr.New(e.Name, e.Host, e.Token, e.SkipRepair, e.DownloadUncached, e.SelectedDebrid, e.Source)
		}
	}
	return nil
}

func (w *WantedSearch) runScheduled(key wantedJobKey) {
	names := w.groups()[key]
	if len(names) == 0 {
		// No Arr has this job's API key any more: the key changed in place
		// (an auto-detected Arr picks up a new key from its download-client
		// login) without a settings save. Re-register the jobs from the
		// current Arrs so the next run happens on schedule; left alone, this
		// job fired forever and searched nothing. On its own goroutine, so a
		// job never removes itself from inside its own run.
		w.logger.Warn().Str("schedule", key.schedule).
			Msg("Wanted search: no Arr has this job's API key any more; rescheduling from the current Arrs")
		go func() {
			if err := w.ApplyConfig(); err != nil {
				w.logger.Warn().Err(err).Msg("Wanted search: rescheduling after an API key change failed")
			}
		}()
		return
	}
	w.search(names, "scheduled", false)
}

func (w *WantedSearch) ctx() context.Context {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.parentCtx
}

// search sends the wanted search through each of names in order, once per
// (app, API key): later names on an instance already searched are skipped.
func (w *WantedSearch) search(names []string, trigger string, force bool) []WantedSearchResult {
	ctx := w.ctx()
	searched := make(map[string]string) // instance -> the Arr name it was searched through
	out := make([]WantedSearchResult, 0, len(names))
	for _, name := range names {
		res := w.searchOne(ctx, name, trigger, force, searched)
		w.resultsMu.Lock()
		w.results[name] = res
		w.resultsMu.Unlock()
		out = append(out, res)
	}
	return out
}

func (w *WantedSearch) searchOne(ctx context.Context, name, trigger string, force bool, searched map[string]string) WantedSearchResult {
	res := WantedSearchResult{At: time.Now(), Trigger: trigger, Arr: name}
	a := w.arrFor(name)
	if a == nil {
		res.Skipped = "no host or API key"
		return res
	}
	callCtx, cancel := context.WithTimeout(ctx, wantedSearchCallTimeout)
	defer cancel()

	st, err := a.GetSystemStatus(callCtx)
	if err != nil {
		res.Error = "could not ask the Arr which app it is: " + err.Error()
		w.logger.Warn().Err(err).Str("arr", name).Msg("Wanted search not sent: the Arr did not answer")
		return res
	}
	res.App = st.AppName
	app := arr.TypeFromAppName(st.AppName)
	if app != arr.Sonarr && app != arr.Radarr {
		res.Skipped = fmt.Sprintf("%q is not Sonarr or Radarr", st.AppName)
		w.logger.Info().Str("arr", name).Str("app", st.AppName).Msg("Wanted search skipped: only Sonarr and Radarr are supported")
		return res
	}

	instance := string(app) + "\x00" + strings.TrimSpace(a.Token)
	if by, ok := searched[instance]; ok {
		res.Skipped = fmt.Sprintf("same %s and API key as %q, searched once", st.AppName, by)
		w.logger.Info().Str("arr", name).Str("searched_via", by).Str("app", st.AppName).
			Msg("Wanted search skipped: same instance as another Arr entry")
		return res
	}

	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	if last, ok := w.lastSent[instance]; ok && !force && time.Since(last.at) < wantedSearchCoalesce {
		searched[instance] = last.arr
		res.Skipped = fmt.Sprintf("%s was searched %s ago through %q", st.AppName, time.Since(last.at).Round(time.Second), last.arr)
		w.logger.Info().Str("arr", name).Str("searched_via", last.arr).Str("app", st.AppName).
			Msg("Wanted search skipped: this instance was just searched")
		return res
	}

	cmd, err := a.SearchWantedMissing(callCtx, app)
	if err != nil {
		res.Error = err.Error()
		w.logger.Warn().Err(err).Str("arr", name).Str("app", st.AppName).Msg("Wanted search failed")
		return res
	}
	w.lastSent[instance] = wantedSent{at: time.Now(), arr: name}
	searched[instance] = name
	res.Sent = true
	res.CommandID = cmd.ID
	res.CommandStatus = cmd.Status
	w.logger.Info().Str("arr", name).Str("app", st.AppName).Str("trigger", trigger).
		Int("command_id", cmd.ID).Str("command_status", cmd.Status).Msg("Wanted search sent")
	return res
}
