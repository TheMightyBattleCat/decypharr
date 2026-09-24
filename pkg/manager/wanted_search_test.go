package manager

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/go-co-op/gocron/v2"
	"github.com/sirrobot01/decypharr/internal/config"
)

// fakeArr answers system/status as appName and records every command POST.
type fakeArr struct {
	url string

	mu         sync.Mutex
	commands   []map[string]any
	statusCode int // system/status reply; 0 means 200
}

func newFakeArr(t *testing.T, appName string) *fakeArr {
	t.Helper()
	f := &fakeArr{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/system/status":
			if f.statusCode != 0 {
				w.WriteHeader(f.statusCode)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"appName": appName, "version": "4.0.0"})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v3/command":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.commands = append(f.commands, body)
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": len(f.commands), "name": body["name"], "status": "queued"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ts.Close)
	f.url = ts.URL
	return f
}

func (f *fakeArr) commandNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.commands))
	for i, c := range f.commands {
		out[i], _ = c["name"].(string)
	}
	return out
}

func wantedArr(name, host, token, schedule, source string) config.Arr {
	return config.Arr{Name: name, Host: host, Token: token, Source: source,
		WantedSearch: config.ArrWantedSearch{Enabled: true, Schedule: schedule}}
}

// newTestWantedSearch sets the live config's Arrs and returns a service
// with no live Arr storage (entries come from the config).
func newTestWantedSearch(t *testing.T, arrs ...config.Arr) *WantedSearch {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	live := config.Get()
	saved := live.Arrs
	t.Cleanup(func() { config.Get().Arrs = saved })
	live.Arrs = arrs

	sched, err := gocron.NewScheduler()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sched.Shutdown() })
	return NewWantedSearch(sched, nil)
}

// fireAll runs every registered job once, as the scheduler would.
func fireAll(t *testing.T, w *WantedSearch) {
	t.Helper()
	if err := w.ApplyConfig(); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}
	w.mu.Lock()
	keys := make([]wantedJobKey, 0, len(w.jobs))
	for k := range w.jobs {
		keys = append(keys, k)
	}
	w.mu.Unlock()
	for _, k := range keys {
		w.runScheduled(k)
	}
}

// A Sonarr listed twice with one API key - once by hand, once auto-detected
// from a download client category, even on another URL - is searched once.
func TestWantedSearchSameAPIKeySearchesOnce(t *testing.T) {
	sonarr := newFakeArr(t, "Sonarr")
	other := newFakeArr(t, "Sonarr") // the same instance through a second URL
	w := newTestWantedSearch(t,
		wantedArr("tv", other.url, "key-1", "12:00", "auto"),
		wantedArr("sonarr", sonarr.url, "key-1", "12:00", ""),
	)

	fireAll(t, w)

	if n := len(w.scheduler.Jobs()); n != 1 {
		t.Fatalf("jobs = %d, want 1 for the shared key and schedule", n)
	}
	if got := sonarr.commandNames(); len(got) != 1 || got[0] != "MissingEpisodeSearch" {
		t.Fatalf("sonarr commands = %v, want one MissingEpisodeSearch (the hand-added entry goes first)", got)
	}
	if got := other.commandNames(); len(got) != 0 {
		t.Fatalf("duplicate entry sent %v, want nothing", got)
	}
	if m := sonarr.commands[0]["monitored"]; m != true {
		t.Fatalf("MissingEpisodeSearch monitored = %v, want true", m)
	}
	st := w.Status()
	for _, a := range st.Arrs {
		switch a.Name {
		case "sonarr":
			if a.Last == nil || !a.Last.Sent || a.Last.CommandID != 1 {
				t.Fatalf("sonarr last = %+v, want sent as command 1", a.Last)
			}
		case "tv":
			if a.Last == nil || a.Last.Sent || !strings.Contains(a.Last.Skipped, `"sonarr"`) {
				t.Fatalf("tv last = %+v, want skipped naming sonarr", a.Last)
			}
		}
	}
}

// A Sonarr and a Radarr that were given the same API key are each searched,
// each with its own command.
func TestWantedSearchSonarrAndRadarrSharingAKeyBothSearched(t *testing.T) {
	sonarr := newFakeArr(t, "Sonarr")
	radarr := newFakeArr(t, "Radarr")
	w := newTestWantedSearch(t,
		wantedArr("sonarr", sonarr.url, "shared", "12:00", ""),
		wantedArr("radarr", radarr.url, "shared", "12:00", ""),
	)

	fireAll(t, w)

	if got := sonarr.commandNames(); len(got) != 1 || got[0] != "MissingEpisodeSearch" {
		t.Fatalf("sonarr commands = %v", got)
	}
	if got := radarr.commandNames(); len(got) != 1 || got[0] != "MissingMoviesSearch" {
		t.Fatalf("radarr commands = %v", got)
	}
}

// Two entries for one Sonarr whose schedules differ in spelling but land on
// the same minute get one search; Run now still sends.
func TestWantedSearchCoalescesSameMinuteAndRunNowForces(t *testing.T) {
	sonarr := newFakeArr(t, "Sonarr")
	w := newTestWantedSearch(t,
		wantedArr("sonarr", sonarr.url, "key-1", "12:00", ""),
		wantedArr("tv", sonarr.url, "key-1", "0 12 * * *", "auto"),
	)

	fireAll(t, w)

	if n := len(w.scheduler.Jobs()); n != 2 {
		t.Fatalf("jobs = %d, want 2 (one per schedule)", n)
	}
	if got := sonarr.commandNames(); len(got) != 1 {
		t.Fatalf("commands = %v, want 1", got)
	}

	res, err := w.RunNow("tv")
	if err != nil || !res.Sent {
		t.Fatalf("RunNow = %+v, %v; want sent", res, err)
	}
	if got := sonarr.commandNames(); len(got) != 2 {
		t.Fatalf("commands after Run now = %v, want 2", got)
	}
}

// An Arr that will not say what it is gets no command; the failure is kept
// for the Settings page.
func TestWantedSearchStatusFailureSendsNothing(t *testing.T) {
	sonarr := newFakeArr(t, "Sonarr")
	sonarr.statusCode = http.StatusUnauthorized
	w := newTestWantedSearch(t, wantedArr("sonarr", sonarr.url, "wrong-key", "12:00", ""))

	fireAll(t, w)

	if got := sonarr.commandNames(); len(got) != 0 {
		t.Fatalf("commands = %v, want none", got)
	}
	st := w.Status()
	if len(st.Arrs) != 1 || st.Arrs[0].Last == nil || st.Arrs[0].Last.Error == "" {
		t.Fatalf("status = %+v, want the error recorded", st.Arrs)
	}
}

// Lidarr and friends are skipped rather than sent a Sonarr command.
func TestWantedSearchSkipsOtherApps(t *testing.T) {
	lidarr := newFakeArr(t, "Lidarr")
	w := newTestWantedSearch(t, wantedArr("music", lidarr.url, "k", "12:00", ""))

	res, err := w.RunNow("music")
	if err != nil || res.Sent || res.Skipped == "" {
		t.Fatalf("RunNow = %+v, %v; want skipped", res, err)
	}
	if got := lidarr.commandNames(); len(got) != 0 {
		t.Fatalf("commands = %v, want none", got)
	}
}

// A bad schedule skips only that Arr's job and is reported.
func TestWantedSearchApplyConfigReportsBadSchedule(t *testing.T) {
	w := newTestWantedSearch(t,
		wantedArr("sonarr", "http://127.0.0.1:1", "a", "6h", ""),
		wantedArr("radarr", "http://127.0.0.1:2", "b", "03:00", ""),
	)
	err := w.ApplyConfig()
	if err == nil || !strings.Contains(err.Error(), "sonarr") {
		t.Fatalf("ApplyConfig err = %v, want one naming sonarr", err)
	}
	if n := len(w.scheduler.Jobs()); n != 1 {
		t.Fatalf("jobs = %d, want radarr's only", n)
	}
}
