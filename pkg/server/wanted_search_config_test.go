package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

// withLiveArrs sets the live config's Arrs for one test and builds a server
// whose Arr storage was loaded from them.
func withLiveArrs(t *testing.T, arrs ...config.Arr) *Server {
	t.Helper()
	live := config.Get()
	saved := live.Arrs
	t.Cleanup(func() { config.Get().Arrs = saved })
	live.Arrs = arrs
	// A settings save fills in the bind address and port, and Save applies
	// the rest of the defaults, so re-posting the live config changes
	// nothing but what the test posts.
	savedBind, savedPort := live.BindAddress, live.Port
	t.Cleanup(func() { config.Get().BindAddress, config.Get().Port = savedBind, savedPort })
	live.BindAddress, live.Port = "0.0.0.0", "8282"
	if err := live.Save(); err != nil {
		t.Fatal(err)
	}
	return &Server{logger: zerolog.Nop(), manager: manager.New()}
}

// liveConfigBody is the live config as a settings save would post it, with
// arrs replaced - so the Arrs are the only thing the save changes.
func liveConfigBody(t *testing.T, arrs any) string {
	t.Helper()
	raw, err := json.Marshal(config.Get())
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	body["arrs"] = arrs
	out, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func persistedArrs(t *testing.T) []config.Arr {
	t.Helper()
	raw, err := os.ReadFile(config.Get().JsonFile())
	if err != nil {
		t.Fatalf("reading persisted config.json: %v", err)
	}
	var persisted struct {
		Arrs []config.Arr `json:"arrs"`
	}
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	return persisted.Arrs
}

func postConfig(t *testing.T, s *Server, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	s.handleUpdateConfig(rec, httptest.NewRequest(http.MethodPost, "/api/config", strings.NewReader(body)))
	return rec
}

func wantedSearchJobs(s *Server) int {
	n := 0
	for _, j := range s.manager.Scheduler().Jobs() {
		for _, tag := range j.Tags() {
			if tag == "arr-wanted-search" {
				n++
			}
		}
	}
	return n
}

// Turning on an Arr's wanted search applies live - no restart - and
// registers its job.
func TestHandleUpdateConfig_ArrWantedSearchAppliesWithoutRestart(t *testing.T) {
	s := withLiveArrs(t, config.Arr{Name: "sonarr", Host: "http://127.0.0.1:1", Token: "k"})

	rec := postConfig(t, s, liveConfigBody(t, []any{map[string]any{
		"name": "sonarr", "host": "http://127.0.0.1:1", "token": "k",
		"wanted_search": map[string]any{"enabled": true, "schedule": " 12:00 "},
	}}))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Restarted bool `json:"restarted"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Restarted {
		t.Fatal("editing an Arr's wanted search restarted decypharr")
	}
	got := persistedArrs(t)
	if len(got) != 1 || got[0].WantedSearch != (config.ArrWantedSearch{Enabled: true, Schedule: "12:00"}) {
		t.Fatalf("persisted arrs = %+v, want wanted search on at 12:00", got)
	}
	if n := wantedSearchJobs(s); n != 1 {
		t.Fatalf("wanted-search jobs = %d, want 1", n)
	}
}

// GET the config and POST it straight back: the setting must survive the
// page round trip (the settings-save bug class this codebase keeps meeting).
func TestHandleUpdateConfig_ArrWantedSearchSurvivesRoundTrip(t *testing.T) {
	want := config.ArrWantedSearch{Enabled: true, Schedule: "0 3 * * *"}
	s := withLiveArrs(t, config.Arr{Name: "tv", Host: "http://127.0.0.1:1", Token: "k", Source: "auto", WantedSearch: want})

	get := httptest.NewRecorder()
	s.handleGetConfig(get, httptest.NewRequest(http.MethodGet, "/api/config", nil))
	if get.Code != http.StatusOK {
		t.Fatalf("GET status = %d", get.Code)
	}
	rec := postConfig(t, s, get.Body.String())
	if rec.Code != http.StatusOK {
		t.Fatalf("POST status = %d, body: %s", rec.Code, rec.Body.String())
	}
	got := persistedArrs(t)
	if len(got) != 1 || got[0].WantedSearch != want {
		t.Fatalf("persisted arrs = %+v, want %+v kept", got, want)
	}
}

// A page that predates the setting posts Arrs without the key; their
// wanted search keeps its live value instead of switching off.
func TestHandleUpdateConfig_ArrWithoutWantedSearchKeyKeepsIt(t *testing.T) {
	want := config.ArrWantedSearch{Enabled: true, Schedule: "12:00"}
	s := withLiveArrs(t, config.Arr{Name: "sonarr", Host: "http://127.0.0.1:1", Token: "k", WantedSearch: want})

	rec := postConfig(t, s, liveConfigBody(t, []any{map[string]any{
		"name": "sonarr", "host": "http://127.0.0.1:1", "token": "k", "skip_repair": true,
	}}))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
	}
	got := persistedArrs(t)
	if len(got) != 1 || got[0].WantedSearch != want || !got[0].SkipRepair {
		t.Fatalf("persisted arrs = %+v, want wanted search kept and skip_repair applied", got)
	}
}

// An interval schedule is refused with a message naming the Arr, and nothing
// is saved.
func TestHandleUpdateConfig_RejectsIntervalWantedSearchSchedule(t *testing.T) {
	s := withLiveArrs(t, config.Arr{Name: "sonarr", Host: "http://127.0.0.1:1", Token: "k"})

	rec := postConfig(t, s, liveConfigBody(t, []any{map[string]any{
		"name": "sonarr", "host": "http://127.0.0.1:1", "token": "k",
		"wanted_search": map[string]any{"enabled": true, "schedule": "6h"},
	}}))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `"sonarr"`) {
		t.Fatalf("status = %d, body %q; want 400 naming sonarr", rec.Code, rec.Body.String())
	}
}
