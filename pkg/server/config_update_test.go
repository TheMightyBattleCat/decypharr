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
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

// newTestManager builds a manager for one test and stops it when the test
// ends, which closes its storage. The storage locks its database files, so
// the next test's manager cannot open them while an earlier one holds them.
func newTestManager(t *testing.T) *manager.Manager {
	t.Helper()
	m := manager.New()
	t.Cleanup(func() { _ = m.Stop() })
	return m
}

// TestHandleUpdateConfig_PreservesWebhookToken guards against the config
// save wiping webhook_token: the settings form has no field for it, so a
// PUT /api/config that omits it decodes to an empty string unless
// handleUpdateConfig explicitly preserves the live value the same way it
// already does for Auth.
//
// The assertion reads back the persisted config.json, which is what a
// restart loads.
func TestHandleUpdateConfig_PreservesWebhookToken(t *testing.T) {
	config.Get().WebhookToken = "existing-secret-token"
	t.Cleanup(func() { config.Get().WebhookToken = "" })

	s := &Server{
		logger:  zerolog.Nop(),
		manager: newTestManager(t),
	}

	// A payload shaped like what the current settings form actually submits:
	// no webhook_token field at all, same as every other field the form
	// doesn't render.
	body := `{"bind_address":"0.0.0.0","port":"8282","download_folder":"/tmp/downloads"}`
	req := httptest.NewRequest(http.MethodPost, "/api/config", strings.NewReader(body))
	rec := httptest.NewRecorder()

	s.handleUpdateConfig(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	raw, err := os.ReadFile(config.Get().JsonFile())
	if err != nil {
		t.Fatalf("reading persisted config.json: %v", err)
	}
	var persisted struct {
		WebhookToken string `json:"webhook_token"`
	}
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatalf("unmarshaling persisted config.json: %v", err)
	}

	if persisted.WebhookToken != "existing-secret-token" {
		t.Errorf("persisted webhook_token = %q, want %q (preserved)", persisted.WebhookToken, "existing-secret-token")
	}
}

// persistedRepair reads the repair block back from config.json on disk.
func persistedRepair(t *testing.T) config.RepairConfig {
	t.Helper()
	raw, err := os.ReadFile(config.Get().JsonFile())
	if err != nil {
		t.Fatalf("reading persisted config.json: %v", err)
	}
	var persisted struct {
		Repair config.RepairConfig `json:"repair"`
	}
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatalf("unmarshaling persisted config.json: %v", err)
	}
	return persisted.Repair
}

// A general Settings save sends only the repair fields the form renders. On
// a production install each save reset ffprobe_path and the other repair settings the
// form has no input for; merged onto the live config they keep their values.
func TestHandleUpdateConfig_KeepsRepairSettingsTheFormDoesNotSend(t *testing.T) {
	live := config.Get()
	saved := live.Repair
	t.Cleanup(func() { config.Get().Repair = saved })
	live.Repair.FFProbePath = "/opt/ffmpeg/ffprobe"
	live.Repair.FFProbeTimeout = "120s"
	live.Repair.CleanupSuperseded = true
	live.Repair.Par2UrgentConcurrency = 5
	live.Repair.Par2RepairOnSweep = true

	s := &Server{logger: zerolog.Nop(), manager: newTestManager(t)}
	body := `{"bind_address":"0.0.0.0","port":"8282","download_folder":"/tmp/downloads",
		"repair":{"enabled":false,"schedule":"23:10","workers":3,"auto_repair":true}}`
	rec := httptest.NewRecorder()
	s.handleUpdateConfig(rec, httptest.NewRequest(http.MethodPost, "/api/config", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
	}

	r := persistedRepair(t)
	if r.FFProbePath != "/opt/ffmpeg/ffprobe" || r.FFProbeTimeout != "120s" || !r.CleanupSuperseded || r.Par2UrgentConcurrency != 5 || !r.Par2RepairOnSweep {
		t.Errorf("repair settings the form does not send were reset: %+v", r)
	}
	if r.Workers != 3 || !r.AutoRepair || r.Schedule != "23:10" {
		t.Errorf("repair settings the form sent were not applied: %+v", r)
	}
}

// The Repair page's Overlay/PAR2 form sends its own fields only; the save
// must not switch Repair off or clear its schedule.
func TestHandleUpdateRepairConfig_PartialBodyKeepsOtherRepairSettings(t *testing.T) {
	live := config.Get()
	saved := live.Repair
	t.Cleanup(func() { config.Get().Repair = saved })
	live.Repair.Enabled = true
	live.Repair.Schedule = "23:10"
	live.Repair.AutoRepair = true
	live.Repair.FFProbePath = "/opt/ffmpeg/ffprobe"

	s := &Server{logger: zerolog.Nop(), manager: newTestManager(t)}
	body := `{"playback_padding":true,"par2_repair":true,"pad_max_run_segments":4,"par2_repair_mode":"auto_threshold","par2_repair_min_segments":32}`
	rec := httptest.NewRecorder()
	s.handleUpdateRepairConfig(rec, httptest.NewRequest(http.MethodPut, "/api/repair/config", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
	}

	r := persistedRepair(t)
	if !r.Enabled || r.Schedule != "23:10" || !r.AutoRepair || r.FFProbePath != "/opt/ffmpeg/ffprobe" {
		t.Errorf("repair settings outside the form were changed: %+v", r)
	}
	if r.PadMaxRunSegments != 4 || r.Par2RepairMode != "auto_threshold" || r.Par2RepairMinSegments != 32 {
		t.Errorf("sent fields not applied: %+v", r)
	}
}

// The pre-cache form sends only its own fields; the rest of the pre-cache
// settings keep their values instead of resetting (the endpoint used to
// replace the whole block with the body).
func TestHandleUpdatePrecacheConfig_PartialBodyKeepsOtherSettings(t *testing.T) {
	live := config.Get()
	saved := live.Precache
	t.Cleanup(func() { config.Get().Precache = saved })
	off := false
	capBytes := int64(42 << 30)
	live.Precache.PrecacheYieldToPlayback = &off
	live.Precache.PrecacheEvictAfterWatched = true
	live.Precache.PrecacheMaxBytes = &capBytes

	s := &Server{logger: zerolog.Nop(), manager: newTestManager(t)}
	body := `{"precache_threshold_percent":20,"precache_whole_season":false,"precache_next_episodes":3}`
	rec := httptest.NewRecorder()
	s.handleUpdatePrecacheConfig(rec, httptest.NewRequest(http.MethodPut, "/api/precache/config", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
	}

	p := config.Get().Precache
	if p.YieldToPlayback() || !p.PrecacheEvictAfterWatched || p.MaxBytes() != capBytes {
		t.Errorf("pre-cache settings outside the form were reset: %+v", p)
	}
	if p.ThresholdPercent() != 20 || p.WholeSeason() || p.EpisodesAhead() != 3 {
		t.Errorf("sent fields not applied: threshold=%d wholeSeason=%v ahead=%d", p.ThresholdPercent(), p.WholeSeason(), p.EpisodesAhead())
	}

	rec = httptest.NewRecorder()
	s.handleUpdatePrecacheConfig(rec, httptest.NewRequest(http.MethodPut, "/api/precache/config", strings.NewReader(`{"precache_next_episodes":99}`)))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("precache_next_episodes=99: status = %d, want 400", rec.Code)
	}
}

// Settings > Providers > Usenet > Prefer Faster Servers saves through the
// general form and applies live: switching it must not restart the service
// (a restart unmounts the DFS mount under anyone watching).
func TestHandleUpdateConfig_PreferFasterServersAppliesWithoutRestart(t *testing.T) {
	live := config.Get()
	saved, savedBind := live.Usenet.PreferFasterServers, live.BindAddress
	t.Cleanup(func() {
		config.Get().Usenet.PreferFasterServers = saved
		config.Get().BindAddress = savedBind
	})
	// A running service's live config has its defaults applied and a bind
	// address; the test singleton has neither until set and saved, and every
	// save would differ from it on those (the handler fills in 0.0.0.0).
	live.BindAddress = "0.0.0.0"
	if err := live.Save(); err != nil {
		t.Fatalf("normalizing the live config: %v", err)
	}

	s := &Server{logger: zerolog.Nop(), manager: newTestManager(t)}
	for _, on := range []bool{false, true} {
		body := `{"usenet":{"prefer_faster_servers":false}}`
		if on {
			body = `{"usenet":{"prefer_faster_servers":true}}`
		}
		rec := httptest.NewRecorder()
		s.handleUpdateConfig(rec, httptest.NewRequest(http.MethodPost, "/api/config", strings.NewReader(body)))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
		}
		var resp struct {
			Restarted bool `json:"restarted"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decoding response: %v", err)
		}
		if resp.Restarted {
			t.Errorf("saving prefer_faster_servers=%v restarted the service", on)
		}
		if got := config.Get().Usenet.PreferFasterServersEnabled(); got != on {
			t.Errorf("live setting = %v after saving %v", got, on)
		}

		raw, err := os.ReadFile(config.Get().JsonFile())
		if err != nil {
			t.Fatalf("reading persisted config.json: %v", err)
		}
		var persisted struct {
			Usenet config.Usenet `json:"usenet"`
		}
		if err := json.Unmarshal(raw, &persisted); err != nil {
			t.Fatalf("unmarshaling persisted config.json: %v", err)
		}
		if got := persisted.Usenet.PreferFasterServersEnabled(); got != on {
			t.Errorf("persisted setting = %v after saving %v", got, on)
		}
	}
}

// A settings read and a settings save must each leave the snapshot that was
// current before them untouched: readers elsewhere may still hold it.
func TestConfigHandlersUseSnapshots(t *testing.T) {
	previousPath := config.GetMainPath()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(func() {
		config.Reset()
		config.SetConfigPath(previousPath)
	})
	// A running service has a bind address; without one the handler fills in
	// 0.0.0.0 and the save below would count as a restart-worthy change.
	if _, err := config.Update(func(next *config.Config) error {
		next.BindAddress = "0.0.0.0"
		return nil
	}); err != nil {
		t.Fatalf("normalizing the config: %v", err)
	}
	before := config.Get()
	mgr := newTestManager(t)
	mgr.Arr().AddOrUpdate(&arr.Arr{Name: "manual", Host: "http://example.test", Token: "token", Source: arr.SourceManual})
	server := &Server{logger: zerolog.Nop(), manager: mgr}
	response := httptest.NewRecorder()
	server.handleGetConfig(response, httptest.NewRequest(http.MethodGet, "/api/config", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET status=%d", response.Code)
	}
	if len(before.Arrs) != 0 {
		t.Fatal("GET changed the current snapshot")
	}
	response = httptest.NewRecorder()
	server.handleUpdateConfig(response, httptest.NewRequest(http.MethodPost, "/api/config", strings.NewReader(`{"app_url":"https://new.example.test"}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("POST status=%d body=%s", response.Code, response.Body.String())
	}
	if before.AppURL == "https://new.example.test" {
		t.Fatal("POST changed the previous snapshot")
	}
	if config.Get().AppURL != "https://new.example.test" {
		t.Fatal("POST did not publish the update")
	}
	var result struct {
		Restarted bool `json:"restarted"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Restarted {
		t.Fatal("live URL update restarted services")
	}
}

// The fork's own settings endpoints (repair, pre-cache, Plex, webhook
// teardown, webhook token) each publish a new config. None may write to the
// one that was current before: other goroutines may still be reading it.
func TestDedicatedSettingsHandlersLeaveThePreviousSnapshotAlone(t *testing.T) {
	previousPath := config.GetMainPath()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(func() {
		config.Reset()
		config.SetConfigPath(previousPath)
	})
	s := &Server{logger: zerolog.Nop(), manager: newTestManager(t)}

	steps := []struct {
		name    string
		call    func(w http.ResponseWriter)
		changed func(c *config.Config) bool
	}{
		{"repair", func(w http.ResponseWriter) {
			s.handleUpdateRepairConfig(w, httptest.NewRequest(http.MethodPut, "/api/repair/config", strings.NewReader(`{"pad_max_run_segments":7}`)))
		}, func(c *config.Config) bool { return c.Repair.PadMaxRunSegments == 7 }},
		{"precache", func(w http.ResponseWriter) {
			s.handleUpdatePrecacheConfig(w, httptest.NewRequest(http.MethodPut, "/api/precache/config", strings.NewReader(`{"precache_threshold_percent":37}`)))
		}, func(c *config.Config) bool { return c.Precache.PrecacheThresholdPercent == 37 }},
		{"plex", func(w http.ResponseWriter) {
			s.handleUpdatePlexConfig(w, httptest.NewRequest(http.MethodPut, "/api/plex/config", strings.NewReader(`{"plex_url":"http://plex.example.test:32400","plex_token":"plex-token"}`)))
		}, func(c *config.Config) bool { return c.Plex.Token == "plex-token" }},
		{"webhook teardown", func(w http.ResponseWriter) {
			s.handleUpdateWebhookTeardown(w, httptest.NewRequest(http.MethodPut, "/api/webhook/teardown", strings.NewReader(`{"enabled":true}`)))
		}, func(c *config.Config) bool { return c.ArrWebhookTeardown }},
		{"webhook token", func(w http.ResponseWriter) {
			if _, err := s.refreshWebhookToken(); err != nil {
				t.Fatalf("refreshWebhookToken: %v", err)
			}
		}, func(c *config.Config) bool { return c.WebhookToken != "" }},
	}
	for _, step := range steps {
		before := config.Get()
		if step.changed(before) {
			t.Fatalf("%s: the setting is already at the value the step saves", step.name)
		}
		rec := httptest.NewRecorder()
		step.call(rec)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status = %d, body: %s", step.name, rec.Code, rec.Body.String())
		}
		if step.changed(before) {
			t.Errorf("%s: the save wrote to the previous snapshot", step.name)
		}
		if config.Get() == before || !step.changed(config.Get()) {
			t.Errorf("%s: the save was not published", step.name)
		}
	}

	// Each save keeps what the earlier ones stored, on disk as well.
	raw, err := os.ReadFile(config.Get().JsonFile())
	if err != nil {
		t.Fatal(err)
	}
	var persisted config.Config
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	for _, step := range steps {
		if !step.changed(&persisted) {
			t.Errorf("%s: not in config.json after the later saves", step.name)
		}
	}
}

// An Arr that only runtime storage knows (auto-detected, never saved) shows as
// a card on the settings page. Deleting that card and saving must remove it;
// a save that follows no settings read must leave it alone.
func TestHandleUpdateConfig_DeletingAnAutoDetectedArrCardRemovesIt(t *testing.T) {
	s := withLiveArrs(t, config.Arr{Name: "sonarr", Host: "http://127.0.0.1:1", Token: "k"})
	detected := &arr.Arr{Name: "detected", Host: "http://127.0.0.1:2", Token: "t", Source: arr.SourceAuto}
	s.manager.Arr().AddOrUpdate(detected)
	before := config.Get()
	saved := []any{map[string]any{"name": "sonarr", "host": "http://127.0.0.1:1", "token": "k"}}

	// No settings read first: the save cannot know the Arr was ever shown.
	if rec := postConfig(t, s, liveConfigBody(t, saved)); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
	}
	if s.manager.Arr().Get("detected") == nil {
		t.Fatal("a save with no settings read before it dropped the auto-detected Arr")
	}

	// The page loads (showing both cards), the card is deleted, the page saves.
	body := liveConfigBody(t, saved)
	s.handleGetConfig(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/config", nil))
	if len(before.Arrs) != 1 || len(config.Get().Arrs) != 1 {
		t.Fatal("the settings read changed the config")
	}
	if rec := postConfig(t, s, body); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
	}
	if s.manager.Arr().Get("detected") != nil {
		t.Fatal("the deleted card's Arr is still in runtime storage")
	}
	if s.manager.Arr().Get("sonarr") == nil {
		t.Fatal("the saved Arr was dropped")
	}

	// The list is used once: detected again later, it survives a save that
	// follows no new settings read.
	s.manager.Arr().AddOrUpdate(detected)
	if rec := postConfig(t, s, liveConfigBody(t, saved)); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
	}
	if s.manager.Arr().Get("detected") == nil {
		t.Fatal("a later save reused the old shown list and dropped the Arr")
	}
}

// Queue and notification settings are read once at start, so changing one
// restarts the service. Saving the page with them unchanged must not: the
// form posts an empty event list as [] where the config holds none, and
// posts the defaults for fields left blank.
func TestHandleUpdateConfig_UnchangedStartupSettingsDoNotRestart(t *testing.T) {
	previousPath := config.GetMainPath()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(func() {
		config.Reset()
		config.SetConfigPath(previousPath)
	})
	if _, err := config.Update(func(next *config.Config) error {
		next.BindAddress = "0.0.0.0"
		return nil
	}); err != nil {
		t.Fatalf("normalizing the config: %v", err)
	}
	s := &Server{logger: zerolog.Nop(), manager: newTestManager(t)}
	save := func(body string) bool {
		t.Helper()
		rec := httptest.NewRecorder()
		s.handleUpdateConfig(rec, httptest.NewRequest(http.MethodPost, "/api/config", strings.NewReader(body)))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
		}
		var resp struct {
			Restarted bool `json:"restarted"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		return resp.Restarted
	}

	unchanged := `{"refresh_interval":"30s","max_active_downloads":5,
		"notifications":{"enabled":false,"webhook_url":"","callback_url":"","events":[]}}`
	for i := range 2 {
		if save(unchanged) {
			t.Fatalf("save %d of unchanged queue and notification settings restarted the service", i+1)
		}
	}
	if !save(`{"remove_stalled_after":"10m"}`) {
		t.Error("changing remove_stalled_after did not restart the service; the queue reads it only at start")
	}
	if save(`{"remove_stalled_after":"10m"}`) {
		t.Error("saving the same remove_stalled_after again restarted the service")
	}
}
