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

// TestHandleUpdateConfig_PreservesWebhookToken guards against the config
// save wiping webhook_token: the settings form has no field for it, so a
// PUT /api/config that omits it decodes to an empty string unless
// handleUpdateConfig explicitly preserves the live value the same way it
// already does for Auth.
//
// The assertion reads back the persisted config.json rather than the live
// in-memory singleton: Save() writes newConfig's fields to disk
// unconditionally, while the singleton is only mutated via ApplyRuntime,
// which is skipped whenever RequiresRestart is true - as it would be here,
// since a wiped WebhookToken alone differs enough from the live config to
// trigger it. Checking the singleton would pass even with the bug present.
func TestHandleUpdateConfig_PreservesWebhookToken(t *testing.T) {
	config.Get().WebhookToken = "existing-secret-token"
	t.Cleanup(func() { config.Get().WebhookToken = "" })

	s := &Server{
		logger:  zerolog.Nop(),
		manager: manager.New(),
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

	s := &Server{logger: zerolog.Nop(), manager: manager.New()}
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

	s := &Server{logger: zerolog.Nop(), manager: manager.New()}
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

	s := &Server{logger: zerolog.Nop(), manager: manager.New()}
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

	s := &Server{logger: zerolog.Nop(), manager: manager.New()}
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
