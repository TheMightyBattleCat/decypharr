package config

import (
	"reflect"
	"testing"

	json "github.com/bytedance/sonic"
)

func boolPtr(b bool) *bool { return &b }

// A general Settings save sends the form's fields only. Merged onto the live
// config and decoded with the handler's decoder, the fields the form has no
// input for keep their values and the ones it sends apply.
func TestMergeJSONSettingsSaveKeepsFieldsTheFormDoesNotSend(t *testing.T) {
	live := Config{
		Port: "8686",
		Arrs: []Arr{
			{Name: "sonarr", Host: "http://s", Token: "t1", SkipRepair: true, SelectedDebrid: "rd"},
			{Name: "radarr", Host: "http://r", Token: "t2"},
		},
		Repair: RepairConfig{
			Enabled:                 true,
			Schedule:                "23:10",
			AutoRepair:              true,
			FFProbePath:             "/opt/ffprobe",
			FFProbeTimeout:          "120s",
			CleanupSuperseded:       true,
			DecodeVerifyTTL:         "240h",
			ImportAvailabilityCheck: boolPtr(false),
			Par2UrgentConcurrency:   5,
		},
	}
	base, err := json.Marshal(&live)
	if err != nil {
		t.Fatal(err)
	}
	// Shaped like config.js's save: repair block from collectRepairConfig
	// (no ffprobe_path etc.), a shorter arrs list without skip_repair.
	patch := []byte(`{"port":"8686","arrs":[{"name":"radarr","host":"http://r2","token":"t2"}],
		"repair":{"enabled":false,"schedule":"23:10","auto_repair":true,"workers":3,"ffprobe_check":true}}`)

	merged, err := MergeJSON(base, patch)
	if err != nil {
		t.Fatal(err)
	}
	var got Config
	if err := json.Unmarshal(merged, &got); err != nil {
		t.Fatal(err)
	}

	r := got.Repair
	if r.Enabled {
		t.Error("enabled: the form sent false, got true")
	}
	if r.Workers != 3 || !r.FFProbeCheck {
		t.Errorf("sent fields not applied: workers=%d ffprobe_check=%v", r.Workers, r.FFProbeCheck)
	}
	if r.FFProbePath != "/opt/ffprobe" || r.FFProbeTimeout != "120s" || !r.CleanupSuperseded ||
		r.DecodeVerifyTTL != "240h" || r.ImportAvailabilityCheck == nil || *r.ImportAvailabilityCheck ||
		r.Par2UrgentConcurrency != 5 {
		t.Errorf("unsent repair fields reset: %+v", r)
	}
	want := []Arr{{Name: "radarr", Host: "http://r2", Token: "t2"}}
	if !reflect.DeepEqual(got.Arrs, want) {
		t.Errorf("arrs must be replaced whole, no fields carried from the old list: got %+v", got.Arrs)
	}
}

func TestMergeJSONKeepsLargeIntegersExact(t *testing.T) {
	base := []byte(`{"precache":{"precache_max_bytes":9007199254740993,"precache_threshold_percent":10}}`)
	merged, err := MergeJSON(base, []byte(`{"precache":{"precache_threshold_percent":20}}`))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"precache":{"precache_max_bytes":9007199254740993,"precache_threshold_percent":20}}`
	if string(merged) != want {
		t.Errorf("got %s want %s", merged, want)
	}
}

func TestMergeJSONRejectsNonObjectPatch(t *testing.T) {
	for _, p := range []string{`[]`, `"x"`, `null`, ``, `{`} {
		if _, err := MergeJSON([]byte(`{}`), []byte(p)); err == nil {
			t.Errorf("patch %q: want error", p)
		}
	}
}

func TestChangedJSONKeys(t *testing.T) {
	before := []byte(`{"port":"8686","repair":{"enabled":true,"schedule":"23:10","ffprobe_path":"/x"},"arrs":[{"name":"a"}]}`)
	after := []byte(`{"port":"8686","repair":{"schedule":"23:10","workers":3},"arrs":[{"name":"b"}],"plex":{"url":"u"}}`)
	got, err := ChangedJSONKeys(before, after)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"arrs", "plex.url", "repair.enabled", "repair.ffprobe_path", "repair.workers"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}
