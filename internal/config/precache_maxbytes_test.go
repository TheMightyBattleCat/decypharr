package config

import "testing"

// TestPrecacheMaxBytes: the cap is no longer on the Repair page, so nothing a
// user can no longer see may switch pre-caching off. Unset and <= 0 mean the
// ceiling; values above it are clamped; values inside it are kept.
func TestPrecacheMaxBytes(t *testing.T) {
	ptr := func(v int64) *int64 { return &v }
	cases := []struct {
		name string
		in   *int64
		want int64
	}{
		{"unset", nil, precacheMaxBytesCeiling},
		{"explicit zero no longer disables", ptr(0), precacheMaxBytesCeiling},
		{"negative", ptr(-5), precacheMaxBytesCeiling},
		{"above ceiling", ptr(300 << 30), precacheMaxBytesCeiling},
		{"inside range", ptr(42 << 30), 42 << 30},
	}
	for _, tc := range cases {
		var c Config
		c.Precache.PrecacheMaxBytes = tc.in
		c.setDefaults()
		if got := c.Precache.MaxBytes(); got != tc.want {
			t.Errorf("%s: MaxBytes() = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// TestPrecacheEpisodesAhead: whole season is on when unset (the walk always
// ran to the season's end before the setting existed), and turning it off
// uses the saved episode count - including an explicit 0, which disables it.
func TestPrecacheEpisodesAhead(t *testing.T) {
	on, off := true, false
	three, zero := 3, 0
	cases := []struct {
		name  string
		whole *bool
		next  *int
		want  int
	}{
		{"unset", nil, nil, -1},
		{"whole season on ignores count", &on, &three, -1},
		{"off uses count", &off, &three, 3},
		{"off with unset count defaults to 1", &off, nil, 1},
		{"off with zero disables", &off, &zero, 0},
	}
	for _, tc := range cases {
		c := PrecacheConfig{PrecacheWholeSeason: tc.whole, PrecacheNextEpisodes: tc.next}
		if got := c.EpisodesAhead(); got != tc.want {
			t.Errorf("%s: EpisodesAhead() = %d, want %d", tc.name, got, tc.want)
		}
	}
	// IsZero must see the new field, or omitzero would drop a config whose
	// only non-default setting is whole season off.
	if (PrecacheConfig{PrecacheWholeSeason: &off}).IsZero() {
		t.Error("IsZero() ignores PrecacheWholeSeason")
	}
}

// TestPrecacheReadAheadConcurrencyUsesUsenetMaxConnections: bursts take the
// Usenet "Max Connections Per File" setting; the saved
// precache_read_ahead_concurrency (12 in every config saved before) is not
// consulted.
func TestPrecacheReadAheadConcurrencyUsesUsenetMaxConnections(t *testing.T) {
	Reset()
	SetConfigPath(t.TempDir())
	t.Cleanup(Reset)
	cfg := Get()

	cfg.Usenet.MaxConnections = 20
	c := PrecacheConfig{PrecacheReadAheadConcurrency: 12}
	if got := c.ReadAheadConcurrency(); got != 20 {
		t.Fatalf("ReadAheadConcurrency() = %d, want 20 (Usenet.MaxConnections)", got)
	}
	cfg.Usenet.MaxConnections = 0
	if got := c.ReadAheadConcurrency(); got != 15 {
		t.Fatalf("ReadAheadConcurrency() = %d with no Usenet setting, want the 15 default", got)
	}
}

// TestPrecacheMaxBytesSurvivesUnrelatedConfigSave proves the field-
// preservation pattern api.handleUpdateConfig applies (copying the live
// Precache section over the freshly-decoded one, since the general settings
// form never submits it) carries an explicitly-set PrecacheMaxBytes through
// an unrelated config save unharmed - same bug class
// TestPrecacheReadAheadSurvivesUnrelatedConfigSave guards for RepairConfig.
func TestPrecacheMaxBytesSurvivesUnrelatedConfigSave(t *testing.T) {
	capBytes := int64(42 * 1024 * 1024 * 1024) // 42 GiB, deliberately not the default
	current := Config{Precache: PrecacheConfig{PrecacheMaxBytes: &capBytes}}

	// incoming simulates a decode of the general settings form's JSON body,
	// which has no "precache" object - it lands as the zero value
	// (PrecacheConfig{}) exactly as handleUpdateConfig sees it before
	// applying its preservation line.
	var incoming Config
	incoming.DownloadFolder = "/changed/unrelated/path" // an unrelated field actually changing

	// This is the exact preservation handleUpdateConfig performs.
	incoming.Precache = current.Precache

	if incoming.Precache.PrecacheMaxBytes == nil || *incoming.Precache.PrecacheMaxBytes != capBytes {
		t.Fatalf("PrecacheMaxBytes = %v after preserving an explicit cap across an unrelated config save, want %d", incoming.Precache.PrecacheMaxBytes, capBytes)
	}
	if incoming.DownloadFolder != "/changed/unrelated/path" {
		t.Fatalf("unrelated field DownloadFolder was not applied from the incoming config")
	}
}
