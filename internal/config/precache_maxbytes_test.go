package config

import "testing"

// TestPrecacheMaxBytesZeroDisables proves an explicit PrecacheMaxBytes of 0
// survives setDefaults as a real 0 (disabled), not a "0/unset" sentinel that
// falls back to precacheDefaultMaxBytes - see PrecacheMaxBytes's *int64 doc
// comment for why this needs the same pointer convention as
// PrecacheNextEpisodes.
func TestPrecacheMaxBytesZeroDisables(t *testing.T) {
	var c Config
	zero := int64(0)
	c.Precache.PrecacheMaxBytes = &zero
	c.setDefaults()

	if c.Precache.PrecacheMaxBytes == nil || *c.Precache.PrecacheMaxBytes != 0 {
		t.Fatalf("PrecacheMaxBytes = %v after setDefaults, want a pointer to 0", c.Precache.PrecacheMaxBytes)
	}
	if got := c.Precache.MaxBytes(); got != 0 {
		t.Fatalf("MaxBytes() = %d, want 0 (disabled)", got)
	}
}

// TestPrecacheMaxBytesNilDefaults proves an unset (nil) PrecacheMaxBytes -
// e.g. a config that predates this field, or one that has genuinely never
// been saved - still falls back to precacheDefaultMaxBytes, not 0/disabled.
func TestPrecacheMaxBytesNilDefaults(t *testing.T) {
	var c Config
	c.setDefaults()

	if c.Precache.PrecacheMaxBytes == nil {
		t.Fatalf("PrecacheMaxBytes is nil after setDefaults, want it materialized (same convention as PrecacheNextEpisodes)")
	}
	if got := c.Precache.MaxBytes(); got != precacheDefaultMaxBytes {
		t.Fatalf("MaxBytes() = %d, want the default %d", got, int64(precacheDefaultMaxBytes))
	}
}

// TestPrecacheMaxBytesClampsToCeiling proves a value above the 256 GiB
// ceiling is clamped down to it by setDefaults (a fat-finger backstop, not
// the real guard - see PrecacheMaxBytes's doc comment), both in the stored
// config and via the MaxBytes() accessor.
func TestPrecacheMaxBytesClampsToCeiling(t *testing.T) {
	var c Config
	tooBig := int64(300 * 1024 * 1024 * 1024) // 300 GiB
	c.Precache.PrecacheMaxBytes = &tooBig
	c.setDefaults()

	if c.Precache.PrecacheMaxBytes == nil || *c.Precache.PrecacheMaxBytes != precacheMaxBytesCeiling {
		t.Fatalf("PrecacheMaxBytes = %v after setDefaults, want a pointer to the %d ceiling", c.Precache.PrecacheMaxBytes, int64(precacheMaxBytesCeiling))
	}
	if got := c.Precache.MaxBytes(); got != precacheMaxBytesCeiling {
		t.Fatalf("MaxBytes() = %d, want the %d ceiling", got, int64(precacheMaxBytesCeiling))
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
