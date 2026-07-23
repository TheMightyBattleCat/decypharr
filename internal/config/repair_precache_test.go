package config

import "testing"

// TestPrecacheReadAheadDefaultsFalseOnFreshConfig proves PrecacheReadAhead is
// left nil (not materialized to true) by setDefaults on a config that has
// never set it - the opposite default from PlaybackPadding/Par2Repair, since
// read-ahead pre-caching trades extra bandwidth/cache pressure for a head
// start and must ship off by default (see RepairConfig.PrecacheReadAhead's
// doc comment).
func TestPrecacheReadAheadDefaultsFalseOnFreshConfig(t *testing.T) {
	var c Config
	c.setDefaults()

	if c.Repair.PrecacheReadAhead != nil {
		t.Fatalf("PrecacheReadAhead = %v on a fresh config, want nil (unset)", *c.Repair.PrecacheReadAhead)
	}
	if c.Repair.PrecacheReadAheadEnabled() {
		t.Fatalf("PrecacheReadAheadEnabled() = true on a fresh config, want false")
	}
}

// TestPrecacheReadAheadSurvivesUnrelatedConfigSave proves the exact
// field-preservation pattern api.handleUpdateConfig applies (copying the
// live value over the freshly-decoded one for every Repair field the
// general settings form doesn't submit) carries PrecacheReadAhead through
// unharmed - the same *bool field-preservation bug class that used to
// silently wipe the Arr webhook token, and the one PrecacheReadAheadEnabled
// duplicate this feature briefly had before being consolidated onto this
// single field.
func TestPrecacheReadAheadSurvivesUnrelatedConfigSave(t *testing.T) {
	enabled := true
	current := Config{Repair: RepairConfig{PrecacheReadAhead: &enabled}}

	// incoming simulates a decode of the general settings form's JSON body,
	// which has no field for repair.precache_read_ahead_enabled - it lands
	// as the zero value (nil), exactly as handleUpdateConfig sees it before
	// applying its preservation line.
	var incoming Config
	incoming.DownloadFolder = "/changed/unrelated/path" // an unrelated field actually changing

	// This is the exact preservation handleUpdateConfig performs.
	incoming.Repair.PrecacheReadAhead = current.Repair.PrecacheReadAhead

	if !incoming.Repair.PrecacheReadAheadEnabled() {
		t.Fatalf("PrecacheReadAheadEnabled() = false after preserving an explicitly-enabled value across an unrelated config save, want true")
	}
	if incoming.DownloadFolder != "/changed/unrelated/path" {
		t.Fatalf("unrelated field DownloadFolder was not applied from the incoming config")
	}
}
