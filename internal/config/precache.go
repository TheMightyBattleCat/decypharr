package config

// PrecacheConfig configures proactive pre-caching and repair ahead of
// playback: read-ahead damage detection for the file currently playing
// (movies and episodes alike, see pkg/manager.Precache), and (for Sonarr
// episodes) pre-caching the next episode before it's needed.
type PrecacheConfig struct {
	// PrecacheReadAheadEnabled turns on aggressive read-ahead caching once a
	// playing file's read position crosses PrecacheThresholdPercent. Off
	// entirely leaves playback exactly as it behaves without this feature -
	// normal streaming prefetch (Usenet.ReadAhead) is unaffected either way.
	// *bool so an existing config.json predating this field defaults to true
	// on load, same convention as Repair.PlaybackPadding.
	PrecacheReadAheadEnabled *bool `json:"precache_read_ahead_enabled,omitempty"`

	// PrecacheThresholdPercent is how far (1-100) into a file playback must
	// reach, by byte position, before read-ahead pre-caching kicks in.
	// Default 10.
	PrecacheThresholdPercent int `json:"precache_threshold_percent,omitempty"`

	// PrecacheReadAheadConcurrency bounds how many segments the read-ahead
	// pass fetches in parallel for the remainder of a playing file -
	// distinct from (and typically higher than) normal streaming prefetch
	// concurrency, since read-ahead is a deliberate burst racing playback,
	// not steady-state streaming. Default 12.
	PrecacheReadAheadConcurrency int `json:"precache_read_ahead_concurrency,omitempty"`
}

func (p PrecacheConfig) IsZero() bool {
	return p.PrecacheReadAheadEnabled == nil &&
		p.PrecacheThresholdPercent == 0 &&
		p.PrecacheReadAheadConcurrency == 0
}

// ReadAheadEnabled reports whether read-ahead precache is active, defaulting
// to true when unset (see PrecacheReadAheadEnabled's doc comment).
func (p PrecacheConfig) ReadAheadEnabled() bool {
	return p.PrecacheReadAheadEnabled == nil || *p.PrecacheReadAheadEnabled
}

// ThresholdPercent returns the configured threshold, clamped to a sane
// [1,100] range and defaulting to 10 when unset/out of range.
func (p PrecacheConfig) ThresholdPercent() int {
	if p.PrecacheThresholdPercent <= 0 || p.PrecacheThresholdPercent > 100 {
		return 10
	}
	return p.PrecacheThresholdPercent
}

// ReadAheadConcurrency returns the configured concurrency, defaulting to 12
// when unset/non-positive.
func (p PrecacheConfig) ReadAheadConcurrency() int {
	if p.PrecacheReadAheadConcurrency > 0 {
		return p.PrecacheReadAheadConcurrency
	}
	return 12
}
