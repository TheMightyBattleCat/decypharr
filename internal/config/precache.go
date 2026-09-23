package config

// PrecacheConfig configures proactive pre-caching and repair ahead of
// playback: read-ahead damage detection for the file currently playing
// (movies and episodes alike, see pkg/manager.Precache), and (for Sonarr
// episodes) pre-caching the next episode before it's needed. Whether the
// read-ahead burst runs at all is gated by RepairConfig.PrecacheReadAhead
// (see its doc comment) rather than a field here, so it lives in the same
// settings section as the other repair-adjacent toggles (PlaybackPadding,
// Par2Repair) and shares their save/load path.
type PrecacheConfig struct {
	// PrecacheThresholdPercent is how far (1-100) into a file playback must
	// reach, by byte position, before read-ahead pre-caching kicks in.
	// Default 10.
	PrecacheThresholdPercent int `json:"precache_threshold_percent,omitempty"`

	// PrecacheReadAheadConcurrency is no longer read: bursts use
	// Usenet.MaxConnections (Settings > Usenet > Max Connections Per File),
	// the per-file connection count playback uses - see ReadAheadConcurrency.
	// Kept so an older config.json still parses.
	PrecacheReadAheadConcurrency int `json:"precache_read_ahead_concurrency,omitempty"`

	// PrecacheNextEpisodes is how many upcoming episodes (same series, same
	// season) to burst-download and repair ahead of the one playing, once it
	// crosses the threshold. Ignored while PrecacheWholeSeason is on. *int so
	// 0 can mean "explicitly disabled" - nil defaults to 1. Movies have no
	// "next episode"; this only applies to Sonarr-tracked episode files.
	PrecacheNextEpisodes *int `json:"precache_next_episodes,omitempty"`

	// PrecacheWholeSeason, when on, walks forward to the end of the playing
	// episode's season instead of stopping PrecacheNextEpisodes ahead. *bool
	// so unset means on: before this field existed the walk always ran to the
	// season's end (every finished burst restarted it at full depth), and an
	// upgrade must not quietly shorten it.
	PrecacheWholeSeason *bool `json:"precache_whole_season,omitempty"`

	// PrecacheEvictAfterWatched, when true, reclaims a pre-cached next
	// episode's disk footprint as soon as it has actually been watched,
	// instead of leaving it to the normal cache idle-timeout.
	PrecacheEvictAfterWatched bool `json:"precache_evict_after_watched,omitempty"`

	// PrecacheMaxBytes bounds the bytes next-episode bursts hold reserved at
	// once. A reservation is released as soon as its burst ends (or, with
	// PrecacheEvictAfterWatched, once the episode is watched), so this is not
	// a disk-footprint cap - the DFS cache's own eviction is what bounds
	// disk. No longer on the Repair page. nil or <= 0 means
	// precacheMaxBytesCeiling, and larger values are clamped to it. It never
	// turns pre-caching off: RepairConfig.PrecacheReadAhead is the switch.
	PrecacheMaxBytes *int64 `json:"precache_max_bytes,omitempty"`

	// PrecacheYieldToPlayback pauses read-ahead bursts on other files while a
	// client's playback is stalling on the network (see
	// pkg/usenet/fs/reader/playback_yield.go). *bool so unset means on; an
	// explicit false turns the pause off. Read when a pause would start and on
	// every poll while paused, so a change applies without a restart.
	PrecacheYieldToPlayback *bool `json:"precache_yield_to_playback,omitempty"`
}

func (p PrecacheConfig) IsZero() bool {
	return p.PrecacheThresholdPercent == 0 &&
		p.PrecacheReadAheadConcurrency == 0 &&
		p.PrecacheNextEpisodes == nil &&
		p.PrecacheWholeSeason == nil &&
		!p.PrecacheEvictAfterWatched &&
		p.PrecacheMaxBytes == nil &&
		p.PrecacheYieldToPlayback == nil
}

// YieldToPlayback reports whether read-ahead bursts pause for stalled playback
// on other files, defaulting to true when unset.
func (p PrecacheConfig) YieldToPlayback() bool {
	return p.PrecacheYieldToPlayback == nil || *p.PrecacheYieldToPlayback
}

// ThresholdPercent returns the configured threshold, clamped to a sane
// [1,100] range and defaulting to 10 when unset/out of range.
func (p PrecacheConfig) ThresholdPercent() int {
	if p.PrecacheThresholdPercent <= 0 || p.PrecacheThresholdPercent > 100 {
		return 10
	}
	return p.PrecacheThresholdPercent
}

// ReadAheadConcurrency returns how many segments a burst fetches at once:
// Usenet.MaxConnections (Settings > Usenet > Max Connections Per File), the
// per-file connection count playback uses. Each in-flight segment holds one
// connection, so segments in parallel and connections are the same number.
// PrecacheReadAheadConcurrency is not consulted - applyPrecacheDefaults used
// to write 12 into every saved config, so honouring it would always win.
func (p PrecacheConfig) ReadAheadConcurrency() int {
	if n := Get().Usenet.MaxConnections; n > 0 {
		return n
	}
	return 15
}

// WholeSeason reports whether the forward walk runs to the end of the
// season, defaulting to true when unset (see PrecacheWholeSeason).
func (p PrecacheConfig) WholeSeason() bool {
	return p.PrecacheWholeSeason == nil || *p.PrecacheWholeSeason
}

// EpisodesAhead is how far the forward walk goes: -1 for the rest of the
// season, otherwise NextEpisodes (0 disables it).
func (p PrecacheConfig) EpisodesAhead() int {
	if p.WholeSeason() {
		return -1
	}
	return p.NextEpisodes()
}

// NextEpisodes returns how many episodes ahead to pre-cache, defaulting to 1
// when unset. An explicit 0 disables next-episode pre-caching.
func (p PrecacheConfig) NextEpisodes() int {
	if p.PrecacheNextEpisodes == nil {
		return 1
	}
	if *p.PrecacheNextEpisodes < 0 {
		return 0
	}
	return *p.PrecacheNextEpisodes
}

// precacheMaxBytesCeiling is both the default and the upper bound for
// PrecacheMaxBytes - see its doc comment.
const precacheMaxBytesCeiling = 256 * 1024 * 1024 * 1024 // 256 GiB

// MaxBytes returns the reservation bound in bytes: nil or <= 0 means
// precacheMaxBytesCeiling, and anything larger is clamped to it.
func (p PrecacheConfig) MaxBytes() int64 {
	if p.PrecacheMaxBytes == nil || *p.PrecacheMaxBytes <= 0 || *p.PrecacheMaxBytes > precacheMaxBytesCeiling {
		return precacheMaxBytesCeiling
	}
	return *p.PrecacheMaxBytes
}
