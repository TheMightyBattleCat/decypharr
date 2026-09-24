// Package manager: plexSessionChecker gates Precache's read-ahead/next-
// episode bursts behind an actual Plex "now playing" session for the file
// being read, so a library scan, thumbnail-generation pass, or metadata
// analysis pass - which issue the same ranged reads through the mount -
// can't spuriously trigger precache. Disabled entirely when
// config.PlexConfig.URL is unset (see PlexConfig's doc comment). Once
// enabled, a failed poll is retried a few times before being treated as a
// failure, and a short grace window lets a momentary Plex blip keep serving
// the last known sessions; only once Plex has stayed unreachable past the
// grace window does the gate fail closed and deny precache (see refresh).
package manager

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	json "github.com/bytedance/sonic"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// plexSessionFetchTimeout bounds one /status/sessions request so an
// unreachable/hanging Plex server can't stall the calling stream request
// (Observe runs inline in the ranged-read path - see stream.go) beyond a
// bounded worst case.
const plexSessionFetchTimeout = 5 * time.Second

// plexWarnInterval throttles the "Plex unreachable" log line - refresh may
// re-run as often as once per PlexConfig.SessionCacheTTL, which would
// otherwise spam the log for as long as Plex stays down.
const plexWarnInterval = 5 * time.Minute

// plexSessionFetchRetries bounds how many times refresh retries a failed
// /status/sessions poll, within one refresh call, before treating it as a
// failure - a single dropped connection or transient 5xx shouldn't be
// enough to move the gate toward fail-closed.
const plexSessionFetchRetries = 3

// plexSessionRetryDelay separates retry attempts within one refresh call.
// Kept short since plexSessionFetchTimeout already bounds each attempt and
// Observe runs inline in the ranged-read path.
const plexSessionRetryDelay = 250 * time.Millisecond

// plexSessionGraceMinimum floors the grace window (see refresh) so a very
// short SessionCacheTTL doesn't shrink the window to nearly nothing.
const plexSessionGraceMinimum = 30 * time.Second

// flexInt64 decodes a JSON value that may arrive as either a bare number
// (e.g. "viewOffset":443142) or a quoted string (e.g. "ratingKey":"208698").
// Plex mixes both forms field-by-field within the same /status/sessions
// object, and the strict decoder fails the WHOLE response on any single
// mismatch - which, behind the fail-closed session gate, silently disables
// precache fleet-wide. Decoding the numeric fields tolerantly contains a
// wire-form surprise to "that value is 0" instead of a total gate outage.
// Empty or absent decodes to 0.
type flexInt64 int64

func (f *flexInt64) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		*f = 0
		return nil
	}
	// Strip surrounding quotes if present.
	if b[0] == '"' && b[len(b)-1] == '"' {
		b = b[1 : len(b)-1]
	}
	if len(b) == 0 {
		*f = 0
		return nil
	}
	if n, err := strconv.ParseInt(string(b), 10, 64); err == nil {
		*f = flexInt64(n)
		return nil
	}
	// A float or exponent form ("443142.0", 2.7e6) is truncated. Anything
	// else - a bool, an out-of-range number, garbage - is 0, as documented:
	// failing here fails the whole response it sits in.
	if v, err := strconv.ParseFloat(string(b), 64); err == nil && !math.IsNaN(v) && v >= math.MinInt64 && v < math.MaxInt64 {
		*f = flexInt64(int64(v))
		return nil
	}
	*f = 0
	return nil
}

// plexSessionsResponse is the subset of Plex's /status/sessions payload
// this checker needs: the file path backing each currently-playing item, and
// its playback progress. ViewOffset/Duration decode via flexInt64 - Plex
// sends these as bare numbers, unlike RatingKey's quoted string form (see
// flexInt64's doc comment for why tolerant decoding matters here).
type plexSessionsResponse struct {
	MediaContainer struct {
		Metadata []struct {
			RatingKey  flexString `json:"ratingKey"`
			ViewOffset flexInt64  `json:"viewOffset"`
			Duration   flexInt64  `json:"duration"`
			Player     struct {
				State string `json:"state"`
			} `json:"Player"`
			Media []struct {
				Part []struct {
					File string `json:"file"`
				} `json:"Part"`
			} `json:"Media"`
		} `json:"Metadata"`
	} `json:"MediaContainer"`
}

// plexSessionFile is one playing file discovered by fetchSessions, paired
// with its session's playback progress.
type plexSessionFile struct {
	path       string
	viewOffset int64
	duration   int64
}

// sessionProgress is a session's live playback position (milliseconds), as
// resolved for one file path in plexSessionChecker.resolved. duration is 0
// if Plex didn't report it or it failed to parse.
type sessionProgress struct {
	viewOffset int64
	duration   int64
}

// plexSessionChecker answers "is this entry+filename part of an active Plex
// playing session right now", backed by a short-lived cache of Plex's
// current session list so Observe (called on every ranged read) doesn't
// query Plex on every call.
type plexSessionChecker struct {
	manager *Manager
	logger  zerolog.Logger
	client  *http.Client

	mu          sync.Mutex
	fetched     time.Time
	lastSuccess time.Time                  // last time a poll succeeded; zero if it never has
	degraded    bool                       // true once a failed poll has exceeded the grace window; isPlexWatching denies everything while degraded
	resolved    map[string]sessionProgress // resolved absolute file path -> playback progress of files in active sessions, from the last successful poll

	lastWarnMu sync.Mutex
	lastWarn   time.Time
}

func newPlexSessionChecker(m *Manager) *plexSessionChecker {
	return &plexSessionChecker{
		manager:  m,
		logger:   logger.New("plex-sessions"),
		client:   &http.Client{Timeout: plexSessionFetchTimeout},
		resolved: make(map[string]sessionProgress),
	}
}

// isPlexWatching reports whether filename, as resolved for entry, is part
// of an active Plex playing session. Always true when the gate is disabled
// (PlexConfig.URL unset). Once enabled, false whenever degraded - Plex has
// stayed unreachable past the grace window in refresh - so an unreachable
// Plex server denies precache rather than firing on background activity.
func (c *plexSessionChecker) isPlexWatching(entry *storage.Entry, filename string) bool {
	cfg := config.Get().Plex
	if !cfg.Enabled() {
		return true
	}
	c.refresh(cfg)

	expected := filepath.Join(c.manager.GetTorrentMountPath(entry), filename)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.degraded {
		return false
	}
	_, ok := c.resolved[expected]
	return ok
}

// sessionProgress returns a snapshot of every active session's resolved path
// and playback progress, from the last successful poll. Returns a copy so
// the caller (progressTriggerLoop) can iterate, reverse-lookup entries, and
// spawn goroutines without holding c.mu.
func (c *plexSessionChecker) sessionProgress() map[string]sessionProgress {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]sessionProgress, len(c.resolved))
	for k, v := range c.resolved {
		out[k] = v
	}
	return out
}

// refresh re-fetches and re-resolves Plex's session list if the cached one
// is older than cfg.SessionTTL(). A failing poll is retried up to
// plexSessionFetchRetries times before being treated as a failure; if every
// attempt fails, the checker keeps serving the last resolved session list
// as though healthy until time.Since(lastSuccess) exceeds the grace window
// (max(2*cfg.SessionTTL(), plexSessionGraceMinimum)), at which point it
// flips degraded so isPlexWatching starts denying precache.
func (c *plexSessionChecker) refresh(cfg config.PlexConfig) {
	c.mu.Lock()
	stale := time.Since(c.fetched) >= cfg.SessionTTL()
	c.mu.Unlock()
	if !stale {
		return
	}

	var files []plexSessionFile
	var err error
	for attempt := 0; attempt < plexSessionFetchRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(plexSessionRetryDelay)
		}
		files, err = c.fetchSessions(cfg)
		if err == nil {
			break
		}
	}
	now := time.Now()

	if err != nil {
		grace := 2 * cfg.SessionTTL()
		if grace < plexSessionGraceMinimum {
			grace = plexSessionGraceMinimum
		}
		c.mu.Lock()
		c.fetched = now
		c.degraded = c.lastSuccess.IsZero() || time.Since(c.lastSuccess) >= grace
		c.mu.Unlock()
		c.warnDebounced(err)
		return
	}

	resolved := resolvePlexSessionPaths(files)
	c.mu.Lock()
	c.fetched = now
	c.lastSuccess = now
	c.degraded = false
	c.resolved = resolved
	c.mu.Unlock()
}

// plexGET issues an authenticated GET against a Plex endpoint and decodes the
// response into out. Both the live-sessions poll and the per-session metadata
// fallback go through here, so their request shape - auth header, Accept,
// client, and timeout - is guaranteed identical and can't silently diverge.
func (c *plexSessionChecker) plexGET(ctx context.Context, cfg config.PlexConfig, path string, out interface{}) error {
	url, err := utils.JoinURL(cfg.URL, path)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Plex-Token", cfg.Token)

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("plex: unexpected status %d from %s", resp.StatusCode, path)
	}
	return json.ConfigDefault.NewDecoder(resp.Body).Decode(out)
}

// fetchSessions queries Plex's /status/sessions and returns the raw file
// path of each currently-playing item's media part. Direct-play sessions
// carry the source file directly. Transcoding sessions don't - Plex omits
// the file on a transcode-decision part and only describes the transcode
// output - so for those we fall back to the item's library metadata, which
// still reports the real source path regardless of playback decision.
func (c *plexSessionChecker) fetchSessions(cfg config.PlexConfig) ([]plexSessionFile, error) {
	ctx, cancel := context.WithTimeout(context.Background(), plexSessionFetchTimeout)
	defer cancel()

	var parsed plexSessionsResponse
	if err := c.plexGET(ctx, cfg, "/status/sessions", &parsed); err != nil {
		return nil, err
	}

	var files []plexSessionFile
	for _, meta := range parsed.MediaContainer.Metadata {
		// Only a genuinely-playing session should open the precache gate. Plex
		// also lists paused/buffering sessions and, during a library scan, items
		// being analysed - none of which are real playback, and all of which
		// would otherwise trigger read-ahead on a scan/metadata read.
		if meta.Player.State != "playing" {
			continue
		}
		// An unparseable value already decoded to 0 in flexInt64.UnmarshalJSON -
		// checkSessionProgress's duration<=0 guard skips it rather than
		// treating it as 0% watched.
		viewOffset := int64(meta.ViewOffset)
		duration := int64(meta.Duration)

		before := len(files)
		for _, media := range meta.Media {
			for _, part := range media.Part {
				if part.File != "" {
					files = append(files, plexSessionFile{path: part.File, viewOffset: viewOffset, duration: duration})
				}
			}
		}

		if len(files) == before && meta.RatingKey != "" {
			mctx, mcancel := context.WithTimeout(context.Background(), plexSessionFetchTimeout)
			var detail plexSessionsResponse
			if err := c.plexGET(mctx, cfg, "/library/metadata/"+string(meta.RatingKey), &detail); err != nil {
				c.logger.Debug().Err(err).Str("ratingKey", string(meta.RatingKey)).Msg("plex: transcode-session metadata lookup failed; leaving title ungated this cycle")
			} else {
				for _, media := range detail.MediaContainer.Metadata {
					for _, part := range media.Media {
						for _, p := range part.Part {
							if p.File != "" {
								files = append(files, plexSessionFile{path: p.File, viewOffset: viewOffset, duration: duration})
							}
						}
					}
				}
			}
			mcancel()
		}
	}
	return files, nil
}

// TestPlexConnection performs an ad-hoc /status/sessions probe against cfg,
// for the repair page's "Test connection" button - independent of the
// long-lived plexSessionChecker cache newPlexSessionChecker builds for
// Precache.Observe, so a test never disturbs the live session cache.
func TestPlexConnection(cfg config.PlexConfig) error {
	c := &plexSessionChecker{client: &http.Client{Timeout: plexSessionFetchTimeout}}
	_, err := c.fetchSessions(cfg)
	return err
}

// resolvePlexSessionPaths resolves each Plex session file path to the
// underlying location precache tracks entries by: readSymlinkTarget first
// (the DownloadActionSymlink setup collectArrFiles/the repair sweep already
// trust), falling back to the raw path itself when it isn't a symlink
// (Plex mounted directly on the DFS/rclone mount, no symlink layer).
func resolvePlexSessionPaths(files []plexSessionFile) map[string]sessionProgress {
	resolved := make(map[string]sessionProgress, len(files))
	for _, f := range files {
		target := readSymlinkTarget(f.path)
		if target == "" {
			target = filepath.Clean(f.path)
		}
		resolved[target] = sessionProgress{viewOffset: f.viewOffset, duration: f.duration}
	}
	return resolved
}

// warnDebounced logs a Plex-unreachable warning at most once per
// plexWarnInterval, so a prolonged outage doesn't spam the log every time
// refresh retries.
func (c *plexSessionChecker) warnDebounced(err error) {
	c.lastWarnMu.Lock()
	defer c.lastWarnMu.Unlock()
	if time.Since(c.lastWarn) < plexWarnInterval {
		return
	}
	c.lastWarn = time.Now()
	c.logger.Warn().Err(err).Msg("failed to fetch Plex sessions after retries; precache session gate will deny once the grace window elapses")
}
