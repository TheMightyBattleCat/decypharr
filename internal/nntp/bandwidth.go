package nntp

import (
    "encoding/json"
    "io"
    "os"
    "path/filepath"
    "strings"
    "sync"
    "sync/atomic"
    "time"

    "github.com/rs/zerolog"
    "github.com/sirrobot01/decypharr/internal/config"
)

// bandwidthFileName is the per-provider usage ledger, stored alongside
// config.json in the main data dir so quota counters survive restarts.
const bandwidthFileName = "usenet_bandwidth.json"

// providerQuota is the parsed quota rule for one provider.
type providerQuota struct {
    limitBytes int64  // 0 = unlimited (usage is still tracked for display)
    period     string // "day" | "week" | "month"
    resetDay   int    // week: 0=Sun..6=Sat ; month: 1..31 ; unused for day
    resetHour  int    // 0..23, server local time
}

// bwProvider is the live per-provider tracker. used is incremented on the
// socket read hot path via a plain atomic add; window rollover happens on the
// far less frequent check/snapshot path under mu.
type bwProvider struct {
    used        atomic.Int64
    periodStart atomic.Int64 // unix nanos of current window start
    quota       providerQuota
    loggedBlock atomic.Bool  // one-shot: have we logged the current block?
    mu          sync.Mutex
}

// persistedProvider is the on-disk form.
type persistedProvider struct {
    BytesUsed   int64     `json:"bytes_used"`
    PeriodStart time.Time `json:"period_start"`
}

type persistedState struct {
    Providers map[string]persistedProvider `json:"providers"`
}

// ProviderBandwidthSnapshot is a read-only view for the stats page.
type ProviderBandwidthSnapshot struct {
    BytesUsed  int64
    QuotaBytes int64
    Period     string
    ResetAt    time.Time
    Exceeded   bool
}

// BandwidthTracker meters per-provider downloaded bytes and enforces quotas.
// byHost is fixed for the client's lifetime, so lookups need no lock.
type BandwidthTracker struct {
    byHost map[string]*bwProvider
    path   string
    dirty  atomic.Bool
    stop   chan struct{}
    wg     sync.WaitGroup
    logger zerolog.Logger
}

func newBandwidthTracker(providers []config.UsenetProvider, log zerolog.Logger) *BandwidthTracker {
    bt := &BandwidthTracker{
        byHost: make(map[string]*bwProvider, len(providers)),
        path:   filepath.Join(config.GetMainPath(), bandwidthFileName),
        stop:   make(chan struct{}),
        logger: log,
    }
    for _, p := range providers {
        bt.byHost[p.Host] = &bwProvider{quota: parseProviderQuota(p)}
    }
    bt.load()

    // Initialize any provider without persisted state to the current window;
    // roll any loaded state forward if we've crossed a boundary while offline.
    now := time.Now()
    for _, bp := range bt.byHost {
        if bp.periodStart.Load() == 0 {
            bp.periodStart.Store(currentWindowStart(now, bp.quota).UnixNano())
        } else {
            bt.rollIfNeeded(bp, now)
        }
    }

    bt.wg.Add(1)
    go bt.saveLoop()
    return bt
}

func parseProviderQuota(p config.UsenetProvider) providerQuota {
    q := providerQuota{
        period:    normalizePeriod(p.QuotaPeriod),
        resetDay:  p.QuotaResetDay,
        resetHour: p.QuotaResetHour,
    }
    if p.Quota != "" {
        if n, err := config.ParseSize(p.Quota); err == nil && n > 0 {
            q.limitBytes = n
        }
    }
    if q.resetHour < 0 || q.resetHour > 23 {
        q.resetHour = 0
    }
    switch q.period {
    case "week":
        q.resetDay = ((q.resetDay % 7) + 7) % 7
    case "month":
        if q.resetDay < 1 {
            q.resetDay = 1
        }
        if q.resetDay > 31 {
            q.resetDay = 31
        }
    }
    return q
}

func normalizePeriod(s string) string {
    switch strings.ToLower(strings.TrimSpace(s)) {
    case "day", "daily":
        return "day"
    case "month", "monthly":
        return "month"
    default:
        return "week"
    }
}

// newCountingReader wraps a socket reader so every byte read from the wire is
// attributed to the given provider. Returns r unchanged if the host is unknown.
func (bt *BandwidthTracker) newCountingReader(r io.Reader, host string) io.Reader {
    bp := bt.byHost[host]
    if bp == nil {
        return r
    }
    return &countingReader{r: r, bp: bp, bt: bt}
}

type countingReader struct {
    r  io.Reader
    bp *bwProvider
    bt *BandwidthTracker
}

func (cr *countingReader) Read(p []byte) (int, error) {
    n, err := cr.r.Read(p)
    if n > 0 {
        cr.bp.used.Add(int64(n))
        // Avoid hammering the shared flag from every connection once it's set.
        if !cr.bt.dirty.Load() {
            cr.bt.dirty.Store(true)
        }
    }
    return n, err
}

// Blocked reports whether the provider is currently over its cap. Rolls the
// window first, and logs once on the transition into a blocked state.
func (bt *BandwidthTracker) Blocked(host string) bool {
    bp := bt.byHost[host]
    if bp == nil || bp.quota.limitBytes <= 0 {
        return false
    }
    bt.rollIfNeeded(bp, time.Now())
    over := bp.used.Load() >= bp.quota.limitBytes
    if over {
        if !bp.loggedBlock.Swap(true) {
            bt.logger.Warn().
                Str("host", host).
                Int64("used", bp.used.Load()).
                Int64("quota", bp.quota.limitBytes).
                Str("period", bp.quota.period).
                Msg("provider bandwidth quota reached; disabling until period resets")
        }
    }
    return over
}

// Snapshot returns a read-only view for stats. Rolls the window first.
func (bt *BandwidthTracker) Snapshot(host string) (ProviderBandwidthSnapshot, bool) {
    bp := bt.byHost[host]
    if bp == nil {
        return ProviderBandwidthSnapshot{}, false
    }
    now := time.Now()
    bt.rollIfNeeded(bp, now)
    used := bp.used.Load()
    s := ProviderBandwidthSnapshot{
        BytesUsed:  used,
        QuotaBytes: bp.quota.limitBytes,
        Period:     bp.quota.period,
        Exceeded:   bp.quota.limitBytes > 0 && used >= bp.quota.limitBytes,
    }
    if bp.quota.limitBytes > 0 {
        s.ResetAt = nextReset(now, bp.quota)
    }
    return s, true
}

// rollIfNeeded resets the counter when the wall clock has moved into a new
// window. Deriving the window start directly from now handles arbitrary
// offline gaps in one step (no catch-up loop needed).
func (bt *BandwidthTracker) rollIfNeeded(bp *bwProvider, now time.Time) {
    ws := currentWindowStart(now, bp.quota).UnixNano()
    if bp.periodStart.Load() == ws {
        return
    }
    bp.mu.Lock()
    if bp.periodStart.Load() != ws {
        bp.periodStart.Store(ws)
        bp.used.Store(0)
        bp.loggedBlock.Store(false)
        bt.dirty.Store(true)
    }
    bp.mu.Unlock()
}

// currentWindowStart returns the most recent reset boundary at or before now.
func currentWindowStart(now time.Time, q providerQuota) time.Time {
    loc := now.Location()
    h := q.resetHour
    switch q.period {
    case "day":
        s := time.Date(now.Year(), now.Month(), now.Day(), h, 0, 0, 0, loc)
        if s.After(now) {
            s = s.AddDate(0, 0, -1)
        }
        return s
    case "month":
        day := clampDay(now.Year(), now.Month(), q.resetDay)
        s := time.Date(now.Year(), now.Month(), day, h, 0, 0, 0, loc)
        if s.After(now) {
            pm := now.AddDate(0, -1, 0)
            day = clampDay(pm.Year(), pm.Month(), q.resetDay)
            s = time.Date(pm.Year(), pm.Month(), day, h, 0, 0, 0, loc)
        }
        return s
    default: // week
        s := time.Date(now.Year(), now.Month(), now.Day(), h, 0, 0, 0, loc)
        back := (int(s.Weekday()) - q.resetDay + 7) % 7
        s = s.AddDate(0, 0, -back)
        if s.After(now) {
            s = s.AddDate(0, 0, -7)
        }
        return s
    }
}

// nextReset returns the next boundary strictly after now, for display.
func nextReset(now time.Time, q providerQuota) time.Time {
    start := currentWindowStart(now, q)
    loc := start.Location()
    switch q.period {
    case "day":
        return start.AddDate(0, 0, 1)
    case "month":
        nm := start.AddDate(0, 1, 0)
        day := clampDay(nm.Year(), nm.Month(), q.resetDay)
        return time.Date(nm.Year(), nm.Month(), day, q.resetHour, 0, 0, 0, loc)
    default: // week
        return start.AddDate(0, 0, 7)
    }
}

// clampDay clamps a desired day-of-month to the last valid day of that month.
func clampDay(y int, m time.Month, want int) int {
    if want < 1 {
        want = 1
    }
    // Day 0 of the next month == last day of month m.
    last := time.Date(y, m+1, 0, 0, 0, 0, 0, time.UTC).Day()
    if want > last {
        return last
    }
    return want
}

func (bt *BandwidthTracker) load() {
    data, err := os.ReadFile(bt.path)
    if err != nil {
        return
    }
    var st persistedState
    if err := json.Unmarshal(data, &st); err != nil {
        return
    }
    for host, pp := range st.Providers {
        if bp := bt.byHost[host]; bp != nil {
            bp.used.Store(pp.BytesUsed)
            if !pp.PeriodStart.IsZero() {
                bp.periodStart.Store(pp.PeriodStart.UnixNano())
            }
        }
    }
}

func (bt *BandwidthTracker) save() {
    st := persistedState{Providers: make(map[string]persistedProvider, len(bt.byHost))}
    for host, bp := range bt.byHost {
        st.Providers[host] = persistedProvider{
            BytesUsed:   bp.used.Load(),
            PeriodStart: time.Unix(0, bp.periodStart.Load()),
        }
    }
    data, err := json.MarshalIndent(st, "", "  ")
    if err != nil {
        return
    }
    tmp := bt.path + ".tmp"
    if err := os.WriteFile(tmp, data, 0644); err != nil {
        return
    }
    _ = os.Rename(tmp, bt.path)
}

func (bt *BandwidthTracker) saveLoop() {
    defer bt.wg.Done()
    t := time.NewTicker(30 * time.Second)
    defer t.Stop()
    for {
        select {
        case <-bt.stop:
            bt.save()
            return
        case <-t.C:
            if bt.dirty.Swap(false) {
                bt.save()
            }
        }
    }
}

// Close flushes state and stops the background saver.
func (bt *BandwidthTracker) Close() {
    select {
    case <-bt.stop:
    default:
        close(bt.stop)
    }
    bt.wg.Wait()
}

// providerBlocked is the Client-side quota gate used by connection acquisition.
func (c *Client) providerBlocked(p config.UsenetProvider) bool {
    return c.bw != nil && c.bw.Blocked(p.Host)
}
