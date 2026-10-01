---
title: Usenet Configuration
description: Direct NNTP streaming configuration.
---

Decypharr supports direct NNTP streaming from Usenet providers - no additional download client required.

## How It Works

Decypharr connects directly to NNTP servers to:

1. Parse NZB files for segment information
2. Stream segments on-demand for playback
3. Download and assemble complete files

## Provider Configuration

### Add Provider

```json
{
  "usenet": {
    "providers": [
      {
        "host": "news.provider.com",
        "port": 563,
        "username": "your_username",
        "password": "your_password",
        "backbone": "Omicron",
        "ssl": true,
        "max_connections": 20,
        "priority": 1
      }
    ]
  }
}
```

### Multiple Providers

Decypharr can use multiple providers with priority and failover:

```json
{
  "usenet": {
    "providers": [
      {
        "host": "primary.news.com",
        "port": 563,
        "username": "user1",
        "password": "pass1",
        "backbone": "UsenetExpress",
        "ssl": true,
        "max_connections": 20,
        "priority": 1
      },
      {
        "host": "backup.news.com",
        "port": 563,
        "username": "user2",
        "password": "pass2",
        "backbone": "Omicron",
        "ssl": true,
        "max_connections": 10,
        "priority": 2
      }
    ]
  }
}
```

Lower `priority` = higher preference.

`backbone` is optional. Set it when two providers share the same article spool so Decypharr can skip same-backbone providers after `423/430 article not found` responses.

### Prefer Faster Servers

Every stream fits inside the priority-1 provider's connection pool, so without help all playback goes to that provider however slowly it serves. **Settings → Providers → Usenet → Prefer Faster Servers** (on by default) measures how fast each provider delivers articles and tries a primary that is far slower than the others after them:

- A primary counts as slow only when it serves under 4 MiB/s per connection **and** the fastest primary is more than 4× faster. While no primary is that slow, providers are used in exact priority order.
- Slow primaries go after the faster primaries, the least slow first. An article the fast providers don't have comes from the least slow provider that has it.
- Backup providers (`"backup": true`) are never reordered and are still only used for articles the primaries can't provide. A primary that has reached its quota reserve still counts as a primary.
- A provider's rate is measured over the last minute of downloads (bytes over download time), not article by article, and the time spent writing to local disk isn't counted against it.
- A slow provider gets one article every 5 minutes to measure it again, and gets its position back once it is 25% above the cut, and not before it has been deferred for 30 seconds.

Turn it off to always use providers in priority order. The setting applies within a second of saving, without a restart:

```json
{
  "usenet": {
    "prefer_faster_servers": false
  }
}
```

The provider stats show each provider's measured rate (`body_mib_s`) and whether it is currently tried after the others (`body_deferred`). Debug logs record each change: `Body routing: provider is too slow, trying the other primaries first` and `... fast enough again, back at its priority position`.

## Performance Tuning

### Connection Limits

```json
{
  "usenet": {
    "max_connections": 15,
    "processing_max_connections": 15
  }
}
```

- `max_connections`: Per-file streaming connection limit
- `processing_max_connections`: Per-file parsing and NZB download connection limit
- Provider `max_connections`: Per-provider limit

**Example:**

- Global: `15`
- Provider A: `20`
- Provider B: `10`

→ Up to 15 connections per file, split between providers based on priority

### Read-Ahead Bursts Yield to Playback

Read-ahead bursts (next-episode pre-caching, read-ahead of a file nobody is watching yet) compete with playback for the same providers. When a client's stream waits 2 seconds or more for its data, bursts on **other** files stop starting new downloads until playback has gone 30 seconds without such a wait. The file being played keeps its own read-ahead running, since that fills the cache it reads next. A burst paused through a long buffering session can end incomplete; the next trigger fills the gaps.

Through the DFS mount, the wait that counts is the player's own read waiting for data the mount doesn't have yet. The mount's downloaders read far ahead of the player, so a slow download there doesn't count as a stall on its own; the debug log shows those as `mount read-ahead: concurrent segment fetch blocked`.

The nightly repair sweep's ffprobe checks yield too, by narrowing rather than pausing: while playback is waiting, each check's prefetch drops from up to 32 connections to 4 (about 24 MiB/s, still well inside ffprobe's time limits) and widens again 30 seconds after the last wait. Checks on import aren't narrowed. The debug log shows `verification prefetch narrowed` / `verification prefetch back to full width`.

This is on by default. To turn it off (no UI setting):

```json
{
  "precache": {
    "precache_yield_to_playback": false
  }
}
```

Pauses and resumes are logged at debug: `read-ahead paused: playback of another file is waiting on the network` / `read-ahead resumed`.

### Read-Ahead Bursts Keep What They Fetch

A read-ahead burst (the rest of the file being played, or the next episode) works through the file in 96 MB chunks. Each chunk is copied into the durable disk cache as soon as it arrives, and a chunk the disk cache already holds is skipped without being downloaded. Segments with a missing article waiting on repair are not copied, so zero-filled data never lands in the disk cache. The completion log carries `fetchedBytes` and `skippedBytes`, and a `durable persist complete` line counts what was written.

### Read-Ahead Buffer

```json
{
  "usenet": {
    "read_ahead": "16MB"
  }
}
```

Prefetch buffer for smoother playback. Higher = smoother but more memory.

### Processing Limits

```json
{
  "max_active_downloads": 5,
  "usenet": {"processing_timeout": "10m"}
}
```

- `max_active_downloads`: Shared active-download limit for torrents and NZBs
- `processing_timeout`: Mark as bad if processing exceeds this

### Availability Checking

```json
{
  "usenet": {
    "availability_sample_percent": 10,
    "import_availability_sample_percent": 1
  }
}
```

Use `availability_sample_percent` for repair checks and
`import_availability_sample_percent` for the availability gate when adding an NZB.

- `100`: Check all segments (slow but accurate)
- `10`: Check 10% (fast but may miss issues)
- `1`: Quick import check (default)

A segment check is a STAT, which asks the server whether an article exists without downloading it. Checks are sent 16 at a time on one connection, followed by a `DATE` command whose reply must come straight after the last STAT reply. If the replies don't line up (the server dropped or added a line), nothing in that batch counts as found or missing, the connection is closed, and the segments are checked on the next provider. The provider stats show each provider's measured time per found article (`stat_ms`) and how many batches were thrown away this way (`stat_desyncs`, normally 0).

A provider that fails a batch or cannot give a connection is treated as slow for a while and takes fewer checks. `stat_errors` counts those failures, and `stat_err_checkout`, `stat_err_timeout`, `stat_err_eof`, `stat_err_closed` and `stat_err_other` split them by cause: no connection could be opened, a reply did not arrive in time, the provider closed the connection, the connection was closed on this side, or anything else. `stat_cancelled` counts batches stopped because the check itself was cancelled, which is not held against the provider. At debug log level each failure is also logged with the error behind it.

A check of one file starts on whichever fast provider is free. Providers answer "no such article" far more slowly than "found", so a file that a provider does not hold at all is slow to check there. Once a provider has answered 32 of a file's segments without finding any, the rest of that check asks it last among the primaries (or last among the backups, for a backup). It is still asked about any segment no other provider has, so a segment is only called missing when every provider says so. `stat_hint_demotions` counts the checks that moved a provider back this way.

The import checks (availability, STAT census, padding and ffprobe) cover every video file of 100 MiB or more, and smaller video files that are at least a quarter of the size of the release's largest video. A lone SD episode or a pack of small episodes is checked; a small extra bundled with a film is not. Files named as samples are dropped when the NZB is added unless `allow_samples` is on. The repair sweep checks every library file whatever its size.

## Disk Buffer

```json
{
  "usenet": {
    "disk_buffer_path": "/cache/usenet/streams"
  }
}
```

Streams use disk buffer for assembly. Ensure sufficient disk space.

## Repair

```json
{
  "usenet": {
    "skip_repair": false
  }
}
```

## Arr Integration

Arrs send NZB files to Decypharr via the Sabnzbd API endpoint:

See [Sabnzbd Integration](./sabnzbd/) for details.

## Troubleshooting

### Connection Failures

- Verify host/port/SSL settings
- Test manually: `telnet news.provider.com 563`
- Check provider status

### Slow Streaming

1. Increase `max_connections` per provider
2. Increase global `max_connections`
3. Increase `read_ahead` buffer

`GET /debug/stats` shows where download time goes under `usenet.fetch_timing`, counted since start: `segment_wait` (a read waiting for a slot in its own connection limit), `checkout` (waiting for a connection from the pool), and per provider `latency` (request sent to first byte back), `transfer` (first byte to article decoded), `hold` (connection checked out to returned) and `idle` (returned to next use). Each has a count, mean, 50th/90th/99th percentile, raw bucket counts and summed time, so two readings taken around a test can be subtracted. Take them while no repair sweep is running, since sweep checks add their own samples. High `latency` with little `idle` means connections wait on the server; long `idle` with short `checkout` waits means reads aren't keeping connections busy.

### Processing Timeouts

- Increase `processing_timeout` for large files
- Reduce `availability_sample_percent` for faster checks
- Increase `max_active_downloads` if the system and providers have capacity

### Incomplete Downloads

- Enable `skip_repair: false` for PAR2 repair
- Check provider retention (old files may be incomplete)
- Try backup provider if available

## Example Configuration

Full Usenet config with optimal settings:

```json
{
  "max_active_downloads": 5,
  "usenet": {
    "providers": [
      {
        "host": "us.news.provider.com",
        "port": 563,
        "username": "user",
        "password": "pass",
        "ssl": true,
        "max_connections": 30,
        "priority": 1
      }
    ],
    "max_connections": 15,
    "processing_max_connections": 15,
    "read_ahead": "32MB",
    "processing_timeout": "15m",
    "availability_sample_percent": 5,
    "disk_buffer_path": "/cache/usenet",
    "skip_repair": false
  }
}
```
