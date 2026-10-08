# Decypharr: Same Same, But Different

![ui](docs/src/assets/images/index.png)

This is a fork of [Decypharr](https://github.com/sirrobot01/decypharr), the media gateway that lets Sonarr,
Radarr and the other Arrs use debrid services and Usenet streaming through one interface. I changed it for my
own use.

I left upstream's `beta` branch in July 2026, partway through getting PAR2 repair to work, and the two have
not been the same codebase since. I have cherry-picked most of what upstream has shipped after that, so this
fork is close to `beta`, with the exceptions listed further down. Where the two differ it is mostly in method:
we are both after the same result, and in a few areas I think this fork gets a better one.

It runs on one server, mine, and that is where all of it has been tested. If you want the version most people
run, with support behind it, use upstream.

## What Decypharr does

- Mock qBittorrent and SABnzbd APIs that the Arrs (Sonarr, Radarr, Lidarr and so on) use as download clients
- Several debrid and Usenet providers behind one interface
- Direct Usenet streaming over NNTP, with no separate download client
- Read-only NFSv4 and SMB servers for the same libraries, including custom virtual folders

## The main differences

### A file that breaks while you are watching is repaired, not just replaced

When upstream finds a broken file it asks Sonarr or Radarr for a different release. This fork does that too,
but when the damage turns up during playback (or pre-cache, below) it first tries to keep the release you
have:

1. **Padding.** If an article is missing on every provider, the gap is filled with zeros and playback carries
   on, usually with a short glitch. There are strict limits (by default 4 missing articles in a row, 64 per
   file, 2% of the file, and never in the first 1%, where the container's headers are). Past those the file
   is treated as broken.
2. **PAR2 repair.** In the background, the missing articles are rebuilt from the release's own PAR2 recovery
   files. The rebuilt bytes are stored as a patch and served from then on, so the next play is clean.
3. **Re-grab.** A file PAR2 cannot fix is deleted, blocklisted and searched for again by the next repair
   sweep.

Damage found at import or by a repair sweep is handled differently, because nobody is watching and the file
is usually not in the cache. Rebuilding it from PAR2 would mean downloading most of the release, while a
re-grab downloads next to nothing until someone plays the new copy. So by default it goes straight to a re-grab, with one exception:
if the file's data happens to be in the cache already, the repair sweep tries PAR2 first, because then it
costs almost nothing.

If you would rather keep the release you have, there is a switch for it on the Repair page: **PAR2 repair
during sweeps**. With it on, the repair sweep tries a full PAR2 repair before any re-grab, and re-grabs in the
same repair sweep if the repair fails. It is off by default and I do not recommend it for most setups. It
eats data, potentially for nothing: think of downloading a 50 GB remux to repair, one that is in your
library for collection's sake, only to find it broken again in a few days when more articles decay with age.

A PAR2 repair can also be started by hand for a single file from the Repair page.

A guard stops the same file being re-grabbed in a loop, and a release that was just found dead is refused for
an hour if the Arr sends it straight back.

Padding and PAR2 repair are on by default. This is the largest difference between the two.

### Bad downloads are caught before they reach the library

- Like upstream, a sample of a new NZB's articles is checked when it is added. This fork checks again once
  the download has finished, before Sonarr or Radarr is told it is complete, and padding cannot hide a gap
  from that check. If articles are missing, the release is rejected and blocklisted, and the Arr grabs
  another. On by default.
- Optionally, every import is also run through `ffprobe`. A file that will not open, or has no playable video
  or audio, is rejected the same way.
- A file that was put together wrongly (as opposed to a damaged posting) is re-grabbed without blocklisting
  the release, because the release itself is fine.
- An article that turns out to belong to a different upload, served under a reused message ID, is refused
  instead of being stitched into your file.

### Repair sweeps look inside the file

Upstream's repair sweep checks that a file's articles still exist and can optionally check the container signature
at the start of the file. This fork can go further:

- Optional `ffprobe` checks in the repair sweep, which find files whose articles all exist but which do not
  play. There are two, and you can run the first without the second:
  - **The cheap check.** `ffprobe` opens the file and reads its headers. That is enough to catch a file that
    will not open, has no video stream, has no duration, or whose running time is wildly different from what
    Sonarr or Radarr expects for that title. It reads very little and takes a few seconds.
  - **The decode check.** `ffprobe` actually decodes short samples of video at points spread across the file,
    including the end. This finds damage in the middle of a file that the headers say nothing about. It reads
    real data from every file, so it costs time and bandwidth. A file that passes is trusted for 30 days by
    default, then decoded again. The samples are taken at slightly different points each time, so repeated
    checks do not keep reading the same few seconds. A sample that decodes cleanly but returns video from
    another part of the file than was asked for leaves the file unverified.
- Each check has a byte budget, so a repair sweep cannot quietly download a whole remux to test it.
- An **Unverified** list on the Repair page for files that play but could not be fully checked, with a
  **Replace** button for the ones that were assembled wrongly at import.
- Multi-volume RAR releases with scrambled file names are ordered by the volume numbers in their own headers,
  not by the order they were posted in. This works for password-protected releases with encrypted headers
  too.
- Files imported before that fix can have their volumes stored in the wrong order: they play until the first
  misplaced volume, then jump or stop, and every article still exists. A repair sweep reads the volume
  headers of each multi-volume RAR file once (one article per volume), remembers the answer, and lists a
  misordered file as Unverified.
- A misordered file is put back in order where it is, with nothing downloaded again, when the volumes that
  move are the same shape and the file's own video timestamps confirm the order (Matroska files stored
  without compression). With Auto-repair on the repair sweep does this by itself; otherwise **Replace** does.
  A file that cannot be reordered this way stays on the Unverified list until you replace it, which deletes
  it and searches again while keeping the release. A copy of the old record is kept in
  `usenet/volume-order-backup`.
- Optionally, an article that fails for good during playback starts a repair of that file straight away
  instead of waiting for the next repair sweep (built-in DFS mount only).

### Pre-cache: finding broken files before you need them

Pre-cache is not there to make things faster. It reads a file before you get to it, so that damage is found
while there is still time to do something about it. It is off by default.

- **Movies.** Once playback passes a threshold (10% by default), the rest of the movie is read ahead of the
  playhead. A missing article found there goes to PAR2 repair as an urgent job, given as long as the playhead
  will take to reach it. If the repair lands in time you never see the damage. If it does not, padding covers
  the gap. The aim is a movie that plays through without interruption while it is repaired underneath you.
- **TV.** While you watch one episode, the next one, or the rest of the season, is downloaded and checked. A
  damaged episode is repaired from PAR2. One that cannot be repaired is re-grabbed there and then, while there
  is still time for the replacement to arrive, and the replacement is pre-cached in its turn.
- **A warm cache helps twice.** PAR2 needs the intact parts of a file to rebuild the missing ones. When those
  are already on disk, a repair only has to download the small recovery files, so it is quick and cheap. And
  a cached episode starts from disk, not from the news server.

Around that:

- The Repair page lists every pre-cached file with how much of it is cached and whether it was clean,
  repaired or still has damage pending. Pre-caching can be paused, for everything or for one file.
- Pre-caching steps back when someone else's playback is struggling for bandwidth.
- Watched episodes can be dropped from the cache as soon as they are finished.
- With a Plex URL and token set, it only starts for files Plex reports as playing, so a library scan or
  thumbnail job does not set it off.

### Usenet providers

Upstream uses your providers in the priority order you set (its own work in this area is about which kind
of request goes first on a busy server, not which server is quickest). That is fine until the server at
priority 1 is having a slow day, at which point everything queues behind it while faster servers sit idle.
This fork still starts from your priority order, but it measures the servers and acts on what it finds.

**Server priority that picks the fastest.** On by default.

- The download speed of every primary server is measured continuously from real traffic.
- A primary that is both slow and more than four times slower than the fastest one is moved to the back of
  the queue. Playback, imports and pre-cache go to the faster servers first.
- A server that has been moved back is tested again every so often, and gets its place back when it
  recovers. It has to stay fast for a while first, so the order does not flip back and forth.
- While every server is performing well, nothing changes: your priority order is used exactly as set.
- Backup servers are never promoted. They are still only used for articles no primary has, so a block
  account is not spent on things your unlimited account could have served.

**Faster STAT checks.** A repair sweep asks the servers whether each article still exists (a STAT request),
millions of times over a large library. Servers differ a great deal in how quickly they answer.

- The checks are shared out across the servers that answer quickly, instead of all going to priority 1.
- A slow server keeps one connection that re-tests it now and then, backing off the longer it stays slow, so
  it is picked up again when it speeds up or comes back from an outage.
- An article is only counted as missing when every server says so, exactly as before.
- On my setup, with one slow server and two fast ones, this made repair sweeps noticeably shorter.

**Quotas with a reserve.** For providers with a download cap, or a block account you want to last.

- Each provider can be given a cap per day, week or month, with the day and hour it resets.
- A slice of that cap is held back as a reserve (10% unless you set it). Once the rest is used, the provider
  stops taking general traffic and only fills in articles the other servers do not have.
- At the cap it is skipped altogether until the period resets, and the next provider takes over.
- An urgent PAR2 repair, the kind racing a playhead, is allowed to dip into the reserve. Pre-cache is not:
  it waits until a provider has room.
- Usage is counted from the bytes actually received and survives restarts. The Stats page shows each
  provider's usage against its cap.

### Housekeeping

**Tidying up after upgrades.** When Sonarr or Radarr upgrades a file (a WEB-DL replaced by a BluRay, or a
PROPER replacing the original), it switches to the new download and stops using the old one. Nothing removes
the old one, so its NZB, its metadata and whatever it had cached stay behind. Over months that adds up to a
library full of downloads nothing points at. This fork clears them out:

- **Clean up stale NZBs.** A button on the Browse page lists every Usenet download that no Arr uses any more,
  with the disk space each would free. You choose what goes. Removing one deletes its record, its NZB and
  metadata files and its cached data.
- **It also finds orphans:** NZB and metadata files left on disk with no download record at all, after a
  crash or a manual delete.
- **Superseded broken files are not chased.** If a file is broken but the Arr has already replaced it, there
  is nothing to repair. It is dropped from the broken list and skipped by later repair sweeps, instead of
  being checked and re-grabbed again and again. This matters most for season packs where single episodes have
  been upgraded one at a time. Optionally, the old download is deleted as well once none of it is in use.
- **Or do it the moment it happens.** With the Arr webhook set up, Sonarr or Radarr deleting or upgrading a
  file removes the matching old download straight away, without waiting for a cleanup. Opt-in, and the
  endpoint can be protected with a token.

It is careful about what it deletes:

- Everything is checked against what the Arrs are using right now, and checked again at the moment of
  deletion. Anything that has come back into use since the preview is skipped, with the reason shown.
- A download less than a day old is never treated as stale, so a new grab the Arr has not imported yet is
  safe.
- If the Arrs cannot be reached, nothing is deleted. A failed lookup is never read as "nothing is in use".
- If more than half the library comes out as stale, the preview warns you, because that points to a
  misconfiguration, not a real clean-up.

Other clean-up tools:

- **Debrid-gone:** torrent entries that no configured debrid can serve any more are listed and can be
  re-grabbed in bulk.
- **Plex stale versions:** removes the "Unavailable" versions Plex keeps after a file is replaced, one title
  at a time. Off by default, with a dry-run mode.
- **Wanted search:** run each Arr's "search all missing" on a schedule.

### Quality of life

Most of the above comes down to the same idea: find out early, fix it quietly, and say what happened.

- **You are told.** Notifications for a PAR2 repair that finished or failed, a file that now needs a re-grab,
  and a next episode that is cached and ready (clean, or with the number of articles repaired ahead of time).
- **A viewer is not cut off.** Once a file has more damage than the padding limits allow it is marked for
  repair or re-grab, but someone already watching it keeps getting padded playback, up to a higher ceiling,
  instead of the player stopping at the first gap past the limit.
- **You can see the damage.** The Repair page lists each damaged file with its missing articles, whether its
  PAR2 data is enough to repair it, live repair progress, a history of past repairs and what the stored
  patches cost in disk space.
- **Damage found during playback is not forgotten.** A file that playback or PAR2 found to be beyond repair
  is flagged for the next repair sweep, instead of waiting out the full recheck interval.
- **A slow provider does not hold up playback or a repair sweep.** Both are sent to the providers that are
  answering quickly at the time (see Usenet providers above).
- **A restart is safe.** What has been padded and patched, and which files have already been re-grabbed, is
  kept on disk. Imports in progress are stopped cleanly before a restart, so a good release is not judged
  unreadable and blocklisted because the server went away mid-check.
- **A good release is not thrown away.** When the fault was in how a file was assembled, the same release is
  grabbed again without being blocklisted.

### Smaller things

- Console logs are written as readable lines, and the log file can keep a more detailed level than the
  console.
- The Docker image includes `ffmpeg`, so the `ffprobe` checks work without extra setup.
- After an unclean shutdown, the disk cache drops any range it had recorded but never finished writing,
  instead of serving a hole.

## What upstream has that this fork does not

I have not ported everything, and some of what upstream does it does differently and arguably better:

- **Reacquisition.** Upstream tracks which Arr file each managed file belongs to and replaces broken ones
  through durable, restart-safe jobs, with a page to follow them. This fork re-grabs through its own repair
  code and has no such page.
- **Token-only login.**
- **Newer streaming internals.** Upstream rewrote its NZB parser, its NNTP scheduling and its stream reader,
  and added pipelined article downloads, in-memory stream buffering and an option to let playback spill onto
  backup providers. This fork still runs its own versions of those parts, with the changes described above.
- **A hard limit on the disk cache**, enforced as data is written. This fork trims its cache in the
  background instead.
- **The settings that go with them:** `body_pipeline_depth`, `stream_backup_wait`, `conn_idle_timeout`,
  `disk_path`, `fuse_max_background`, `fuse_max_read_ahead`, `token_only` and `verify_content`.
- **Hearsay, properly.** The Hearsay code has been brought across and runs, but it is not fully wired into
  this fork yet. Doing it properly means reworking a good part of my codebase, and I would rather do that
  once. I am waiting for upstream to release a final version before I make that change.
- Whatever upstream has shipped most recently. I pick changes up in batches, so the newest fixes may not be
  here yet.

Shares (NFS and SMB), virtual folders, `.strm` export and the responsive UI have all been brought across
from upstream.

## Quick start

```yaml
services:
  decypharr:
    image: ghcr.io/themightybattlecat/decypharr:latest
    container_name: decypharr
    ports:
      - "8282:8282"
      # Optional: NFSv4 (when NFS is enabled in Settings)
      # - "2049:20490/tcp"
      # Optional: SMB — Windows clients require host port 445 (when SMB is enabled in Settings)
      # - "445:1445/tcp"
    volumes:
      - /mnt/:/mnt:rshared
      - ./configs/:/app # config.json must be in this directory
    restart: unless-stopped
    devices:
      - /dev/fuse:/dev/fuse:rwm
    cap_add:
      - SYS_ADMIN
    security_opt:
      - apparmor:unconfined
```

The image is built from the `usenet-improvements` branch on every push.

### Switching from upstream beta

- Take a copy of your `/app` folder first.
- Change the image (`cy01/blackhole` or `ghcr.io/sirrobot01/decypharr`) to the one above. Your `config.json`
  is read as it is.
- The upstream-only settings listed above are ignored here, and the first save from the Settings page writes
  `config.json` without them. Your copy is how you get them back.
- The database format is the same as upstream beta's. A database from an older build is upgraded on first
  start, with a backup written beside each file; older builds cannot read the upgraded files.
- Padding, PAR2 repair, the import article check and "prefer faster servers" are on as soon as you start.
  The other options described above (the `ffprobe` checks, pre-cache, quotas, the clean-up tools and the
  Plex features) stay off until you turn them on in Settings.

## Supported debrid providers

- [Real Debrid](https://real-debrid.com)
- [Torbox](https://torbox.app)
- [Debrid Link](https://debrid-link.com)
- [All Debrid](https://alldebrid.com)
- [Premiumize](https://www.premiumize.me)

## Documentation

The [official documentation](https://docs.decypharr.com) describes upstream and covers everything the two
have in common. The repair and Usenet guides in this repository's `docs/` folder are updated for this fork.

## Contributing

Anything that is not specific to this fork is better sent to
[upstream](https://github.com/sirrobot01/decypharr), where more people will benefit from it. Issues and pull
requests about the differences above are welcome here.

## License

MIT, the same as upstream. See [LICENSE](LICENSE). Decypharr is the work of
[sirrobot01](https://github.com/sirrobot01) and its contributors; this fork only exists because of it.
