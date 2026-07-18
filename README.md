# Decypharr

![ui](docs/src/assets/images/index.png)

**Decypharr** is a **Media Gateway** for Debrid services and Usenet written in Go.

## What is Decypharr?

Decypharr provides a unified interface for Sonarr, Radarr, and other *Arr applications to access Debrid providers and
Usenet streaming.

## What this branch adds

This branch (`usenet-improvements`) is rebuilt from upstream's `beta` and layers the
following fixes and features on top of it. It gets rebuilt from upstream beta again
whenever upstream makes a major change, so commit hashes on this branch itself are
not stable references - the branch names and commit hashes below are, and this table
is kept up to date as the branch is rebuilt.

| Feature | Source |
|---|---|
| Fix obfuscated multi-volume RAR assembly order | `fix/obfuscated-rar-volume-order` @ `66a770b` |
| Auto-repair on playback failure (NNTP 430, DFS only) | `feature/NNTP-430-auto-repair` @ `61a5dc3` |
| ffprobe validation at sweep and import | `feature/ffprobe-sweep-check` @ `9bc4503` |
| Superseded/broken entry cleanup + stale NZB button (also folds in orphaned-record filtering) | `feature/superseded-broken-cleanup` @ `25bec66` |
| Per-provider bandwidth monitoring and quotas | `feature/usenet-bandwidth-monitor` @ `a665011` |

Optional stop schedule for repair sweeps was merged upstream directly (beta `d9713b8`, #349) and is no longer a fork-only layer.

## Features

- Mock Qbittorent and Sabnzbd API that supports the Arrs (Sonarr, Radarr, Lidarr etc)
- Multiple Debrid and usenet providers support with a single interface
- Direct Usenet streaming via NNTP (no separate download client required)

## Supported Debrid Providers

- [Real Debrid](https://real-debrid.com)
- [Torbox](https://torbox.app)
- [Debrid Link](https://debrid-link.com)
- [All Debrid](https://alldebrid.com)
- [Premiumize](https://www.premiumize.me)

## Quick Start

### Docker (Recommended)

```yaml
services:
  decypharr:
    image: cy01/blackhole:latest
    container_name: decypharr
    ports:
      - "8282:8282"
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

> Prefer not to self-host? A managed Decypharr instance is available
> via [ElfHosted](https://store.elfhosted.com/product/decypharr/?utm_source=github&utm_medium=readme&utm_campaign=decypharr-readme),
> preconfigured alongside Sonarr/Radarr to route requests to your debrid provider (7-day trial).

## Documentation

For complete documentation, please visit our [Documentation](https://docs.decypharr.com).

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
