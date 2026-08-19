# Argus Polymarket DB (APDB)

An external database backing the Argus Polymarket Dispatcher, replacing the large in-memory dictionary the dispatcher previously kept for every tracked market.

> **Status: working service, running in production.** APDB runs as a long-lived background service — a Unix domain socket accepting NDJSON requests, with a periodic crawl-and-refresh loop and a real on-disk index — rather than the stdin prototype it started as. It's been running on Linux for 7+ straight days under a dispatcher churning 8,000+ clients, in addition to macOS (Apple Silicon) development use. Releases ship prebuilt binaries for four targets (see [Platform parity](#platform-parity-with-argus)).

## Why

The Polymarket dispatcher used to be Argus's largest component by memory footprint — 4.5GB+ under load, driven mostly by the sheer number of markets it tracked in-process, compounded by Python's allocation behavior elsewhere in Argus. APDB moves that state out of the dispatcher and into an external store the dispatcher queries instead of holding in memory. In local testing on an Apple Silicon Mac, an APDB instance holding ~16,000 Polymarket events measured at 13.1MB RSS (sleeping, not running) – peak 500MB RSS (active) most importantly, it returns to the original memory footprint when the load is over. On production testing with continious load it averages 100-200MB on prod.

## What it does today

- **Crawls Polymarket's Gamma API** for all open events and writes them to an append-only NDJSON log on disk (`APDB_DB_PATH`).
- **Indexes that log in memory** as a ticker-sorted array of `(ticker, byte offset, length)` — lookups are a binary search plus a positioned `pread`, not a line scan.
- **Serves queries over a Unix domain socket** (`APDB_BIND_ADDRESS`) using a line-delimited JSON request/response protocol: `get_event`, `list_events`, `list_tickers`, `list_asset_ids`, `prefix_search`, and `db_info`. Full wire protocol in [`docs/API.md`](docs/API.md).
- **Refreshes itself in the background** on a timer (`POLYMARKET_FULL_MARKET_CACHE_REFRESH_INTERVAL`), re-crawling Gamma, compacting the log, and atomically swapping in the new snapshot without dropping in-flight readers. Tickers Gamma stops returning are carried forward rather than evicted, until their `endDate` has been past for more than a 4-hour grace period.
- **Syncs from a tailnet peer instead of crawling, when it can.** On boot, if the local database is missing or past its TTL, APDB asks every reachable Tailscale peer how old its own database is (over a dedicated control port) and, if any peer's data is fresh enough, pulls it directly (gzip-compressed, over a second dedicated port) instead of doing a full Gamma crawl. Falls straight through to a normal crawl if no tailnet, no peers, or no fresh-enough peer is found. Full protocol in [`docs/MESH_SYNC.md`](docs/MESH_SYNC.md).
- **Routes outbound Polymarket requests through an optional SOCKS5 proxy pool** (`SOCKS5_ADDRS`), racing candidates and using whichever answers first (or no proxy, if `NULL_DISABLED` isn't set and the direct/"null" path wins the race).

Not yet true: auto-discovered proxies and verified Linux/ARM production use (see [Roadmap](#roadmap)) are still in progress.

## Roadmap

The long-term goal is for APDB to be a fully portable, self-coordinating service that a fleet of Argus instances can share, not just a socket one process happens to be listening on.

### Platform parity with Argus


| Target triplet                   | Generalizes to                  | Status in APDB                                                                 |
| --------------------------------- | -------------------------------- | --------------------------------------------------------------------------------- |
| `arm64-apple-darwin`              | Apple Silicon Macs                | Working — primary development platform                                          |
| `aarch64-unknown-linux-gnu`       | 64-bit ARM Linux                  | Builds and ships in every release; not yet run in production                     |
| `armv7l-unknown-linux-gnueabihf`  | 32-bit ARMv7 Linux (hard-float)   | Builds and ships in every release; not yet run in production                     |
| `x86_64-unknown-linux-gnu`        | 64-bit x86 Linux                  | **Proven in production** — ran 7+ straight days under a dispatcher churning 8,000+ clients |

`build_system/build-everywhere.sh` builds all four targets (via `cargo-zigbuild` for the non-macOS ones) and publishes them to [GitHub releases](https://github.com/The-Sal/argus-polymarket-db/releases) — every release since v1.2.0 ships prebuilt binaries for all four. 
### Everything else planned

- **A real service boundary.** Done — Argus (or anything else) talks to APDB over the Unix socket described in [`docs/API.md`](docs/API.md) instead of running it interactively.
- **A real on-disk index.** Done for lookup — the sorted in-memory index plus positioned reads means a query doesn't get slower as the file grows. Crash recovery is still limited to "drop the torn final line and reload"; there's no write-ahead log or corruption repair.
- **MeshData (tailnet sync).** Done for the boot-time case — a fresh or stale instance queries every reachable Tailscale peer and pulls a fresh-enough database directly instead of re-crawling Gamma; see [`docs/MESH_SYNC.md`](docs/MESH_SYNC.md). Not yet done: routing a *live* query to whichever mesh node can answer it fastest — today mesh sync only runs once, at boot.

## Running it

Ideally, don't. It should be managed by Argus automatically. If you're running it manually:

```
cargo run
```

On first run (no database file present at `APDB_DB_PATH`) it crawls all open Polymarket events and builds the file; on subsequent runs it loads the existing file and reindexes it in memory. It then starts a background refresh loop and binds a Unix domain socket, and runs until killed — it no longer reads from stdin.

```
cargo run -- --version
```

prints the build version and exits without starting anything.

### Configuration

Read from the real environment or a `.env` file in the working directory (via `dotenvy`):

| Variable                                          | Default                          | Meaning                                                                                 |
| -------------------------------------------------- | --------------------------------- | ----------------------------------------------------------------------------------------- |
| `APDB_DB_PATH`                                     | `~/.argus/polymarket_events.db`   | Path to the on-disk NDJSON event log.                                                    |
| `APDB_BIND_ADDRESS`                                | `/tmp/argus_polymarket_db.sock`   | Unix domain socket path the server listens on.                                           |
| `POLYMARKET_FULL_MARKET_CACHE_REFRESH_INTERVAL`    | `300`                             | Seconds between background refresh cycles.                                               |
| `SOCKS5_ADDRS`                                     | *(empty)*                         | Comma-separated `socks5://host:port` proxy pool for outbound Polymarket requests.        |
| `NULL_DISABLED`                                    | `false`                           | When truthy, disables the direct (no-proxy) path from the proxy race.                    |

See [`docs/API.md`](docs/API.md) for the full request/response protocol served over the socket, including an example using `socat`.

If a working `tailscale` CLI is available, APDB also binds two fixed
Tailscale-interface ports — `9563` (control) and `9564` (raw transfer) — for
mesh sync (see above). Neither is configurable via env var, and neither is
required: without a working `tailscale`, mesh sync is skipped and APDB
behaves exactly as it did before this feature existed. Full protocol in
[`docs/MESH_SYNC.md`](docs/MESH_SYNC.md).

## Compatibility
APDB is fully compatible with Argus 1.1.0 and later. Track the [Argus PR](https://github.com/The-Sal/Argus/pull/94) which introduces APDB to the codebase.
