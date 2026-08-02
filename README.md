# Argus Polymarket DB (APDB)

An external database backing the Argus Polymarket Dispatcher, replacing the large in-memory dictionary the dispatcher previously kept for every tracked market.

> **Status: early prototype.** This is a proof of concept, not a released component. It has only ever been run on macOS (Apple Silicon) — it has not been built, run, or tested on Linux, and near-certainly needs portability work before it will. Expect rough edges: liberal `unwrap()`s, no crash recovery, no service/API surface (currently just a `main()` you run and talk to over stdin), and a storage format that will change. Treat everything below the fold as direction of travel, not a shipped feature list.

## Why

The Polymarket dispatcher used to be Argus's largest component by memory footprint — 4.5GB+ under load, driven mostly by the sheer number of markets it tracked in-process, compounded by Python's allocation behavior elsewhere in Argus. APDB moves that state out of the dispatcher and into an external store the dispatcher queries instead of holding in memory. In local testing on an Apple Silicon Mac, an APDB instance holding ~16,000 Polymarket events measured at 13.1MB RSS — that number is from a single machine and hasn't been reproduced elsewhere yet, but it's the kind of reduction the rewrite is chasing.

## Roadmap

The long-term goal is for APDB to be a real service Argus talks to over the network, not a binary you pipe tickers into over stdin — with the reliability and portability guarantees that implies.

### Platform parity with Argus

Argus itself targets four platform tiers (its own internal tier list runs to eight, but tiers 5–8 are reserved for embedded/mobile targets Argus doesn't run on today):

| Tier | Target triplet                   | Generalizes to                  | Status in APDB                              |
| ---- | -------------------------------- | ------------------------------- | ------------------------------------------- |
| 1    | `arm64-apple-darwin`             | Apple Silicon Macs              | Working — the only platform this has run on |
| 2    | `aarch64-unknown-linux-gnu`      | 64-bit ARM Linux                | Untested                                    |
| 3    | `armv7l-unknown-linux-gnueabihf` | 32-bit ARMv7 Linux (hard-float) | Untested                                    |
| 4    | `x86_64-unknown-linux-gnu`       | 64-bit x86 Linux                | Untested                                    |

Nothing in the current source is macOS-specific, so tiers 2–4 may already build, but "may build" and "tested" are different claims — none of these have been exercised, and CI to actually verify them doesn't exist yet.

### Everything else planned

- **A real on-disk index.** Replace the line-scan lookup in `Database` with something that doesn't get slower as the file grows, and that survives a crash mid-write.
- **A service boundary.** Give Argus something to query over the network (or a local socket) instead of running this interactively — the exact request/response mechanism between Argus and APDB hasn't been finalized.
- **Proxy auto-discovery.** Today `SOCKS5_ADDRS` has to be fully populated by hand. The plan is for APDB to derive candidate proxy addresses itself from the default ports of Argus-managed WireGuard instances, so Argus only has to point it at a `WIREPROXY_BIND_ADDRESS` rather than enumerate every proxy.
- **MeshData.** Multiple APDB-backed Argus instances typically run at once — prod, dev boxes, etc. — some reachable directly, some only through a proxy. MeshData would let those instances share cached data over a Tailscale mesh instead of each one hitting Gamma independently, and, further out, route a request to whichever mesh node can answer it fastest when several cache entries expire at once. None of this exists yet; it's a shape, not a design.

## Running it

```
cargo run
```

On first run (no `polymarket_events.db` present) it crawls all open Polymarket events and builds the file; on subsequent runs it loads the existing file. It then drops into a loop that reads a ticker from stdin and prints the matching event, if any. `SOCKS5_ADDRS` (comma-separated `socks5://host:port` entries) and `NULL_DISABLED` are read from `.env`.
