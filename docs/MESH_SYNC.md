# APDB Mesh Sync — Tailnet Peer Discovery & Database Transfer Protocol

Mesh sync lets a booting APDB instance skip a full Gamma crawl by copying a
database wholesale from a peer on the same Tailscale tailnet, if one exists
with data fresh enough to use as-is. This document describes the wire
protocol implemented in `src/p2p_db_server.rs` (server/sender role),
`src/mesh_sync.rs` (client/requester role, run at boot), and
`src/tailnet_fns.rs` (peer discovery).

Unlike the request/response API in [`docs/API.md`](API.md), this protocol is
**not** exposed over the Unix domain socket and does not share its op set —
it runs over two dedicated TCP ports, bound to the host's Tailscale
interface, with an intentionally minimal surface (two operations) and no
authentication (see [Trust model](#trust-model)).

## Why two ports

| Port                 | Carries                                   | Framing |
| --------------------- | ------------------------------------------ | ------- |
| `9563` (control plane) | Tiny JSON handshake messages: `db_info`, `request_pull` | Newline-delimited JSON (NDJSON), like the Unix-socket API |
| `9564` (raw channel)   | The database file itself, gzip-compressed | **None** — a single continuous byte stream, EOF-terminated |

The database can run 1-2GB on disk. Framing that as NDJSON would mean
base64-encoding every chunk (≈33% size overhead) purely to dodge a stray
`\n` byte colliding with a line delimiter — and there's no delimiter to
collide with on a channel that carries exactly one message per connection.
9564 exists so the bulk transfer never has to pretend to be text: it's
opened, filled with gzip bytes, and closed, nothing else ever happens on it.

Both ports bind to `TailnetFns::get_my_address()` (the host's own Tailscale
IP, from `tailscale ip -4`), not `0.0.0.0` — mesh sync is tailnet-only by
construction, not merely by convention.

## When mesh sync runs

Mesh sync is consulted from `main.rs` **only when the local on-disk
database is already known to be absent or past its own TTL** — never when
the local snapshot is already fresh. A local snapshot within
`POLYMARKET_FULL_MARKET_CACHE_REFRESH_INTERVAL` of its `built_at_unix` is
always served as-is; pulling 1-2GB over the network to replace something
already fresh would be pure waste. See
[`docs/db_specs/v1.md`](db_specs/v1.md) for how that TTL is computed.

#### Env loading is cwd-relative

`refresh_interval_secs` — the same value used both as the local snapshot's
staleness check and as the mesh sync TTL above — comes from
`POLYMARKET_FULL_MARKET_CACHE_REFRESH_INTERVAL`, which `main.rs` reads after
`dotenvy::from_filename(".env")`. That call resolves `.env` relative to the
**current working directory the process was launched from**, not to the
binary's location. For a sidecar binary meant to be launched from anywhere
(e.g. `~/.argus/sidecars/APDB`), this means the exact same boot can go two
different ways depending purely on which directory you happened to be `cd`'d
into:

- Launched from a directory whose `.env` sets
  `POLYMARKET_FULL_MARKET_CACHE_REFRESH_INTERVAL=6000` → a peer whose
  database was built 5600s ago is within TTL → mesh sync pulls it.
- Launched from a directory whose `.env` exists but doesn't set that
  variable at all (e.g. it only sets `SOCKS5_ADDRS`) → the value silently
  falls back to the hardcoded default (300s) → that same peer is now
  considered stale → `try_bootstrap_from_peers` returns `None` → a full
  local crawl runs instead, even though a perfectly good peer database was
  sitting one tailnet hop away.

Nothing here is a bug in the discovery/TTL logic itself — it's operating
correctly on whatever `refresh_interval_secs` it was handed. The footgun is
purely that two `.env` files with different variables present produce two
different effective configs for what looks like "the same command." See the
Configuration section of the top-level [`README.md`](../README.md) for the
general version of this caveat.

```
local db missing or stale
        │
        ▼
tailscale_available()? ──no──► fall back to full local crawl
        │ yes
        ▼
get_peers() empty? ──yes──► fall back to full local crawl
        │ no
        ▼
query db_info from every peer concurrently (§ Control plane)
        │
        ▼
any peer in-TTL and format_version-compatible? ──no──► fall back to full local crawl
        │ yes
        ▼
try each candidate, freshest first, until one succeeds (§ Pull sequence)
        │
        ▼
all candidates failed? ──yes──► fall back to full local crawl
        │ no
        ▼
validated snapshot installed — boot continues with it, no crawl
```

Every failure mode — no tailscale, no peers, no in-TTL candidate, a
declined/failed/timed-out pull — falls through to exactly the same local
crawl (`refresh::full_crawl_and_compact`) that runs today when mesh sync
doesn't find anything. Mesh sync is a pure optimization; it is never a hard
dependency for the process to start serving traffic.

## Peer discovery (`tailnet_fns.rs`)

`TailnetFns::get_peers()` shells out to `tailscale status --json` and reads
the `Peer` map. For each entry:

- A fixed hostname skiplist (`funnel-ingress-node`, `ip-172-31-20-232`,
  `localhost`) filters out non-peer entries.
- The single `AllowedIPs` entry containing `/32` is taken as the peer's
  address; entries with zero or multiple `/32` addresses are skipped.
- The `/32` CIDR suffix is stripped before the address is returned —
  `get_my_address()` and every consumer here expect a bare IP
  (`"100.64.1.2"`, not `"100.64.1.2/32"`).
- A peer whose resolved bare IP matches this node's own
  `get_my_address()` is excluded — a second, address-based guard on top of
  the hostname skiplist, so the local node is never dialed as if it were a
  remote peer.

Any malformed or unexpected shape in the CLI's JSON output (missing `Peer`
key, a peer entry missing `HostName`/`AllowedIPs`, unparseable JSON) is
treated as "no usable peers," not a fatal error — this function is on the
boot path, and a crash here would take the whole daemon down before it ever
reaches the local-crawl fallback.

## Control plane (`9563`)

Same wire shape as the Unix-socket API: one JSON object per line in, one out.
`P2pDbServer::run_server` accepts connections forever, one OS thread per
connection (mirrors `server::run`). Only two ops are recognized; anything
else gets `{"ok": false, "error": "unknown_op: <name>"}`.

**Framing guard:** every `read_line` on this channel is capped at
`MAX_CONTROL_LINE_BYTES` (64KB) via `Read::take`, generous for any real
handshake message but enough to stop a peer that never sends a `\n` from
growing the line buffer without bound — the idle read timeout below only
bounds *gaps between bytes*, not total line size, so this cap is the actual
memory ceiling on this channel.

**Timeouts:** `CONTROL_READ_TIMEOUT_SECS` (30s) / `CONTROL_WRITE_TIMEOUT_SECS`
(10s) idle timeouts on every control-plane connection, matching the pattern
`server.rs` already uses for the Unix-socket API.

### `db_info`

Request:
```json
{"op":"db_info"}
```

Response:
```json
{"ok":true,"result":{"major":1,"minor":5,"patch":0,"version":"1.5.0","lines":16234,"built_at_unix":1755000000,"db_format_version":1}}
```

Identical shape to the Unix-socket API's `db_info` result — both are built
by the same shared `api::db_info_json` helper, so the two can never drift
apart. See [`docs/API.md#db_info`](API.md#db_info) for field meanings.

### `request_pull`

Request:
```json
{"op":"request_pull"}
```

Response — either:
```json
{"ok":true}
```
meaning: the raw transfer port is now bound and the sender is waiting for
this requester to connect on `9564`; or:
```json
{"ok":false,"error":"busy"}
{"ok":false,"error":"bind_failed: <os error>"}
```
meaning the sender is declining — see [Failure modes](#failure-modes). No
further messages are ever sent on this connection after a `request_pull`
response; the connection is dropped by the server immediately after (the
handler `return`s regardless of outcome).

## The atomic send operation

This is the "from the moment a peer asks, we're already committed"
operation the sender runs per `request_pull`, entirely inside
`P2pDbServer::handle_request_pull`:

```
1. try_lock the `sending` mutex
     already held?  → respond {"ok":false,"error":"busy"}, stop. Port never touched.
2. pin the current Arc<Snapshot> (`self.database.snapshot()`)
     — the same snapshot backs both the db_info numbers already sent
       and the bytes about to be sent; a background refresh swapping
       snapshots mid-handshake cannot split those two views
3. bind 9564
     fails? → respond {"ok":false,"error":"bind_failed: ..."}, stop.
4. respond {"ok":true}
     — only now, because steps 1-3 already committed to sending
5. accept on 9564, polling non-blocking with a 120s deadline
     - a connection from an IP other than the requester's is refused
       and polling continues (another peer racing onto the port)
     - deadline reached with no matching connection → log & stop
5.5. force the accepted stream back to blocking mode
     — on macOS/BSD, `accept()` on a non-blocking listener hands back
       an already non-blocking stream (inherited, unlike on Linux),
       which would silently defeat the read/write timeouts set below;
       see docs/PLATFORM_NOTES.md for the full story
6. stream_snapshot(): gzip(level 9)-compress the pinned snapshot's
   file, 256KB at a time, straight onto the accepted connection
7. drop the 9564 listener (closes the port)
   the `sending` guard drops here too (every path above), releasing
   the lock for the next request
```

Every exit path — busy, bind failure, accept timeout, a mid-stream I/O
error — releases the lock via Rust's ordinary scope-exit `Drop`, not by
reaching an explicit "done" branch a bug could skip. A panic anywhere inside
this section poisons the `Mutex`, but the poisoned state is recovered
(`poisoned.into_inner()`) rather than left to reject every future request as
"busy" forever — the lock only ever guards a `()`, so there's no invariant a
poison could have corrupted.

### Two failure-reporting surfaces, not one

A failure **before** the raw connection is accepted (busy, bind failure,
accept timeout) has a live control-plane connection to report on, and does:
the requester gets an explicit `{"ok":false,"error":"..."}`. A failure
**after** bytes start flowing (a read error on the local file, a write error
mid-stream) has nowhere to put a JSON error message — the requester is by
then reading raw gzip bytes from `9564`, not JSON from `9563`. That failure
surfaces implicitly: the sender simply stops writing and closes the
connection, which the receiver observes as a truncated gzip stream (a failed
CRC/trailer check inside `GzDecoder`, see [Validation](#validation--atomicity-order)) —
never a hang, never data silently accepted as complete.

## Raw transfer channel (`9564`)

No JSON, no chunk headers, no length prefix. The entire message is:

```
gzip(level 9) of the sender's database file, offset 0 through EOF
```

That includes the file's line-0 `__apdb_meta__` record (see
[`docs/db_specs/v1.md`](db_specs/v1.md)) — it's not a protocol field here,
it's simply part of the file being sent, and it's how the receiver's
`Snapshot::from_file` recovers `built_at_unix`/`format_version` on the other
end without this protocol needing to carry them separately. The stream ends
when the sender closes the connection; there is no explicit end-of-data
marker beyond gzip's own trailer (CRC32 + uncompressed size, checked by the
decoder).

**Sender** (`stream_snapshot` in `p2p_db_server.rs`): reads the pinned
snapshot's backing file via `FileExt::read_at` (positioned reads/`pread`) in
fixed `TRANSFER_CHUNK_BYTES` (256KB) buffers, feeding each one into a
`flate2::write::GzEncoder` wrapping the socket, then `.finish()`s the
encoder explicitly (not left to `Drop`, so a failed trailer flush is
actually observed as an error rather than silently swallowed). `read_at` is
deliberate, not just idiomatic — `snap.file.try_clone()` would share the
*original* fd's cursor position on POSIX, which `Snapshot::from_file`'s
one-time indexing scan already left at EOF, so a naive clone-and-sequential-
read would read zero bytes immediately. `read_at` never touches any shared
cursor.

**Receiver** (`pull_from_peer` in `mesh_sync.rs`): wraps the socket in
`flate2::read::GzDecoder` and copies straight into a `BufWriter` over the
on-disk `.tmp` file in fixed `PULL_PROGRESS_CHUNK_BYTES` (256KB) reads —
a hand-rolled loop rather than `io::copy`, purely so each chunk's size is
visible to print a `\r`-animated running total (`Pulling from <ip>: N.N MB`)
to stdout as the transfer progresses. Behaves identically to `io::copy` for
error purposes: a truncated/corrupted stream still fails via `GzDecoder`'s
gzip-trailer check on `read()`, before any ticker JSON is parsed. The
progress line goes to stdout, never stderr, so it never interleaves with
this daemon's `log`-facade output (see `main.rs`'s `StderrLogger`); a final
`println!()` once the loop ends moves the cursor off that line.

**Memory ceiling on both ends:** a fixed constant (256KB on both the sender's
chunk buffer and the receiver's), independent of whether the database is
10MB or 2GB — this is the same design premise the rest of APDB is built on
(see the top-level README's "Why").

## Validation & atomicity order

The receiver deliberately does **not** follow `full_crawl_and_compact`'s
rename-then-reopen order (write `.tmp` → sync → rename → reopen `path`
fresh). A network pull is untrusted input in a way a local crawl isn't, so
`pull_from_peer` validates before committing:

```
1. decompress the incoming stream straight into <db_path>.tmp
     (a truncated/corrupt stream fails right here — GzDecoder checks
      the gzip trailer and errors on a short/garbled stream, before
      any ticker JSON is even parsed)
2. flush + fsync the .tmp file
3. Snapshot::from_file(<db_path>.tmp)     ← still the .tmp path
     fails? → delete .tmp, propagate the error (fall back to local crawl)
4. only now: rename <db_path>.tmp → <db_path>
     the already-open Snapshot's fd stays valid and correct across the
     rename (same guarantee documented at database.rs:25-32 for the
     ordinary refresh path) — no need to reopen `db_path` afterward
```

A corrupt or malicious pull can therefore never clobber a working local
`db_path`: the rename only happens after the file has already been proven
loadable. Any failure at any step deletes the partial `.tmp` file rather
than leaving a multi-hundred-MB partial download on disk.

## Failure modes

| Failure                                              | Surfaces as                                      | Effect |
| ------------------------------------------------------ | --------------------------------------------------- | -------- |
| No `tailscale` / not logged in                        | `tailscale_available()` false                       | Mesh sync skipped entirely; local crawl runs |
| Zero peers, or all peers unreachable/timed out          | Empty candidate list                                 | Local crawl runs |
| Peer's `db_format_version` ≠ this build's `DB_FORMAT_VERSION` | Peer excluded from candidates                  | Tried peers only if format-compatible; local crawl if none are |
| Peer's data outside the requester's TTL                 | Peer excluded from candidates                        | Same |
| Sender already mid-transfer to someone else             | `{"ok":false,"error":"busy"}`                        | Requester tries the next-freshest candidate, if any |
| Sender can't bind `9564` (e.g. unexpected port conflict) | `{"ok":false,"error":"bind_failed: ..."}`            | Same |
| Requester never connects to `9564` within 120s           | Sender logs and gives up; requester's own connect/read eventually times out | Requester tries next candidate |
| Truncated/corrupted stream on `9564`                     | `GzDecoder` error inside `io::copy`                  | Partial `.tmp` deleted; requester tries next candidate |
| Decompressed `.tmp` fails `Snapshot::from_file`          | Validation error before rename                       | Partial `.tmp` deleted; `db_path` untouched; requester tries next candidate |
| All candidates exhausted                                 | `try_bootstrap_from_peers` returns `None`            | Local crawl runs, exactly as if mesh sync didn't exist |

## Concurrency & timeouts

| Constant                    | Value  | Where               | Meaning |
| ----------------------------- | ------ | -------------------- | --------- |
| `CONTROL_PORT`                | 9563   | `p2p_db_server.rs`   | Control-plane bind port |
| `RAW_TRANSFER_PORT`           | 9564   | `p2p_db_server.rs`   | Raw transfer bind port |
| `TRANSFER_CHUNK_BYTES`        | 256KB  | `p2p_db_server.rs`   | Sender read/compress buffer size |
| `PULL_TIMEOUT_SECS`           | 120s   | both files           | Accept-wait deadline, and idle read/write timeout during transfer (a stall cap, not a hard cap on total transfer duration — a large database over a slow link still finishes as long as it keeps progressing) |
| `ACCEPT_POLL_INTERVAL`        | 100ms  | `p2p_db_server.rs`   | Poll interval while waiting for the requester to connect on `9564` |
| `CONTROL_READ_TIMEOUT_SECS`   | 30s    | `p2p_db_server.rs`   | Idle timeout on `9563` connections (server side) |
| `CONTROL_WRITE_TIMEOUT_SECS`  | 10s    | `p2p_db_server.rs`   | Write timeout on `9563` connections (server side) |
| `MAX_CONTROL_LINE_BYTES`      | 64KB   | `p2p_db_server.rs`   | Hard cap on a single control-plane JSON line |
| `PEER_QUERY_TIMEOUT_SECS`     | 5s     | `mesh_sync.rs`       | Connect+read timeout for the `db_info` fan-out at boot |
| `PULL_PROGRESS_CHUNK_BYTES`   | 256KB  | `mesh_sync.rs`       | Receiver decompress/write chunk size; also the granularity of the `\r`-animated MB-pulled progress line |

Only one raw transfer can be in flight **per sender** at a time (the
`sending` mutex in `P2pDbServer`) — this is a deliberate simplicity choice,
not a fundamental limit: a node with many fresh-enough tailnet neighbors all
booting at once will simply serve them one at a time, and every `db_info`
query is issued in parallel (bounded by peer count, one thread each) so
discovery latency doesn't scale with tailnet size even though transfers do.

## Trust model

Per the original design note in `tailnet_fns.rs`: there is no authentication
anywhere in this protocol, on the premise that the tailnet itself is the
security boundary (only devices already authorized on the Tailscale network
can reach these ports at all). There is also no remote-code-execution
surface — a received database is JSON-derivative data, never executed. The
practical implication for anyone extending this protocol: don't add a new op
that trusts unvalidated peer input beyond what's described in
[Validation & atomicity order](#validation--atomicity-order) without
reconsidering this assumption.

## Related docs

- [`docs/API.md`](API.md) — the Unix-socket request/response protocol this
  system is deliberately separate from.
- [`docs/db_specs/v1.md`](db_specs/v1.md) — the on-disk format
  (`built_at_unix`, `format_version`) that both the TTL check and the
  `db_info` op depend on.
- [`docs/PLATFORM_NOTES.md`](PLATFORM_NOTES.md) — the macOS/BSD
  `accept()`-inherits-non-blocking quirk that step 5.5 above works around.
