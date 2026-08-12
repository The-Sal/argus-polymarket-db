# Argus Polymarket DB (APDB) — API Spec

APDB exposes a single request/response API over a **Unix domain socket**,
using **newline-delimited JSON (NDJSON)**: one JSON object per request line
in, one JSON object per response line out. There is no HTTP layer.

This document describes the wire protocol implemented in `src/api.rs` and
`src/server.rs`.

## Transport

- **Socket type:** Unix domain stream socket (`std::os::unix::net::UnixListener`).
- **Bind path:** `APDB_BIND_ADDRESS` env var, default `/tmp/argus_polymarket_db.sock`.
- **Framing:** one JSON object per line (`\n`-terminated). Blank lines are
  ignored. A malformed line still gets a response (`ok:false`); it never
  closes the connection.
- **Concurrency:** the server spawns one OS thread per accepted connection.
  A single connection is handled strictly request-then-response, in order —
  there is no request pipelining/multiplexing on one connection.
- **Timeouts:** a connection is closed by the server if the client goes
  60s (`READ_TIMEOUT`) without sending a request line, or if a write to the
  client blocks for more than 10s (`WRITE_TIMEOUT`). An idle timeout is
  logged at `info` level and is not an error condition.
- **Lifetime:** a connection stays open across many requests until the
  client closes it (EOF) or a timeout/IO error occurs.

### Connecting (example)

```bash
# using socat, one request per invocation
echo '{"op":"db_info"}' | socat - UNIX-CONNECT:/tmp/argus_polymarket_db.sock
```

## Configuration (environment variables)

Read once at process startup (via `.env`, loaded with `dotenvy`, or the
real environment):

| Variable                     | Default                          | Meaning                                                   |
| ----------------------------- | --------------------------------- | ---------------------------------------------------------- |
| `APDB_DB_PATH`                | `polymarket_events.db`            | Path to the on-disk NDJSON event log backing the database. |
| `APDB_BIND_ADDRESS`           | `/tmp/argus_polymarket_db.sock`   | Unix domain socket path the server listens on.             |
| `APDB_REFRESH_INTERVAL_SECS`  | `300`                             | How often the background refresh loop re-crawls Polymarket. |
| `SOCKS5_ADDRS`                | *(empty)*                         | Comma-separated `socks5://host:port` proxy pool used for outbound Polymarket requests. |
| `NULL_DISABLED`               | `false`                           | When truthy, disables routing through the null/no-op proxy path. |

`--version` as a CLI flag prints `Argus Polymarket Database v<major.minor.patch>` and exits 0 before anything else runs.

## Request envelope

```json
{ "id": <any JSON value, optional>, "op": "<operation name>", ...op-specific fields }
```

- `op` (string, required) — selects the operation; see [Operations](#operations).
- `id` (any JSON value, optional) — opaque request identifier, echoed back
  verbatim in the response. Useful for matching responses to requests when
  a client has multiple in-flight requests logically (note: physically,
  one connection is still request-then-response — `id` is for the caller's
  own bookkeeping, not for out-of-order delivery). If omitted, `id` is `null`
  in the response.
- All other top-level fields are the operation's own parameters (flattened
  into the same JSON object as `op`/`id`).

## Response envelope

### Success

```json
{
  "id": <echoed id>,
  "op": "<operation name>",
  "ok": true,
  "db_version": "<major.minor.patch>",
  "result": { ...op-specific fields... }
}
```

### Error

```json
{
  "id": <echoed id, or null if the request couldn't be parsed>,
  "op": "<operation name, or null if unknown/unparseable>",
  "ok": false,
  "db_version": "<major.minor.patch>",
  "error": { "code": "<error code>", "message": "<human-readable detail>" }
}
```

`db_version` is the APDB build's own version (from `versionx.json`), included
on every response — success or error — so a client can detect a version
mismatch even on a failed call.

### Error codes

| Code          | Meaning                                                                 |
| ------------- | ------------------------------------------------------------------------ |
| `bad_request` | Malformed JSON, unknown `op`, missing/invalid params, or an explicitly invalid value (e.g. `limit: 0`, empty `ticker`/`prefix`). |
| `internal`    | The server failed to read/parse a record it already located on disk (data corruption or an I/O error). |

## Operations

### `get_event`

Look up a single event by exact ticker.

**Params**

| Field    | Type   | Required | Notes                    |
| -------- | ------ | -------- | ------------------------- |
| `ticker` | string | yes      | Trimmed before lookup; empty (after trim) is `bad_request`. |

**Result**

| Field   | Type          | Notes                                           |
| ------- | ------------- | ------------------------------------------------ |
| `event` | object \| null | The full event JSON record, or `null` if the ticker doesn't exist (this is `ok:true`, not an error). |

**Example**

```json
→ {"op":"get_event","ticker":"btc-updown-1"}
← {"id":null,"op":"get_event","ok":true,"db_version":"1.3.0","result":{"event":{"ticker":"btc-updown-1","markets":[...]}}}
```

### `list_events`

Cursor-paginated listing of full event records, in ticker-sorted order.

**Params**

| Field   | Type    | Required | Default | Max | Notes |
| ------- | ------- | -------- | ------- | --- | ----- |
| `after` | string  | no       | *(start)* | —   | Keyset cursor: returns tickers strictly greater than this value. |
| `limit` | integer | no       | 100     | 500 | `0` is `bad_request`; values above the max are clamped, not rejected. |

**Result**

| Field        | Type           | Notes |
| ------------ | -------------- | ----- |
| `events`     | array\<object\> | Full event records for this page. |
| `next_after` | string \| null  | Pass as `after` to fetch the next page; `null` means this was the last page. |

### `list_tickers`

Cursor-paginated listing of ticker strings only (cheaper than `list_events`
when the caller doesn't need full records).

**Params** — same as `list_events`, except default limit is 5000 and max is 20,000.

**Result**

| Field        | Type            | Notes |
| ------------ | --------------- | ----- |
| `tickers`    | array\<string\>  | Tickers for this page. |
| `next_after` | string \| null   | Same semantics as `list_events`. |

### `list_asset_ids`

Cursor-paginated listing of `(asset_id, ticker, market_index)` triples,
derived from each event's `markets[].clobTokenIds`. One ticker can expand to
zero or many entries (each market can have multiple asset IDs, and events
can have multiple markets), so the pagination cursor bounds **tickers
scanned per page**, not entries returned — a page can be larger or smaller
than `ticker_limit` entries.

**Params**

| Field          | Type    | Required | Default | Max   | Notes |
| -------------- | ------- | -------- | ------- | ----- | ----- |
| `after`        | string  | no       | *(start)* | —   | Keyset cursor over **tickers**, same as other ops. |
| `ticker_limit` | integer | no       | 500     | 2000  | Number of tickers to scan for this page. `0` is `bad_request`. |

**Result**

| Field        | Type            | Notes |
| ------------ | --------------- | ----- |
| `entries`    | array\<object\>  | Each: `{"asset_id": string, "ticker": string, "market_index": integer}`. |
| `next_after` | string \| null   | Cursor for the next page of tickers. |

`clobTokenIds` is normally a JSON-encoded string containing an array (as
Gamma emits it and Argus's own parser expects); a native JSON array is also
accepted defensively. Markets with no parseable `clobTokenIds` contribute no
entries.

### `prefix_search`

Ticker autocomplete: all tickers starting with a given prefix, sorted.

**Params**

| Field    | Type    | Required | Default | Max | Notes |
| -------- | ------- | -------- | ------- | --- | ----- |
| `prefix` | string  | yes      | —       | —   | Empty string is `bad_request`. |
| `limit`  | integer | no       | 50      | 500 | `0` is `bad_request`. |

**Result**

| Field        | Type            | Notes |
| ------------ | --------------- | ----- |
| `tickers`    | array\<string\>  | Up to `limit` matching tickers. |
| `truncated`  | boolean         | `true` if more matches existed than `limit` allowed (no cursor for this op — re-issue with a higher `limit` or narrow the prefix). |

### `db_info`

Metadata about the currently loaded database snapshot. No params.

**Result**

| Field           | Type    | Notes |
| --------------- | ------- | ----- |
| `major`         | integer | APDB major version. |
| `minor`         | integer | APDB minor version. |
| `patch`         | integer | APDB patch version. |
| `version`       | string  | `"major.minor.patch"`, same as top-level `db_version`. |
| `lines`         | integer | Number of events currently indexed. |
| `built_at_unix` | integer | Unix timestamp the loaded snapshot was built (crawl completion time, or the on-disk file's mtime if loaded from an existing file at startup). |

## Data model notes

- Events are read from an append-only NDJSON log (`APDB_DB_PATH`). Each line
  is a JSON object that must contain a non-empty `ticker` field; malformed
  or ticker-less lines are skipped at load time and logged, not fatal.
  Records are also periodically compacted by the background refresh loop.
- When a ticker appears more than once in the log, the **last** occurrence
  wins (upsert semantics) — earlier occurrences are dropped from the index.
- All listing/pagination operations return results in **ticker-sorted
  (lexicographic) order**. Cursors (`after`) are the last-seen ticker string,
  not an offset — safe to keep paging across a concurrent background
  refresh without skipping or repeating entries, since the index only grows
  or updates entries in place and never removes a ticker out from under an
  in-progress pagination.

## Versioning

APDB's own version is `major.minor.patch`, sourced from `versionx.json` at
compile time and surfaced two ways:

- Every API response includes it as top-level `db_version` (and `db_info`
  also returns it broken out as `major`/`minor`/`patch`/`version`).
- Running the binary with `--version` prints it and exits without starting
  the server.

There is currently no protocol-level version negotiation between client and
server — a client that needs to enforce compatibility should check
`db_version` (or call `db_info`) itself.
