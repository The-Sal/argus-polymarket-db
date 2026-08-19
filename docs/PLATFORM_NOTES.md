# Platform Notes

Host-OS quirks that bit this codebase in a way not obvious from the Rust
source alone — each entry explains the underlying kernel behavior, where it
showed up, and how it was worked around. Consult this doc before assuming
`std::net` behaves identically across platforms; several of its guarantees
are POSIX-shaped, not OS-shaped.

## macOS/BSD: `accept()` inherits the listening socket's non-blocking flag

### Symptom

```
[WARN] [p2p_db_server] Raw transfer to 100.90.130.19 failed: Resource temporarily unavailable (os error 35)
```

surfacing seconds after a peer connected to `RAW_TRANSFER_PORT` (9564) —
not a timeout, not a real network failure, and only ever on macOS. `os error
35` is `EWOULDBLOCK`/`EAGAIN`.

### Root cause

`P2pDbServer::handle_request_pull` (`src/p2p_db_server.rs`) binds the raw
transfer listener and puts it in non-blocking mode on purpose:

```rust
listener.set_nonblocking(true)?;
```

so its accept loop can poll with a deadline (`PULL_TIMEOUT_SECS`) instead of
blocking forever on a requester that never connects — see [`docs/MESH_SYNC.md`
§ The atomic send operation](MESH_SYNC.md#the-atomic-send-operation), step 5.

Once a connection matching the requester's IP arrives, the code sets
timeouts on the **accepted** stream and starts writing to it:

```rust
let _ = stream.set_read_timeout(Some(Duration::from_secs(PULL_TIMEOUT_SECS)));
let _ = stream.set_write_timeout(Some(Duration::from_secs(PULL_TIMEOUT_SECS)));
```

The assumption baked into that code is that a freshly-accepted `TcpStream`
starts life in ordinary blocking mode regardless of what the *listening*
socket's mode was — `set_write_timeout` then governs how long a blocking
write is allowed to wait for kernel send-buffer space before giving up.

That assumption holds on Linux, but not on macOS/BSD:

| Kernel family | Does the socket returned by `accept()` inherit `O_NONBLOCK` from the listening socket? |
| --- | --- |
| Linux | **No.** [`accept(2)`](https://man7.org/linux/man-pages/man2/accept.2.html): "file status flags... are not inherited across an accept()". A fresh fd is created from scratch. |
| macOS / BSD / Solaris | **Yes.** Documented independently in [Python bpo-7995](https://bugs.python.org/issue7995) and observed again in [.NET runtime #25069](https://github.com/dotnet/runtime/issues/25069) — the accepted fd is a clone of the listening fd's descriptor state, `O_NONBLOCK` included. |

So on this host, `stream` comes out of `listener.accept()` already
non-blocking, inherited from the 9564 listener a few lines above. Setting
`SO_SNDTIMEO` via `set_write_timeout` doesn't change that: `O_NONBLOCK` and
`SO_SNDTIMEO` are two independent kernel mechanisms, and a non-blocking
socket's `write(2)` never consults `SO_SNDTIMEO` at all — it just returns
`EWOULDBLOCK` the instant the send buffer can't take the write, which is
exactly what a slow tailnet link to `100.90.130.19` triggers as soon as
`stream_snapshot`'s gzip writer fills the socket buffer faster than the peer
drains it. The 120s timeout was never actually in effect.

### Fix

`handle_request_pull` now explicitly forces the accepted stream back to
blocking mode before using it, right after the accept loop exits:

```rust
if let Err(e) = stream.set_nonblocking(false) {
    log::warn!("[p2p_db_server] Failed to clear non-blocking mode on raw transfer stream to {requester_ip}: {e}");
    return;
}
```

This makes the socket's blocking mode explicit rather than inherited, so
`set_read_timeout`/`set_write_timeout` behave the same way on every
platform: a stalled peer now genuinely gets up to `PULL_TIMEOUT_SECS` before
the transfer is abandoned, instead of failing immediately the first time the
send buffer fills.

### Why this didn't need to touch the listener itself

The listener has to stay non-blocking — that's what lets the accept loop
poll (`ACCEPT_POLL_INTERVAL`) against a deadline instead of blocking forever
on `accept()`. Only the *accepted connection* needed correcting; nothing
about the polling accept loop changes.

### Where else to watch for this

Any code in this repo that calls `set_nonblocking(true)` on a `TcpListener`
and later hands the accepted `TcpStream` to blocking-style I/O (rather than
polling it too) is exposed to the same issue. As of this writing that's only
`p2p_db_server.rs`'s raw transfer listener — `server.rs`'s Unix-socket
listener and `mesh_sync.rs`'s outbound `TcpStream::connect_timeout` calls
are both already blocking by construction and unaffected.
