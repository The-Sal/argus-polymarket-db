use crate::api;
use crate::database::{Database, Snapshot};
use crate::tailnet_fns::TailnetFns;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::{IpAddr, TcpListener, TcpStream};
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub(crate) const CONTROL_PORT: u16 = 9563;
/// Carries raw compressed database bytes only — never JSON, never NDJSON.
/// Kept separate from `CONTROL_PORT` so the bulk transfer never has to be
/// framed as text: it's one continuous gzip stream, EOF-terminated, and
/// nothing about it needs to dodge a `\n` delimiter the way control-plane
/// messages do.
pub(crate) const RAW_TRANSFER_PORT: u16 = 9564;
/// Read/compress buffer size for a raw transfer — bounds sender-side memory
/// use regardless of how large the on-disk database gets (it can run
/// 1-2GB), matching this daemon's whole reason for existing: never hold the
/// full database in RAM.
const TRANSFER_CHUNK_BYTES: usize = 256 * 1024;
/// Deadline for a requester to connect to `RAW_TRANSFER_PORT` after being
/// told "ok", and also the idle read/write timeout once the transfer is
/// underway (no progress for this long, not a hard cap on total transfer
/// time — a large database over a slow tailnet link should still finish as
/// long as it keeps making progress).
const PULL_TIMEOUT_SECS: u64 = 120;
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CONTROL_READ_TIMEOUT_SECS: u64 = 30;
const CONTROL_WRITE_TIMEOUT_SECS: u64 = 10;
/// Control-plane messages are tiny handshake JSON (`db_info`/`request_pull`
/// requests and responses) — generously larger than any of them, but caps
/// `read_line`'s buffer growth so a peer trickling bytes with no `\n` can't
/// grow it unboundedly (the idle read timeout above only bounds *gaps
/// between bytes*, not total line size).
pub(crate) const MAX_CONTROL_LINE_BYTES: u64 = 64 * 1024;

/// The tailnet-facing side of mesh sync: answers other instances' `db_info`
/// and `request_pull` queries. The client/requester role (querying peers and
/// pulling from one) lives in `mesh_sync.rs`.
pub(crate) struct P2pDbServer {
    pub(crate) port: u16,
    listener: TcpListener,
    database: Arc<Database>,
    my_ip: String,
    /// Gates `request_pull` so only one raw transfer is ever in flight at a
    /// time. A `Mutex` rather than an `AtomicBool` deliberately — the guard
    /// must release on every exit path of `handle_request_pull` (early
    /// return, error, or normal completion) via RAII, not by reaching an
    /// explicit "done" branch that a bug could skip.
    sending: Mutex<()>,
}

impl P2pDbServer {
    /// Returns `None` (logging why) rather than panicking on either failure
    /// mode — this runs after `main.rs` may have just spent minutes on a
    /// local crawl, and mesh sync is an optional accelerant, never a hard
    /// requirement for the process to serve traffic.
    pub(crate) fn new(database: Arc<Database>) -> Option<P2pDbServer> {
        let my_ip = match TailnetFns::get_my_address() {
            Some(ip) => ip,
            None => {
                log::warn!("[p2p_db_server] No tailscale address available; mesh sync server disabled");
                return None;
            }
        };
        let bind_addr = format!("{my_ip}:{CONTROL_PORT}");
        let listener = match TcpListener::bind(&bind_addr) {
            Ok(l) => l,
            Err(e) => {
                log::warn!("[p2p_db_server] Failed to bind {bind_addr}: {e}; mesh sync server disabled");
                return None;
            }
        };
        Some(P2pDbServer {
            port: CONTROL_PORT,
            listener,
            database,
            my_ip,
            sending: Mutex::new(()),
        })
    }

    /// Accepts control-plane connections forever, one thread per connection
    /// — mirrors `server::run`. Deliberately supports only `db_info` and
    /// `request_pull`; the tailnet-facing surface is intentionally smaller
    /// than the full local Unix-socket API.
    pub(crate) fn run_server(self: Arc<Self>) {
        for stream in self.listener.incoming() {
            match stream {
                Ok(stream) => {
                    let this = Arc::clone(&self);
                    std::thread::spawn(move || this.handle_connection(stream));
                }
                Err(e) => log::error!("[p2p_db_server] Failed to accept control-plane connection: {e}"),
            }
        }
    }

    fn handle_connection(&self, stream: TcpStream) {
        let peer_ip = match stream.peer_addr() {
            Ok(addr) => addr.ip(),
            Err(e) => {
                log::warn!("[p2p_db_server] Failed to read peer address: {e}");
                return;
            }
        };
        let _ = stream.set_read_timeout(Some(Duration::from_secs(CONTROL_READ_TIMEOUT_SECS)));
        let _ = stream.set_write_timeout(Some(Duration::from_secs(CONTROL_WRITE_TIMEOUT_SECS)));

        let mut writer = match stream.try_clone() {
            Ok(s) => s,
            Err(e) => {
                log::error!("[p2p_db_server] Failed to clone connection for writing: {e}");
                return;
            }
        };
        let mut reader = BufReader::new(stream);
        let mut line = String::new();

        loop {
            line.clear();
            let bytes_read = match (&mut reader).take(MAX_CONTROL_LINE_BYTES).read_line(&mut line) {
                Ok(n) => n,
                Err(e) => {
                    if !matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) {
                        log::warn!("[p2p_db_server] Control-plane read error from {peer_ip}: {e}");
                    }
                    return;
                }
            };
            if bytes_read == 0 {
                return; // EOF: peer closed the connection
            }
            if !line.ends_with('\n') {
                log::warn!(
                    "[p2p_db_server] Control-plane line from {peer_ip} exceeded {MAX_CONTROL_LINE_BYTES} bytes without a newline; closing connection"
                );
                write_response(&mut writer, false, Some("line_too_long"));
                return;
            }
            let trimmed = line.trim_end_matches(['\r', '\n']);
            if trimmed.is_empty() {
                continue;
            }

            let parsed: serde_json::Value = match serde_json::from_str(trimmed) {
                Ok(v) => v,
                Err(e) => {
                    write_response(&mut writer, false, Some(&format!("bad_request: {e}")));
                    continue;
                }
            };
            let op = parsed.get("op").and_then(|v| v.as_str()).unwrap_or("");

            match op {
                "db_info" => {
                    let value = serde_json::json!({"ok": true, "result": api::db_info_json(&self.database)});
                    write_json_line(&mut writer, &value);
                }
                "request_pull" => {
                    self.handle_request_pull(peer_ip, &mut writer);
                    return; // the raw transfer (if any) already happened on RAW_TRANSFER_PORT
                }
                other => {
                    write_response(&mut writer, false, Some(&format!("unknown_op: {other}")));
                }
            }
        }
    }

    /// The atomic "send my db" operation, from the moment `request_pull`
    /// arrives to the moment `RAW_TRANSFER_PORT` is closed again. Every exit
    /// path — busy, bind failure, accept timeout, mid-transfer I/O error —
    /// releases the `sending` lock (RAII) and, where the failure happens
    /// before any bytes are committed to the wire, tells the requester "I
    /// can't send my db" over the control-plane connection. A failure after
    /// the raw stream is already flowing has no control-plane message to
    /// land in (the requester is reading from `RAW_TRANSFER_PORT` by then,
    /// not 9563) — it surfaces to the requester as a truncated gzip stream,
    /// which `mesh_sync.rs`'s decoder detects on its own.
    fn handle_request_pull(&self, requester_ip: IpAddr, writer: &mut impl Write) {
        // A panic elsewhere while this lock was held would otherwise poison
        // it permanently, silently disabling sending on this node forever
        // (every future request rejected as "busy" with no way to recover
        // short of a restart) — the lock only ever guards a `()`, so there
        // is no invariant a poisoned state could have corrupted, and
        // recovering it here is safe.
        let _guard = match self.sending.try_lock() {
            Ok(g) => g,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                write_response(writer, false, Some("busy"));
                return;
            }
        };

        // Pin one snapshot for the whole operation so the bytes sent below
        // always match the built_at_unix/lines the peer saw in `db_info` —
        // a background refresh swapping snapshots mid-handshake must not be
        // able to split those two views.
        let snap = self.database.snapshot();

        let bind_addr = format!("{}:{}", self.my_ip, RAW_TRANSFER_PORT);
        let listener = match TcpListener::bind(&bind_addr) {
            Ok(l) => l,
            Err(e) => {
                log::warn!("[p2p_db_server] Failed to bind raw transfer port {bind_addr}: {e}");
                write_response(writer, false, Some(&format!("bind_failed: {e}")));
                return;
            }
        };
        if let Err(e) = listener.set_nonblocking(true) {
            log::warn!("[p2p_db_server] Failed to set raw transfer listener non-blocking: {e}");
            write_response(writer, false, Some(&format!("bind_failed: {e}")));
            return;
        }

        // From here on the port is open and compression is about to start —
        // saying "ok" is honest at this point, not a promise made in
        // advance of actually being able to deliver.
        write_response(writer, true, None);

        let deadline = Instant::now() + Duration::from_secs(PULL_TIMEOUT_SECS);
        let stream = loop {
            match listener.accept() {
                Ok((s, addr)) if addr.ip() == requester_ip => break Some(s),
                Ok((_, addr)) => {
                    // Another peer raced onto the port before the actual
                    // requester connected; refuse it and keep waiting.
                    log::warn!(
                        "[p2p_db_server] Ignoring raw-transfer connection from {addr} (expected {requester_ip})"
                    );
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        break None;
                    }
                    std::thread::sleep(ACCEPT_POLL_INTERVAL);
                }
                Err(e) => {
                    log::warn!("[p2p_db_server] Accept error on raw transfer port: {e}");
                    break None;
                }
            }
        };
        drop(listener);

        let Some(stream) = stream else {
            log::warn!("[p2p_db_server] Raw transfer to {requester_ip} timed out waiting for a connection");
            return;
        };

        // On macOS/BSD, a socket returned by `accept()` can inherit the
        // listening socket's non-blocking flag rather than starting fresh in
        // blocking mode — since `listener` above was set non-blocking to
        // support the polling accept loop, `stream` can come out
        // non-blocking too. If left that way, the write timeout below sets
        // SO_SNDTIMEO but O_NONBLOCK still takes precedence, so a full send
        // buffer surfaces immediately as EWOULDBLOCK ("Resource temporarily
        // unavailable") instead of blocking up to the timeout. Force it back
        // to blocking mode so the timeouts actually govern the transfer.
        if let Err(e) = stream.set_nonblocking(false) {
            log::warn!("[p2p_db_server] Failed to clear non-blocking mode on raw transfer stream to {requester_ip}: {e}");
            return;
        }

        let _ = stream.set_read_timeout(Some(Duration::from_secs(PULL_TIMEOUT_SECS)));
        let _ = stream.set_write_timeout(Some(Duration::from_secs(PULL_TIMEOUT_SECS)));

        match stream_snapshot(&snap, stream) {
            Ok(bytes) => log::info!("[p2p_db_server] Sent {bytes} compressed bytes to {requester_ip}"),
            Err(e) => log::warn!("[p2p_db_server] Raw transfer to {requester_ip} failed: {e}"),
        }
    }
}

fn write_response(writer: &mut impl Write, ok: bool, error: Option<&str>) {
    let value = if ok {
        serde_json::json!({"ok": true})
    } else {
        serde_json::json!({"ok": false, "error": error.unwrap_or("unknown_error")})
    };
    write_json_line(writer, &value);
}

fn write_json_line(writer: &mut impl Write, value: &serde_json::Value) {
    if let Err(e) = writeln!(writer, "{value}") {
        log::warn!("[p2p_db_server] Failed to write control-plane response: {e}");
    }
}

/// Streams `snap`'s entire backing file — offset 0 through EOF, including
/// the line-0 `__apdb_meta__` record — through a level-9 gzip encoder
/// straight onto `stream`, one `TRANSFER_CHUNK_BYTES` buffer at a time.
/// Uses `FileExt::read_at` (positioned reads, same idiom as
/// `Snapshot::read_raw`) rather than `snap.file.try_clone()` — a cloned fd
/// shares the underlying file offset with the original on POSIX, and that
/// offset was left at EOF by `Snapshot::from_file`'s one-time indexing scan
/// at load time, so a naive clone-and-sequential-read would read 0 bytes
/// immediately. `read_at` never touches any shared cursor, so it's safe
/// regardless of what else holds the same fd.
fn stream_snapshot(snap: &Snapshot, stream: TcpStream) -> std::io::Result<u64> {
    let mut pos: u64 = 0;
    let mut buf = vec![0u8; TRANSFER_CHUNK_BYTES];
    let mut encoder = GzEncoder::new(BufWriter::new(stream), Compression::new(9));
    loop {
        let n = snap.file.read_at(&mut buf, pos)?;
        if n == 0 {
            break;
        }
        encoder.write_all(&buf[..n])?;
        pos += n as u64;
    }
    // Explicit finish (not just Drop) so a failure to flush the gzip
    // trailer (CRC32 + uncompressed size) is actually observed here rather
    // than silently swallowed.
    encoder.finish()?;
    Ok(pos)
}
