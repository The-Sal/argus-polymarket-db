use std::fs::File;
use std::sync::Arc;
use std::path::Path;
use std::time::{Duration};
use crate::utils::now_unix;
use flate2::read::GzDecoder;
use crate::refresh::tmp_path_for;
use crate::tailnet_fns::TailnetFns;
use std::net::{SocketAddr, TcpStream};
use crate::database::{Snapshot, DB_FORMAT_VERSION};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use crate::p2p_db_server::{CONTROL_PORT, MAX_CONTROL_LINE_BYTES, RAW_TRANSFER_PORT};

/// Connect+read timeout for the lightweight `db_info` handshake — kept
/// short since this fans out to every peer on the tailnet and a single
/// unresponsive peer must not stall boot.
const PEER_QUERY_TIMEOUT_SECS: u64 = 5;
/// Idle timeout for the bulk pull itself, once a peer has agreed to send —
/// see the matching constant and comment in `p2p_db_server.rs`.
const PULL_TIMEOUT_SECS: u64 = 120;
/// Read/write chunk size for the decompress-and-write loop in
/// `pull_from_peer` — also the granularity of the `\r`-animated progress
/// counter, matching `TRANSFER_CHUNK_BYTES` on the sending side.
const PULL_PROGRESS_CHUNK_BYTES: usize = 256 * 1024;


/// Boot-time client/orchestrator role: asks every tailnet peer how old
/// their database is, and if any of them is within `refresh_interval_secs`,
/// pulls it instead of doing a full local crawl. Returns `None` on any
/// failure — no tailscale, no peers, no peer fresh enough, or the pull
/// itself failing — so the caller in `main.rs` always has the exact same
/// local-crawl fallback available as it does today when mesh sync doesn't
/// exist at all. This must only be called when the local database is
/// already known to be absent or stale; it does not check local freshness
/// itself.
pub(crate) fn try_bootstrap_from_peers(db_path: &Path, refresh_interval_secs: u64) -> Option<Arc<Snapshot>> {
    if !TailnetFns::tailscale_available() {
        return None;
    }
    let peers = match TailnetFns::get_peers() {
        Ok(p) if !p.is_empty() => p,
        _ => return None,
    };

    log::info!("[mesh_sync] Querying {} tailnet peer(s) for a fresh database...", peers.len());

    // Queried concurrently so total wall time is ~PEER_QUERY_TIMEOUT_SECS
    // regardless of peer count, not peer_count * PEER_QUERY_TIMEOUT_SECS.
    let handles: Vec<_> = peers
        .into_iter()
        .map(|ip| std::thread::spawn(move || query_peer_db_info(&ip).map(|(built_at_unix, fv)| (ip, built_at_unix, fv))))
        .collect();

    let now = now_unix();
    let mut candidates: Vec<(String, u64)> = Vec::new();
    for handle in handles {
        let Ok(Some((ip, built_at_unix, format_version))) = handle.join() else {
            continue;
        };
        // A future format bump's bytes aren't guaranteed parseable by this
        // build's Snapshot::from_file, so an exact version match is
        // required rather than treating "not legacy 0" as good enough —
        // refusing a mismatched pull is cheaper than installing something
        // this binary can't actually read.
        if format_version != DB_FORMAT_VERSION {
            continue;
        }
        if now.saturating_sub(built_at_unix) >= refresh_interval_secs {
            continue;
        }
        candidates.push((ip, built_at_unix));
    }
    // Freshest first, but every in-TTL candidate is tried in turn — a
    // single "busy" peer (e.g. several instances restarting at once and all
    // picking the same freshest source) must not fall all the way back to a
    // full crawl when another peer with in-TTL data was available.
    candidates.sort_by_key(|(_, built_at_unix)| std::cmp::Reverse(*built_at_unix));

    for (peer_ip, built_at_unix) in candidates {
        log::info!(
            "[mesh_sync] {peer_ip} has a database built {}s ago (within the {refresh_interval_secs}s TTL); pulling it",
            now.saturating_sub(built_at_unix)
        );
        match pull_from_peer(&peer_ip, db_path) {
            Ok(snapshot) => {
                log::info!("[mesh_sync] Pulled {} events from {peer_ip}", snapshot.lines);
                return Some(snapshot);
            }
            Err(e) => {
                log::warn!("[mesh_sync] Pull from {peer_ip} failed: {e}; trying next candidate if any");
            }
        }
    }
    None
}

/// Asks one peer for its `db_info`. `None` on any failure — connect
/// refused, timeout, malformed response — all treated identically as "this
/// peer isn't a usable source," never propagated as a hard error.
fn query_peer_db_info(peer_ip: &str) -> Option<(u64, u32)> {
    let timeout = Duration::from_secs(PEER_QUERY_TIMEOUT_SECS);
    let addr: SocketAddr = format!("{peer_ip}:{CONTROL_PORT}").parse().ok()?;
    let stream = TcpStream::connect_timeout(&addr, timeout).ok()?;
    stream.set_read_timeout(Some(timeout)).ok()?;
    stream.set_write_timeout(Some(timeout)).ok()?;

    let mut writer = stream.try_clone().ok()?;
    writeln!(writer, r#"{{"op":"db_info"}}"#).ok()?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    (&mut reader).take(MAX_CONTROL_LINE_BYTES).read_line(&mut line).ok()?;
    if !line.ends_with('\n') {
        return None; // truncated/oversized response — not a usable peer
    }
    let parsed: serde_json::Value = serde_json::from_str(line.trim()).ok()?;
    if parsed.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        return None;
    }
    let result = parsed.get("result")?;
    let built_at_unix = result.get("built_at_unix")?.as_u64()?;
    let format_version = result.get("db_format_version")?.as_u64()? as u32;
    Some((built_at_unix, format_version))
}

/// Requests and receives `peer_ip`'s database. Writes the decompressed
/// stream straight to `tmp_path_for(db_path)` without ever buffering the
/// whole thing in memory, validates it as a real `Snapshot` while it is
/// still the `.tmp` file (so a corrupt pull can never clobber a working
/// local `db_path`), and only then renames it into place — a deliberate
/// divergence from `full_crawl_and_compact`'s rename-then-reopen order,
/// since a network pull is untrusted input in a way a local crawl isn't.
fn pull_from_peer(peer_ip: &str, db_path: &Path) -> io::Result<Arc<Snapshot>> {
    let handshake_timeout = Duration::from_secs(PEER_QUERY_TIMEOUT_SECS);
    let control_addr: SocketAddr = format!("{peer_ip}:{CONTROL_PORT}")
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("bad peer address: {e}")))?;
    let control = TcpStream::connect_timeout(&control_addr, handshake_timeout)?;
    control.set_read_timeout(Some(Duration::from_secs(PULL_TIMEOUT_SECS)))?;
    control.set_write_timeout(Some(handshake_timeout))?;

    let mut writer = control.try_clone()?;
    writeln!(writer, r#"{{"op":"request_pull"}}"#)?;

    let mut reader = BufReader::new(control);
    let mut line = String::new();
    (&mut reader).take(MAX_CONTROL_LINE_BYTES).read_line(&mut line)?;
    if !line.ends_with('\n') {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "response line too long or truncated"));
    }
    let parsed: serde_json::Value = serde_json::from_str(line.trim())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("malformed response: {e}")))?;
    if parsed.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        let reason = parsed.get("error").and_then(|v| v.as_str()).unwrap_or("unknown");
        return Err(io::Error::other(format!("peer declined pull: {reason}")));
    }

    let raw_addr: SocketAddr = format!("{peer_ip}:{RAW_TRANSFER_PORT}")
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("bad peer address: {e}")))?;
    let raw_stream = TcpStream::connect_timeout(&raw_addr, Duration::from_secs(PULL_TIMEOUT_SECS))?;
    raw_stream.set_read_timeout(Some(Duration::from_secs(PULL_TIMEOUT_SECS)))?;

    let tmp_path = tmp_path_for(db_path);
    let result = (|| -> io::Result<Arc<Snapshot>> {
        let mut decoder = GzDecoder::new(BufReader::new(raw_stream));
        let mut file_writer = BufWriter::new(File::create(&tmp_path)?);
        // Hand-rolled in place of `io::copy` so each chunk's byte count is
        // visible for the progress counter below; behaves identically for
        // error purposes — a truncated or corrupted stream still fails here
        // when GzDecoder's `read` hits the gzip trailer (CRC32 +
        // uncompressed size) check, before any ticker JSON is even parsed.
        let mut buf = vec![0u8; PULL_PROGRESS_CHUNK_BYTES];
        let mut total_bytes: u64 = 0;
        let mut stdout = io::stdout();
        loop {
            let n = decoder.read(&mut buf)?;
            if n == 0 {
                break;
            }
            file_writer.write_all(&buf[..n])?;
            total_bytes += n as u64;
            // \r keeps this to one animated line rather than flooding
            // scrollback; logging goes to stderr (see main.rs's
            // `StderrLogger`) so this stdout line never interleaves with it.
            // Explicit flush because stdout is line-buffered and this line
            // never ends in '\n'.
            print!("\r[mesh_sync] Pulling from {peer_ip}: {:.1} MB", total_bytes as f64 / 1_000_000.0);
            let _ = stdout.flush();
        }
        println!();
        file_writer.flush()?;
        file_writer.get_ref().sync_all()?;
        drop(file_writer);

        let tmp_file = File::open(&tmp_path)?;
        let snapshot = Snapshot::from_file(tmp_file)?;
        std::fs::rename(&tmp_path, db_path)?;
        Ok(Arc::new(snapshot))
    })();

    if result.is_err() {
        // Don't leave a multi-hundred-MB partial file behind on a
        // low-RAM/small-disk box after a failed pull.
        let _ = std::fs::remove_file(&tmp_path);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use crate::p2p_db_server::P2pDbServer;
    use std::fs::File as StdFile;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct ScratchFile(PathBuf);
    impl Drop for ScratchFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn scratch_path(tag: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "apdb_mesh_sync_test_{}_{}_{tag}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ));
        p
    }

    /// End-to-end over the real tailnet-bound ports (this repo's existing
    /// `tailnet_fns` tests already require a working `tailscale` CLI, so
    /// this follows the same assumption rather than mocking it away):
    /// stands up one real `P2pDbServer` and exercises the whole feature
    /// against it — `db_info`, a successful pull (gzip streaming,
    /// decompression, tmp-file validation, and rename all included), and a
    /// concurrent second pull correctly rejected as busy. Combined into one
    /// test because `CONTROL_PORT`/`RAW_TRANSFER_PORT` are fixed constants:
    /// only one `P2pDbServer` can ever be bound per process, so a second
    /// `#[test]` standing up its own server would fail to bind against the
    /// first one's still-running accept loop.
    #[test]
    fn db_info_pull_and_busy_rejection_over_real_tailnet() {
        if !TailnetFns::tailscale_available() {
            panic!("Tailscale is not available in this test environment");
        }
        let my_ip = TailnetFns::get_my_address().expect("tailscale ip -4 failed");

        let source_path = scratch_path("source_db");
        let _source_guard = ScratchFile(source_path.clone());
        let built_at_unix = now_unix();
        {
            let mut f = StdFile::create(&source_path).unwrap();
            writeln!(
                f,
                "{}",
                serde_json::json!({
                    "__apdb_meta__": true,
                    "format_version": DB_FORMAT_VERSION,
                    "built_at_unix": built_at_unix,
                })
            )
            .unwrap();
            writeln!(f, "{}", serde_json::json!({"ticker": "mesh-sync-smoke-test"})).unwrap();
            // Padding so a pull takes long enough for a concurrent second
            // pull to reliably observe the `sending` lock still held.
            for i in 0..20_000 {
                writeln!(f, "{}", serde_json::json!({"ticker": format!("busy-test-{i}")})).unwrap();
            }
        }
        let snap = Snapshot::from_file(StdFile::open(&source_path).unwrap()).unwrap();
        let expected_lines = snap.lines;
        let db = Arc::new(Database::new(Arc::new(snap)));

        let server = Arc::new(P2pDbServer::new(Arc::clone(&db)).expect("P2pDbServer::new failed"));
        {
            let server = Arc::clone(&server);
            std::thread::spawn(move || server.run_server());
        }
        // Give the accept loop a moment to be ready, matching server.rs's
        // own test pattern rather than building a connect-retry helper.
        std::thread::sleep(Duration::from_millis(100));

        let (got_built_at, got_format_version) = query_peer_db_info(&my_ip).expect("db_info query failed");
        assert_eq!(got_built_at, built_at_unix);
        assert_eq!(got_format_version, DB_FORMAT_VERSION);

        let dest_path_a = scratch_path("dest_db_a");
        let _dest_guard_a = ScratchFile(dest_path_a.clone());
        let dest_path_b = scratch_path("dest_db_b");
        let _dest_guard_b = ScratchFile(dest_path_b.clone());

        let ip_a = my_ip.clone();
        let dest_path_a_for_thread = dest_path_a.clone();
        let handle_a = std::thread::spawn(move || pull_from_peer(&ip_a, &dest_path_a_for_thread));
        // Give the first pull a head start so it holds the `sending` lock
        // by the time the second one asks.
        std::thread::sleep(Duration::from_millis(20));
        let result_b = pull_from_peer(&my_ip, &dest_path_b);
        assert!(result_b.is_err(), "second concurrent pull should have been rejected as busy");

        let pulled = handle_a.join().unwrap().expect("first pull should have succeeded");
        assert_eq!(pulled.lines, expected_lines);
        assert_eq!(pulled.built_at_unix, built_at_unix);
        assert!(pulled.find("mesh-sync-smoke-test").is_some());
        assert!(dest_path_a.exists());
    }
}
