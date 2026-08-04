use crate::api;
use std::sync::Arc;
use std::path::Path;
use std::time::Duration;
use crate::database::Database;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};

const READ_TIMEOUT: Duration = Duration::from_secs(60);
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// Binds a Unix domain socket at `path`, clearing a stale socket file left
/// over from an unclean shutdown. A *live* listener at that path is never
/// stolen: probing with a connect attempt first distinguishes "stale file,
/// nothing listening" (safe to remove and rebind) from "another instance is
/// actually running" (refuse to start rather than fight over the socket).
pub(crate) fn bind(path: &Path) -> std::io::Result<UnixListener> {
    if UnixStream::connect(path).is_ok() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
            format!("another APDB instance is already listening on {}", path.display()),
        ));
    }
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    UnixListener::bind(path)
}

/// Accepts connections forever, one `std::thread::spawn` per connection —
/// appropriate given the expected client population (a single long-lived
/// Argus dispatcher process, maybe a handful of concurrent connections; no
/// thread pool or async runtime needed, and neither is available without a
/// new dependency).
pub(crate) fn run(listener: UnixListener, db: Arc<Database>) {
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let db = Arc::clone(&db);
                std::thread::spawn(move || handle_connection(stream, &db));
            }
            Err(e) => log::error!("[server] Failed to accept connection: {e}"),
        }
    }
}

/// One JSON request per line in, one JSON response per line out, until EOF
/// or an I/O error. A malformed or unknown-op line yields an `ok:false`
/// response (see `api::handle_line`) but never closes the connection — only
/// a real I/O failure or client-initiated close ends the loop.
fn handle_connection(stream: UnixStream, db: &Database) {
    let _ = stream.set_read_timeout(Some(READ_TIMEOUT));
    let _ = stream.set_write_timeout(Some(WRITE_TIMEOUT));

    let mut writer = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            log::error!("[server] Failed to clone connection for writing: {e}");
            return;
        }
    };
    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = match reader.read_line(&mut line) {
            Ok(n) => n,
            Err(e) => {
                // WouldBlock/TimedOut here just means READ_TIMEOUT elapsed
                // with no data from the client (e.g. it's holding the
                // connection open between requests) — expected, not a fault,
                // so it's kept at info instead of warn.
                if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) {
                    log::info!(
                        "[server] Client idle {READ_TIMEOUT:?} with no request, closing connection (normal)"
                    );
                } else {
                    log::warn!("[server] Connection read error: {e}");
                }
                return;
            }
        };
        if bytes_read == 0 {
            return; // EOF: client closed the connection
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            continue;
        }

        let response = api::handle_line(db, trimmed);
        if writer.write_all(response.as_bytes()).is_err() || writer.write_all(b"\n").is_err() {
            log::warn!("[server] Connection write error, closing");
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Snapshot;
    use std::fs::File;

    struct ScratchFile(std::path::PathBuf);
    impl Drop for ScratchFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn scratch_path(tag: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "apdb_server_test_{}_{}_{tag}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        p
    }

    #[test]
    fn end_to_end_request_response() {
        let db_path = scratch_path("db");
        let _db_guard = ScratchFile(db_path.clone());
        {
            let mut f = File::create(&db_path).unwrap();
            writeln!(f, "{}", serde_json::json!({"ticker": "hello-world"})).unwrap();
        }
        let snap = Snapshot::from_file(File::open(&db_path).unwrap()).unwrap();
        let db = Arc::new(Database::new(Arc::new(snap)));

        let sock_path = scratch_path("sock");
        let _sock_guard = ScratchFile(sock_path.clone());
        let listener = bind(&sock_path).unwrap();
        let server_db = Arc::clone(&db);
        std::thread::spawn(move || run(listener, server_db));

        // Short-lived local test: give the accept loop a moment to be ready
        // rather than building a connect-retry helper for one test.
        std::thread::sleep(Duration::from_millis(50));

        let mut client = UnixStream::connect(&sock_path).unwrap();
        client
            .write_all(b"{\"op\":\"get_event\",\"ticker\":\"hello-world\"}\n")
            .unwrap();
        let mut reader = BufReader::new(client);
        let mut response = String::new();
        reader.read_line(&mut response).unwrap();
        let v: serde_json::Value = serde_json::from_str(response.trim_end()).unwrap();
        assert_eq!(v["result"]["event"]["ticker"], "hello-world");
    }

    #[test]
    fn bind_refuses_to_steal_a_live_socket() {
        let sock_path = scratch_path("sock_live");
        let _sock_guard = ScratchFile(sock_path.clone());
        let _first = bind(&sock_path).unwrap();
        let second = bind(&sock_path);
        assert!(second.is_err());
    }

    #[test]
    fn bind_cleans_up_a_stale_socket_file() {
        let sock_path = scratch_path("sock_stale");
        let _sock_guard = ScratchFile(sock_path.clone());
        {
            let listener = UnixListener::bind(&sock_path).unwrap();
            drop(listener); // leaves the socket file behind without a live listener
        }
        assert!(bind(&sock_path).is_ok());
    }
}
