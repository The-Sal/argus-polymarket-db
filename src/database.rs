use std::fs::File;
use crate::utils::now_unix;
use std::sync::{Arc, RwLock};
use std::os::unix::fs::FileExt;
use std::io::{self, BufRead, BufReader};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// On-disk format version, written as `format_version` in the line-0 meta
/// record by `full_crawl_and_compact` and bumped whenever the file layout or
/// meta line's meaning changes. `0` denotes the pre-meta-line legacy format
/// (no line-0 record at all — see `docs/db_specs/v0.md`). See
/// `docs/db_specs/v1.md` for what `1` (the current format) adds.
pub(crate) const DB_FORMAT_VERSION: u32 = 1;

/// One ticker's location in the on-disk log: byte offset of its JSON payload
/// and the payload's length (excluding the trailing `\n`), enough to read it
/// back with a single positioned `pread` (`FileExt::read_exact_at`) instead
/// of seeking a shared cursor.
#[derive(Debug, Clone)]
pub(crate) struct IndexEntry {
    pub(crate) ticker: String,
    pub(crate) offset: u64,
    pub(crate) len: u32,
}

/// An immutable, point-in-time view of the database: a ticker-sorted index
/// paired with the exact file whose byte offsets it describes. The pairing
/// matters — swapping the index without swapping the file (or vice versa)
/// lets a reader's offsets point at content that's no longer there. Because
/// an open file descriptor keeps referencing its inode's data even after the
/// path is renamed over, a reader holding an `Arc<Snapshot>` keeps working
/// correctly for the lifetime of that `Arc`, regardless of what `refresh`
/// does to the path afterward.
pub(crate) struct Snapshot {
    pub(crate) index: Vec<IndexEntry>,
    pub(crate) file: File,
    pub(crate) lines: u32,
    pub(crate) built_at_unix: u64,
    pub(crate) format_version: u32,
}


/// Sorted input; for each run of equal tickers, keeps only the last
/// occurrence (later in the file = more recent write) — mirrors the
/// upsert semantics `add_event`/refresh give a repeated ticker.
pub(crate) fn dedupe_keep_last(entries: Vec<IndexEntry>) -> Vec<IndexEntry> {
    let mut out: Vec<IndexEntry> = Vec::with_capacity(entries.len());
    let mut iter = entries.into_iter().peekable();
    while let Some(mut current) = iter.next() {
        while let Some(next) = iter.peek() {
            if next.ticker == current.ticker {
                current = iter.next().unwrap();
            } else {
                break;
            }
        }
        out.push(current);
    }
    out
}

impl Snapshot {
    /// Scans the whole file once, building a ticker-sorted index. Tolerant
    /// of a torn final line (crash mid-append), unparseable records, and
    /// blank/whitespace-only tickers — each is logged and skipped rather
    /// than panicking the whole startup, since a single bad record must not
    /// take down the server.
    pub(crate) fn from_file(file: File) -> io::Result<Snapshot> {
        let start = Instant::now();
        // `built_at_unix` is reported to clients via `db_info` as "when this
        // data was built", and is also what a TTL-based refresh check
        // compares against — so it must reflect when the *data* was built,
        // not when the file happened to last touch disk (mtime survives
        // copies, rsync, backups, and `touch` without meaning any of those
        // things rebuilt the data). The authoritative value lives in the
        // meta line (offset 0, `__apdb_meta__: true`) written by
        // `full_crawl_and_compact`. Files predating that meta line, or with
        // an unparseable one, fall back to mtime (then `now()`) so old
        // on-disk databases still load.
        let mut built_at_unix = file
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or_else(now_unix);
        // `0` = pre-meta-line legacy format (docs/db_specs/v0.md); overwritten
        // below if a line-0 meta record is found.
        let mut format_version: u32 = 0;
        let mut entries: Vec<IndexEntry> = Vec::new();
        let mut reader = BufReader::new(file.try_clone()?);
        let mut offset: u64 = 0;
        let mut line = String::new();
        let mut skipped = 0u32;

        loop {
            line.clear();
            let bytes_read = reader.read_line(&mut line)?;
            if bytes_read == 0 {
                break;
            }
            if !line.ends_with('\n') {
                log::warn!(
                    "[database] Dropping torn final line at offset {offset} ({} bytes, no trailing newline)",
                    line.len()
                );
                skipped += 1;
                break;
            }
            let content = line.trim_end_matches('\n');
            let content_len = content.len() as u32;
            let line_offset = offset;
            offset += bytes_read as u64;

            match serde_json::from_str::<serde_json::Value>(content) {
                Ok(value) => {
                    if line_offset == 0 && value.get("__apdb_meta__").and_then(|v| v.as_bool()) == Some(true) {
                        if let Some(ts) = value.get("built_at_unix").and_then(|v| v.as_u64()) {
                            built_at_unix = ts;
                        }
                        format_version = value
                            .get("format_version")
                            .and_then(|v| v.as_u64())
                            .map(|v| v as u32)
                            .unwrap_or(1);
                        continue;
                    }
                    let ticker = value.get("ticker").and_then(|t| t.as_str()).unwrap_or("").trim();
                    if ticker.is_empty() {
                        log::warn!("[database] Skipping record with empty/missing ticker at offset {line_offset}");
                        skipped += 1;
                    } else {
                        entries.push(IndexEntry {
                            ticker: ticker.to_string(),
                            offset: line_offset,
                            len: content_len,
                        });
                    }
                }
                Err(e) => {
                    log::warn!("[database] Skipping unparseable record at offset {line_offset}: {e}");
                    skipped += 1;
                }
            }
        }

        let raw_count = entries.len() as u32;
        entries.sort_by(|a, b| a.ticker.cmp(&b.ticker));
        let entries = dedupe_keep_last(entries);
        let lines = entries.len() as u32;
        let duplicates = raw_count - lines;

        log::info!(
            "[database] Loaded {lines} events in {:.2}s (format_version={format_version}, {duplicates} duplicate tickers collapsed, {skipped} bad records skipped)",
            start.elapsed().as_secs_f64()
        );

        Ok(Snapshot {
            index: entries,
            file,
            lines,
            built_at_unix,
            format_version,
        })
    }

    pub(crate) fn find(&self, ticker: &str) -> Option<&IndexEntry> {
        self.index
            .binary_search_by(|e| e.ticker.as_str().cmp(ticker))
            .ok()
            .map(|i| &self.index[i])
    }

    /// Positioned read (`pread`) — no shared cursor, safe to call
    /// concurrently from any number of threads against the same `Snapshot`.
    pub(crate) fn read_raw(&self, entry: &IndexEntry) -> io::Result<Vec<u8>> {
        let mut buf = vec![0u8; entry.len as usize];
        self.file.read_exact_at(&mut buf, entry.offset)?;
        Ok(buf)
    }

    pub(crate) fn read_value(&self, entry: &IndexEntry) -> io::Result<serde_json::Value> {
        let buf = self.read_raw(entry)?;
        serde_json::from_slice(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Keyset pagination: entries with ticker strictly greater than `after`
    /// (or from the start if `after` is `None`), up to `limit` of them, plus
    /// the cursor to pass as `after` for the next page (`None` = last page).
    /// Never eviction-sensitive: because the index only grows/updates in
    /// place and never removes a ticker out from under a cursor, a client
    /// paging through a background refresh never sees a skip or a repeat.
    pub(crate) fn cursor_range(&self, after: Option<&str>, limit: usize) -> (&[IndexEntry], Option<String>) {
        let start = match after {
            Some(a) => self.index.partition_point(|e| e.ticker.as_str() <= a),
            None => 0,
        };
        let end = (start + limit).min(self.index.len());
        let page = &self.index[start..end];
        let next_after = if end < self.index.len() {
            page.last().map(|e| e.ticker.clone())
        } else {
            None
        };
        (page, next_after)
    }

    pub(crate) fn prefix_range(&self, prefix: &str) -> &[IndexEntry] {
        let start = self.index.partition_point(|e| e.ticker.as_str() < prefix);
        let end = start + self.index[start..].partition_point(|e| e.ticker.as_str().starts_with(prefix));
        &self.index[start..end]
    }
}

/// Thread-safe handle to the current `Snapshot`. Readers pay for exactly one
/// `RwLock::read` + `Arc::clone` (a refcount bump) and then work entirely
/// against their own local `Arc<Snapshot>` — no lock is held across any I/O
/// or parsing. The refresh loop builds a whole new `Snapshot` off to the
/// side and only takes the write lock for the pointer swap. Neither
/// critical section can panic, so `RwLock` poisoning is structurally
/// unreachable rather than merely unlikely.
pub(crate) struct Database {
    inner: RwLock<Arc<Snapshot>>,
}

impl Database {
    pub(crate) fn new(snapshot: Arc<Snapshot>) -> Self {
        Self {
            inner: RwLock::new(snapshot),
        }
    }

    pub(crate) fn snapshot(&self) -> Arc<Snapshot> {
        self.inner.read().unwrap().clone()
    }

    pub(crate) fn swap(&self, new: Arc<Snapshot>) {
        *self.inner.write().unwrap() = new;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    struct ScratchFile(std::path::PathBuf);
    impl Drop for ScratchFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn write_scratch_db(records: &[(&str, serde_json::Value)]) -> (ScratchFile, File) {
        let mut path = std::env::temp_dir();
        path.push(format!("apdb_test_{}_{}.db", std::process::id(), now_unix_nanos()));
        let mut f = File::create(&path).unwrap();
        for (_ticker, value) in records {
            let line = serde_json::to_string(value).unwrap();
            f.write_all(line.as_bytes()).unwrap();
            f.write_all(b"\n").unwrap();
        }
        f.flush().unwrap();
        let file = File::open(&path).unwrap();
        (ScratchFile(path), file)
    }

    fn now_unix_nanos() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    fn event(ticker: &str) -> serde_json::Value {
        serde_json::json!({ "ticker": ticker, "endDate": null })
    }

    #[test]
    fn legacy_file_without_meta_line_has_format_version_zero() {
        let (_guard, file) = write_scratch_db(&[("only", event("only"))]);
        let snap = Snapshot::from_file(file).unwrap();
        assert_eq!(snap.format_version, 0);
        assert_eq!(snap.lines, 1);
        assert!(snap.find("only").is_some());
    }

    #[test]
    fn meta_line_supplies_built_at_and_format_version_and_is_not_indexed() {
        let mut path = std::env::temp_dir();
        path.push(format!("apdb_test_meta_{}_{}.db", std::process::id(), now_unix_nanos()));
        let mut f = File::create(&path).unwrap();
        let meta = serde_json::json!({
            "__apdb_meta__": true,
            "format_version": DB_FORMAT_VERSION,
            "built_at_unix": 12345u64,
        });
        f.write_all(serde_json::to_string(&meta).unwrap().as_bytes()).unwrap();
        f.write_all(b"\n").unwrap();
        f.write_all(serde_json::to_string(&event("only")).unwrap().as_bytes()).unwrap();
        f.write_all(b"\n").unwrap();
        f.flush().unwrap();
        let _guard = ScratchFile(path.clone());
        let file = File::open(&path).unwrap();

        let snap = Snapshot::from_file(file).unwrap();
        assert_eq!(snap.built_at_unix, 12345);
        assert_eq!(snap.format_version, DB_FORMAT_VERSION);
        assert_eq!(snap.lines, 1, "meta line must not be counted as a ticker record");
        assert!(snap.find("only").is_some());
        assert!(snap.find("__apdb_meta__").is_none());
    }

    #[test]
    fn sorted_and_binary_searchable() {
        let (_guard, file) = write_scratch_db(&[
            ("zeta", event("zeta")),
            ("alpha", event("alpha")),
            ("mid", event("mid")),
        ]);
        let snap = Snapshot::from_file(file).unwrap();
        assert_eq!(snap.lines, 3);
        let tickers: Vec<&str> = snap.index.iter().map(|e| e.ticker.as_str()).collect();
        assert_eq!(tickers, vec!["alpha", "mid", "zeta"]);

        let entry = snap.find("mid").expect("mid should be found");
        let value = snap.read_value(entry).unwrap();
        assert_eq!(value["ticker"], "mid");

        assert!(snap.find("missing").is_none());
    }

    #[test]
    fn duplicate_ticker_keeps_last() {
        let (_guard, file) = write_scratch_db(&[
            ("dup", serde_json::json!({"ticker": "dup", "version": 1})),
            ("dup", serde_json::json!({"ticker": "dup", "version": 2})),
        ]);
        let snap = Snapshot::from_file(file).unwrap();
        assert_eq!(snap.lines, 1);
        let entry = snap.find("dup").unwrap();
        let value = snap.read_value(entry).unwrap();
        assert_eq!(value["version"], 2);
    }

    #[test]
    fn empty_ticker_is_skipped() {
        let (_guard, file) = write_scratch_db(&[
            ("", serde_json::json!({"ticker": "", "x": 1})),
            ("real", event("real")),
        ]);
        let snap = Snapshot::from_file(file).unwrap();
        assert_eq!(snap.lines, 1);
        assert!(snap.find("real").is_some());
    }

    #[test]
    fn cursor_pagination_covers_everything_once() {
        let records: Vec<(&str, serde_json::Value)> = ["a", "b", "c", "d", "e"]
            .iter()
            .map(|t| (*t, event(t)))
            .collect();
        let (_guard, file) = write_scratch_db(&records);
        let snap = Snapshot::from_file(file).unwrap();

        let mut seen = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let (page, next_after) = snap.cursor_range(after.as_deref(), 2);
            if page.is_empty() {
                break;
            }
            seen.extend(page.iter().map(|e| e.ticker.clone()));
            match next_after {
                Some(next) => after = Some(next),
                None => break,
            }
        }
        assert_eq!(seen, vec!["a", "b", "c", "d", "e"]);
    }

    #[test]
    fn prefix_range_matches_only_prefix() {
        let records: Vec<(&str, serde_json::Value)> = ["btc-updown-1", "btc-updown-2", "eth-updown-1"]
            .iter()
            .map(|t| (*t, event(t)))
            .collect();
        let (_guard, file) = write_scratch_db(&records);
        let snap = Snapshot::from_file(file).unwrap();

        let matches: Vec<&str> = snap
            .prefix_range("btc-")
            .iter()
            .map(|e| e.ticker.as_str())
            .collect();
        assert_eq!(matches, vec!["btc-updown-1", "btc-updown-2"]);
    }

    // Run explicitly against the real on-disk DB (not part of the default
    // test run): cargo test --release -- --ignored --nocapture query_perf
    #[test]
    #[ignore]
    fn query_perf() {
        use std::time::Instant;

        const DB_FILE: &str = "polymarket_events.db";
        let load_start = Instant::now();
        let snap = Snapshot::from_file(File::open(DB_FILE).unwrap()).unwrap();
        println!("load: {} events in {:?}", snap.lines, load_start.elapsed());

        let n = snap.index.len();
        for frac in [0.0, 0.25, 0.5, 0.75, 0.99] {
            let idx = ((n - 1) as f64 * frac) as usize;
            let ticker = snap.index[idx].ticker.clone();
            let start = Instant::now();
            let found = snap.find(&ticker).is_some();
            println!(
                "sorted_idx={idx:>6}/{n} ticker={ticker:<50} elapsed={:?} found={found}",
                start.elapsed()
            );
        }

        let ticker = snap.index[n - 1].ticker.clone();
        for i in 0..3 {
            let start = Instant::now();
            let entry = snap.find(&ticker).unwrap();
            let _ = snap.read_value(entry).unwrap();
            println!("repeat {i}: ticker={ticker} elapsed={:?}", start.elapsed());
        }
    }
}
