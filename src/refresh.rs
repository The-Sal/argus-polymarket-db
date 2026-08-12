use std::fs::File;
use std::sync::Arc;
use crate::poly_api;
use std::path::{Path, PathBuf};
use crate::poly_api::OpenEventsIter;
use std::io::{self, BufWriter, Write};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use crate::database::{dedupe_keep_last, Database, IndexEntry, Snapshot};

/// Minimum gap between "still crawling" progress log lines during Phase 1.
/// A full crawl is ~170 requests to Gamma; logging every page would flood
/// stderr, so progress is throttled to wall-clock time instead of page count.
const CRAWL_PROGRESS_INTERVAL: Duration = Duration::from_secs(5);

/// A ticker Gamma's `closed=false` crawl stops returning is presumed
/// closed, but its `endDate` is what's trusted to say *when* — the API's
/// own `closed` field only reflects state as of that ticker's last crawl
/// and goes stale the moment it falls out of the open-events crawl (an
/// event can sit with `closed: false` in our store for months after its
/// endDate passed). The grace period absorbs crawl-timing noise right
/// around the boundary rather than evicting the instant a ticker drops out.
const PRUNE_GRACE_SECS: i64 = 4 * 3600;

/// Parses an `endDate` string (RFC3339, e.g. `2025-11-23T00:00:00Z`) into a
/// Unix timestamp. Returns `None` on anything unparseable rather than
/// erroring — an unparseable or absent `endDate` means "can't tell if this
/// is closed," which must fall back to the existing never-evict behavior,
/// not be treated as eligible for pruning.
fn parse_rfc3339_unix(date_str: &str) -> Option<i64> {
    time::OffsetDateTime::parse(date_str, &time::format_description::well_known::Rfc3339)
        .ok()
        .map(|dt| dt.unix_timestamp())
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn tmp_path_for(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    PathBuf::from(s)
}

/// Crawls all open Gamma events page-by-page — driving `OpenEventsIter`
/// directly rather than `poly_api::fetch_all_markets_cached`, which
/// materializes every event into one `HashMap` at once and would recreate
/// the multi-GB-resident problem this crate exists to eliminate, on every
/// refresh cycle — merges the result against `old` (if any) so tickers
/// Gamma stopped returning are *kept*, not evicted (mirrors Python's
/// never-evict `.update()` merge semantics), and compacts the on-disk log
/// to contain exactly that merged, deduped, sorted set. One function serves
/// both first-run bootstrap (`old: None`) and every periodic refresh.
pub(crate) fn full_crawl_and_compact(path: &Path, old: Option<Arc<Snapshot>>) -> io::Result<Arc<Snapshot>> {
    let mode = if old.is_some() { "refresh" } else { "initial crawl" };
    let old_count = old.as_ref().map(|s| s.lines);
    let start = Instant::now();
    log::info!("[refresh] Starting {mode}: crawling open Gamma events...");

    let tmp_path = tmp_path_for(path);
    let mut entries: Vec<IndexEntry> = Vec::new();
    let mut offset: u64 = 0;

    let mut writer = BufWriter::new(File::create(&tmp_path)?);

    // Phase 1: stream the crawl page by page, writing straight to disk.
    // Never holds more than one page's worth of deserialized events at a
    // time — the whole point of driving the iterator directly.
    let mut pages = 0u32;
    let mut last_progress = Instant::now();
    for page in OpenEventsIter::default() {
        let events = page.map_err(io::Error::other)?;
        pages += 1;
        for event in events {
            let ticker = event.ticker.trim().to_string();
            if ticker.is_empty() {
                log::warn!("[refresh] Skipping crawled record with empty ticker");
                continue;
            }
            let json = serde_json::to_string(&event)?;
            writer.write_all(json.as_bytes())?;
            writer.write_all(b"\n")?;
            entries.push(IndexEntry {
                ticker,
                offset,
                len: json.len() as u32,
            });
            offset += json.len() as u64 + 1;
        }
        if last_progress.elapsed() >= CRAWL_PROGRESS_INTERVAL {
            log::info!(
                "[refresh] {mode}: still crawling — {pages} pages, {} events fetched so far ({:.0}s elapsed)",
                entries.len(),
                start.elapsed().as_secs_f64()
            );
            last_progress = Instant::now();
        }
    }

    log::info!(
        "[refresh] {mode}: crawl fetched {} events across {pages} pages in {:.1}s; compacting...",
        entries.len(),
        start.elapsed().as_secs_f64()
    );

    entries.sort_by(|a, b| a.ticker.cmp(&b.ticker));
    let mut entries = dedupe_keep_last(entries);

    // A first-run bootstrap that crawls zero events (Gamma unreachable, an
    // empty first page, etc.) must not silently produce and swap in an
    // empty database — unlike a periodic refresh, there's no `old`
    // snapshot's tickers to fall back on. A periodic refresh in this same
    // situation is already safe without a special case: Phase 2 below
    // carries every old ticker forward when the new crawl is empty or
    // partial, so the previous data survives untouched.
    if old.is_none() && entries.is_empty() {
        return Err(io::Error::other(
            "initial crawl returned zero events; refusing to write an empty database",
        ));
    }

    // Phase 2: carry forward any ticker the fresh crawl didn't return (e.g.
    // a market Gamma stopped listing), copying its bytes into the same temp
    // file, so nothing already known is ever evicted by a refresh.
    if let Some(old_snapshot) = &old {
        let (carried, pruned) = merge_carry_forward(&mut writer, &mut entries, old_snapshot, &mut offset)?;
        if carried > 0 {
            log::info!("[refresh] {mode}: carried forward {carried} ticker(s) not seen in this crawl");
        }
        if pruned > 0 {
            log::info!("[refresh] {mode}: pruned {pruned} ticker(s) closed more than {}h ago", PRUNE_GRACE_SECS / 3600);
        }
    }

    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);

    std::fs::rename(&tmp_path, path)?;
    let file = File::open(path)?;
    let lines = entries.len() as u32;

    let delta = match old_count {
        Some(old) => format!(" ({:+})", lines as i64 - old as i64),
        None => String::new(),
    };
    log::info!(
        "[refresh] {mode} complete: {lines} events{delta} in {:.1}s",
        start.elapsed().as_secs_f64()
    );

    Ok(Arc::new(Snapshot {
        index: entries,
        file,
        lines,
        built_at_unix: now_unix(),
    }))
}

/// Copies forward any ticker present in `old` but absent from the fresh
/// crawl already recorded in `entries` (e.g. a market Gamma stopped
/// listing), appending its raw bytes to `writer` and extending `entries` so
/// nothing already known is evicted by a refresh — mirrors Python's
/// never-evict `.update()` merge semantics. `entries` must already be
/// sorted by ticker on entry; it is left sorted (now including the carried
/// forward records) on return. Split out from `full_crawl_and_compact` so
/// the offset/length bookkeeping — the one place a bug here would corrupt
/// the on-disk database — is unit-testable without a network crawl.
fn merge_carry_forward<W: Write>(
    writer: &mut W,
    entries: &mut Vec<IndexEntry>,
    old: &Snapshot,
    offset: &mut u64,
) -> io::Result<(usize, usize)> {
    let mut new_iter = entries.iter().peekable();
    let mut carried_forward = Vec::new();
    let mut pruned = 0usize;
    let now = now_unix() as i64;

    for old_entry in &old.index {
        while matches!(new_iter.peek(), Some(n) if n.ticker.as_str() < old_entry.ticker.as_str()) {
            new_iter.next();
        }
        let already_present = matches!(new_iter.peek(), Some(n) if n.ticker == old_entry.ticker);
        if !already_present {
            match old.read_value(old_entry) {
                Ok(old_value) => {
                    let as_event: poly_api::PolymarketEvent = serde_json::from_value(old_value).unwrap_or_else(|_| {
                        poly_api::PolymarketEvent{
                            ticker: "null".to_string(),
                            end_date: None,
                            extra: Default::default(),
                        }
                    });
                    if as_event.ticker == "null".to_string() {
                        log::warn!("[refresh] Skipping crawled record with empty ticker");
                        continue;
                    }

                    if let Some(end_unix) = as_event.end_date.as_deref().and_then(parse_rfc3339_unix) {
                        if now - end_unix > PRUNE_GRACE_SECS {
                            log::debug!(
                                "[refresh] Pruning {}: endDate {} was more than {}h ago",
                                old_entry.ticker,
                                as_event.end_date.as_deref().unwrap_or(""),
                                PRUNE_GRACE_SECS / 3600
                            );
                            pruned += 1;
                            continue;
                        }
                    }
                }
                _ => {}
            }
            let raw = old.read_raw(old_entry)?;
            writer.write_all(&raw)?;
            writer.write_all(b"\n")?;
            carried_forward.push(IndexEntry {
                ticker: old_entry.ticker.clone(),
                offset: *offset,
                len: raw.len() as u32,
            });
            *offset += raw.len() as u64 + 1;
        }
    }

    let carried_count = carried_forward.len();
    entries.extend(carried_forward);
    entries.sort_by(|a, b| a.ticker.cmp(&b.ticker));
    Ok((carried_count, pruned))
}

/// Runs `full_crawl_and_compact` forever on `interval`, swapping the result
/// into `db` on success. A failed cycle (e.g. Gamma unreachable) is logged
/// and leaves the previous snapshot serving traffic untouched — a refresh
/// is never partially applied.
pub(crate) fn spawn_refresh_loop(db: Arc<Database>, path: PathBuf, interval: Duration) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || loop {
        std::thread::sleep(interval);
        let old = db.snapshot();
        match full_crawl_and_compact(&path, Some(old)) {
            Ok(new_snapshot) => db.swap(new_snapshot),
            Err(e) => log::error!("[refresh] Refresh failed, keeping previous snapshot: {e}"),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ScratchFile(PathBuf);
    impl Drop for ScratchFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn scratch_path(tag: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "apdb_refresh_test_{}_{}_{tag}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ));
        p
    }

    fn write_db(path: &Path, records: &[(&str, serde_json::Value)]) {
        let mut f = File::create(path).unwrap();
        for (_ticker, value) in records {
            f.write_all(serde_json::to_string(value).unwrap().as_bytes()).unwrap();
            f.write_all(b"\n").unwrap();
        }
        f.flush().unwrap();
    }

    /// The test the merge logic actually needs: an old snapshot with
    /// tickers a/b/c, a "fresh crawl" (already Phase-1-written, exactly as
    /// `full_crawl_and_compact` would hand off) that re-crawled b (with new
    /// content) and found a brand new ticker d, but didn't see a or c this
    /// cycle. After merging, a and c must survive with their *old* content,
    /// b must carry its *new* content (not be duplicated or reverted), and
    /// d must be present — verified by reloading the produced file through
    /// `Snapshot::from_file` independently, so a subtly-wrong offset would
    /// show up as a read failure or wrong content, not just a
    /// self-consistent-but-wrong `entries` vec.
    #[test]
    fn merge_carry_forward_preserves_old_tickers_not_recrawled() {
        let old_path = scratch_path("old_db");
        let _old_guard = ScratchFile(old_path.clone());
        write_db(
            &old_path,
            &[
                ("a", serde_json::json!({"ticker": "a", "v": 1})),
                ("b", serde_json::json!({"ticker": "b", "v": 1})),
                ("c", serde_json::json!({"ticker": "c", "v": 1})),
            ],
        );
        let old_snapshot = Snapshot::from_file(File::open(&old_path).unwrap()).unwrap();

        let tmp_path = scratch_path("tmp_db");
        let _tmp_guard = ScratchFile(tmp_path.clone());
        let mut writer = File::create(&tmp_path).unwrap();
        let mut entries = Vec::new();
        let mut offset: u64 = 0;
        for (ticker, value) in [
            ("b", serde_json::json!({"ticker": "b", "v": 2})),
            ("d", serde_json::json!({"ticker": "d", "v": 1})),
        ] {
            let json = serde_json::to_string(&value).unwrap();
            writer.write_all(json.as_bytes()).unwrap();
            writer.write_all(b"\n").unwrap();
            entries.push(IndexEntry {
                ticker: ticker.to_string(),
                offset,
                len: json.len() as u32,
            });
            offset += json.len() as u64 + 1;
        }

        merge_carry_forward(&mut writer, &mut entries, &old_snapshot, &mut offset).unwrap();
        writer.flush().unwrap();
        drop(writer);

        let reloaded = Snapshot::from_file(File::open(&tmp_path).unwrap()).unwrap();
        let tickers: Vec<&str> = reloaded.index.iter().map(|e| e.ticker.as_str()).collect();
        assert_eq!(tickers, vec!["a", "b", "c", "d"]);
        assert_eq!(reloaded.read_value(reloaded.find("a").unwrap()).unwrap()["v"], 1);
        assert_eq!(reloaded.read_value(reloaded.find("b").unwrap()).unwrap()["v"], 2);
        assert_eq!(reloaded.read_value(reloaded.find("c").unwrap()).unwrap()["v"], 1);
        assert_eq!(reloaded.read_value(reloaded.find("d").unwrap()).unwrap()["v"], 1);
    }

    #[test]
    fn merge_carry_forward_is_noop_when_everything_was_recrawled() {
        let old_path = scratch_path("old_db2");
        let _old_guard = ScratchFile(old_path.clone());
        write_db(&old_path, &[("a", serde_json::json!({"ticker": "a", "v": 1}))]);
        let old_snapshot = Snapshot::from_file(File::open(&old_path).unwrap()).unwrap();

        let tmp_path = scratch_path("tmp_db2");
        let _tmp_guard = ScratchFile(tmp_path.clone());
        let mut writer = File::create(&tmp_path).unwrap();
        let json = serde_json::to_string(&serde_json::json!({"ticker": "a", "v": 2})).unwrap();
        writer.write_all(json.as_bytes()).unwrap();
        writer.write_all(b"\n").unwrap();
        let mut entries = vec![IndexEntry {
            ticker: "a".to_string(),
            offset: 0,
            len: json.len() as u32,
        }];
        let mut offset = json.len() as u64 + 1;

        merge_carry_forward(&mut writer, &mut entries, &old_snapshot, &mut offset).unwrap();
        assert_eq!(entries.len(), 1, "must not duplicate a ticker that was already re-crawled");
        writer.flush().unwrap();
        drop(writer);

        let reloaded = Snapshot::from_file(File::open(&tmp_path).unwrap()).unwrap();
        assert_eq!(reloaded.lines, 1);
        assert_eq!(reloaded.read_value(reloaded.find("a").unwrap()).unwrap()["v"], 2);
    }

    fn rfc3339_secs_ago(secs: i64) -> String {
        let then = time::OffsetDateTime::now_utc() - time::Duration::seconds(secs);
        then.format(&time::format_description::well_known::Rfc3339).unwrap()
    }

    /// Carry-forward candidates (tickers the fresh crawl didn't return) are
    /// only ever pruned, never anything the crawl just re-fetched — pruning
    /// only makes sense for "Gamma stopped returning this," not "still
    /// open." Three old tickers not seen in this crawl: one closed 5 hours
    /// ago (past the 4h grace period, must be dropped), one closed 1 hour
    /// ago (inside the grace period, must survive), and one with no
    /// `endDate` at all (can't tell if it's closed, so the never-evict
    /// default must win and it must survive too).
    #[test]
    fn merge_carry_forward_prunes_only_tickers_closed_past_the_grace_period() {
        let old_path = scratch_path("old_db3");
        let _old_guard = ScratchFile(old_path.clone());
        write_db(
            &old_path,
            &[
                ("stale", serde_json::json!({"ticker": "stale", "endDate": rfc3339_secs_ago(5 * 3600)})),
                ("recent", serde_json::json!({"ticker": "recent", "endDate": rfc3339_secs_ago(3600)})),
                ("undated", serde_json::json!({"ticker": "undated"})),
            ],
        );
        let old_snapshot = Snapshot::from_file(File::open(&old_path).unwrap()).unwrap();

        let tmp_path = scratch_path("tmp_db3");
        let _tmp_guard = ScratchFile(tmp_path.clone());
        let mut writer = File::create(&tmp_path).unwrap();
        let mut entries: Vec<IndexEntry> = Vec::new();
        let mut offset: u64 = 0;

        let (carried, pruned) = merge_carry_forward(&mut writer, &mut entries, &old_snapshot, &mut offset).unwrap();
        assert_eq!(carried, 2, "recent and undated tickers must be carried forward");
        assert_eq!(pruned, 1, "only the ticker closed past the grace period must be pruned");
        writer.flush().unwrap();
        drop(writer);

        let reloaded = Snapshot::from_file(File::open(&tmp_path).unwrap()).unwrap();
        let tickers: Vec<&str> = reloaded.index.iter().map(|e| e.ticker.as_str()).collect();
        assert_eq!(tickers, vec!["recent", "undated"]);
        assert!(reloaded.find("stale").is_none(), "stale ticker closed 5h ago must be pruned");
    }
}
