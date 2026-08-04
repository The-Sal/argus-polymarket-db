use crate::version;
use serde_json::Value;
use serde::Deserialize;
use crate::database::Database;

const DEFAULT_LIST_EVENTS_LIMIT: u32 = 100;
const MAX_LIST_EVENTS_LIMIT: u32 = 500;
const DEFAULT_LIST_TICKERS_LIMIT: u32 = 5000;
const MAX_LIST_TICKERS_LIMIT: u32 = 20_000;
const DEFAULT_LIST_ASSET_IDS_LIMIT: u32 = 500;
const MAX_LIST_ASSET_IDS_LIMIT: u32 = 2_000;
const DEFAULT_PREFIX_SEARCH_LIMIT: u32 = 50;
const MAX_PREFIX_SEARCH_LIMIT: u32 = 500;

/// `{"id": <opaque, optional>, "op": "<name>", ...op-specific fields}`. The
/// op-specific fields are captured via flatten so each op can deserialize
/// its own params struct out of `data` without this envelope knowing their
/// shape.
#[derive(Deserialize)]
struct Envelope {
    #[serde(default)]
    id: Value,
    op: String,
    #[serde(flatten)]
    data: Value,
}

#[derive(Deserialize)]
struct GetEventParams {
    ticker: String,
}

#[derive(Deserialize, Default)]
struct CursorParams {
    #[serde(default)]
    after: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
}

/// Distinct from `CursorParams`: `ticker_limit` bounds how many *tickers*
/// are scanned per page, not how many `entries` come back — one ticker can
/// contribute zero or many `(asset_id, market_index)` pairs, so those two
/// counts are never the same number and conflating them under the name
/// `limit` would be misleading to a caller.
#[derive(Deserialize, Default)]
struct AssetIdCursorParams {
    #[serde(default)]
    after: Option<String>,
    #[serde(default)]
    ticker_limit: Option<u32>,
}

#[derive(Deserialize)]
struct PrefixSearchParams {
    prefix: String,
    #[serde(default)]
    limit: Option<u32>,
}

/// Reads one NDJSON request line and returns one NDJSON response line
/// (including the trailing behavior of never panicking on malformed input —
/// a bad line always yields an `ok:false` response, never a dropped
/// connection). This is the sole entry point `server.rs` calls; it owns all
/// request/response shape decisions so the socket-framing code doesn't have
/// to know about them.
pub(crate) fn handle_line(db: &Database, line: &str) -> String {
    let response = match serde_json::from_str::<Envelope>(line) {
        Ok(env) => dispatch(db, env),
        Err(e) => err_response(Value::Null, None, "bad_request", format!("invalid request: {e}")),
    };
    serde_json::to_string(&response).unwrap_or_else(|_| {
        "{\"ok\":false,\"op\":null,\"error\":{\"code\":\"internal\",\"message\":\"failed to serialize response\"}}"
            .to_string()
    })
}

fn dispatch(db: &Database, env: Envelope) -> Value {
    let Envelope { id, op, data } = env;
    match op.as_str() {
        "get_event" => handle_get_event(db, id, data),
        "list_events" => handle_list_events(db, id, data),
        "list_tickers" => handle_list_tickers(db, id, data),
        "list_asset_ids" => handle_list_asset_ids(db, id, data),
        "prefix_search" => handle_prefix_search(db, id, data),
        "db_info" => handle_db_info(db, id),
        other => err_response(id, Some(other), "bad_request", format!("unknown op '{other}'")),
    }
}

fn ok_response(id: Value, op: &str, result: Value) -> Value {
    serde_json::json!({
        "id": id,
        "op": op,
        "ok": true,
        "db_version": version::version_string(),
        "result": result,
    })
}

fn err_response(id: Value, op: Option<&str>, code: &str, message: impl Into<String>) -> Value {
    serde_json::json!({
        "id": id,
        "op": op,
        "ok": false,
        "db_version": version::version_string(),
        "error": { "code": code, "message": message.into() },
    })
}

fn parse_params<T: for<'de> Deserialize<'de>>(data: Value) -> Result<T, String> {
    serde_json::from_value(data).map_err(|e| format!("invalid params: {e}"))
}

/// `limit == 0` is rejected rather than silently treated as "use the
/// default" — a caller that explicitly asked for zero results almost
/// certainly made a mistake, and silently substituting a default would hide
/// it.
fn resolve_limit(requested: Option<u32>, default: u32, max: u32) -> Result<u32, String> {
    match requested {
        Some(0) => Err("limit must be greater than 0".to_string()),
        Some(n) => Ok(n.min(max)),
        None => Ok(default),
    }
}

fn handle_get_event(db: &Database, id: Value, data: Value) -> Value {
    let params: GetEventParams = match parse_params(data) {
        Ok(p) => p,
        Err(e) => return err_response(id, Some("get_event"), "bad_request", e),
    };
    let ticker = params.ticker.trim();
    if ticker.is_empty() {
        return err_response(id, Some("get_event"), "bad_request", "ticker must not be empty");
    }

    let snap = db.snapshot();
    let event = match snap.find(ticker) {
        Some(entry) => match snap.read_value(entry) {
            Ok(v) => Some(v),
            Err(e) => {
                return err_response(
                    id,
                    Some("get_event"),
                    "internal",
                    format!("failed to read record '{ticker}': {e}"),
                )
            }
        },
        None => None,
    };
    ok_response(id, "get_event", serde_json::json!({ "event": event }))
}

fn handle_list_events(db: &Database, id: Value, data: Value) -> Value {
    let params: CursorParams = match parse_params(data) {
        Ok(p) => p,
        Err(e) => return err_response(id, Some("list_events"), "bad_request", e),
    };
    let limit = match resolve_limit(params.limit, DEFAULT_LIST_EVENTS_LIMIT, MAX_LIST_EVENTS_LIMIT) {
        Ok(n) => n,
        Err(e) => return err_response(id, Some("list_events"), "bad_request", e),
    };

    let snap = db.snapshot();
    let (page, next_after) = snap.cursor_range(params.after.as_deref(), limit as usize);
    let mut events = Vec::with_capacity(page.len());
    for entry in page {
        match snap.read_value(entry) {
            Ok(v) => events.push(v),
            Err(e) => {
                return err_response(
                    id,
                    Some("list_events"),
                    "internal",
                    format!("failed to read record '{}': {e}", entry.ticker),
                )
            }
        }
    }
    ok_response(
        id,
        "list_events",
        serde_json::json!({ "events": events, "next_after": next_after }),
    )
}

fn handle_list_tickers(db: &Database, id: Value, data: Value) -> Value {
    let params: CursorParams = match parse_params(data) {
        Ok(p) => p,
        Err(e) => return err_response(id, Some("list_tickers"), "bad_request", e),
    };
    let limit = match resolve_limit(params.limit, DEFAULT_LIST_TICKERS_LIMIT, MAX_LIST_TICKERS_LIMIT) {
        Ok(n) => n,
        Err(e) => return err_response(id, Some("list_tickers"), "bad_request", e),
    };

    let snap = db.snapshot();
    let (page, next_after) = snap.cursor_range(params.after.as_deref(), limit as usize);
    let tickers: Vec<&str> = page.iter().map(|e| e.ticker.as_str()).collect();
    ok_response(
        id,
        "list_tickers",
        serde_json::json!({ "tickers": tickers, "next_after": next_after }),
    )
}

/// Gamma's `markets[].clobTokenIds` arrives as a JSON string containing a
/// JSON-encoded array (not a native array) — Argus's own `Market.from_dict`
/// parses it the same way. Handled defensively for a native array too, in
/// case that ever changes upstream.
fn extract_asset_ids(markets: &Value) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    let Some(arr) = markets.as_array() else {
        return out;
    };
    for (market_index, market) in arr.iter().enumerate() {
        let ids: Vec<String> = match market.get("clobTokenIds") {
            Some(Value::String(s)) => serde_json::from_str::<Vec<String>>(s).unwrap_or_default(),
            Some(Value::Array(a)) => a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect(),
            _ => Vec::new(),
        };
        for asset_id in ids {
            out.push((market_index, asset_id));
        }
    }
    out
}

fn handle_list_asset_ids(db: &Database, id: Value, data: Value) -> Value {
    let params: AssetIdCursorParams = match parse_params(data) {
        Ok(p) => p,
        Err(e) => return err_response(id, Some("list_asset_ids"), "bad_request", e),
    };
    let limit = match resolve_limit(params.ticker_limit, DEFAULT_LIST_ASSET_IDS_LIMIT, MAX_LIST_ASSET_IDS_LIMIT) {
        Ok(n) => n,
        Err(e) => return err_response(id, Some("list_asset_ids"), "bad_request", e),
    };

    let snap = db.snapshot();
    let (page, next_after) = snap.cursor_range(params.after.as_deref(), limit as usize);
    let mut entries = Vec::new();
    for entry in page {
        let value = match snap.read_value(entry) {
            Ok(v) => v,
            Err(e) => {
                return err_response(
                    id,
                    Some("list_asset_ids"),
                    "internal",
                    format!("failed to read record '{}': {e}", entry.ticker),
                )
            }
        };
        let markets = value.get("markets").cloned().unwrap_or(Value::Null);
        for (market_index, asset_id) in extract_asset_ids(&markets) {
            entries.push(serde_json::json!({
                "asset_id": asset_id,
                "ticker": entry.ticker,
                "market_index": market_index,
            }));
        }
    }
    ok_response(
        id,
        "list_asset_ids",
        serde_json::json!({ "entries": entries, "next_after": next_after }),
    )
}

fn handle_prefix_search(db: &Database, id: Value, data: Value) -> Value {
    let params: PrefixSearchParams = match parse_params(data) {
        Ok(p) => p,
        Err(e) => return err_response(id, Some("prefix_search"), "bad_request", e),
    };
    if params.prefix.is_empty() {
        return err_response(id, Some("prefix_search"), "bad_request", "prefix must not be empty");
    }
    let limit = match resolve_limit(params.limit, DEFAULT_PREFIX_SEARCH_LIMIT, MAX_PREFIX_SEARCH_LIMIT) {
        Ok(n) => n,
        Err(e) => return err_response(id, Some("prefix_search"), "bad_request", e),
    };

    let snap = db.snapshot();
    let matches = snap.prefix_range(&params.prefix);
    let truncated = matches.len() > limit as usize;
    let tickers: Vec<&str> = matches.iter().take(limit as usize).map(|e| e.ticker.as_str()).collect();
    ok_response(
        id,
        "prefix_search",
        serde_json::json!({ "tickers": tickers, "truncated": truncated }),
    )
}

fn handle_db_info(db: &Database, id: Value) -> Value {
    let snap = db.snapshot();
    let (major, minor, patch) = version::version_tuple();
    ok_response(
        id,
        "db_info",
        serde_json::json!({
            "major": major,
            "minor": minor,
            "patch": patch,
            "version": version::version_string(),
            "lines": snap.lines,
            "built_at_unix": snap.built_at_unix,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Snapshot;
    use std::fs::File;
    use std::io::Write;
    use std::sync::Arc;

    struct ScratchFile(std::path::PathBuf);
    impl Drop for ScratchFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn test_db() -> (ScratchFile, Database) {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "apdb_api_test_{}_{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut f = File::create(&path).unwrap();
        let records = [
            serde_json::json!({
                "ticker": "btc-updown-1",
                "markets": [{"slug": "m1", "clobTokenIds": "[\"111\",\"222\"]"}],
            }),
            serde_json::json!({ "ticker": "eth-updown-1", "markets": [] }),
        ];
        for record in &records {
            f.write_all(serde_json::to_string(record).unwrap().as_bytes()).unwrap();
            f.write_all(b"\n").unwrap();
        }
        f.flush().unwrap();
        let file = File::open(&path).unwrap();
        let snap = Snapshot::from_file(file).unwrap();
        let db = Database::new(Arc::new(snap));
        (ScratchFile(path), db)
    }

    #[test]
    fn get_event_hit_and_miss() {
        let (_guard, db) = test_db();
        let hit = handle_line(&db, r#"{"op":"get_event","ticker":"btc-updown-1"}"#);
        let v: Value = serde_json::from_str(&hit).unwrap();
        assert_eq!(v["ok"], true);
        assert_eq!(v["result"]["event"]["ticker"], "btc-updown-1");

        let miss = handle_line(&db, r#"{"op":"get_event","ticker":"nope"}"#);
        let v: Value = serde_json::from_str(&miss).unwrap();
        assert_eq!(v["ok"], true);
        assert_eq!(v["result"]["event"], Value::Null);
    }

    #[test]
    fn unknown_op_and_bad_json_are_errors_not_crashes() {
        let (_guard, db) = test_db();
        let bad_op = handle_line(&db, r#"{"op":"nonsense"}"#);
        let v: Value = serde_json::from_str(&bad_op).unwrap();
        assert_eq!(v["ok"], false);
        assert_eq!(v["error"]["code"], "bad_request");

        let bad_json = handle_line(&db, "not json");
        let v: Value = serde_json::from_str(&bad_json).unwrap();
        assert_eq!(v["ok"], false);
    }

    #[test]
    fn list_tickers_pagination() {
        let (_guard, db) = test_db();
        let resp = handle_line(&db, r#"{"op":"list_tickers","limit":1}"#);
        let v: Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(v["result"]["tickers"].as_array().unwrap().len(), 1);
        assert!(v["result"]["next_after"].is_string());
    }

    #[test]
    fn list_asset_ids_extracts_stringified_clob_ids() {
        let (_guard, db) = test_db();
        let resp = handle_line(&db, r#"{"op":"list_asset_ids"}"#);
        let v: Value = serde_json::from_str(&resp).unwrap();
        let entries = v["result"]["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0]["ticker"], "btc-updown-1");
        assert_eq!(entries[0]["market_index"], 0);
    }

    #[test]
    fn list_asset_ids_ticker_limit_bounds_tickers_scanned_not_entries_returned() {
        let (_guard, db) = test_db();
        // btc-updown-1 alone contributes 2 entries; ticker_limit=1 should
        // scan only that one ticker (not stop at 1 entry).
        let resp = handle_line(&db, r#"{"op":"list_asset_ids","ticker_limit":1}"#);
        let v: Value = serde_json::from_str(&resp).unwrap();
        let entries = v["result"]["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
        assert!(v["result"]["next_after"].is_string());
    }

    #[test]
    fn limit_zero_is_bad_request() {
        let (_guard, db) = test_db();
        let resp = handle_line(&db, r#"{"op":"list_tickers","limit":0}"#);
        let v: Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(v["ok"], false);
        assert_eq!(v["error"]["code"], "bad_request");
    }

    #[test]
    fn db_info_reports_version_and_line_count() {
        let (_guard, db) = test_db();
        let resp = handle_line(&db, r#"{"op":"db_info"}"#);
        let v: Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(v["result"]["lines"], 2);
        assert!(v["result"]["version"].as_str().unwrap().split('.').count() == 3);
    }
}
