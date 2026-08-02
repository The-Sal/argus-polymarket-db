// Rust port of the Gamma events crawl from `argus/polymarket_direct/rest.py`
// (`fetch_events`, `iter_open_events`) and `argus/polymarket/__init__.py`
// (`fetch_all_markets_cached`).

use std::fmt;
use ureq::Agent;
use crate::proxy;
use std::error::Error;
use serde_json::Value;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

const EVENTS_ENDPOINT: &str = "https://gamma-api.polymarket.com/events";

#[derive(Debug)]
pub struct PolyMarketError(String);

impl fmt::Display for PolyMarketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for PolyMarketError {}

// Mirrors `pm_types.PolymarketEvent`. Only the fields this module reads
// (`ticker`, `endDate`) are named; everything else round-trips through
// `extra` so un-modelled fields aren't dropped.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PolymarketEvent {
    pub ticker: String,
    #[serde(rename = "endDate")]
    pub end_date: Option<String>,
    #[serde(flatten)]
    pub extra: Value,
}

pub struct FetchEventsParams<'a> {
    pub offset: u32,
    pub limit: u32,
    pub order: &'a str,
    pub ascending: bool,
    pub closed: bool,
    pub end_date_min: Option<&'a str>,
}

impl Default for FetchEventsParams<'_> {
    fn default() -> Self {
        Self {
            offset: 0,
            limit: 100,
            order: "id",
            ascending: false,
            closed: false,
            end_date_min: None,
        }
    }
}

/// Fetch a single page of events from Gamma.
///
/// Gamma's offset pagination is hard-capped at offset 2100. Beyond it the
/// endpoint returns a validation-error *object*
/// (`{"type": "validation error", "error": "offset too large, use
/// /events/keyset for deeper pagination"}`) instead of an array. That's
/// surfaced as an `Err` here rather than silently returning an empty page,
/// so a truncated crawl can never be mistaken for "no more data" (the bug
/// that used to leave ~80% of open markets out of the cache). Use
/// `iter_open_events` to crawl the full set past that cap.
pub fn fetch_events(
    params: &FetchEventsParams,
) -> Result<Vec<PolymarketEvent>, PolyMarketError> {
    let agent: Agent = proxy::get_proxy_agent();

    let mut request = agent
        .get(EVENTS_ENDPOINT)
        .query("order", params.order)
        .query("ascending", &params.ascending.to_string())
        .query("closed", &params.closed.to_string())
        .query("limit", &params.limit.to_string())
        .query("offset", &params.offset.to_string());

    if let Some(end_date_min) = params.end_date_min {
        request = request.query("end_date_min", end_date_min);
    }

    let mut response = request
        .call()
        .map_err(|e| PolyMarketError(format!("Gamma /events request failed: {e}")))?;

    let payload: Value = response
        .body_mut()
        .read_json()
        .map_err(|e| PolyMarketError(format!("Gamma /events returned invalid JSON: {e}")))?;

    let items = payload.as_array().ok_or_else(|| {
        PolyMarketError(format!(
            "Gamma /events returned a non-list response (offset={}, limit={}): {payload:?}",
            params.offset, params.limit
        ))
    })?;

    let mut events = Vec::with_capacity(items.len());
    for item in items {
        match serde_json::from_value::<PolymarketEvent>(item.clone()) {
            Ok(event) => events.push(event),
            Err(e) => log::warn!("[poly_api] Error parsing event: {e}"),
        }
    }

    Ok(events)
}

/// Lazily yields successive pages covering ALL open (`closed=false`) events,
/// working around Gamma's offset>2100 hard cap.
///
/// Strategy: page events ordered by `endDate` ascending. Within one pass,
/// offset-paginate until approaching the 2100 cap (`cursor_cap`), then
/// advance an `end_date_min` cursor to the last `endDate` seen and restart
/// the offset at 0. The crawl ends when a fetch returns an empty page (no
/// events remain at/after the cursor). Events sitting exactly on the cursor
/// boundary are re-yielded on the next pass and MUST be de-duplicated by the
/// caller (callers key events by ticker, so this is harmless).
///
/// Caveat: events with a null `endDate` do not sort under `order=endDate`
/// and will not be yielded. All tradeable open markets carry an `endDate`,
/// so this is acceptable; the alternative (an id-desc offset crawl) used to
/// silently drop ~80% of markets instead.
pub struct OpenEventsIter {
    page_limit: u32,
    cursor_cap: u32,
    max_iterations: u32,
    boundary: Option<String>,
    offset: u32,
    pass_count: u32,
    finished: bool,
}

impl OpenEventsIter {
    pub fn new(page_limit: u32, cursor_cap: u32, max_iterations: u32) -> Self {
        Self {
            page_limit,
            cursor_cap,
            max_iterations,
            boundary: None,
            offset: 0,
            pass_count: 0,
            finished: false,
        }
    }
}

impl Default for OpenEventsIter {
    fn default() -> Self {
        // page_limit=100, cursor_cap=2000, max_iterations=50 — same defaults
        // as the Python `iter_open_events`.
        Self::new(100, 2000, 50)
    }
}

impl Iterator for OpenEventsIter {
    type Item = Result<Vec<PolymarketEvent>, PolyMarketError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        if self.pass_count >= self.max_iterations {
            log::warn!(
                "[poly_api] iter_open_events hit max_iterations={}; crawl may be incomplete.",
                self.max_iterations
            );
            self.finished = true;
            return None;
        }

        let params = FetchEventsParams {
            offset: self.offset,
            limit: self.page_limit,
            order: "endDate",
            ascending: true,
            closed: false,
            end_date_min: self.boundary.as_deref(),
            ..Default::default()
        };

        let page = match fetch_events(&params) {
            Ok(page) => page,
            Err(e) => {
                self.finished = true;
                return Some(Err(e));
            }
        };

        if page.is_empty() {
            // No events remain at/after the cursor: crawl complete.
            self.finished = true;
            return None;
        }

        self.offset += page.len() as u32;
        let last_end_date = page.last().and_then(|e| e.end_date.clone());

        if self.offset >= self.cursor_cap {
            match &last_end_date {
                Some(end_date) if Some(end_date.as_str()) != self.boundary.as_deref() => {
                    self.boundary = Some(end_date.clone());
                    self.offset = 0;
                    self.pass_count += 1;
                }
                _ => {
                    // Cannot advance the cursor (e.g. > cursor_cap events
                    // share one endDate). Stop rather than spin forever.
                    log::warn!(
                        "[poly_api] iter_open_events could not advance past endDate {:?}; \
                         stopping with a partial crawl.",
                        self.boundary
                    );
                    self.finished = true;
                }
            }
        }

        Some(Ok(page))
    }
}

/// Drives `OpenEventsIter` to rebuild the full open-markets cache, keyed by
/// ticker. Mirrors the nested `fetch_all_markets_cached` closure in
/// `argus/polymarket/__init__.py`: cache merging into a shared store,
/// memory pruning, and the `_max_seen_markets` progress bound all live in
/// the caller, same as they did in the Python method this closure was
/// nested inside.
pub fn fetch_all_markets_cached() -> HashMap<String, PolymarketEvent> {
    let mut cache: HashMap<String, PolymarketEvent> = HashMap::new();
    let mut fetched = 0usize;

    for page in OpenEventsIter::default() {
        match page {
            Ok(page) => {
                fetched += page.len();
                for event in page {
                    cache.insert(event.ticker.clone(), event);
                }
                log::debug!(
                    "[poly_api] Refreshing Polymarket markets cache: {fetched} fetched, {} unique",
                    cache.len()
                );
            }
            Err(e) => {
                log::error!("[poly_api] Error refreshing all markets cache: {e}");
                break;
            }
        }
    }

    log::info!(
        "[poly_api] Refreshed all markets cache with {} markets.",
        cache.len()
    );

    cache
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn open_events_iter_test() {
        let iter = OpenEventsIter::default();
        let mut count = 0;
        for page in iter {
            count += 1;
            // do not print the page, just count it
            assert!(page.is_ok());
            println!("Total pages: {}", count);
            if count > 5 {
                break;
            }
        }
    }
}
