use std::fs;
use std::io::*;
use std::fs::File;
use std::collections::HashMap;
use crate::poly_api::PolymarketEvent;

const DB_FILE: &str = "polymarket_events.db";


pub(crate) struct Database {
    pub(crate) mapping: HashMap<String, u64>,
    pub(crate) file_handle: fs::File,
    pub(crate) lines: u32,
}

impl Default for Database {
    fn default() -> Self {
        Self {
            mapping: HashMap::new(),
            file_handle: fs::File::open(DB_FILE).unwrap(),
            lines: 0,
        }
    }
}

impl Database{
    pub(crate) fn new(mapping: HashMap<String, u64>, file_handle: File) -> Self{
        let lines = mapping.len() as u32;
        Self{mapping, file_handle, lines }
    }

    pub(crate) fn from_file(file_handle: File) -> Self{
        let mut mapping = HashMap::new();
        let mut lines = 0;
        let mut reader = BufReader::new(file_handle.try_clone().unwrap());
        let mut offset: u64 = 0;
        let mut line = String::new();
        loop {
            line.clear();
            let bytes_read = reader.read_line(&mut line).unwrap();
            if bytes_read == 0 {
                break;
            }
            let event: PolymarketEvent = serde_json::from_str(line.trim_end()).unwrap();
            mapping.insert(event.ticker, offset);
            offset += bytes_read as u64;
            lines += 1;
        }

        Self{mapping, file_handle, lines}
    }

    /// Seeks straight to the record's byte offset and reads exactly one
    /// line, instead of scanning from the start of the file counting
    /// newlines. Cost is proportional to the record's own length, not to
    /// its position in the file.
    pub(crate) fn get_event(&self, ticker: &str) -> Option<PolymarketEvent> {
        let offset = *self.mapping.get(ticker)?;
        let mut file_handle = self.file_handle.try_clone().unwrap();
        file_handle.seek(SeekFrom::Start(offset)).unwrap();

        let mut reader = BufReader::new(file_handle);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();

        let event: PolymarketEvent = serde_json::from_str(line.trim_end()).unwrap();
        Some(event)
    }


    /// Seeks to the end of the file and writes a new event to it,
    /// recording the byte offset the record was written at (not just a
    /// line index) so `get_event` can seek straight to it later.
    pub(crate) fn add_event(&mut self, event: PolymarketEvent){
        let offset = self.file_handle.seek(SeekFrom::End(0)).unwrap();
        let json_string = serde_json::to_string(&event).unwrap();
        self.file_handle.write(json_string.as_bytes()).unwrap();
        self.file_handle.write(b"\n").unwrap();
        self.mapping.insert(event.ticker, offset);
        self.lines += 1;
    }
}

#[cfg(test)]
mod perf_tests {
    use super::*;
    use std::time::Instant;

    // Run explicitly against the real on-disk DB (not part of the default
    // test run): cargo test --release -- --ignored --nocapture query_perf
    #[test]
    #[ignore]
    fn query_perf() {
        let load_start = Instant::now();
        let db = Database::from_file(fs::File::open(DB_FILE).unwrap());
        println!(
            "load: {} events in {:?}",
            db.lines,
            load_start.elapsed()
        );

        let mut by_pos: Vec<(u64, String)> =
            db.mapping.iter().map(|(t, p)| (*p, t.clone())).collect();
        by_pos.sort_by_key(|(p, _)| *p);
        let n = by_pos.len();

        for frac in [0.0, 0.25, 0.5, 0.75, 0.99] {
            let idx = ((n - 1) as f64 * frac) as usize;
            let (pos, ticker) = &by_pos[idx];
            let start = Instant::now();
            let found = db.get_event(ticker).is_some();
            println!(
                "pos={pos:>6}/{n} ticker={ticker:<50} elapsed={:?} found={found}",
                start.elapsed()
            );
        }

        // Repeat the last-record lookup a few times to show whether OS page
        // cache masks the cost of repeated worst-case queries.
        let (pos, ticker) = &by_pos[n - 1];
        for i in 0..3 {
            let start = Instant::now();
            let found = db.get_event(ticker).is_some();
            println!(
                "repeat {i}: pos={pos} ticker={ticker} elapsed={:?} found={found}",
                start.elapsed()
            );
        }
    }
}

