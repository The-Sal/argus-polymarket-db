use std::fs;
use std::io::*;
use std::collections::HashMap;
use crate::poly_api::PolymarketEvent;

const DB_FILE: &str = "polymarket_events.db";


pub(crate) struct Database {
    pub(crate) mapping: HashMap<String, u32>,
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
    pub(crate) fn new(mapping: HashMap<String, u32>, file_handle: fs::File) -> Self{
        let lines = mapping.len() as u32;
        Self{mapping, file_handle, lines }
    }

    pub(crate) fn get_event(&self, ticker: &str) -> Option<PolymarketEvent> {
        let pos = self.mapping.get(ticker)?;
        let mut file_handle = self.file_handle.try_clone().unwrap();
        file_handle.seek(SeekFrom::Start(*pos as u64)).unwrap();

        let mut reader = BufReader::new(file_handle);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();

        let event: PolymarketEvent = serde_json::from_str(&line).unwrap();
        Some(event)
    }


    /// Seeks to the end of the file and writes a new event to it.
    /// incrementing the internal mapping
    pub(crate) fn add_event(&mut self, event: PolymarketEvent){
        let json_string = serde_json::to_string(&event).unwrap();
        self.file_handle.write(json_string.as_bytes()).unwrap();
        self.file_handle.write(b"\n").unwrap();
        self.mapping.insert(event.ticker, self.lines);
        self.lines += 1;
    }
}

