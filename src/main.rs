mod proxy;
mod poly_api;
mod database;
use sysinfo::{Pid, System};
use std::collections::HashMap;
use crate::database::Database;

fn print_rss() -> Option<f64> {
    let mut sys = System::new() ;
    let pid = Pid::from_u32(std::process::id());
    sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
    if let Some(process) = sys.process(pid) {
        return Some(process.memory() as f64 / 1024.0 / 1024.0);
    }
    None
}


fn file_size(path: &str) -> u64 {
    let metadata = std::fs::metadata(path).unwrap();
    metadata.len() / 1024 / 1024
}

fn main_internal() {
    let mut iterator = poly_api::OpenEventsIter::default();
    let mut db: Database;
    if std::fs::metadata("polymarket_events.db").is_ok() {
        // File exists, proceed with existing database
        db = database::Database::from_file(std::fs::File::open("polymarket_events.db").unwrap());
    } else {
        db = database::Database::new(HashMap::new(), std::fs::File::create_new("polymarket_events.db").unwrap());
        loop{
            let events_unwrapped = iterator.next();
            if let Some(events) = events_unwrapped{
                for event in events.unwrap(){
                    db.add_event(event);
                }
            }else{
                println!("No more events");
                break;
            }
            println!("Events processed: {}", db.lines);
            let rss = print_rss().unwrap();
            let file_size = file_size("polymarket_events.db");
            println!("RSS: {} MB, File size: {} MB. Ratio rss/file_size = {}", rss, file_size, rss / file_size as f64);
        }
    }

    println!("Database size: {}", db.lines);
    loop {
        println!("Enter event ticker:");
        let mut ticker = String::new();
        std::io::stdin().read_line(&mut ticker).unwrap();
        let ticker = ticker.trim();
        if let Some(event) = db.get_event(ticker){
            println!("Event found with ticker: {}, end_date: {}", event.ticker, event.end_date.unwrap());
        }else{
            println!("Event not found");
        }
    }
}


fn main() {
    let iterations = 1; // testing knob
    for n in 0..iterations{
        if iterations > 1{
            println!("Iteration: {}", n);
        }
        main_internal();
    }

}
