mod api;
mod proxy;
mod server;
mod refresh;
mod version;
mod database;
mod poly_api;
mod tailnet_sync;

use std::fs::File;
use std::sync::Arc;
use std::path::PathBuf;
use shellexpand::tilde;
use database::{Database, Snapshot};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_DB_PATH: &str = "~/.argus/polymarket_events.db";
const DEFAULT_BIND_ADDRESS: &str = "/tmp/argus_polymarket_db.sock";
const DEFAULT_REFRESH_INTERVAL_SECS: u64 = 300;

struct StderrLogger;

/// Time-of-day only (no date/timezone math needed) — enough to correlate
/// "when did the refresh start" against a clock without pulling in a date
/// dependency for a stderr backend this minimal.
fn now_hms_utc() -> String {
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let rem = secs % 86400;
    format!("{:02}:{:02}:{:02}", rem / 3600, (rem % 3600) / 60, rem % 60)
}

impl log::Log for StderrLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::Level::Info
    }
    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            eprintln!("{} [{}] {}", now_hms_utc(), record.level(), record.args());
        }
    }
    fn flush(&self) {}
}

static LOGGER: StderrLogger = StderrLogger;

fn init_logging() {
    // Every background component (server.rs, refresh.rs) logs via the `log`
    // facade; without a backend registered here, none of it would ever be
    // seen, which matters a lot more for a long-running service than it did
    // for the old interactive prototype. `log` is already a dependency —
    // this is a minimal stderr backend, not a new one.
    let _ = log::set_logger(&LOGGER).map(|()| log::set_max_level(log::LevelFilter::Info));
}

fn rss_mb() -> Option<f64> {
    let mut sys = sysinfo::System::new();
    let pid = sysinfo::Pid::from_u32(std::process::id());
    sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
    sys.process(pid).map(|p| p.memory() as f64 / 1024.0 / 1024.0)
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn format_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m{}s", secs / 60, secs % 60)
    } else if secs < 86400 {
        format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
    } else {
        format!("{}d{}h", secs / 86400, (secs % 86400) / 3600)
    }
}


fn check_and_print_version() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.contains(&"--version".to_string()) {
        println!("Argus Polymarket Database v{}", version::version_string());
        std::process::exit(0);
    }
}

fn main() {
    check_and_print_version();
    init_logging();
    _ = dotenvy::from_filename(".env");

    let default_db_path = format!("{}", tilde(DEFAULT_DB_PATH));
    let db_path = PathBuf::from(env_or("APDB_DB_PATH", &default_db_path));
    let bind_address = PathBuf::from(env_or("APDB_BIND_ADDRESS", DEFAULT_BIND_ADDRESS));
    let refresh_interval_secs: u64 = env_or("POLYMARKET_FULL_MARKET_CACHE_REFRESH_INTERVAL", &DEFAULT_REFRESH_INTERVAL_SECS.to_string())
        .parse()
        .unwrap_or(DEFAULT_REFRESH_INTERVAL_SECS);

    let snapshot: Arc<Snapshot> = if db_path.exists() {
        let file = File::open(&db_path).expect("failed to open existing database file");
        if let Ok(meta) = file.metadata() {
            let age_secs = meta
                .modified()
                .ok()
                .and_then(|m| SystemTime::now().duration_since(m).ok())
                .map(|d| d.as_secs());
            let age_str = age_secs.map(format_duration).unwrap_or_else(|| "unknown".to_string());
            log::info!(
                "Loading existing database from {} ({:.1}MB, last written {age_str} ago)...",
                db_path.display(),
                meta.len() as f64 / 1024.0 / 1024.0
            );
        } else {
            log::info!("Loading existing database from {}...", db_path.display());
        }
        Arc::new(Snapshot::from_file(file).expect("failed to load existing database file"))
    } else {
        log::info!(
            "No existing database found at {}; running initial crawl (this can take a while)...",
            db_path.display()
        );
        refresh::full_crawl_and_compact(&db_path, None).expect("initial crawl failed")
    };

    log::info!(
        "Database ready: {} events, db_version={}, RSS={:.1}MB",
        snapshot.lines,
        version::version_string(),
        rss_mb().unwrap_or(-1.0)
    );

    let db = Arc::new(Database::new(snapshot));

    log::info!("Background refresh will run every {refresh_interval_secs}s");
    refresh::spawn_refresh_loop(
        Arc::clone(&db),
        db_path.clone(),
        Duration::from_secs(refresh_interval_secs),
    );

    let listener = server::bind(&bind_address).expect("failed to bind Unix domain socket listener");
    log::info!("Listening on {}", bind_address.display());

    server::run(listener, db);
}
