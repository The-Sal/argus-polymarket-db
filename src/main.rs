mod api;
mod proxy;
mod server;
mod refresh;
mod version;
mod database;
mod poly_api;
mod tailnet_fns;
mod p2p_db_server;
mod mesh_sync;
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

    // The refresh interval doubles as a TTL on the on-disk snapshot: if it's
    // older than `refresh_interval_secs`, it must not be served as-is, since
    // that would mean serving arbitrarily stale data for a full interval
    // after boot (e.g. kill the process, restart 10 days later with a 16h
    // interval, and the loop below would otherwise wait another 16h before
    // even looking). Instead a stale snapshot is refreshed synchronously
    // before the server starts accepting traffic.
    //
    // Age is measured from `Snapshot::built_at_unix`, which comes from the
    // meta line `full_crawl_and_compact` writes at offset 0 of the db file
    // — not the file's mtime, which a copy/rsync/backup/restore can change
    // without the data actually being any fresher (see `database.rs`).
    let (snapshot, initial_refresh_delay): (Arc<Snapshot>, Duration) = if db_path.exists() {
        let file = File::open(&db_path).expect("failed to open existing database file");
        log::info!("Loading existing database from {}...", db_path.display());
        let loaded = Arc::new(Snapshot::from_file(file).expect("failed to load existing database file"));
        let age_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .saturating_sub(loaded.built_at_unix);

        if age_secs < refresh_interval_secs {
            log::info!(
                "Database built {} ago, within the {} TTL; serving as-is",
                format_duration(age_secs),
                format_duration(refresh_interval_secs)
            );
            (loaded, Duration::from_secs(refresh_interval_secs - age_secs))
        } else {
            log::info!(
                "Database built {} ago exceeds the {} TTL; checking tailnet peers before crawling...",
                format_duration(age_secs),
                format_duration(refresh_interval_secs)
            );
            // Mesh sync is a pure optimization: only attempted because the
            // local snapshot is already known to be stale, and any failure
            // falls straight through to the exact same crawl this branch
            // has always done.
            if let Some(pulled) = mesh_sync::try_bootstrap_from_peers(&db_path, refresh_interval_secs) {
                let age = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs().saturating_sub(pulled.built_at_unix);
                (pulled, Duration::from_secs(refresh_interval_secs.saturating_sub(age)))
            } else {
                let fresh = refresh::full_crawl_and_compact(&db_path, Some(loaded)).expect("startup refresh failed");
                (fresh, Duration::from_secs(refresh_interval_secs))
            }
        }
    } else {
        log::info!(
            "No existing database found at {}; checking tailnet peers before running initial crawl...",
            db_path.display()
        );
        if let Some(pulled) = mesh_sync::try_bootstrap_from_peers(&db_path, refresh_interval_secs) {
            let age = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs().saturating_sub(pulled.built_at_unix);
            (pulled, Duration::from_secs(refresh_interval_secs.saturating_sub(age)))
        } else {
            log::info!("No usable peer found; running initial crawl (this can take a while)...");
            let snap = refresh::full_crawl_and_compact(&db_path, None).expect("initial crawl failed");
            (snap, Duration::from_secs(refresh_interval_secs))
        }
    };

    log::info!(
        "Database ready: {} events, db_version={}, RSS={:.1}MB",
        snapshot.lines,
        version::version_string(),
        rss_mb().unwrap_or(-1.0)
    );

    let db = Arc::new(Database::new(snapshot));

    log::info!(
        "Background refresh will run every {refresh_interval_secs}s (next in {})",
        format_duration(initial_refresh_delay.as_secs())
    );
    refresh::spawn_refresh_loop(
        Arc::clone(&db),
        db_path.clone(),
        initial_refresh_delay,
        Duration::from_secs(refresh_interval_secs),
    );

    let listener = server::bind(&bind_address).expect("failed to bind Unix domain socket listener");
    log::info!("Listening on {}", bind_address.display());

    match p2p_db_server::P2pDbServer::new(Arc::clone(&db)) {
        Some(p2p_db_server) => {
            let p2p_db_server = Arc::new(p2p_db_server);
            log::info!("P2P DB Server listening on {}", p2p_db_server.port);
            std::thread::spawn(move || p2p_db_server.run_server());
        }
        None => log::warn!("P2P DB Server disabled (see preceding warning)"),
    }

    server::run(listener, db);
}
