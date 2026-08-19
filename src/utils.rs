use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
