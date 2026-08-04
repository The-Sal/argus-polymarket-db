use serde::Deserialize;
use std::sync::OnceLock;

const VERSIONX_JSON: &str = include_str!("../versionx.json");

#[derive(Deserialize)]
struct VersionFile {
    major_version: u32,
    minor_version: u32,
    patch_version: u32,
}

static VERSION: OnceLock<(u32, u32, u32)> = OnceLock::new();

fn version() -> (u32, u32, u32) {
    *VERSION.get_or_init(|| {
        let v: VersionFile =
            serde_json::from_str(VERSIONX_JSON).expect("versionx.json must parse at compile time");
        (v.major_version, v.minor_version, v.patch_version)
    })
}

pub(crate) fn version_tuple() -> (u32, u32, u32) {
    version()
}

pub(crate) fn version_string() -> String {
    let (major, minor, patch) = version();
    format!("{major}.{minor}.{patch}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_embedded_versionx_json() {
        let (major, _minor, _patch) = version_tuple();
        assert!(major >= 1);
        assert!(version_string().split('.').count() == 3);
    }
}
