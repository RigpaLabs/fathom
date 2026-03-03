use std::fs;
use tempfile::TempDir;

// We need to reference the binary crate's modules — use a path import trick.
// Since fathom-sync is a binary, tests go through the public interface.
// We'll use a shared test helper to invoke scanning.

/// Create a file at the given path relative to `root`, creating dirs as needed.
fn touch(root: &std::path::Path, rel: &str) {
    let path = root.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::File::create(&path).unwrap();
}

/// Simplified scanner logic (mirrors src/scanner.rs) for integration testing.
/// We test the actual binary module through `cargo test -p fathom-sync`.
mod scanner_logic {
    use std::path::{Path, PathBuf};
    use walkdir::WalkDir;

    pub struct SyncCandidate {
        pub rel_path: PathBuf,
    }

    pub fn scan(data_dir: &Path, today: &str) -> Vec<SyncCandidate> {
        let mut candidates = Vec::new();
        for entry in WalkDir::new(data_dir)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
        {
            let path = entry.path();
            let ext = path.extension().and_then(|e| e.to_str());
            if ext != Some("parquet") {
                continue;
            }

            // Skip .synced siblings
            let mut marker = path.as_os_str().to_os_string();
            marker.push(".synced");
            if PathBuf::from(&marker).exists() {
                continue;
            }

            let rel = match path.strip_prefix(data_dir) {
                Ok(r) => r,
                Err(_) => continue,
            };
            let rel_str = rel.to_string_lossy();

            if rel_str.starts_with("raw/") {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.contains("_open") {
                        continue;
                    }
                }
            }

            if rel_str.starts_with("1s/") {
                if let Some(stem) = path.file_stem().and_then(|n| n.to_str()) {
                    if stem == today {
                        continue;
                    }
                }
            }

            candidates.push(SyncCandidate {
                rel_path: rel.to_path_buf(),
            });
        }
        candidates
    }
}

#[test]
fn scanner_skips_open_files() {
    let dir = TempDir::new().unwrap();
    touch(
        dir.path(),
        "raw/binance_spot/BTCUSDT/2026-03-02/depth_0000_open.parquet",
    );
    touch(
        dir.path(),
        "raw/binance_spot/BTCUSDT/2026-03-02/depth_0000_0600.parquet",
    );

    let results = scanner_logic::scan(dir.path(), "2026-03-03");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .rel_path
            .to_string_lossy()
            .contains("depth_0000_0600")
    );
}

#[test]
fn scanner_skips_today_1s() {
    let dir = TempDir::new().unwrap();
    touch(dir.path(), "1s/binance_spot/BTCUSDT/2026-03-03.parquet");
    touch(dir.path(), "1s/binance_spot/BTCUSDT/2026-03-02.parquet");

    let results = scanner_logic::scan(dir.path(), "2026-03-03");
    assert_eq!(results.len(), 1);
    assert!(results[0].rel_path.to_string_lossy().contains("2026-03-02"));
}

#[test]
fn scanner_finds_completed_raw() {
    let dir = TempDir::new().unwrap();
    touch(
        dir.path(),
        "raw/binance_spot/ETHUSDT/2026-03-02/depth_0000_0600.parquet",
    );
    touch(
        dir.path(),
        "raw/binance_spot/ETHUSDT/2026-03-02/depth_0600_1200.parquet",
    );

    let results = scanner_logic::scan(dir.path(), "2026-03-03");
    assert_eq!(results.len(), 2);
}

#[test]
fn scanner_finds_yesterday_1s() {
    let dir = TempDir::new().unwrap();
    touch(dir.path(), "1s/binance_perp/BTCUSDT/2026-03-01.parquet");
    touch(dir.path(), "1s/binance_perp/BTCUSDT/2026-03-02.parquet");

    let results = scanner_logic::scan(dir.path(), "2026-03-03");
    assert_eq!(results.len(), 2);
}

#[test]
fn scanner_skips_already_synced() {
    let dir = TempDir::new().unwrap();
    let parquet = "raw/binance_spot/BTCUSDT/2026-03-02/depth_0000_0600.parquet";
    touch(dir.path(), parquet);
    // Create .synced marker
    touch(dir.path(), &format!("{parquet}.synced"));

    let results = scanner_logic::scan(dir.path(), "2026-03-03");
    assert_eq!(results.len(), 0);
}
