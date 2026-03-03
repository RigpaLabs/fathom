use std::fs;

use fathom_sync::scanner;
use tempfile::TempDir;

fn touch(root: &std::path::Path, rel: &str) {
    let path = root.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::File::create(&path).unwrap();
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

    let results = scanner::scan_with_today(dir.path(), "2026-03-03");
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

    let results = scanner::scan_with_today(dir.path(), "2026-03-03");
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

    let results = scanner::scan_with_today(dir.path(), "2026-03-03");
    assert_eq!(results.len(), 2);
}

#[test]
fn scanner_finds_yesterday_1s() {
    let dir = TempDir::new().unwrap();
    touch(dir.path(), "1s/binance_perp/BTCUSDT/2026-03-01.parquet");
    touch(dir.path(), "1s/binance_perp/BTCUSDT/2026-03-02.parquet");

    let results = scanner::scan_with_today(dir.path(), "2026-03-03");
    assert_eq!(results.len(), 2);
}

#[test]
fn scanner_skips_already_synced() {
    let dir = TempDir::new().unwrap();
    let parquet = "raw/binance_spot/BTCUSDT/2026-03-02/depth_0000_0600.parquet";
    touch(dir.path(), parquet);
    touch(dir.path(), &format!("{parquet}.synced"));

    let results = scanner::scan_with_today(dir.path(), "2026-03-03");
    assert_eq!(results.len(), 0);
}

// Versioned DATA_DIR: fathom blue-green deploy writes to data_dir/v20260301-abc/raw/...
#[test]
fn scanner_skips_open_files_versioned_datadir() {
    let dir = TempDir::new().unwrap();
    touch(
        dir.path(),
        "v20260301-abc/raw/binance_spot/BTCUSDT/2026-03-02/depth_0000_open.parquet",
    );
    touch(
        dir.path(),
        "v20260301-abc/raw/binance_spot/BTCUSDT/2026-03-02/depth_0000_0600.parquet",
    );

    let results = scanner::scan_with_today(dir.path(), "2026-03-03");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .rel_path
            .to_string_lossy()
            .contains("depth_0000_0600")
    );
}

#[test]
fn scanner_skips_today_1s_versioned_datadir() {
    let dir = TempDir::new().unwrap();
    touch(
        dir.path(),
        "v20260301-abc/1s/binance_spot/BTCUSDT/2026-03-03.parquet",
    );
    touch(
        dir.path(),
        "v20260301-abc/1s/binance_spot/BTCUSDT/2026-03-02.parquet",
    );

    let results = scanner::scan_with_today(dir.path(), "2026-03-03");
    assert_eq!(results.len(), 1);
    assert!(results[0].rel_path.to_string_lossy().contains("2026-03-02"));
}
