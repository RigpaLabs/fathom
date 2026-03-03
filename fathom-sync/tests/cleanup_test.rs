use std::fs;
use std::time::{Duration, SystemTime};

use fathom_sync::{cleanup, state};
use tempfile::TempDir;

/// Create a parquet file and its .synced marker, then backdate the marker mtime.
fn create_synced_file(root: &std::path::Path, rel: &str, marker_age: Duration) {
    let path = root.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::File::create(&path).unwrap();

    // Create .synced marker via the real state module
    state::mark_synced(&path).unwrap();

    // Backdate the marker mtime (cleanup uses marker mtime, not parquet mtime)
    let marker = state::marker_path(&path);
    let past = SystemTime::now() - marker_age;
    let ft = filetime::FileTime::from_system_time(past);
    filetime::set_file_mtime(&marker, ft).unwrap();
}

#[test]
fn cleanup_deletes_old_synced() {
    let dir = TempDir::new().unwrap();
    let rel = "raw/binance_spot/BTCUSDT/2026-03-01/depth_0000_0600.parquet";
    create_synced_file(dir.path(), rel, Duration::from_secs(100 * 3600));

    let parquet = dir.path().join(rel);
    let marker = state::marker_path(&parquet);

    assert!(parquet.exists());
    assert!(marker.exists());

    let retention = Duration::from_secs(72 * 3600);
    cleanup::run(dir.path(), retention).unwrap();

    assert!(!parquet.exists());
    assert!(!marker.exists());
}

#[test]
fn cleanup_preserves_recent() {
    let dir = TempDir::new().unwrap();
    let rel = "raw/binance_spot/BTCUSDT/2026-03-02/depth_0000_0600.parquet";
    create_synced_file(dir.path(), rel, Duration::from_secs(2 * 3600));

    let parquet = dir.path().join(rel);

    let retention = Duration::from_secs(72 * 3600);
    cleanup::run(dir.path(), retention).unwrap();

    // Should still exist — marker only 2 hours old, retention is 72h
    assert!(parquet.exists());
}

#[test]
fn cleanup_no_premature_delete_after_backlog_upload() {
    // Simulate: parquet written 100h ago, but marker created just now (fresh upload).
    // Cleanup should NOT delete because marker mtime is recent.
    let dir = TempDir::new().unwrap();
    let rel = "raw/binance_spot/BTCUSDT/2026-03-01/depth_0000_0600.parquet";
    let path = dir.path().join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::File::create(&path).unwrap();

    // Backdate the parquet file (old write time)
    let old = SystemTime::now() - Duration::from_secs(100 * 3600);
    filetime::set_file_mtime(&path, filetime::FileTime::from_system_time(old)).unwrap();

    // Mark synced NOW (fresh marker)
    state::mark_synced(&path).unwrap();

    let retention = Duration::from_secs(72 * 3600);
    cleanup::run(dir.path(), retention).unwrap();

    // Parquet should still exist — marker is fresh despite old parquet mtime
    assert!(path.exists());
}
