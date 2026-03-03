use std::fs;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

/// Create a file and its .synced marker, then backdate mtime.
fn create_synced_file(root: &std::path::Path, rel: &str, age: Duration) {
    let path = root.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::File::create(&path).unwrap();

    // Create .synced marker
    let mut marker = path.as_os_str().to_os_string();
    marker.push(".synced");
    fs::File::create(std::path::PathBuf::from(&marker)).unwrap();

    // Backdate the parquet file mtime
    let past = SystemTime::now() - age;
    let mtime = filetime::FileTime::from_system_time(past);
    filetime::set_file_mtime(&path, mtime).unwrap();
}

#[test]
fn cleanup_deletes_old_synced() {
    let dir = TempDir::new().unwrap();
    let rel = "raw/binance_spot/BTCUSDT/2026-03-01/depth_0000_0600.parquet";
    create_synced_file(dir.path(), rel, Duration::from_secs(100 * 3600)); // 100 hours old

    let parquet = dir.path().join(rel);
    let mut marker_os = parquet.as_os_str().to_os_string();
    marker_os.push(".synced");
    let marker = std::path::PathBuf::from(marker_os);

    assert!(parquet.exists());
    assert!(marker.exists());

    // Walk synced files and delete old ones (same logic as cleanup::run)
    let retention = Duration::from_secs(72 * 3600);
    for entry in walkdir::WalkDir::new(dir.path())
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_type().is_file()
                && e.path()
                    .extension()
                    .and_then(|x| x.to_str())
                    .is_some_and(|x| x == "synced")
        })
    {
        let m = entry.path();
        let pq = m.with_extension("");
        if let Ok(meta) = pq.metadata() {
            if let Ok(mtime) = meta.modified() {
                let age = SystemTime::now()
                    .duration_since(mtime)
                    .unwrap_or(Duration::ZERO);
                if age >= retention {
                    fs::remove_file(&pq).unwrap();
                    fs::remove_file(m).unwrap();
                }
            }
        }
    }

    assert!(!parquet.exists());
    assert!(!marker.exists());
}

#[test]
fn cleanup_preserves_recent() {
    let dir = TempDir::new().unwrap();
    let rel = "raw/binance_spot/BTCUSDT/2026-03-02/depth_0000_0600.parquet";
    create_synced_file(dir.path(), rel, Duration::from_secs(2 * 3600)); // 2 hours old

    let parquet = dir.path().join(rel);

    let retention = Duration::from_secs(72 * 3600);
    // Same cleanup walk
    for entry in walkdir::WalkDir::new(dir.path())
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_type().is_file()
                && e.path()
                    .extension()
                    .and_then(|x| x.to_str())
                    .is_some_and(|x| x == "synced")
        })
    {
        let m = entry.path();
        let pq = m.with_extension("");
        if let Ok(meta) = pq.metadata() {
            if let Ok(mtime) = meta.modified() {
                let age = SystemTime::now()
                    .duration_since(mtime)
                    .unwrap_or(Duration::ZERO);
                if age >= retention {
                    fs::remove_file(&pq).unwrap();
                    fs::remove_file(m).unwrap();
                }
            }
        }
    }

    // Should still exist — only 2 hours old, retention is 72h
    assert!(parquet.exists());
}
