use std::fs;
use tempfile::TempDir;

#[test]
fn state_marker_create_and_check() {
    let dir = TempDir::new().unwrap();
    let parquet = dir.path().join("test.parquet");
    fs::File::create(&parquet).unwrap();

    let marker = {
        let mut s = parquet.as_os_str().to_os_string();
        s.push(".synced");
        std::path::PathBuf::from(s)
    };

    // Not synced yet
    assert!(!marker.exists());

    // Create marker
    fs::File::create(&marker).unwrap();

    // Now detected as synced
    assert!(marker.exists());

    // Marker path convention: parquet + ".synced"
    assert_eq!(
        marker.file_name().unwrap().to_str().unwrap(),
        "test.parquet.synced"
    );
}
