use std::fs;

use fathom_sync::state;
use tempfile::TempDir;

#[test]
fn state_marker_create_and_check() {
    let dir = TempDir::new().unwrap();
    let parquet = dir.path().join("test.parquet");
    fs::File::create(&parquet).unwrap();

    assert!(!state::is_synced(&parquet));

    state::mark_synced(&parquet).unwrap();

    assert!(state::is_synced(&parquet));
    assert!(state::marker_path(&parquet).exists());
    assert_eq!(
        state::marker_path(&parquet)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap(),
        "test.parquet.synced"
    );
}
