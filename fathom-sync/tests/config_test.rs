use std::fs;

use fathom_sync::config::Config;
use tempfile::TempDir;

#[test]
fn config_parse_minimal() {
    let dir = TempDir::new().unwrap();
    let cfg_path = dir.path().join("config.toml");
    fs::write(
        &cfg_path,
        r#"
data_dir = "/app/data"

[s3]
bucket = "my-bucket"
"#,
    )
    .unwrap();

    let cfg = Config::load(&cfg_path).unwrap();
    assert_eq!(cfg.data_dir.to_str().unwrap(), "/app/data");
    assert_eq!(cfg.s3.bucket, "my-bucket");
}

#[test]
fn config_defaults() {
    let dir = TempDir::new().unwrap();
    let cfg_path = dir.path().join("config.toml");
    fs::write(
        &cfg_path,
        r#"
data_dir = "/app/data"

[s3]
bucket = "test"
"#,
    )
    .unwrap();

    let cfg = Config::load(&cfg_path).unwrap();
    assert_eq!(cfg.data_dir.to_str().unwrap(), "/app/data");
    assert_eq!(cfg.scan_interval_s, 300);
    assert_eq!(cfg.s3.bucket, "test");
    assert_eq!(cfg.s3.prefix, "");
    assert_eq!(cfg.s3.region, "auto");
    assert_eq!(cfg.s3.endpoint, "");
    assert!(cfg.cleanup.enabled);
    assert_eq!(cfg.cleanup.retention_hours, 72);
}
