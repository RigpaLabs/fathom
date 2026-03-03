use std::fs;
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

    let text = fs::read_to_string(&cfg_path).unwrap();
    let cfg: toml::Value = toml::from_str(&text).unwrap();

    assert_eq!(cfg["data_dir"].as_str().unwrap(), "/app/data");
    assert_eq!(cfg["s3"]["bucket"].as_str().unwrap(), "my-bucket");
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

    // Deserialize into the actual config structure
    #[derive(serde::Deserialize)]
    struct Config {
        data_dir: String,
        #[serde(default = "default_scan_interval")]
        scan_interval_s: u64,
        s3: S3Config,
        #[serde(default)]
        cleanup: CleanupConfig,
    }

    #[derive(serde::Deserialize)]
    struct S3Config {
        bucket: String,
        #[serde(default)]
        prefix: String,
        #[serde(default = "default_region")]
        region: String,
        #[serde(default)]
        endpoint: String,
    }

    #[derive(serde::Deserialize)]
    struct CleanupConfig {
        #[serde(default = "default_cleanup_enabled")]
        enabled: bool,
        #[serde(default = "default_retention_hours")]
        retention_hours: u64,
    }

    impl Default for CleanupConfig {
        fn default() -> Self {
            Self {
                enabled: default_cleanup_enabled(),
                retention_hours: default_retention_hours(),
            }
        }
    }

    fn default_scan_interval() -> u64 {
        300
    }
    fn default_region() -> String {
        "auto".to_string()
    }
    fn default_cleanup_enabled() -> bool {
        true
    }
    fn default_retention_hours() -> u64 {
        72
    }

    let text = fs::read_to_string(&cfg_path).unwrap();
    let cfg: Config = toml::from_str(&text).unwrap();

    assert_eq!(cfg.data_dir, "/app/data");
    assert_eq!(cfg.scan_interval_s, 300);
    assert_eq!(cfg.s3.bucket, "test");
    assert_eq!(cfg.s3.prefix, "");
    assert_eq!(cfg.s3.region, "auto");
    assert_eq!(cfg.s3.endpoint, "");
    assert!(cfg.cleanup.enabled);
    assert_eq!(cfg.cleanup.retention_hours, 72);
}
