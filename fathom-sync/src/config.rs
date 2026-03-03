use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{Error, Result};

#[derive(Debug, Deserialize)]
pub struct Config {
    pub data_dir: PathBuf,
    #[serde(default = "default_scan_interval")]
    pub scan_interval_s: u64,
    pub s3: S3Config,
    #[serde(default)]
    pub cleanup: CleanupConfig,
}

#[derive(Debug, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default)]
    pub endpoint: String,
}

#[derive(Debug, Deserialize)]
pub struct CleanupConfig {
    #[serde(default = "default_cleanup_enabled")]
    pub enabled: bool,
    #[serde(default = "default_retention_hours")]
    pub retention_hours: u64,
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

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path).map_err(|e| Error::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        toml::from_str(&text).map_err(|e| Error::Config(e.to_string()))
    }
}
