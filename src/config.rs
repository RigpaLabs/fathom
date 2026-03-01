use std::path::PathBuf;

use config::{Config as ConfigBuilder, File};
use serde::Deserialize;

use crate::error::Result;

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Exchange {
    BinanceSpot,
    BinancePerp,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ConnectionConfig {
    pub name: String,
    pub exchange: Exchange,
    pub symbols: Vec<String>,
    pub depth_ms: u64,
    /// Override WebSocket URL (full URL including ?streams=...). Used in tests.
    #[serde(default)]
    pub ws_url_override: Option<String>,
    /// Override snapshot base URL template. Use `{symbol}` as placeholder.
    /// Example: "http://127.0.0.1:9001/depth?symbol={symbol}&limit=5000"
    #[serde(default)]
    pub snapshot_url_override: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub data_dir: PathBuf,
    /// How often to rotate raw parquet files, in hours.  Must divide 24 evenly.
    /// Default: 1.
    #[serde(default = "default_raw_rotate_hours")]
    pub raw_rotate_hours: u32,
    pub connections: Vec<ConnectionConfig>,
}

fn default_raw_rotate_hours() -> u32 {
    1
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let cfg = ConfigBuilder::builder()
            .add_source(File::with_name(path))
            .build()?;
        Ok(cfg.try_deserialize()?)
    }
}
