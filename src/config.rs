use std::path::PathBuf;

use config::{Config as ConfigBuilder, File};
use serde::Deserialize;

use crate::error::Result;

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Exchange {
    BinanceSpot,
    BinancePerp,
    BybitSpot,
    BybitPerp,
    Dydx,
    Hyperliquid,
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
    /// Override the second (market) WebSocket URL — binance_perp only, see
    /// `ExchangeAdapter::market_ws_url`. Used in tests.
    #[serde(default)]
    pub market_ws_url_override: Option<String>,
    /// Override snapshot base URL template. Use `{symbol}` as placeholder.
    /// Example: "http://127.0.0.1:9001/depth?symbol={symbol}&limit=5000"
    #[serde(default)]
    pub snapshot_url_override: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NatsConfig {
    pub url: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Symbol allowlist for NATS publishing only — Parquet collection always
    /// covers the full symbol set regardless of this field. `None` or an
    /// empty list publishes every symbol (default, backward-compatible with
    /// prod behavior before this field existed). Non-empty restricts NATS
    /// publish to the listed symbols, matched case-sensitively as they
    /// appear upstream (e.g. "ETHUSDT").
    #[serde(default)]
    pub symbols: Option<Vec<String>>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub data_dir: PathBuf,
    /// How often to rotate parquet files, in hours. Governs all four writers
    /// (raw, trades, 1s, deriv). Must divide 24 evenly. Default: 1.
    #[serde(default = "default_raw_rotate_hours")]
    pub raw_rotate_hours: u32,
    pub connections: Vec<ConnectionConfig>,
    #[serde(default)]
    pub nats: Option<NatsConfig>,
}

fn default_raw_rotate_hours() -> u32 {
    1
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let cfg = ConfigBuilder::builder()
            .add_source(File::with_name(path))
            .build()?;
        let config: Self = cfg.try_deserialize()?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        let h = self.raw_rotate_hours;
        if h == 0 || 24 % h != 0 {
            return Err(crate::error::AppError::Config(
                config::ConfigError::Message(format!(
                    "raw_rotate_hours must divide 24 evenly (1,2,3,4,6,8,12,24), got {h}"
                )),
            ));
        }
        for conn in &self.connections {
            if conn.symbols.is_empty() {
                return Err(crate::error::AppError::Config(
                    config::ConfigError::Message(format!(
                        "connection '{}' has empty symbols list",
                        conn.name
                    )),
                ));
            }
        }
        Ok(())
    }
}
