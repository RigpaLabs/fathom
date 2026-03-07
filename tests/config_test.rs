use fathom::config::{Config, Exchange};
use std::io::Write;
use tempfile::Builder;

/// Create a named temp file with .toml extension (required by config crate).
fn toml_file(content: &str) -> tempfile::NamedTempFile {
    let mut f = Builder::new().suffix(".toml").tempfile().unwrap();
    f.write_all(content.as_bytes()).unwrap();
    f
}

#[test]
fn test_load_valid_config() {
    let f = toml_file(
        r#"
data_dir = "/tmp/fathom_test"

[[connections]]
name     = "spot"
exchange = "binance_spot"
symbols  = ["ETHUSDT", "BTCUSDT"]
depth_ms = 100
"#,
    );
    let cfg = Config::load(f.path().to_str().unwrap()).unwrap();
    assert_eq!(cfg.data_dir.to_str().unwrap(), "/tmp/fathom_test");
    assert_eq!(cfg.connections.len(), 1);
    assert_eq!(cfg.connections[0].name, "spot");
    assert!(matches!(cfg.connections[0].exchange, Exchange::BinanceSpot));
    assert_eq!(cfg.connections[0].symbols, ["ETHUSDT", "BTCUSDT"]);
    assert_eq!(cfg.connections[0].depth_ms, 100);
}

#[test]
fn test_load_perp_exchange() {
    let f = toml_file(
        r#"
data_dir = "data"

[[connections]]
name     = "perp"
exchange = "binance_perp"
symbols  = ["BTCUSDT"]
depth_ms = 250
"#,
    );
    let cfg = Config::load(f.path().to_str().unwrap()).unwrap();
    assert!(matches!(cfg.connections[0].exchange, Exchange::BinancePerp));
    assert_eq!(cfg.connections[0].depth_ms, 250);
}

#[test]
fn test_load_multiple_connections() {
    let f = toml_file(
        r#"
data_dir = "data"

[[connections]]
name     = "spot"
exchange = "binance_spot"
symbols  = ["ETHUSDT"]
depth_ms = 100

[[connections]]
name     = "perp"
exchange = "binance_perp"
symbols  = ["ETHUSDT"]
depth_ms = 100
"#,
    );
    let cfg = Config::load(f.path().to_str().unwrap()).unwrap();
    assert_eq!(cfg.connections.len(), 2);
    assert_eq!(cfg.connections[0].name, "spot");
    assert_eq!(cfg.connections[1].name, "perp");
}

#[test]
fn test_load_with_url_overrides() {
    let f = toml_file(
        r#"
data_dir = "data"

[[connections]]
name                   = "spot"
exchange               = "binance_spot"
symbols                = ["ETHUSDT"]
depth_ms               = 100
ws_url_override        = "ws://127.0.0.1:9999/stream?streams=ethusdt@depth@100ms"
snapshot_url_override  = "http://127.0.0.1:9998/depth?symbol={symbol}&limit=5000"
"#,
    );
    let cfg = Config::load(f.path().to_str().unwrap()).unwrap();
    let conn = &cfg.connections[0];
    assert!(conn.ws_url_override.is_some());
    assert!(conn.snapshot_url_override.is_some());
    assert!(
        conn.snapshot_url_override
            .as_ref()
            .unwrap()
            .contains("{symbol}")
    );
}

#[test]
fn test_load_default_no_url_overrides() {
    let f = toml_file(
        r#"
data_dir = "data"

[[connections]]
name     = "spot"
exchange = "binance_spot"
symbols  = ["ETHUSDT"]
depth_ms = 100
"#,
    );
    let cfg = Config::load(f.path().to_str().unwrap()).unwrap();
    assert!(cfg.connections[0].ws_url_override.is_none());
    assert!(cfg.connections[0].snapshot_url_override.is_none());
}

#[test]
fn test_raw_rotate_hours_default() {
    let f = toml_file(
        r#"
data_dir = "data"

[[connections]]
name     = "spot"
exchange = "binance_spot"
symbols  = ["ETHUSDT"]
depth_ms = 100
"#,
    );
    let cfg = Config::load(f.path().to_str().unwrap()).unwrap();
    assert_eq!(cfg.raw_rotate_hours, 1, "default should be 1 hour");
}

#[test]
fn test_raw_rotate_hours_explicit() {
    let f = toml_file(
        r#"
data_dir = "data"
raw_rotate_hours = 6

[[connections]]
name     = "spot"
exchange = "binance_spot"
symbols  = ["ETHUSDT"]
depth_ms = 100
"#,
    );
    let cfg = Config::load(f.path().to_str().unwrap()).unwrap();
    assert_eq!(cfg.raw_rotate_hours, 6);
}

#[test]
fn test_load_missing_file() {
    let result = Config::load("/tmp/this_file_definitely_does_not_exist_fathom.toml");
    assert!(result.is_err());
}

#[test]
fn test_load_invalid_toml() {
    let f = toml_file("this is not valid toml [[[");
    let result = Config::load(f.path().to_str().unwrap());
    assert!(result.is_err());
}

#[test]
fn test_load_missing_required_field() {
    // Missing 'symbols' field
    let f = toml_file(
        r#"
data_dir = "data"

[[connections]]
name     = "spot"
exchange = "binance_spot"
depth_ms = 100
"#,
    );
    let result = Config::load(f.path().to_str().unwrap());
    assert!(result.is_err());
}

#[test]
fn test_load_hyperliquid_exchange() {
    let f = toml_file(
        r#"
data_dir = "data"

[[connections]]
name     = "hl"
exchange = "hyperliquid"
symbols  = ["ETH", "BTC"]
depth_ms = 0
"#,
    );
    let cfg = Config::load(f.path().to_str().unwrap()).unwrap();
    assert!(matches!(cfg.connections[0].exchange, Exchange::Hyperliquid));
    assert_eq!(cfg.connections[0].symbols, ["ETH", "BTC"]);
}

#[test]
fn test_load_dydx_exchange() {
    let f = toml_file(
        r#"
data_dir = "data"

[[connections]]
name     = "dydx"
exchange = "dydx"
symbols  = ["ETH-USD"]
depth_ms = 0
"#,
    );
    let cfg = Config::load(f.path().to_str().unwrap()).unwrap();
    assert!(matches!(cfg.connections[0].exchange, Exchange::Dydx));
}

#[test]
fn test_load_invalid_exchange() {
    let f = toml_file(
        r#"
data_dir = "data"

[[connections]]
name     = "bad"
exchange = "kraken"
symbols  = ["ETH"]
depth_ms = 100
"#,
    );
    let result = Config::load(f.path().to_str().unwrap());
    assert!(
        result.is_err(),
        "unknown exchange variant should fail deserialization"
    );
}

#[test]
fn test_load_all_four_exchanges() {
    let f = toml_file(
        r#"
data_dir = "data"

[[connections]]
name = "spot"
exchange = "binance_spot"
symbols = ["ETHUSDT"]
depth_ms = 100

[[connections]]
name = "perp"
exchange = "binance_perp"
symbols = ["ETHUSDT"]
depth_ms = 100

[[connections]]
name = "hl"
exchange = "hyperliquid"
symbols = ["ETH"]
depth_ms = 0

[[connections]]
name = "dydx"
exchange = "dydx"
symbols = ["ETH-USD"]
depth_ms = 0
"#,
    );
    let cfg = Config::load(f.path().to_str().unwrap()).unwrap();
    assert_eq!(cfg.connections.len(), 4);
    assert!(matches!(cfg.connections[0].exchange, Exchange::BinanceSpot));
    assert!(matches!(cfg.connections[1].exchange, Exchange::BinancePerp));
    assert!(matches!(cfg.connections[2].exchange, Exchange::Hyperliquid));
    assert!(matches!(cfg.connections[3].exchange, Exchange::Dydx));
}
