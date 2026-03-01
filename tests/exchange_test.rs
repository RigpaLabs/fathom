use fathom::exchange::{BinancePerp, BinanceSpot, ExchangeAdapter};

// ── BinanceSpot ──────────────────────────────────────────────────────────────

#[test]
fn test_spot_name() {
    assert_eq!(BinanceSpot.name(), "binance_spot");
}

#[test]
fn test_spot_ws_url_single_symbol() {
    let url = BinanceSpot.ws_url(&["ETHUSDT".to_string()], 100);
    assert_eq!(url, "wss://stream.binance.com/stream?streams=ethusdt@depth@100ms");
}

#[test]
fn test_spot_ws_url_multiple_symbols() {
    let symbols = vec!["ETHUSDT".to_string(), "BTCUSDT".to_string()];
    let url = BinanceSpot.ws_url(&symbols, 100);
    assert!(url.starts_with("wss://stream.binance.com/stream?streams="));
    assert!(url.contains("ethusdt@depth@100ms"));
    assert!(url.contains("btcusdt@depth@100ms"));
    assert!(url.contains('/'), "multiple streams joined by /");
}

#[test]
fn test_spot_ws_url_250ms() {
    let url = BinanceSpot.ws_url(&["BTCUSDT".to_string()], 250);
    assert!(url.contains("@depth@250ms"));
}

#[test]
fn test_spot_ws_url_lowercases_symbols() {
    let url = BinanceSpot.ws_url(&["BNBUSDT".to_string()], 100);
    assert!(url.contains("bnbusdt"), "symbol should be lowercase in WS URL");
    assert!(!url.contains("BNBUSDT"));
}

#[test]
fn test_spot_snapshot_url() {
    let url = BinanceSpot.snapshot_url("ETHUSDT");
    assert_eq!(url, "https://api.binance.com/api/v3/depth?symbol=ETHUSDT&limit=5000");
}

#[test]
fn test_spot_snapshot_url_uppercases() {
    let url = BinanceSpot.snapshot_url("ethusdt");
    assert!(url.contains("ETHUSDT"), "symbol should be uppercase in REST URL");
}

// ── BinancePerp ──────────────────────────────────────────────────────────────

#[test]
fn test_perp_name() {
    assert_eq!(BinancePerp.name(), "binance_perp");
}

#[test]
fn test_perp_ws_url() {
    let url = BinancePerp.ws_url(&["ETHUSDT".to_string()], 100);
    assert!(url.starts_with("wss://fstream.binance.com"), "perp uses fstream subdomain");
    assert!(url.contains("ethusdt@depth@100ms"));
}

#[test]
fn test_perp_snapshot_url() {
    let url = BinancePerp.snapshot_url("BTCUSDT");
    assert!(url.starts_with("https://fapi.binance.com"), "perp uses fapi");
    assert!(url.contains("BTCUSDT"));
    assert!(url.contains("limit=1000"));
}

#[test]
fn test_spot_perp_different_urls() {
    let sym = vec!["ETHUSDT".to_string()];
    assert_ne!(
        BinanceSpot.ws_url(&sym, 100),
        BinancePerp.ws_url(&sym, 100),
        "spot and perp WS URLs must differ"
    );
    assert_ne!(
        BinanceSpot.snapshot_url("ETHUSDT"),
        BinancePerp.snapshot_url("ETHUSDT"),
        "spot and perp snapshot URLs must differ"
    );
}
