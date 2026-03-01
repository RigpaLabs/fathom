use super::ExchangeAdapter;

const WS_BASE: &str = "wss://stream.binance.com/stream";
const REST_BASE: &str = "https://api.binance.com/api/v3";

pub struct BinanceSpot;

impl ExchangeAdapter for BinanceSpot {
    fn name(&self) -> &str {
        "binance_spot"
    }

    fn ws_url(&self, symbols: &[String], depth_ms: u64) -> String {
        let streams = symbols
            .iter()
            .map(|s| format!("{}@depth@{}ms", s.to_lowercase(), depth_ms))
            .collect::<Vec<_>>()
            .join("/");
        format!("{WS_BASE}?streams={streams}")
    }

    fn snapshot_url(&self, symbol: &str) -> String {
        format!(
            "{REST_BASE}/depth?symbol={}&limit=5000",
            symbol.to_uppercase()
        )
    }
}
