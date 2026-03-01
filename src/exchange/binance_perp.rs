use super::ExchangeAdapter;

const WS_BASE: &str = "wss://fstream.binance.com/stream";
const REST_BASE: &str = "https://fapi.binance.com/fapi/v1";

pub struct BinancePerp;

impl ExchangeAdapter for BinancePerp {
    fn name(&self) -> &str {
        "binance_perp"
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
            "{REST_BASE}/depth?symbol={}&limit=1000",
            symbol.to_uppercase()
        )
    }
}
