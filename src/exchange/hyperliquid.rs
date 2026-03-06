use super::ExchangeAdapter;

pub const WS_URL: &str = "wss://api.hyperliquid.xyz/ws";

pub struct Hyperliquid;

impl ExchangeAdapter for Hyperliquid {
    fn name(&self) -> &str {
        "hyperliquid"
    }

    /// Hyperliquid uses a single WS endpoint; subscriptions are sent as messages.
    /// The returned URL is the bare endpoint — symbols are subscribed after connect.
    fn ws_url(&self, _symbols: &[String], _depth_ms: u64) -> String {
        WS_URL.to_string()
    }

    /// Hyperliquid sends full snapshots over WS — no REST snapshot endpoint needed.
    fn snapshot_url(&self, _symbol: &str) -> String {
        String::new()
    }
}
