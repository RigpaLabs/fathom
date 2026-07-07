use super::ExchangeAdapter;

const WS_URL: &str = "wss://stream.bybit.com/v5/public/spot";

pub struct BybitSpot;

impl ExchangeAdapter for BybitSpot {
    fn name(&self) -> &str {
        "bybit_spot"
    }

    /// Bybit's category base URL takes no query params — subscriptions are
    /// sent as an `{"op": "subscribe", "args": [...]}` WS message after
    /// connecting (see specs/bybit-collection.md's "Channels / topics"
    /// section). WP2's connection task builds that subscribe message; this
    /// adapter only exposes the fixed endpoint.
    fn ws_url(&self, _symbols: &[String], _depth_ms: u64) -> String {
        WS_URL.to_string()
    }

    /// Bybit pushes the initial book as a `type: "snapshot"` message over the
    /// same WS connection right after subscribing — no REST snapshot call
    /// needed (unlike Binance).
    fn snapshot_url(&self, _symbol: &str) -> String {
        String::new()
    }
}
