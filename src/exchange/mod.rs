mod binance_perp;
mod binance_spot;
pub mod dydx;
pub mod hyperliquid;

pub use binance_perp::BinancePerp;
pub use binance_spot::BinanceSpot;
pub use hyperliquid::Hyperliquid;

/// Adapter for an exchange's WebSocket and REST API endpoints.
pub trait ExchangeAdapter: Send + Sync {
    fn name(&self) -> &str;
    /// Build combined-stream WS URL for the given symbols.
    fn ws_url(&self, symbols: &[String], depth_ms: u64) -> String;
    /// Build REST depth snapshot URL for a single symbol.
    fn snapshot_url(&self, symbol: &str) -> String;
    /// REST open-interest URL for a single symbol. `None` when the venue has
    /// no OI REST endpoint (spot has no OI; Hyperliquid delivers OI over WS).
    fn open_interest_url(&self, _symbol: &str) -> Option<String> {
        None
    }
}
