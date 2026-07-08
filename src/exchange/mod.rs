mod binance_perp;
mod binance_spot;
mod bybit_perp;
mod bybit_spot;
pub mod dydx;
pub mod hyperliquid;

pub use binance_perp::BinancePerp;
pub use binance_spot::BinanceSpot;
pub use bybit_perp::BybitPerp;
pub use bybit_spot::BybitSpot;
pub use hyperliquid::Hyperliquid;

/// Adapter for an exchange's WebSocket and REST API endpoints.
pub trait ExchangeAdapter: Send + Sync {
    fn name(&self) -> &str;
    /// Build combined-stream WS URL for the given symbols. For `binance_perp`
    /// this carries depth only (routed `/public` streams) — see `market_ws_url`.
    fn ws_url(&self, symbols: &[String], depth_ms: u64) -> String;
    /// Second WS connection for venues that route event categories to
    /// separate endpoints (currently `binance_perp` only: aggTrade/markPrice/
    /// forceOrder on the routed `/market` endpoint, fathom#62). `None` means a
    /// single connection (`ws_url`) carries everything, which is every other
    /// venue's behavior. When `Some`, `connection::binance::connection_task`
    /// opens both connections and merges their events onto the same per-symbol
    /// book + accumulator.
    fn market_ws_url(&self, _symbols: &[String]) -> Option<String> {
        None
    }
    /// Build REST depth snapshot URL for a single symbol.
    fn snapshot_url(&self, symbol: &str) -> String;
    /// REST open-interest URL for a single symbol. `None` when the venue has
    /// no OI REST endpoint (spot has no OI; Hyperliquid delivers OI over WS).
    fn open_interest_url(&self, _symbol: &str) -> Option<String> {
        None
    }
}
