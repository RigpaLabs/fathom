use super::ExchangeAdapter;

// 2026-03 Binance USDM Futures WS routing upgrade: the legacy unrouted
// `wss://fstream.binance.com/stream` endpoint now silently delivers only
// /public-category streams (depth, bookTicker) — aggTrade/markPrice/forceOrder
// (the /market category) go nowhere on that URL. fathom#62: this made
// binance_perp lose trades, funding/mark, and liquidations for weeks with no
// error (depth kept flowing, so reconnect/gap logic never noticed).
//
// Fix: split into two routed connections. Depth stays on `/public/stream`
// (`ws_url`); aggTrade/markPrice/forceOrder move to `/market/stream`
// (`market_ws_url`). Both are opened and merged onto the same per-symbol book
// + accumulator by `connection::binance::connection_task`. See:
// https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice
const WS_PUBLIC_BASE: &str = "wss://fstream.binance.com/public/stream";
const WS_MARKET_BASE: &str = "wss://fstream.binance.com/market/stream";
const REST_BASE: &str = "https://fapi.binance.com/fapi/v1";

pub struct BinancePerp;

impl ExchangeAdapter for BinancePerp {
    fn name(&self) -> &str {
        "binance_perp"
    }

    /// Depth-only stream, routed via `/public/stream` (see module doc comment).
    fn ws_url(&self, symbols: &[String], depth_ms: u64) -> String {
        let streams = symbols
            .iter()
            .map(|s| format!("{}@depth@{depth_ms}ms", s.to_lowercase()))
            .collect::<Vec<_>>()
            .join("/");
        format!("{WS_PUBLIC_BASE}?streams={streams}")
    }

    /// aggTrade + markPrice@1s + forceOrder, routed via `/market/stream` (see
    /// module doc comment) — merged with the depth connection in
    /// `connection::binance::connection_task`.
    fn market_ws_url(&self, symbols: &[String]) -> Option<String> {
        let streams = symbols
            .iter()
            .map(|s| {
                let sym = s.to_lowercase();
                format!("{sym}@aggTrade/{sym}@markPrice@1s/{sym}@forceOrder")
            })
            .collect::<Vec<_>>()
            .join("/");
        Some(format!("{WS_MARKET_BASE}?streams={streams}"))
    }

    /// Binance USDM Futures max depth is 1000 (vs 5000 for spot).
    fn snapshot_url(&self, symbol: &str) -> String {
        format!(
            "{REST_BASE}/depth?symbol={}&limit=1000",
            symbol.to_uppercase()
        )
    }

    /// USDM Futures has no OI WebSocket channel — polled via REST instead
    /// (specs/derivatives-feeds.md).
    fn open_interest_url(&self, symbol: &str) -> Option<String> {
        Some(format!(
            "{REST_BASE}/openInterest?symbol={}",
            symbol.to_uppercase()
        ))
    }
}
