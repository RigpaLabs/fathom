//! Shared types for Fathom L2 order book data.
//!
//! This crate exists to decouple fathom (public) from sigil (private).
//! Both import the same structs — zero duplication, zero coupling.

use serde::{Deserialize, Serialize};

/// One-second aggregated snapshot of L2 order book state.
///
/// Produced by fathom's `WindowAccumulator::flush()`. One struct for all exchanges;
/// optional fields cover exchange-specific data (e.g. trades from HL/dYdX).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot1s {
    /// Snapshot timestamp in microseconds since epoch.
    pub ts_us: i64,
    /// Exchange identifier (e.g. "binance_perp", "hyperliquid").
    pub exchange: String,
    /// Trading symbol (e.g. "ETHUSDT").
    pub symbol: String,

    // -- L2 book (top 10 levels) --
    /// Top 10 bid levels: (price, size).
    pub bids: Vec<(f64, f64)>,
    /// Top 10 ask levels: (price, size).
    pub asks: Vec<(f64, f64)>,

    // -- Derived prices --
    pub mid_px: Option<f64>,
    pub microprice: Option<f64>,
    pub spread_bps: Option<f32>,

    // -- Imbalance --
    pub imbalance_l1: Option<f32>,
    pub imbalance_l5: Option<f32>,
    pub imbalance_l10: Option<f32>,

    // -- Depth --
    pub bid_depth_l5: f64,
    pub bid_depth_l10: f64,
    pub ask_depth_l5: f64,
    pub ask_depth_l10: f64,

    // -- Order flow --
    /// Order Flow Imbalance at L1.
    pub ofi_l1: f64,
    /// Churn = cumulative absolute size changes on bid side.
    pub churn_bid: f64,
    /// Churn = cumulative absolute size changes on ask side.
    pub churn_ask: f64,

    // -- Volatility --
    /// Intra-second mid-price standard deviation (USD, population stddev).
    pub intra_sigma: f32,

    // -- Candle-like --
    pub open_px: Option<f64>,
    pub close_px: Option<f64>,

    /// Number of raw events accumulated in this second.
    pub n_events: u32,

    // -- Trade fields (populated by HL/dYdX; zero for Binance) --
    pub volume_delta: f64,
    pub buy_vol: f64,
    pub sell_vol: f64,
    pub trade_count: u32,
}

/// Raw depth update (diff or full snapshot) from an exchange.
///
/// Published to NATS at ~100ms frequency for real-time consumers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthUpdate {
    /// Timestamp in microseconds since epoch.
    pub ts_us: i64,
    /// Exchange identifier.
    pub exchange: String,
    /// Trading symbol.
    pub symbol: String,
    /// Changed bid levels: (price, size). Size=0 means level removed.
    pub bids: Vec<(f64, f64)>,
    /// Changed ask levels: (price, size). Size=0 means level removed.
    pub asks: Vec<(f64, f64)>,
    /// True if this is a full book snapshot, false if incremental diff.
    pub is_snapshot: bool,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_roundtrip_json() {
        let snap = Snapshot1s {
            ts_us: 1_741_430_400_000_000,
            exchange: "binance_perp".into(),
            symbol: "ETHUSDT".into(),
            bids: vec![(2500.0, 10.0), (2499.5, 20.0)],
            asks: vec![(2500.5, 15.0), (2501.0, 25.0)],
            mid_px: Some(2500.25),
            microprice: Some(2500.30),
            spread_bps: Some(2.0),
            imbalance_l1: Some(0.4),
            imbalance_l5: Some(0.3),
            imbalance_l10: Some(0.2),
            bid_depth_l5: 100.0,
            bid_depth_l10: 200.0,
            ask_depth_l5: 120.0,
            ask_depth_l10: 220.0,
            ofi_l1: 5.0,
            churn_bid: 50.0,
            churn_ask: 60.0,
            intra_sigma: 0.15,
            open_px: Some(2498.0),
            close_px: Some(2500.25),
            n_events: 42,
            volume_delta: 0.0,
            buy_vol: 0.0,
            sell_vol: 0.0,
            trade_count: 0,
        };

        let json = serde_json::to_string(&snap).unwrap();
        let back: Snapshot1s = serde_json::from_str(&json).unwrap();
        assert_eq!(back.ts_us, snap.ts_us);
        assert_eq!(back.exchange, snap.exchange);
        assert_eq!(back.bids.len(), 2);
    }

    #[test]
    fn depth_update_roundtrip_json() {
        let update = DepthUpdate {
            ts_us: 1_741_430_400_100_000,
            exchange: "hyperliquid".into(),
            symbol: "ETH".into(),
            bids: vec![(2500.0, 0.0)],
            asks: vec![(2501.0, 30.0)],
            is_snapshot: false,
        };

        let json = serde_json::to_string(&update).unwrap();
        let back: DepthUpdate = serde_json::from_str(&json).unwrap();
        assert_eq!(back.ts_us, update.ts_us);
        assert!(!back.is_snapshot);
    }
}
