use serde::{Deserialize, Serialize};

/// NATS wire format version. Prepended as single byte before bincode payload.
pub const WIRE_VERSION: u8 = 1;

/// Bid/ask level data: Vec of (price, size) pairs.
pub type Levels = Vec<(f64, f64)>;

/// One-second snapshot row emitted by Fathom's WindowAccumulator.
///
/// Published to NATS subject `fathom.v1.{exchange}.{symbol}.snapshot` at 1/sec.
/// Wire format: `[WIRE_VERSION: u8][bincode payload]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot1s {
    pub ts_us: i64,
    pub exchange: String,
    pub symbol: String,
    /// Top 10 bid levels: (price, size) descending
    pub bids: Vec<(f64, f64)>,
    /// Top 10 ask levels: (price, size) ascending
    pub asks: Vec<(f64, f64)>,
    pub mid_px: Option<f64>,
    pub microprice: Option<f64>,
    pub spread_bps: Option<f32>,
    pub imbalance_l1: Option<f32>,
    pub imbalance_l5: Option<f32>,
    pub imbalance_l10: Option<f32>,
    pub bid_depth_l5: f64,
    pub bid_depth_l10: f64,
    pub ask_depth_l5: f64,
    pub ask_depth_l10: f64,
    pub ofi_l1: f64,
    pub churn_bid: f64,
    pub churn_ask: f64,
    pub intra_sigma: f32,
    pub open_px: Option<f64>,
    pub close_px: Option<f64>,
    pub n_events: u32,
    // Trade fields (populated by HL/dYdX trade streams; zero for Binance)
    pub volume_delta: f64,
    pub buy_vol: f64,
    pub sell_vol: f64,
    pub trade_count: u32,
}

/// A raw depth diff event from an exchange.
///
/// Published to NATS subject `fathom.v1.{exchange}.{symbol}.depth` at ~100ms.
/// Wire format: `[WIRE_VERSION: u8][bincode payload]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawDiff {
    pub timestamp_us: i64,
    pub exchange: String,
    pub symbol: String,
    pub seq_id: i64,
    pub prev_seq_id: i64,
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
}

/// Encode a value into the Fathom NATS wire format: `[version][bincode]`.
pub fn wire_encode<T: Serialize>(value: &T) -> Result<Vec<u8>, bincode::Error> {
    let payload = bincode::serialize(value)?;
    let mut buf = Vec::with_capacity(1 + payload.len());
    buf.push(WIRE_VERSION);
    buf.extend(payload);
    Ok(buf)
}

/// Decode a value from the Fathom NATS wire format.
/// Returns an error if the version byte doesn't match `WIRE_VERSION`.
pub fn wire_decode<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T, WireDecodeError> {
    if bytes.is_empty() {
        return Err(WireDecodeError::Empty);
    }
    let version = bytes[0];
    if version != WIRE_VERSION {
        return Err(WireDecodeError::VersionMismatch {
            expected: WIRE_VERSION,
            got: version,
        });
    }
    bincode::deserialize(&bytes[1..]).map_err(WireDecodeError::Bincode)
}

#[derive(Debug, thiserror::Error)]
pub enum WireDecodeError {
    #[error("empty payload")]
    Empty,
    #[error("wire version mismatch: expected {expected}, got {got}")]
    VersionMismatch { expected: u8, got: u8 },
    #[error("bincode: {0}")]
    Bincode(#[from] bincode::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_snapshot() {
        let snap = Snapshot1s {
            ts_us: 1_700_000_000_000_000,
            exchange: "binance_perp".into(),
            symbol: "ETHUSDT".into(),
            bids: vec![(3000.0, 1.5), (2999.0, 2.0)],
            asks: vec![(3001.0, 1.0), (3002.0, 3.0)],
            mid_px: Some(3000.5),
            microprice: Some(3000.4),
            spread_bps: Some(3.33),
            imbalance_l1: Some(0.2),
            imbalance_l5: Some(0.1),
            imbalance_l10: Some(0.05),
            bid_depth_l5: 100.0,
            bid_depth_l10: 200.0,
            ask_depth_l5: 90.0,
            ask_depth_l10: 180.0,
            ofi_l1: 0.5,
            churn_bid: 10.0,
            churn_ask: 12.0,
            intra_sigma: 0.01,
            open_px: Some(2998.0),
            close_px: Some(3000.5),
            n_events: 42,
            volume_delta: 5.0,
            buy_vol: 15.0,
            sell_vol: 10.0,
            trade_count: 8,
        };

        let bytes = wire_encode(&snap).expect("encode");
        assert_eq!(bytes[0], WIRE_VERSION);
        let decoded: Snapshot1s = wire_decode(&bytes).expect("decode");
        assert_eq!(decoded.ts_us, snap.ts_us);
        assert_eq!(decoded.exchange, snap.exchange);
        assert_eq!(decoded.symbol, snap.symbol);
        assert_eq!(decoded.bids.len(), snap.bids.len());
        assert_eq!(decoded.mid_px, snap.mid_px);
        assert_eq!(decoded.trade_count, snap.trade_count);
    }

    #[test]
    fn roundtrip_raw_diff() {
        let diff = RawDiff {
            timestamp_us: 1_700_000_000_000_000,
            exchange: "binance_spot".into(),
            symbol: "BTCUSDT".into(),
            seq_id: 100,
            prev_seq_id: 99,
            bids: vec![(50000.0, 0.1)],
            asks: vec![(50001.0, 0.2)],
        };

        let bytes = wire_encode(&diff).expect("encode");
        let decoded: RawDiff = wire_decode(&bytes).expect("decode");
        assert_eq!(decoded.seq_id, diff.seq_id);
        assert_eq!(decoded.exchange, diff.exchange);
    }

    #[test]
    fn version_mismatch() {
        let mut bytes = wire_encode(&42u32).expect("encode");
        bytes[0] = 99; // wrong version
        let result = wire_decode::<u32>(&bytes);
        assert!(matches!(result, Err(WireDecodeError::VersionMismatch { .. })));
    }

    #[test]
    fn empty_payload() {
        let result = wire_decode::<u32>(&[]);
        assert!(matches!(result, Err(WireDecodeError::Empty)));
    }
}
