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

/// A single trade from an exchange's trade tape.
///
/// Published to NATS subject `fathom.v1.{exchange}.{symbol}.trade`.
/// Wire format: `[WIRE_VERSION: u8][bincode payload]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawTrade {
    /// Exchange trade time (µs).
    pub timestamp_us: i64,
    pub exchange: String,
    pub symbol: String,
    /// Binance aggTrade id / Hyperliquid tid.
    pub trade_id: i64,
    pub price: f64,
    /// Base units.
    pub qty: f64,
    /// True when the buyer was the maker (i.e. the taker sold).
    /// Binance `m` flag; Hyperliquid side mapped to the same semantics.
    pub is_buyer_maker: bool,
}

/// Mark price + funding snapshot for a perp symbol.
///
/// Mark and funding arrive in one exchange event on both venues (Binance
/// `markPrice@1s`, Hyperliquid `activeAssetCtx`), so a single struct is
/// published once on NATS subject `fathom.v1.{exchange}.{symbol}.funding`
/// (stream `FATHOM_DERIV`) — there is no separate `.mark` subject.
/// Wire format: `[WIRE_VERSION: u8][bincode payload]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarkFunding {
    /// Exchange event time (µs); receipt time for venues without one (HL).
    pub timestamp_us: i64,
    pub exchange: String,
    pub symbol: String,
    pub mark_px: f64,
    /// Index/oracle price (Binance `i`, HL `oraclePx`). None if unavailable.
    pub index_px: Option<f64>,
    /// Funding rate as sent by the venue (per funding interval, not annualized).
    pub funding_rate: f64,
    /// Next funding time (µs). None for venues without a discrete
    /// next-funding timestamp in the feed (Hyperliquid).
    pub next_funding_ts: Option<i64>,
}

/// Open interest snapshot for a perp symbol.
///
/// Published to NATS subject `fathom.v1.{exchange}.{symbol}.oi`
/// (stream `FATHOM_DERIV`).
/// Wire format: `[WIRE_VERSION: u8][bincode payload]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenInterest {
    /// Exchange-provided time (µs); receipt time for venues without one (HL).
    pub timestamp_us: i64,
    pub exchange: String,
    pub symbol: String,
    /// Open interest in base units.
    pub oi_base: f64,
    /// Open interest in quote units, when the venue provides it directly.
    pub oi_quote: Option<f64>,
}

/// A liquidation (forced) order.
///
/// Published to NATS subject `fathom.v1.{exchange}.{symbol}.liq`
/// (stream `FATHOM_DERIV`).
/// Wire format: `[WIRE_VERSION: u8][bincode payload]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Liquidation {
    /// Exchange trade time (µs).
    pub timestamp_us: i64,
    pub exchange: String,
    pub symbol: String,
    /// Liquidation order side as sent by the venue (`BUY` / `SELL`).
    /// SELL = a long position was liquidated.
    pub side: String,
    /// Average fill price (Binance forceOrder `ap`).
    pub price: f64,
    /// Base units.
    pub qty: f64,
}

/// Envelope wrapping any payload with metadata for cross-service traceability.
/// Opt-in — existing consumers can still decode raw payloads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope<T> {
    /// Wire format version (currently 1).
    pub schema_version: u16,
    /// Producer identifier (e.g. "fathom-0.3.1").
    pub producer: String,
    /// Original exchange event timestamp (microseconds).
    pub source_ts_us: i64,
    /// Timestamp when the producer ingested the event (microseconds).
    pub ingest_ts_us: i64,
    /// Optional correlation ID for request tracing.
    pub correlation_id: Option<String>,
    /// The actual payload.
    pub payload: T,
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
    fn roundtrip_raw_trade() {
        let trade = RawTrade {
            timestamp_us: 1_700_000_000_123_000,
            exchange: "binance_perp".into(),
            symbol: "ETHUSDT".into(),
            trade_id: 987_654_321,
            price: 3000.25,
            qty: 1.5,
            is_buyer_maker: true,
        };

        let bytes = wire_encode(&trade).expect("encode");
        assert_eq!(bytes[0], WIRE_VERSION);
        let decoded: RawTrade = wire_decode(&bytes).expect("decode");
        assert_eq!(decoded.timestamp_us, trade.timestamp_us);
        assert_eq!(decoded.exchange, trade.exchange);
        assert_eq!(decoded.symbol, trade.symbol);
        assert_eq!(decoded.trade_id, trade.trade_id);
        assert_eq!(decoded.price, trade.price);
        assert_eq!(decoded.qty, trade.qty);
        assert!(decoded.is_buyer_maker);
    }

    #[test]
    fn roundtrip_mark_funding() {
        let mf = MarkFunding {
            timestamp_us: 1_700_000_000_000_000,
            exchange: "binance_perp".into(),
            symbol: "ETHUSDT".into(),
            mark_px: 3000.12,
            index_px: Some(3000.05),
            funding_rate: 0.0001,
            next_funding_ts: Some(1_700_000_400_000_000),
        };

        let bytes = wire_encode(&mf).expect("encode");
        assert_eq!(bytes[0], WIRE_VERSION);
        let decoded: MarkFunding = wire_decode(&bytes).expect("decode");
        assert_eq!(decoded.timestamp_us, mf.timestamp_us);
        assert_eq!(decoded.exchange, mf.exchange);
        assert_eq!(decoded.symbol, mf.symbol);
        assert_eq!(decoded.mark_px, mf.mark_px);
        assert_eq!(decoded.index_px, mf.index_px);
        assert_eq!(decoded.funding_rate, mf.funding_rate);
        assert_eq!(decoded.next_funding_ts, mf.next_funding_ts);
    }

    #[test]
    fn roundtrip_mark_funding_nullable_fields_none() {
        // HL shape: no discrete next funding timestamp; index may be absent.
        let mf = MarkFunding {
            timestamp_us: 1_700_000_000_000_000,
            exchange: "hyperliquid".into(),
            symbol: "ETH".into(),
            mark_px: 3000.12,
            index_px: None,
            funding_rate: 0.0000125,
            next_funding_ts: None,
        };
        let decoded: MarkFunding = wire_decode(&wire_encode(&mf).expect("encode")).expect("decode");
        assert_eq!(decoded.index_px, None);
        assert_eq!(decoded.next_funding_ts, None);
    }

    #[test]
    fn roundtrip_open_interest() {
        let oi = OpenInterest {
            timestamp_us: 1_700_000_000_000_000,
            exchange: "binance_perp".into(),
            symbol: "BTCUSDT".into(),
            oi_base: 10_659.509,
            oi_quote: None,
        };
        let decoded: OpenInterest =
            wire_decode(&wire_encode(&oi).expect("encode")).expect("decode");
        assert_eq!(decoded.timestamp_us, oi.timestamp_us);
        assert_eq!(decoded.exchange, oi.exchange);
        assert_eq!(decoded.symbol, oi.symbol);
        assert_eq!(decoded.oi_base, oi.oi_base);
        assert_eq!(decoded.oi_quote, None);
    }

    #[test]
    fn roundtrip_liquidation() {
        let liq = Liquidation {
            timestamp_us: 1_700_000_000_123_000,
            exchange: "binance_perp".into(),
            symbol: "ETHUSDT".into(),
            side: "SELL".into(),
            price: 2998.4,
            qty: 0.014,
        };
        let decoded: Liquidation =
            wire_decode(&wire_encode(&liq).expect("encode")).expect("decode");
        assert_eq!(decoded.timestamp_us, liq.timestamp_us);
        assert_eq!(decoded.exchange, liq.exchange);
        assert_eq!(decoded.symbol, liq.symbol);
        assert_eq!(decoded.side, "SELL");
        assert_eq!(decoded.price, liq.price);
        assert_eq!(decoded.qty, liq.qty);
    }

    #[test]
    fn version_mismatch() {
        let mut bytes = wire_encode(&42u32).expect("encode");
        bytes[0] = 99; // wrong version
        let result = wire_decode::<u32>(&bytes);
        assert!(matches!(
            result,
            Err(WireDecodeError::VersionMismatch { .. })
        ));
    }

    #[test]
    fn empty_payload() {
        let result = wire_decode::<u32>(&[]);
        assert!(matches!(result, Err(WireDecodeError::Empty)));
    }

    #[test]
    fn envelope_roundtrip() {
        let envelope = EventEnvelope {
            schema_version: 1,
            producer: "fathom-test".into(),
            source_ts_us: 1234567890,
            ingest_ts_us: 1234567891,
            correlation_id: None,
            payload: "test data".to_string(),
        };
        let bytes = wire_encode(&envelope).unwrap();
        let decoded: EventEnvelope<String> = wire_decode(&bytes).unwrap();
        assert_eq!(decoded.schema_version, 1);
        assert_eq!(decoded.producer, "fathom-test");
        assert_eq!(decoded.payload, "test data");
    }
}
