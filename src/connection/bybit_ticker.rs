//! Bybit `tickers.{symbol}` state-merge logic (`specs/bybit-collection.md`'s
//! "Ticker" section).
//!
//! Pure, I/O-free: no WS, no async. Bybit's ticker topic is snapshot + partial
//! delta — "if a response param is not found in the message, then its value
//! has not changed" (docs). This module keeps an in-memory per-symbol
//! last-known state, merges each incoming message onto it (snapshot resets
//! and reseeds; delta overwrites only the fields present in the JSON), and
//! decides when to emit `MarkFunding`/`OpenInterest` rows from the merged
//! state. Wiring this into the Bybit connection task's per-symbol state map
//! and its `tickers.*` dispatch is WP2's job — this module only owns the
//! merge/emit decision so it is fully unit-testable without a mock WS server.

use serde::Deserialize;

use crate::writer::deriv::{MarkFunding, OpenInterest};

/// Per-symbol last-known ticker state, seeded from a `snapshot` message and
/// updated by each subsequent `delta`. All fields are `Option` because
/// nothing is known before the first snapshot arrives, and because a
/// server-initiated re-snapshot resets any field it omits (see
/// `merge_ticker_message`'s snapshot handling).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct BybitTickerState {
    pub mark_price: Option<f64>,
    pub index_price: Option<f64>,
    pub funding_rate: Option<f64>,
    pub next_funding_time: Option<i64>,
    pub open_interest: Option<f64>,
    pub open_interest_value: Option<f64>,
}

impl BybitTickerState {
    /// Overwrite only the fields present in `fields` — the delta-merge rule.
    fn apply_delta(&mut self, fields: &BybitTickerFields) {
        if let Some(v) = fields.mark_price.as_ref().and_then(json_f64) {
            self.mark_price = Some(v);
        }
        if let Some(v) = fields.index_price.as_ref().and_then(json_f64) {
            self.index_price = Some(v);
        }
        if let Some(v) = fields.funding_rate.as_ref().and_then(json_f64) {
            self.funding_rate = Some(v);
        }
        if let Some(v) = fields.next_funding_time.as_ref().and_then(json_i64) {
            self.next_funding_time = Some(v);
        }
        if let Some(v) = fields.open_interest.as_ref().and_then(json_f64) {
            self.open_interest = Some(v);
        }
        if let Some(v) = fields.open_interest_value.as_ref().and_then(json_f64) {
            self.open_interest_value = Some(v);
        }
    }
}

impl From<&BybitTickerFields> for BybitTickerState {
    /// Snapshot seeding: build a *fresh* state from whatever the snapshot
    /// carries — any field absent even in a snapshot resets to `None` rather
    /// than inheriting a pre-reconnect value. This matters on server-initiated
    /// resync and on the client-detected-gap reconnect path (spec's "clear
    /// order books AND the in-memory ticker-merge state" note): a stale value
    /// from before a gap must never survive into the post-reconnect state.
    fn from(fields: &BybitTickerFields) -> Self {
        Self {
            mark_price: fields.mark_price.as_ref().and_then(json_f64),
            index_price: fields.index_price.as_ref().and_then(json_f64),
            funding_rate: fields.funding_rate.as_ref().and_then(json_f64),
            next_funding_time: fields.next_funding_time.as_ref().and_then(json_i64),
            open_interest: fields.open_interest.as_ref().and_then(json_f64),
            open_interest_value: fields.open_interest_value.as_ref().and_then(json_f64),
        }
    }
}

/// The subset of `tickers.{symbol}` `data` fields fathom persists
/// (`specs/bybit-collection.md`'s capture matrix). Every field is optional:
/// deltas legitimately omit anything unchanged, and this struct simply never
/// gains fields for the dropped 24h-stats group (`lastPrice`, `volume24h`,
/// `bid1Price`, ...) — those are never parsed, so they can never trigger a
/// spurious emit. Numeric fields accept either a JSON string (Bybit's usual
/// encoding, matching orderbook/trade payloads) or a plain number, same
/// defensive parsing as Hyperliquid's `json_f64`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct BybitTickerFields {
    #[serde(rename = "markPrice")]
    mark_price: Option<serde_json::Value>,
    #[serde(rename = "indexPrice")]
    index_price: Option<serde_json::Value>,
    #[serde(rename = "fundingRate")]
    funding_rate: Option<serde_json::Value>,
    #[serde(rename = "nextFundingTime")]
    next_funding_time: Option<serde_json::Value>,
    #[serde(rename = "openInterest")]
    open_interest: Option<serde_json::Value>,
    #[serde(rename = "openInterestValue")]
    open_interest_value: Option<serde_json::Value>,
}

/// One `tickers.{symbol}` WS message: `type` is `"snapshot"` or `"delta"`,
/// `data` is the (partial, for deltas) field set above. Fields outside
/// `BybitTickerFields` (symbol, 24h-stats, ...) are ignored by serde's
/// default "unknown fields allowed" behavior — no `deny_unknown_fields`.
#[derive(Debug, Deserialize)]
pub struct BybitTickerMsg {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub data: BybitTickerFields,
}

/// Parse a JSON value that is either a numeric string (Bybit's usual
/// encoding) or a plain number.
fn json_f64(v: &serde_json::Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_str()?.parse::<f64>().ok())
}

/// Same as `json_f64` but for integer (millisecond timestamp) fields.
fn json_i64(v: &serde_json::Value) -> Option<i64> {
    v.as_i64().or_else(|| v.as_str()?.parse::<i64>().ok())
}

/// Which deriv row group(s) changed as a result of merging one message.
/// Grouping mirrors the two structs a merged state can produce:
/// `MarkFunding` (mark/index price, funding rate, next funding time) and
/// `OpenInterest` (open interest, open interest value).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TickerChange {
    pub mark_funding_changed: bool,
    pub open_interest_changed: bool,
}

impl TickerChange {
    /// No relevant field changed — nothing to emit.
    pub fn is_empty(self) -> bool {
        !self.mark_funding_changed && !self.open_interest_changed
    }
}

/// Merge one ticker message onto `state`, returning which deriv row group(s)
/// changed as a result.
///
/// `type: "snapshot"` replaces `state` wholesale (see `BybitTickerState`'s
/// `From` impl doc). `type: "delta"` (or anything else — defensive default,
/// though the wire protocol only ever sends these two) overwrites only the
/// fields present in `msg.data`, per Bybit's "absent = unchanged" rule.
pub fn merge_ticker_message(state: &mut BybitTickerState, msg: &BybitTickerMsg) -> TickerChange {
    let before = *state;

    if msg.msg_type == "snapshot" {
        *state = BybitTickerState::from(&msg.data);
    } else {
        state.apply_delta(&msg.data);
    }

    TickerChange {
        mark_funding_changed: before.mark_price != state.mark_price
            || before.index_price != state.index_price
            || before.funding_rate != state.funding_rate
            || before.next_funding_time != state.next_funding_time,
        open_interest_changed: before.open_interest != state.open_interest
            || before.open_interest_value != state.open_interest_value,
    }
}

/// Build the deriv rows to emit from the *merged* state, gated by `change`.
///
/// Emit boundary (the design choice `specs/bybit-collection.md`/the
/// implementation plan explicitly leave to this module): a row is built only
/// when its backing field group actually changed, not on every message —
/// avoids emitting duplicate `MarkFunding`/`OpenInterest` rows for ticker
/// noise fathom doesn't persist (`volume24h`, `bid1Price`, ...; those fields
/// aren't even parsed, see `BybitTickerFields`) and avoids emitting a row for
/// a group whose fields simply weren't touched by this particular delta.
/// Additionally, even a "changed" group only emits once its required fields
/// (`mark_price`+`funding_rate` for `MarkFunding`, `open_interest` for
/// `OpenInterest`) have actually been seen at least once — a delta touching
/// only, say, `next_funding_time` before any snapshot ever supplied a mark
/// price can't produce a valid row yet.
pub fn build_deriv_events(
    exchange: &str,
    symbol: &str,
    ts_us: i64,
    state: &BybitTickerState,
    change: TickerChange,
) -> (Option<MarkFunding>, Option<OpenInterest>) {
    let mark_funding = change
        .mark_funding_changed
        .then(|| {
            let mark_px = state.mark_price?;
            let funding_rate = state.funding_rate?;
            Some(MarkFunding {
                timestamp_us: ts_us,
                exchange: exchange.to_string(),
                symbol: symbol.to_string(),
                mark_px,
                index_px: state.index_price,
                funding_rate,
                next_funding_ts: state.next_funding_time,
            })
        })
        .flatten();

    let open_interest = change
        .open_interest_changed
        .then(|| {
            state.open_interest.map(|oi_base| OpenInterest {
                timestamp_us: ts_us,
                exchange: exchange.to_string(),
                symbol: symbol.to_string(),
                oi_base,
                oi_quote: state.open_interest_value,
            })
        })
        .flatten();

    (mark_funding, open_interest)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn msg(msg_type: &str, data: serde_json::Value) -> BybitTickerMsg {
        BybitTickerMsg {
            msg_type: msg_type.to_string(),
            data: serde_json::from_value(data).unwrap(),
        }
    }

    /// Real-shaped snapshot: full field set, numeric strings (Bybit's usual
    /// encoding), plus 24h-stats noise that must be ignored/dropped.
    fn snapshot_json() -> serde_json::Value {
        serde_json::json!({
            "symbol": "BTCUSDT",
            "markPrice": "43001.20",
            "indexPrice": "43000.50",
            "fundingRate": "0.0001",
            "nextFundingTime": "1700006400000",
            "openInterest": "1234.567",
            "openInterestValue": "53112345.6",
            "lastPrice": "43001.00",
            "volume24h": "98765.4",
            "bid1Price": "43000.9"
        })
    }

    #[test]
    fn test_snapshot_seeds_full_state_and_emits_both_rows() {
        let mut state = BybitTickerState::default();
        let change = merge_ticker_message(&mut state, &msg("snapshot", snapshot_json()));

        assert_eq!(state.mark_price, Some(43001.20));
        assert_eq!(state.index_price, Some(43000.50));
        assert_eq!(state.funding_rate, Some(0.0001));
        assert_eq!(state.next_funding_time, Some(1_700_006_400_000));
        assert_eq!(state.open_interest, Some(1234.567));
        assert_eq!(state.open_interest_value, Some(53_112_345.6));

        assert!(change.mark_funding_changed);
        assert!(change.open_interest_changed);

        let (mf, oi) = build_deriv_events("bybit_perp", "BTCUSDT", 1_000, &state, change);
        let mf = mf.expect("mark+funding present after snapshot");
        let oi = oi.expect("open interest present after snapshot");
        assert_eq!(mf.mark_px, 43001.20);
        assert_eq!(mf.index_px, Some(43000.50));
        assert_eq!(mf.funding_rate, 0.0001);
        assert_eq!(mf.next_funding_ts, Some(1_700_006_400_000));
        assert_eq!(oi.oi_base, 1234.567);
        assert_eq!(oi.oi_quote, Some(53_112_345.6));
    }

    /// Core spec behavior: a delta missing several fields must keep the prior
    /// values for the absent ones and update only what's present.
    #[test]
    fn test_delta_missing_fields_keeps_prior_values() {
        let mut state = BybitTickerState::default();
        merge_ticker_message(&mut state, &msg("snapshot", snapshot_json()));

        // Delta only carries fundingRate — everything else absent.
        let delta = serde_json::json!({ "fundingRate": "0.00015" });
        let change = merge_ticker_message(&mut state, &msg("delta", delta));

        assert_eq!(state.funding_rate, Some(0.00015), "present field updates");
        assert_eq!(
            state.mark_price,
            Some(43001.20),
            "absent field keeps prior value"
        );
        assert_eq!(
            state.index_price,
            Some(43000.50),
            "absent field keeps prior value"
        );
        assert_eq!(
            state.next_funding_time,
            Some(1_700_006_400_000),
            "absent field keeps prior value"
        );
        assert_eq!(
            state.open_interest,
            Some(1234.567),
            "absent field keeps prior value"
        );

        assert!(
            change.mark_funding_changed,
            "funding_rate is in the mark/funding group"
        );
        assert!(
            !change.open_interest_changed,
            "OI fields untouched by this delta"
        );
    }

    /// Emit boundary: a delta that changes only a dropped/unparsed field
    /// (volume24h etc. aren't even fields on `BybitTickerFields`) must not
    /// register as a change and must emit nothing.
    #[test]
    fn test_delta_with_only_ignored_fields_emits_nothing() {
        let mut state = BybitTickerState::default();
        merge_ticker_message(&mut state, &msg("snapshot", snapshot_json()));

        let noise_delta = serde_json::json!({
            "lastPrice": "43005.00",
            "volume24h": "99999.9",
            "bid1Price": "43004.0"
        });
        let change = merge_ticker_message(&mut state, &msg("delta", noise_delta));

        assert!(change.is_empty(), "no persisted field changed");
        let (mf, oi) = build_deriv_events("bybit_perp", "BTCUSDT", 2_000, &state, change);
        assert!(mf.is_none());
        assert!(oi.is_none());
    }

    /// Emit boundary: a delta changing only `openInterest` emits an
    /// `OpenInterest` row but no `MarkFunding` row.
    #[test]
    fn test_delta_changing_only_open_interest_emits_only_that_row() {
        let mut state = BybitTickerState::default();
        merge_ticker_message(&mut state, &msg("snapshot", snapshot_json()));

        let delta = serde_json::json!({ "openInterest": "1300.0" });
        let change = merge_ticker_message(&mut state, &msg("delta", delta));

        assert!(!change.mark_funding_changed);
        assert!(change.open_interest_changed);

        let (mf, oi) = build_deriv_events("bybit_perp", "BTCUSDT", 3_000, &state, change);
        assert!(mf.is_none(), "mark/funding group untouched by this delta");
        let oi = oi.expect("OI group changed");
        assert_eq!(oi.oi_base, 1300.0);
        assert_eq!(
            oi.oi_quote,
            Some(53_112_345.6),
            "openInterestValue unchanged, keeps prior merged value"
        );
    }

    /// A delta that resends the exact same value for a field is not a
    /// "change" — comparison is against the merged state's prior value, not
    /// against "was this field present in the message".
    #[test]
    fn test_delta_repeating_same_value_is_not_a_change() {
        let mut state = BybitTickerState::default();
        merge_ticker_message(&mut state, &msg("snapshot", snapshot_json()));

        let delta = serde_json::json!({ "fundingRate": "0.0001" });
        let change = merge_ticker_message(&mut state, &msg("delta", delta));

        assert!(change.is_empty(), "same value resent — not a real change");
    }

    /// Server-initiated re-snapshot: a second `snapshot` message that omits a
    /// field must reset it to `None`, not inherit the pre-reconnect value —
    /// stale state must never survive a resync.
    #[test]
    fn test_second_snapshot_resets_fields_absent_from_it() {
        let mut state = BybitTickerState::default();
        merge_ticker_message(&mut state, &msg("snapshot", snapshot_json()));
        assert_eq!(state.open_interest, Some(1234.567));

        let resync_snapshot = serde_json::json!({
            "markPrice": "44000.0",
            "fundingRate": "0.0002"
        });
        merge_ticker_message(&mut state, &msg("snapshot", resync_snapshot));

        assert_eq!(state.mark_price, Some(44000.0));
        assert_eq!(state.funding_rate, Some(0.0002));
        assert_eq!(
            state.open_interest, None,
            "reset — a stale pre-reconnect OI must not survive a new snapshot"
        );
        assert_eq!(
            state.index_price, None,
            "reset — absent from the new snapshot"
        );
        assert_eq!(
            state.next_funding_time, None,
            "reset — absent from the new snapshot"
        );
    }

    /// Defensive: accept plain JSON numbers too, not only Bybit's usual
    /// numeric-string encoding.
    #[test]
    fn test_numeric_fields_as_plain_numbers() {
        let mut state = BybitTickerState::default();
        let data = serde_json::json!({
            "markPrice": 43001.2,
            "fundingRate": 0.0001,
            "nextFundingTime": 1_700_006_400_000_i64,
            "openInterest": 1234.5
        });
        let change = merge_ticker_message(&mut state, &msg("snapshot", data));

        assert_eq!(state.mark_price, Some(43001.2));
        assert_eq!(state.funding_rate, Some(0.0001));
        assert_eq!(state.next_funding_time, Some(1_700_006_400_000));
        assert_eq!(state.open_interest, Some(1234.5));
        assert!(change.mark_funding_changed);
        assert!(change.open_interest_changed);
    }

    /// Before any snapshot has ever seeded `mark_price`/`funding_rate`, a
    /// delta cannot produce a valid `MarkFunding` row even if it flags the
    /// group as "changed" (e.g. it supplies `next_funding_time` for the first
    /// time but no mark price has ever been seen).
    #[test]
    fn test_no_emit_before_required_fields_ever_seen() {
        let mut state = BybitTickerState::default();
        let first = serde_json::json!({ "nextFundingTime": "1700006400000" });
        let change = merge_ticker_message(&mut state, &msg("snapshot", first));

        assert!(
            change.mark_funding_changed,
            "next_funding_time is in the group"
        );
        let (mf, oi) = build_deriv_events("bybit_perp", "BTCUSDT", 1, &state, change);
        assert!(
            mf.is_none(),
            "mark_price/funding_rate never seen — can't build a valid row yet"
        );
        assert!(oi.is_none());
    }
}
