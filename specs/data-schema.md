# Data Schema — Parquet & Wire Format

Status: **stable** (v2 additions marked planned)

Canonical structs live in `crates/fathom-types` (public crate, used by downstream consumers as a git dependency). Schema evolution rules: `docs/schema-versioning.md`.

## Raw depth diff (`RawDiff`)

One row per WS depth event. Same schema for all exchanges (`src/schema.rs`):

| Column | Type | Notes |
|---|---|---|
| `timestamp_us` | i64 | Exchange event time (µs) |
| `exchange` | utf8 | `binance_spot` / `binance_perp` / `hyperliquid` |
| `symbol` | utf8 | Exchange-native symbol |
| `seq_id` | i64 | Binance: `u` (final update id); HL: `book.time` (ms) |
| `prev_seq_id` | i64 | Binance: `U` (first update id); HL: `0` (snapshots have no sequence) |
| `bid_prices`, `bid_qtys` | List<f64> | Binance: changed levels only (diff); HL: snapshot levels |
| `ask_prices`, `ask_qtys` | List<f64> | same |

Semantics per exchange:
- **Binance**: a row is a *diff* — levels with qty 0 mean deletion. Reconstructing the book requires replaying diffs onto a snapshot.
- **Hyperliquid**: a row is a *full snapshot* (full depth as sent by the exchange) — each row stands alone.

### Planned (schema v2)

- HL: per-level order count `n` (`num_orders` List<i64>, null for Binance)

## Raw trade (`RawTrade`)

One row per trade — Binance aggTrade (spot + perp) and Hyperliquid `trades`. Struct: `crates/fathom-types/src/lib.rs::RawTrade`; Arrow schema: `src/schema.rs::trades_schema`. Full contract: [trades-feed.md](trades-feed.md).

| Column | Type | Notes |
|---|---|---|
| `timestamp_us` | i64 | Exchange trade time (µs); Binance `T`, HL `time` |
| `exchange` | utf8 | `binance_spot` / `binance_perp` / `hyperliquid` |
| `symbol` | utf8 | Exchange-native symbol |
| `trade_id` | i64 | Binance aggTrade `a` / HL `tid` |
| `price` | f64 | |
| `qty` | f64 | Base units |
| `is_buyer_maker` | bool | true ⇔ taker sold. Binance `m`; HL side `"A"` → true, `"B"` → false |

Files: `{data_dir}/trades/{exchange}/{symbol}/{date}/trades_HHMM_HHMM.parquet` (hourly rotation, `src/writer/trades.rs`).

## Derivatives feeds (`MarkFunding`, `OpenInterest`, `Liquidation`)

Structs: `crates/fathom-types/src/lib.rs`; Arrow schemas: `src/schema.rs::{mark_funding_schema, open_interest_schema, liquidation_schema}`. Full contract: [derivatives-feeds.md](derivatives-feeds.md).

**`MarkFunding`** — one row per markPrice event (Binance, 1/s) / activeAssetCtx message (HL, ~1/s). Mark+funding arrive together on both venues, so mark is folded in — there is no separate mark feed.

| Column | Type | Notes |
|---|---|---|
| `timestamp_us` | i64 | Binance `E` (µs); HL: receipt time (message has no exchange ts) |
| `exchange`, `symbol` | utf8 | |
| `mark_px` | f64 | Binance `p` / HL `markPx` |
| `index_px` | f64 (nullable) | Binance `i` (index) / HL `oraclePx` |
| `funding_rate` | f64 | As sent by venue (per funding interval, not annualized) |
| `next_funding_ts` | i64 (nullable, µs) | Binance `T`; null for HL (hourly funding, no discrete ts) |

**`OpenInterest`** — Binance: REST poll 60 s; HL: from activeAssetCtx.

| Column | Type | Notes |
|---|---|---|
| `timestamp_us` | i64 | Binance REST `time` (µs); HL: receipt time |
| `exchange`, `symbol` | utf8 | |
| `oi_base` | f64 | Base units |
| `oi_quote` | f64 (nullable) | Null on both current venues (base-only sources) |

**`Liquidation`** — Binance perp `forceOrder` only (HL has no public liquidation channel here). Note: Binance streams at most one forceOrder per symbol per second — a signal, not a complete tape.

| Column | Type | Notes |
|---|---|---|
| `timestamp_us` | i64 | `o.T` (µs) |
| `exchange`, `symbol` | utf8 | |
| `side` | utf8 | `o.S` — `SELL` = long liquidated |
| `price` | f64 | `o.ap` (average fill price) |
| `qty` | f64 | `o.q`, base units |

Files: `{data_dir}/deriv/{exchange}/{symbol}/{date}/{funding|oi|liq}_HHMM_HHMM.parquet` (hourly rotation, `src/writer/deriv.rs`).

## 1s snapshot (`Snapshot1s`)

1 row/sec/symbol, 64 columns (`src/schema.rs`):

- Identity: `ts_us`, `exchange`, `symbol`
- Book: `bid_px_0..9`, `ask_px_0..9`, `bid_sz_0..9`, `ask_sz_0..9` (top-10)
- Derived (computed at capture for convenience): `mid_px`, `microprice`, `spread_bps`, `imbalance_l1/l5/l10`, `bid_depth_l5/l10`, `ask_depth_l5/l10`, `ofi_l1`, `churn_bid`, `churn_ask`, `intra_sigma`, `open_px`, `close_px`, `n_events`
- Trades aggregate: `volume_delta`, `buy_vol`, `sell_vol`, `trade_count`

Field caveats (read before using):
- `intra_sigma` — event-weighted population stddev of **mid price in quote units** within the second. It is not a return volatility and is not annualized (`src/accumulator.rs`).
- `n_events` — Binance: diff events; HL: snapshots received. Not comparable across exchanges.
- `buy_vol`/`sell_vol`/`volume_delta`/`trade_count` — populated for all exchanges: HL from `trades`, Binance from `aggTrade` ([trades-feed.md](trades-feed.md)). Buy/sell attribution is by taker side.
- `ofi_l1`, `churn_*` — summed over the 1s window, reset at flush.

## Wire format (NATS payloads)

`wire_encode` = `[WIRE_VERSION: 1 byte][bincode of the struct]` (`crates/fathom-types/src/lib.rs`). Consumers must check the version byte. Payloads carry the same structs as Parquet rows — no truncation relative to files. The format has no type discriminant: the NATS subject suffix identifies the struct (see [nats-streams.md](nats-streams.md)).
