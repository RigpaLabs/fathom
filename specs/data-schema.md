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

## 1s snapshot (`Snapshot1s`)

1 row/sec/symbol, 64 columns (`src/schema.rs`):

- Identity: `ts_us`, `exchange`, `symbol`
- Book: `bid_px_0..9`, `ask_px_0..9`, `bid_sz_0..9`, `ask_sz_0..9` (top-10)
- Derived (computed at capture for convenience): `mid_px`, `microprice`, `spread_bps`, `imbalance_l1/l5/l10`, `bid_depth_l5/l10`, `ask_depth_l5/l10`, `ofi_l1`, `churn_bid`, `churn_ask`, `intra_sigma`, `open_px`, `close_px`, `n_events`
- Trades aggregate: `volume_delta`, `buy_vol`, `sell_vol`, `trade_count`

Field caveats (read before using):
- `intra_sigma` — event-weighted population stddev of **mid price in quote units** within the second. It is not a return volatility and is not annualized (`src/accumulator.rs`).
- `n_events` — Binance: diff events; HL: snapshots received. Not comparable across exchanges.
- `buy_vol`/`sell_vol`/`volume_delta`/`trade_count` — populated for HL only until Binance trades land ([trades-feed.md](trades-feed.md)); **always 0 for Binance today**.
- `ofi_l1`, `churn_*` — summed over the 1s window, reset at flush.

## Wire format (NATS payloads)

`wire_encode` = `[WIRE_VERSION: 1 byte][bincode of the struct]` (`crates/fathom-types/src/lib.rs`). Consumers must check the version byte. Payloads carry the same structs as Parquet rows — no truncation relative to files.
