# Trades Feed — Raw Trade Tape

Status: **planned**

## Problem

Fathom currently keeps no individual trades and no trade prices anywhere:
- Binance connections don't subscribe to trades at all → 1s `buy_vol`/`sell_vol`/`volume_delta`/`trade_count` are 0.
- Hyperliquid receives trades (for the 1s aggregate) but the `px` field is parsed and discarded.

Anything price-by-volume (volume profile, VWAP, trade-size distributions) is impossible to reconstruct from the current data. Per the design principle (capture raw, compute later), the tape must be persisted.

## Sources

| Exchange | Channel | Notes |
|---|---|---|
| binance spot | `{sym}@aggTrade` | Aggregated by taker order — right granularity for flow analysis; `m` flag gives side |
| binance perp | `{sym}@aggTrade` | Same |
| hyperliquid | `trades` (already subscribed) | Stop dropping `px`; message carries px, sz, side, time, tid |

## Schema (`RawTrade`, new struct in `fathom-types`)

| Column | Type | Notes |
|---|---|---|
| `timestamp_us` | i64 | Exchange trade time (µs) |
| `exchange` | utf8 | |
| `symbol` | utf8 | |
| `trade_id` | i64 | aggTrade id / HL tid |
| `price` | f64 | |
| `qty` | f64 | Base units |
| `is_buyer_maker` | bool | Binance `m`; HL side mapped (`B`/`A` → taker side) |

## Persistence

- Parquet: `{data_dir}/trades/{exchange}/{symbol}/{date}/trades_HHMM_HHMM.parquet` — hourly rotation, same writer pattern as `src/writer/raw.rs`.
- NATS: `fathom.v1.{exchange}.{symbol}.trade` on stream `FATHOM_TRADES` (file storage; retention sized like FATHOM_DEPTH).
- 1s aggregates (`buy_vol` etc.) keep working and become populated for Binance too — same accumulator path fed by the new subscription.

## Acceptance

- Binance 1s rows show non-zero `trade_count` on active symbols.
- Sum of tape qty over a minute ≈ 1s aggregate sums for the same window.
- Tape survives writer restart without dropping the open hour (same rotation guarantees as raw depth).
