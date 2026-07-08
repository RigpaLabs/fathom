# Trades Feed — Raw Trade Tape

Status: **stable**

## Problem (solved)

Fathom previously kept no individual trades and no trade prices anywhere:
- Binance connections didn't subscribe to trades → 1s `buy_vol`/`sell_vol`/`volume_delta`/`trade_count` were 0.
- Hyperliquid received trades (for the 1s aggregate) but the `px` field was parsed and discarded.

Anything price-by-volume (volume profile, VWAP, trade-size distributions) was impossible to reconstruct. Per the design principle (capture raw, compute later), the tape is now persisted.

## Sources

| Exchange | Channel | Notes |
|---|---|---|
| binance spot | `{sym}@aggTrade` | Aggregated by taker order — right granularity for flow analysis; `m` flag gives side |
| binance perp | `{sym}@aggTrade` | Same |
| hyperliquid | `trades` (already subscribed) | `px` now persisted; message carries px, sz, side, time, tid |

## Schema (`RawTrade` — `crates/fathom-types/src/lib.rs::RawTrade`)

| Column | Type | Notes |
|---|---|---|
| `timestamp_us` | i64 | Exchange trade time (µs). Binance: `T` (trade time), not `E` (event time) |
| `exchange` | utf8 | |
| `symbol` | utf8 | |
| `trade_id` | i64 | Binance aggTrade `a` / HL `tid` |
| `price` | f64 | |
| `qty` | f64 | Base units |
| `is_buyer_maker` | bool | Binance `m`; HL side mapped: `"A"` (taker sold) → true, `"B"` (taker bought) → false — comparable across exchanges |

Side semantics: `is_buyer_maker = true` ⇔ the taker **sold** (aggressive sell into resting bid). The 1s accumulator feeds `is_buy = !is_buyer_maker`.

## Persistence

- Parquet: `{data_dir}/trades/{exchange}/{symbol}/{date}/trades_HHMM_HHMM.parquet` — hourly rotation, same writer pattern as raw depth (`src/writer/trades.rs::run_trades_writer`; Arrow schema in `src/schema.rs::trades_schema`).
- NATS: `fathom.v1.{exchange}.{symbol}.trade` on stream `FATHOM_TRADES` (file storage, 24 h / 200 MB — see [nats-streams.md](nats-streams.md)).
- 1s aggregates (`buy_vol` etc.) keep working and are now populated for Binance too — the aggTrade path feeds the same accumulator.

## Implementation notes

- `binance_spot` combined stream subscribes `{sym}@depth@{ms}ms/{sym}@aggTrade` per symbol (`src/exchange/binance_spot.rs::ws_url`). `binance_perp` subscribes aggTrade on its separate `/market/stream` connection (`src/exchange/binance_perp.rs::market_ws_url` — depth stays on `ws_url`/`/public/stream`; see `specs/collection.md` for why the two are split). Events are dispatched by stream-name suffix regardless of which physical connection they arrive on (`src/connection/binance.rs::dispatch_non_depth`); aggTrade handled in both the sync-phase replay and the main event loop (`::handle_agg_trade`, parse in `::agg_trade_to_raw`).
- Hyperliquid: `src/connection/hyperliquid.rs::build_raw_trade` (side-mapping comment lives there). The pre-existing `accumulate_trade` path is unchanged.
- Channel: `trade_tx` broadcast (`src/main.rs`), drop-on-backpressure per ADR-002; NATS publisher best-effort (`src/nats_sink.rs::publish_trades`).
- Trades writer is supervised as fatal, same as the raw/snap writers (`src/main.rs::wait_for_shutdown_or_writer_exit`).

## Acceptance

- Binance 1s rows show non-zero `trade_count` on active symbols.
- Sum of tape qty over a minute ≈ 1s aggregate sums for the same window.
- Tape survives writer restart without dropping the open hour (same rotation guarantees as raw depth).
