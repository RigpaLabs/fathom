# Derivatives Feeds — Funding, Mark/Oracle, Open Interest, Liquidations

Status: **stable**

Low-rate, high-value context feeds for perp venues. Tiny volume (~single MB/day per feed), one Parquet family + one NATS stream for all four.

## Sources

| Exchange | Channel | Provides | Implementation |
|---|---|---|---|
| binance perp | `{sym}@markPrice@1s` | mark price, index price, funding rate + next funding time | `src/connection/binance.rs::mark_price_to_funding` (dispatch by stream suffix, sync replay + event loop); subscribed on the `/market/stream` connection, `src/exchange/binance_perp.rs::market_ws_url` (depth-only `ws_url` is a separate `/public/stream` connection — see `specs/collection.md`) |
| binance perp | `{sym}@forceOrder` | liquidation orders (side, avg price `ap`, qty, time) | `src/connection/binance.rs::force_order_to_liquidation`; also subscribed on `market_ws_url` |
| binance perp | REST poll `/fapi/v1/openInterest` (no WS) | open interest per symbol, polled every 60 s | `src/connection/binance.rs::poll_open_interest`, URL from `ExchangeAdapter::open_interest_url` (None for spot/HL) |
| hyperliquid | `activeAssetCtx` (subscribe per coin) | funding, oracle px, mark px, open interest — one channel covers all four | `src/connection/hyperliquid.rs::asset_ctx_to_events` — each message emits one `MarkFunding` + one `OpenInterest` |

Spot subscribes none of these (`binance_spot` ws_url unchanged). Binance liquidations note: since 2021 Binance streams at most one forceOrder per symbol per second — the feed is a liquidation *signal*, not a complete tape.

## Schemas (structs in `crates/fathom-types/src/lib.rs`)

**`MarkFunding`**: `timestamp_us, exchange, symbol, mark_px, index_px (nullable), funding_rate, next_funding_ts (nullable, µs)`

**`OpenInterest`**: `timestamp_us, exchange, symbol, oi_base, oi_quote (nullable)`

**`Liquidation`**: `timestamp_us, exchange, symbol, side ("BUY"/"SELL"), price, qty`

Field mapping per venue:
- **Binance markPrice**: `E`→timestamp (ms→µs), `p`→mark_px, `i`→index_px, `r`→funding_rate, `T`→next_funding_ts (ms→µs).
- **Binance forceOrder**: `o.T`→timestamp, `o.S`→side, `o.ap`→price (average fill, not order price), `o.q`→qty.
- **Binance openInterest REST**: `time`→timestamp, `openInterest`→oi_base; oi_quote None (endpoint is base-only).
- **HL activeAssetCtx**: `markPx`→mark_px, `oraclePx`→index_px, `funding`→funding_rate, `openInterest`→oi_base. Timestamp = receipt time (message carries no exchange ts). `next_funding_ts` None (HL funds hourly, no discrete next-funding time in the feed). oi_quote None.

## Persistence

- Parquet: `{data_dir}/deriv/{exchange}/{symbol}/{date}/{feed}_HHMM_HHMM.parquet`, `feed` ∈ `funding|oi|liq` — hourly-rotated files (`raw_rotate_hours`, shared with raw/trades/1s), via the temp-file-then-rename `Bucket` pattern in `src/writer/rotation.rs`. Writer: `src/writer/deriv.rs::run_deriv_writer` — bucket rollover on event-time hour/date boundary (raw/trades pattern) + 5 s buffer flush with forced row groups; a periodic tick additionally force-rotates a bucket whose window has elapsed with no new events, so a sparse feed (e.g. `liq`) can't hold a file open indefinitely. Bounds restart data loss to a single open bucket rather than a full day (docs/adr/005). Arrow schemas: `src/schema.rs::{mark_funding_schema, open_interest_schema, liquidation_schema}`.
- NATS: stream `FATHOM_DERIV` (file storage, 24 h / 200 MB), subjects `fathom.v1.{exchange}.{symbol}.{funding|oi|liq}` — `src/nats_sink.rs::publish_deriv`. One subject per struct: `.funding` = `MarkFunding`, `.oi` = `OpenInterest`, `.liq` = `Liquidation`.

**Decision — `.mark` is folded into `.funding`:** mark+funding arrive together in one exchange event on both venues, so a single `MarkFunding` row is published once on the `.funding` subject. There is no separate `.mark` subject or feed file — consumers wanting mark price subscribe `.funding`.

Internally all three structs share one broadcast channel (`DerivEvent` enum, `src/writer/deriv.rs`); the wire format has no type discriminant, so the NATS publisher routes each variant to its own subject and encodes the inner struct.

## Liveness

Derivatives events do **not** feed `record_event` depth-liveness (same decision as trades): a connection receiving only mark prices while depth is stalled must still look stale to the monitor.

## Acceptance

- Funding rate visible for every perp symbol with < 1 min staleness.
- Liquidation rows appear during volatile periods (verify against exchange UI).
- OI poll degrades gracefully (REST error → warn + retry next tick, never affects WS collection — the poll task lives outside the reconnect loop).
