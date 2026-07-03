# Derivatives Feeds — Funding, Mark/Oracle, Open Interest, Liquidations

Status: **planned**

Low-rate, high-value context feeds for perp venues. Tiny volume (~single MB/day per feed), one Parquet family + one NATS stream for all four.

## Sources

| Exchange | Channel | Provides |
|---|---|---|
| binance perp | `{sym}@markPrice@1s` | mark price, index price, funding rate + next funding time |
| binance perp | `{sym}@forceOrder` | liquidation orders (side, price, qty, time) |
| binance perp | REST poll `openInterest` (no WS) | open interest per symbol (poll ~1/min) |
| hyperliquid | `activeAssetCtx` (subscribe per coin) | funding, oracle px, mark px, open interest — one channel covers all four |

## Schemas (new structs in `fathom-types`)

**`MarkFunding`**: `timestamp_us, exchange, symbol, mark_px, index_px (nullable), funding_rate, next_funding_ts (nullable)`

**`OpenInterest`**: `timestamp_us, exchange, symbol, oi_base, oi_quote (nullable)`

**`Liquidation`**: `timestamp_us, exchange, symbol, side, price, qty`

## Persistence

- Parquet: `{data_dir}/deriv/{exchange}/{symbol}/{date}/{feed}.parquet` — daily files (rates are low; hourly rotation unnecessary).
- NATS: `fathom.v1.{exchange}.{symbol}.{funding|mark|oi|liq}` on stream `FATHOM_DERIV`.

Mark+funding arrive together on both venues → one `MarkFunding` row per event, not separate feeds.

## Acceptance

- Funding rate visible for every perp symbol with < 1 min staleness.
- Liquidation rows appear during volatile periods (verify against exchange UI).
- OI poll degrades gracefully (REST error → log + retry, never affects WS collection).
