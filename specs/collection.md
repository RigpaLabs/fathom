# Collection — Exchanges, Channels, Capture Matrix

Status: **stable** (matrix rows marked otherwise)

## Connections

One WebSocket connection per configured `[[connections]]` entry (`config.toml`), one tokio task each (`src/main.rs`). All book state is in-memory; a restart re-seeds from snapshots.

| Exchange | Transport | Book model | Update cadence |
|---|---|---|---|
| `binance_spot` | Combined stream `{sym}@depth@100ms` + `{sym}@aggTrade` | REST snapshot (limit 5000) + sequenced diffs applied to BTreeMap (`src/orderbook/mod.rs`) | 100 ms diffs + event-rate trades |
| `binance_perp` | Combined stream `{sym}@depth@100ms` + `{sym}@aggTrade` + `{sym}@markPrice@1s` + `{sym}@forceOrder`; REST OI poll (60s) | REST snapshot (limit 1000) + sequenced diffs, `pu`-based sequencing | 100 ms diffs + event-rate trades + 1s mark/funding |
| `hyperliquid` | Single WS, subscribe per symbol: `l2Book` (`nSigFigs: 5`) + `trades` + `activeAssetCtx` | Full snapshot each message — no diff protocol, no gap detection needed | ~500 ms snapshots |
| `bybit_spot` | WS `{"op":"subscribe"}` after connect: `orderbook.1000.{sym}` + `publicTrade.{sym}`, batched ≤10 args/request (spot's per-request cap) | WS-native `type:"snapshot"` (initial sync + server-initiated resync) + `type:"delta"` sequenced by `u`, gap-checked client-side (`src/connection/bybit.rs::check_orderbook_gap`) — applied to the shared `OrderBook` (`src/orderbook/mod.rs`), no REST snapshot call | 200 ms diffs (`orderbook.1000` cadence) + event-rate trades |
| `bybit_perp` | Same as `bybit_spot` plus `tickers.{sym}` + `allLiquidation.{sym}`, one `subscribe` message (24 args, under cap) | Same book model as `bybit_spot`, plus snapshot+delta ticker-merge state (`src/connection/bybit_ticker.rs`) for funding/mark/OI; no REST OI poll | 200 ms diffs + event-rate trades + 100 ms ticker push |
| `dydx` | **deprecated — removal in progress** | — | — |

See [bybit-collection.md](bybit-collection.md) for Bybit's full channel/message schemas and gap-detection semantics.

Reference symbol set (production `config.toml`): 6 Binance spot, 6 Binance perp, 7 Hyperliquid.
Bybit (`bybit_spot`/`bybit_perp`, 6 symbols each — same reference set) is wired in code and
`config.toml.example`; production infra config rollout is tracked separately
(`docs/plans/bybit-collector-implementation.md` WP4).

## Gap semantics (Binance)

See ADR-004. Spot and perp differ — getting this wrong causes spurious reconnects:

- **Spot:** valid continuation is `U == last_update_id + 1`.
- **Perp (USDM):** diff events carry `pu` (previous final update id). Valid continuation is `pu == last_update_id`; `pu > last_update_id` → gap (resync); `pu < last_update_id` → stale, drop.

Both implemented in `src/orderbook/mod.rs::apply_diff`. On gap: REST re-snapshot. Reconnect/gap counts tracked by `src/monitor.rs`.

## Capture matrix

What each exchange offers vs what fathom persists. "dropped" = arrives over the wire, discarded before any writer.

| Exchange | Channel / field | State |
|---|---|---|
| binance spot+perp | `depth@100ms` diff levels (all changed levels, full depth) | **collected** (raw + book) |
| binance spot+perp | `aggTrade` (price, qty, side, trade id) | **collected** ([trades-feed.md](trades-feed.md)) — trade tape (Parquet + NATS) and 1s `buy_vol`/`sell_vol`/`volume_delta`/`trade_count` |
| binance spot+perp | `bookTicker` (event-rate L1) | not subscribed — L1 available at 100ms from depth; revisit only with a use case |
| binance perp | `markPrice@1s` (mark + index + funding), `forceOrder` (liquidations), open interest (REST poll 60s) | **collected** ([derivatives-feeds.md](derivatives-feeds.md)) — deriv Parquet family + `FATHOM_DERIV` |
| hyperliquid | `l2Book` full snapshot depth | **collected** in full (raw + book) — raw writer persists all (price, size) levels (`src/connection/hyperliquid.rs::build_raw_diff`); 1s snapshot columns remain top-10 by design |
| hyperliquid | `l2Book` per-level order count `n` | **dropped** → planned: persist ([data-schema.md](data-schema.md) v2) — the raw row's bid/ask lists carry (price, size) pairs only; adding `n` changes the Parquet/NATS schema, unlike depth which was variable-length already |
| hyperliquid | `trades` — `px` (price) | **collected** ([trades-feed.md](trades-feed.md)) — full trade (px, sz, side, tid) persisted to the tape; 1s aggregates unchanged |
| hyperliquid | `bbo`, `candle` | not subscribed — derivable from l2Book/trades |
| hyperliquid | `activeAssetCtx` (funding, oracle px, mark px, OI) | **collected** ([derivatives-feeds.md](derivatives-feeds.md)) — one message → `MarkFunding` + `OpenInterest` rows |
| bybit spot+linear | `orderbook.1000` full depth levels (all changed levels) | **collected** (raw + book, same as Binance/HL) — see [bybit-collection.md](bybit-collection.md) |
| bybit spot+linear | `publicTrade` (price, qty, side, trade id) | **collected** — trade tape (Parquet + NATS) + 1s `buy_vol`/`sell_vol`/`volume_delta`/`trade_count`, same as existing venues — see [bybit-collection.md](bybit-collection.md) |
| bybit linear | `tickers` → funding rate, mark price, open interest | **collected** — deriv Parquet family + `FATHOM_DERIV`, via snapshot+delta ticker-state merge (`src/connection/bybit_ticker.rs`) — see [bybit-collection.md](bybit-collection.md) |
| bybit linear | `allLiquidation` | **collected** — deriv Parquet family + `FATHOM_DERIV` — see [bybit-collection.md](bybit-collection.md) |
| bybit spot+linear | `tickers` 24h-stats fields (`lastPrice`, `volume24h`, `bid1Price`, etc.) | **dropped** — no current use case, same "not subscribed / not persisted without a reason" discipline as Binance's unused `bookTicker` |
| bybit spot+linear | `publicTrade`'s `L` (tick direction), `BT`/`RPI` (block/RPI trade flags) | **dropped** — not used by any existing venue's trade tape either |

## Known gaps / quirks

- Reliability backlog (from TODO): per-symbol REST re-snapshot on gap instead of whole-WS reconnect; per-symbol WS connections.
- `depth_ms` config field is only honored by Binance adapters; the HL and Bybit adapters ignore it (hardcoded subscription params — Bybit hardcodes `orderbook.1000`).
