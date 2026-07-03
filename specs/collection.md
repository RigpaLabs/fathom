# Collection — Exchanges, Channels, Capture Matrix

Status: **stable** (matrix rows marked otherwise)

## Connections

One WebSocket connection per configured `[[connections]]` entry (`config.toml`), one tokio task each (`src/main.rs`). All book state is in-memory; a restart re-seeds from snapshots.

| Exchange | Transport | Book model | Update cadence |
|---|---|---|---|
| `binance_spot` | Combined stream `{sym}@depth@100ms` + `{sym}@aggTrade` | REST snapshot (limit 5000) + sequenced diffs applied to BTreeMap (`src/orderbook/mod.rs`) | 100 ms diffs + event-rate trades |
| `binance_perp` | Combined stream `{sym}@depth@100ms` + `{sym}@aggTrade` + `{sym}@markPrice@1s` + `{sym}@forceOrder`; REST OI poll (60s) | REST snapshot (limit 1000) + sequenced diffs, `pu`-based sequencing | 100 ms diffs + event-rate trades + 1s mark/funding |
| `hyperliquid` | Single WS, subscribe per symbol: `l2Book` (`nSigFigs: 5`) + `trades` + `activeAssetCtx` | Full snapshot each message — no diff protocol, no gap detection needed | ~500 ms snapshots |
| `dydx` | **deprecated — removal in progress** | — | — |

Reference symbol set (production `config.toml`): 6 Binance spot, 6 Binance perp, 7 Hyperliquid.

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

## Known gaps / quirks

- Reliability backlog (from TODO): per-symbol REST re-snapshot on gap instead of whole-WS reconnect; per-symbol WS connections.
- `depth_ms` config field is only honored by Binance adapters; the HL adapter ignores it (hardcoded subscription params).
