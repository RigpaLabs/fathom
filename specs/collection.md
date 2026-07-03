# Collection — Exchanges, Channels, Capture Matrix

Status: **stable** (matrix rows marked otherwise)

## Connections

One WebSocket connection per configured `[[connections]]` entry (`config.toml`), one tokio task each (`src/main.rs`). All book state is in-memory; a restart re-seeds from snapshots.

| Exchange | Transport | Book model | Update cadence |
|---|---|---|---|
| `binance_spot` | Combined stream `{sym}@depth@100ms` | REST snapshot (limit 5000) + sequenced diffs applied to BTreeMap (`src/orderbook/mod.rs`) | 100 ms diffs |
| `binance_perp` | Combined stream `{sym}@depth@100ms` | REST snapshot (limit 1000) + sequenced diffs, `pu`-based sequencing | 100 ms diffs |
| `hyperliquid` | Single WS, subscribe per symbol: `l2Book` (`nSigFigs: 5`) + `trades` | Full snapshot each message — no diff protocol, no gap detection needed | ~500 ms snapshots |
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
| binance spot+perp | `aggTrade` (price, qty, side, trade id) | **not subscribed → planned** ([trades-feed.md](trades-feed.md)). Until then 1s `buy_vol`/`sell_vol`/`volume_delta`/`trade_count` are **always 0** for Binance |
| binance spot+perp | `bookTicker` (event-rate L1) | not subscribed — L1 available at 100ms from depth; revisit only with a use case |
| binance perp | `markPrice` (mark + funding), `forceOrder` (liquidations), open interest | **not subscribed → planned** ([derivatives-feeds.md](derivatives-feeds.md)) |
| hyperliquid | `l2Book` full snapshot depth | book: full; **raw writer: truncated to top-10** (`src/connection/hyperliquid.rs` `.take(10)`) → **planned: persist full depth** |
| hyperliquid | `l2Book` per-level order count `n` | **dropped** → planned: persist ([data-schema.md](data-schema.md) v2) |
| hyperliquid | `trades` — `px` (price) | **dropped** (only size+side consumed) → planned ([trades-feed.md](trades-feed.md)) |
| hyperliquid | `bbo`, `candle` | not subscribed — derivable from l2Book/trades |
| hyperliquid | `activeAssetCtx` (funding, oracle px, OI) | **not subscribed → planned** ([derivatives-feeds.md](derivatives-feeds.md)) |

## Known gaps / quirks

- HL raw truncation to top-10 is silent and asymmetric vs Binance full-diff rows — fix planned (schema v2).
- Reliability backlog (from TODO): per-symbol REST re-snapshot on gap instead of whole-WS reconnect; per-symbol WS connections.
- `depth_ms` config field is only honored by Binance adapters; the HL adapter ignores it (hardcoded subscription params).
