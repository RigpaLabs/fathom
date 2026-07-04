# Bybit v5 Collection — Channels, Gap Semantics, Capture Matrix

Status: **planned** (spec only, no implementation yet — see
[`docs/plans/bybit-collector-implementation.md`](../docs/plans/bybit-collector-implementation.md))

Sources: [Bybit V5 orderbook](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook),
[trade](https://bybit-exchange.github.io/docs/v5/websocket/public/trade),
[ticker](https://bybit-exchange.github.io/docs/v5/websocket/public/ticker),
[all-liquidation](https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation),
[connect](https://bybit-exchange.github.io/docs/v5/ws/connect). Verified 2026-07-04 against
current public docs — re-check before implementing in case Bybit has changed anything since.

## Why Bybit, why now

Third venue after Binance + Hyperliquid. Same 6 reference symbols (BTC/ETH/SOL/XRP/DOGE/BNB),
spot + linear perp — same shape as the existing `binance_spot`/`binance_perp` split. Goal:
maximum raw depth capture, same philosophy as the rest of fathom ("собираем максимум сырых
данных" — see `rigpa/docs/roadmap.md`).

## Connections

Two separate WebSocket connections (category-scoped base URLs, like Binance's spot/perp split):

| Exchange name | Category | WS base URL (mainnet) | Book model |
|---|---|---|---|
| `bybit_spot` | `spot` | `wss://stream.bybit.com/v5/public/spot` | WS-native snapshot + sequenced deltas |
| `bybit_perp` | `linear` | `wss://stream.bybit.com/v5/public/linear` | same, plus funding/OI/liquidations |

**Architecturally different from both existing patterns** — worth stating explicitly since it
changes how the connection task is shaped:

- **Binance**: WS gives only diffs; the book snapshot comes from a separate REST call, then the
  connection task buffers WS diffs during the REST round-trip and replays them once synced
  (`src/connection/binance.rs`, "buffer replay" sync phase).
- **Hyperliquid**: WS gives a full snapshot on *every* message — no diff protocol, no gap
  detection, no REST call at all.
- **Bybit**: WS gives the initial book as a `type: "snapshot"` message *over the same WS
  connection*, immediately after subscribing — no REST call needed for the initial sync. All
  subsequent messages are `type: "delta"`. If the server needs to resync a client (e.g. after a
  brief server-side hiccup), it pushes a fresh `type: "snapshot"` message on the wire, unprompted
  — the client just watches the `type` field and resets when it sees `"snapshot"` again after the
  first one. **No REST snapshot fetch, no buffer-replay sync phase.** Reuses the existing
  `OrderBook::apply_snapshot`/`apply_diff` (`src/orderbook/mod.rs`) unchanged — those are already
  generic over `SnapshotMsg`/`DepthDiff`, no Bybit-specific book-model code needed there.

## Channels / topics

Subscribe message: `{"op": "subscribe", "args": ["<topic1>", "<topic2>", ...]}`. **Per-request
arg cap: spot allows at most 10 topics per `subscribe` request** (linear/option are far higher; the
~21000 figure Bybit cites is the message-length limit in bytes, not an arg count). Spot carries 2
topics/symbol (`orderbook` + `publicTrade` — no ticker/liq on spot), so 6 symbols = 12 args, over
the spot cap → the **spot connection must split its subscription across multiple `subscribe`
messages** (batch ≤10 args each) on the same socket. Linear (4 topics/symbol = 24 args) is under
its cap and can subscribe in one message. Heartbeat: send `{"op": "ping"}` about every 20s (Bybit's
recommended interval); a socket with no data and no ping-pong is dropped after ~10 min idle — in
practice data keeps it busy, but the periodic ping is still the required liveness keepalive.

| Channel | Topic | Category | Purpose |
|---|---|---|---|
| Orderbook | `orderbook.{depth}.{symbol}` | spot, linear | Book snapshot + deltas |
| Public trade | `publicTrade.{symbol}` | spot, linear | Trade tape |
| Ticker | `tickers.{symbol}` | linear only (perp deriv data) | Funding rate, mark price, open interest |
| All liquidations | `allLiquidation.{symbol}` | linear only | Liquidation events (batched, ~500ms window) |

`{symbol}` uses the same base+quote concatenation Binance already uses (e.g. `BTCUSDT`) — the
existing 6-symbol reference set should map directly, but **confirm against Bybit's live symbol
list at implementation time** (`GET /v5/market/instruments-info?category=spot|linear`) rather than
assuming — exchange symbol naming quirks (delistings, contract-type suffixes) are exactly the kind
of thing that looks obvious until it isn't.

### Orderbook depth level — pick the deepest, matching fathom's "maximum data" philosophy

Available depth levels (same set for spot and linear): **1, 50, 200, 1000**, with push frequency
tied to the chosen level (10ms / 20ms / 100ms / 200ms respectively) — **unlike Binance's
`depth_ms` config field, which selects push frequency independently of book breadth, Bybit
couples depth-breadth and update-frequency into one choice.** `config.toml`'s existing `depth_ms`
field does not map cleanly onto this and should be **ignored by the Bybit adapter**, same
precedent as the existing Hyperliquid adapter (`specs/collection.md`'s "Known gaps / quirks":
"`depth_ms` config field is only honored by Binance adapters"). Recommendation: hardcode
`orderbook.1000.{symbol}` (deepest available, 200ms cadence) for both spot and linear — deepest
book wins over higher frequency, consistent with how Hyperliquid's full-depth `l2Book` and
Binance's un-truncated diff stream are already the deepest thing each venue offers.

## Message schemas

### Orderbook (`orderbook.1000.{symbol}`)

Both `snapshot` and `delta` messages share this shape:

```json
{
  "topic": "orderbook.1000.BTCUSDT",
  "type": "snapshot",       // or "delta"
  "ts": 1700000000000,      // system timestamp, ms
  "cts": 1700000000000,     // matching-engine timestamp, ms — correlates with publicTrade's T
  "data": {
    "s": "BTCUSDT",
    "b": [["43000.5", "1.234"], ...],   // [price, size] strings, descending
    "a": [["43001.0", "0.567"], ...],   // ascending
    "u": 123456,             // update ID
    "seq": 9876543210        // cross sequence — smaller seq = generated earlier
  }
}
```

- `type: "snapshot"` → `OrderBook::apply_snapshot`, full reset of local book.
- `type: "delta"` → `OrderBook::apply_diff`: for each level, size `"0"` = remove, new price =
  insert, existing price = update. Bids descending, asks ascending (already how
  `src/orderbook/mod.rs` expects them — same as Binance/HL).

### Gap detection (this is the part that needs care)

Bybit's `u` field is a **per-symbol, monotonically increasing update ID** — no `pu`
(previous-update-id) field like Binance USDM perp. Documented behavior: on subscribe, first
message is `type: "snapshot"`; every `delta` after it should have `u` exactly one greater than the
previous message's `u`. Gap = `u_new != u_prev + 1`. Documented special case: **`u == 1`
mid-stream means the server restarted and is re-sending a snapshot for that update — the field
`type` will also say `"snapshot"` in that case per the docs, so in practice checking `type` is the
primary signal and `u`-sequence checking is the secondary/defense-in-depth check.**

**Not fully nailed down by public docs — verify against a live capture before implementing**:
whether `u` increments by exactly 1 per WS *message* regardless of how many price levels that
message's `data.b`/`data.a` arrays touch (most likely, matching how every other exchange's
"one WS frame = one atomic update = one sequence increment" convention works), or whether it can
jump by more than 1 in a single message under some circumstance. Treat any `u` discontinuity
(other than the documented `u==1`/`type==snapshot` restart case) as a gap requiring resync.

**Resync on gap** — distinguish two cases, because Bybit's unsolicited-snapshot promise covers only
the first:
- **Server-initiated resync**: Bybit may push a fresh `type: "snapshot"` unprompted when *it*
  detects a server-side issue. The client just watches `type` and resets local book state on
  `"snapshot"`. No client action needed.
- **Client-detected gap** (a `u` discontinuity with no accompanying snapshot): Bybit does **not**
  promise an unsolicited snapshot for a loss the client detects on its own — passively waiting for
  one can hang indefinitely. Correct action is to **reconnect and resubscribe immediately** (same
  category WS, all topics), discard local book/ticker state, and rebuild from the fresh initial
  `snapshot`. Log the gap (mirrors `src/monitor.rs`'s existing gap-count tracking). Still simpler
  than Binance — no explicit REST re-snapshot call — just an active reconnect rather than a passive
  wait.

`seq` (cross sequence) is for comparing orderbook state across **different depth levels of the
same symbol** (e.g. if you were subscribed to both `orderbook.50` and `orderbook.1000` — smaller
`seq` = older). Not needed here since fathom only subscribes to one depth level per symbol; note
it in the schema for completeness, don't build logic around it.

### Public trade (`publicTrade.{symbol}`)

```json
{
  "topic": "publicTrade.BTCUSDT",
  "type": "snapshot",
  "ts": 1700000000000,
  "data": [
    {
      "T": 1700000000000,    // execution time, ms
      "s": "BTCUSDT",
      "S": "Buy",             // taker side: "Buy" or "Sell"
      "v": "0.012",           // quantity, string
      "p": "43000.5",         // price, string
      "L": "PlusTick",        // tick direction (perp/futures only, not spot)
      "i": "2100000000012345",// trade ID
      "seq": 9876543210
    }
  ]
}
```

`data` is an array — **up to 1024 trades per message** for spot/linear (batching under load,
unlike Binance's one-`aggTrade`-per-WS-frame). The connection task must iterate `data` and emit
one `RawTrade` per element, not one per message. `S` maps directly to fathom's existing
`is_buy`-style taker-side attribution (`"Buy"` → `is_buy = true`), matching how
`agg_trade_to_raw` already does it for Binance (`is_buy = !is_buyer_maker` there; Bybit gives the
side directly, no maker/taker inversion needed — simpler).

### Ticker (`tickers.{symbol}`, linear only)

**This is the gotcha for this venue**: the ticker topic is **snapshot + delta, with the delta
messages only containing the fields that changed** ("if a response param is not found in the
message, then its value has not changed" — direct quote from docs). Unlike Binance's
`markPrice@1s`, which pushes a complete mark/index/funding tuple every second regardless of
whether anything changed, a Bybit ticker delta might contain *only* `openInterest`, or *only*
`fundingRate`, with every other field absent.

**Required design**: the adapter must keep an **in-memory last-known ticker state per symbol**
(a small struct: `mark_price`, `index_price`, `funding_rate`, `next_funding_time`,
`open_interest`, `open_interest_value`), seeded from the first `snapshot` message, and merge each
subsequent `delta` onto it (only overwrite fields present in the delta's JSON). Emit a
`MarkFunding` / `OpenInterest` row (fathom's existing derivatives-feed structs, per
`specs/derivatives-feeds.md`) from the *merged* state whenever a relevant field group changes —
mirroring the "one activeAssetCtx message → one MarkFunding + one OpenInterest row" pattern
Hyperliquid already uses (`src/connection/hyperliquid.rs`), not Binance's "one WS message = one
row" pattern (which happens to also hold there, but only because Binance's markPrice push is
always complete).

Full field list available (see cited docs): `markPrice`, `indexPrice`, `fundingRate`,
`nextFundingTime`, `fundingIntervalHour`, `openInterest`, `openInterestValue`, plus a large set of
24h-stats fields (`lastPrice`, `volume24h`, `bid1Price`, etc.) that fathom has no current use for
and should drop at parse time (same "collected vs dropped" discipline as the capture matrix
below) — no need to carry unused fields into `MarkFunding`/`OpenInterest`.

Push frequency: 100ms for linear tickers.

### All liquidations (`allLiquidation.{symbol}`, linear only)

```json
{
  "topic": "allLiquidation.BTCUSDT",
  "ts": 1700000000000,
  "data": [
    { "T": 1700000000000, "s": "BTCUSDT", "S": "Buy", "v": "1.5", "p": "42998.0" }
  ]
}
```

One message can batch multiple liquidations (~500ms batching window). `allLiquidation` was
introduced Feb 2025, replacing the older one-per-second `liquidation` topic — **use
`allLiquidation`, not the deprecated `liquidation` topic**. Maps directly to fathom's existing `Liquidation` struct — same
shape as Binance's `forceOrder`-derived liquidation rows.

## Open interest — no REST poll needed (simpler than Binance)

Binance USDM perp has no OI WebSocket channel, forcing a 60s REST poll
(`src/connection/binance.rs::poll_open_interest`, `ExchangeAdapter::open_interest_url`). **Bybit's
`tickers` channel already carries `openInterest`/`openInterestValue` as WS push fields** — no REST
polling loop needed for Bybit at all. `ExchangeAdapter::open_interest_url` should return `None`
for `bybit_perp` (matching its documented default, and matching how spot returns `None` today
since spot has no OI concept).

## Capture matrix

| Category | Channel / field | State |
|---|---|---|
| spot, linear | `orderbook.1000` full depth levels (all changed levels) | **planned: collect** (raw + book, same as Binance/HL) |
| spot, linear | `publicTrade` (price, qty, side, trade id) | **planned: collect** — trade tape (Parquet + NATS) + 1s `buy_vol`/`sell_vol`/`volume_delta`/`trade_count`, same as existing venues |
| linear | `tickers` → funding rate, mark price, open interest | **planned: collect** — deriv Parquet family + `FATHOM_DERIV`, via the ticker-state-merge design above |
| linear | `allLiquidation` | **planned: collect** — deriv Parquet family + `FATHOM_DERIV` |
| spot, linear | `tickers` 24h-stats fields (`lastPrice`, `volume24h`, `bid1Price`, etc.) | **dropped** — no current use case, same "not subscribed / not persisted without a reason" discipline as `specs/collection.md`'s treatment of Binance's unused `bookTicker` |
| spot, linear | `publicTrade`'s `L` (tick direction), `BT`/`RPI` (block/RPI trade flags) | **dropped** — not used by any existing venue's trade tape either |
| option | everything | **out of scope** — fathom doesn't trade or model options anywhere; no adapter planned |

## Known gaps / quirks (anticipated, confirm during implementation)

- `depth_ms` config field ignored by Bybit adapters (see depth-level section above) — same
  documented precedent as the Hyperliquid adapter.
- Ticker delta merge-state (above) is the one genuinely novel piece of adapter logic this venue
  needs that no existing adapter has — budget real design/test time for it, don't treat it as a
  trivial parse-and-forward like Binance's markPrice.
- Bybit's exact `u`-increment-per-message guarantee is inferred from documented conventions, not
  explicitly spelled out for the gap-count-by-more-than-one case — capture a short live WS session
  during implementation and eyeball the `u` sequence before writing the gap-detection unit tests,
  the same way `src/orderbook/mod.rs`'s Binance gap semantics were originally verified (ADR-004).
- Confirm Bybit's spot/linear symbol strings match the existing 6-symbol reference set exactly
  (via `GET /v5/market/instruments-info`) before wiring `config.toml` — don't assume.
