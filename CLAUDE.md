# fathom — Multi-Exchange LOB Collector

Collects Level-2 order book data from Binance (Spot + USDM Futures), Hyperliquid, and dYdX v4 via WebSocket and writes Parquet files.

**Behavior contracts live in `specs/`** (capture matrix, schemas, streams, planned feeds) — check them before changing collection behavior. Decisions: `docs/adr/`.

## Commands

```bash
cargo build                  # build
cargo test                   # unit + integration + e2e (no smoke)
cargo clippy -- -D warnings  # lint
cargo fmt --check            # format check

make run                     # cargo run (uses config.toml)
make smoke                   # smoke tests against live Binance (manual only)

# Exchange-specific smoke tests (live network, manual only):
cargo test --test smoke_hl_test -- --include-ignored --test-threads 1 --nocapture
cargo test --test smoke_dydx_test -- --include-ignored --test-threads 1 --nocapture
```

## Architecture

```
Binance (spot/perp):
  WebSocket combined stream ({sym}@depth@100ms + {sym}@aggTrade;
                             perp adds {sym}@markPrice@1s + {sym}@forceOrder)
    → gap detection (per-symbol sequence validation)
    → apply diff to BTreeMap L2 book (src/orderbook/mod.rs)
    → OFI / churn / microprice accumulation (src/accumulator.rs)
    → aggTrade → trade tape + 1s buy/sell volume accumulation
    → markPrice/forceOrder → deriv feeds (perp only; no depth-liveness impact)
  Perp REST: openInterest polled per symbol every 60s → deriv feeds

Hyperliquid:
  WebSocket (single endpoint, subscribe after connect)
    → full L2 snapshots (~500ms) + trades + activeAssetCtx
    → snapshot-to-snapshot OFI / churn (src/connection/hyperliquid.rs)
    → accumulation via WindowAccumulator::on_diff_from_levels
    → activeAssetCtx → MarkFunding + OpenInterest per message

dYdX v4:
  WebSocket Indexer API (subscribe after connect)
    → initial snapshot + batched diffs (~250ms) + trades
    → local BTreeMap book (DydxBook in src/connection/dydx.rs)
    → accumulation via WindowAccumulator::on_diff_from_levels

All paths → four parallel writers + optional NATS sink, each hourly-rotated
(shared `rotate_hours`, `src/writer/rotation.rs::Bucket`):
  raw diff  → {data_dir}/raw/{exchange}/{symbol}/{date}/depth_HHMM_HHMM.parquet
  1s snap   → {data_dir}/1s/{exchange}/{symbol}/{date}/snap_HHMM_HHMM.parquet  (64 columns, 1 row/sec)
  trades    → {data_dir}/trades/{exchange}/{symbol}/{date}/trades_HHMM_HHMM.parquet  (Binance aggTrade + HL trades)
  deriv     → {data_dir}/deriv/{exchange}/{symbol}/{date}/{funding|oi|liq}_HHMM_HHMM.parquet  (perp venues)

Optional NATS streaming (src/nats_sink.rs):
  1s snap   → fathom.v1.{exchange}.{symbol}.snapshot  (FATHOM_SNAPSHOTS, file storage, critical)
  raw diff  → fathom.v1.{exchange}.{symbol}.depth     (FATHOM_DEPTH, file storage, 1h retention)
  trade     → fathom.v1.{exchange}.{symbol}.trade     (FATHOM_TRADES, file storage, 24h retention)
  deriv     → fathom.v1.{exchange}.{symbol}.{funding|oi|liq}  (FATHOM_DERIV, file storage, 24h retention;
              .funding carries MarkFunding — mark is folded in, no .mark subject)
```

**1s snapshot columns:** `ts_us`, `exchange`, `symbol`, `bid_px_0..9`, `ask_px_0..9`, `bid_sz_0..9`, `ask_sz_0..9`,
`mid_px`, `microprice`, `spread_bps`, `imbalance_l1`, `imbalance_l5`, `imbalance_l10`,
`bid_depth_l5`, `bid_depth_l10`, `ask_depth_l5`, `ask_depth_l10`,
`ofi_l1`, `churn_bid`, `churn_ask`, `intra_sigma`, `open_px`, `close_px`, `n_events`,
`volume_delta`, `buy_vol`, `sell_vol`, `trade_count`.

The trade columns (`volume_delta`, `buy_vol`, `sell_vol`, `trade_count`) are populated for all
exchanges: HL from the `trades` channel, Binance from `aggTrade` (attribution by taker side,
`is_buy = !is_buyer_maker`). See `specs/trades-feed.md`.

Derivatives feeds (`specs/derivatives-feeds.md`): Binance perp markPrice@1s + forceOrder +
OI REST poll; HL activeAssetCtx. Structs `MarkFunding` / `OpenInterest` / `Liquidation` in
`crates/fathom-types`. Deriv events never feed depth-liveness (`record_event`).

## Key source files

| File | Responsibility |
|------|---------------|
| `src/main.rs` | Load config, spawn connection tasks + writers + monitor |
| `src/connection/binance.rs` | Binance WS connect → REST snapshot → event loop |
| `src/connection/hyperliquid.rs` | Hyperliquid WS: L2 snapshots + trades, OFI from snapshot diffs |
| `src/connection/dydx.rs` | dYdX v4 WS: snapshot + batched diffs + trades, local DydxBook |
| `src/orderbook/mod.rs` | BTreeMap L2 book, Binance sync protocol |
| `src/accumulator.rs` | 1s window stats (shared by all exchanges) |
| `src/exchange/` | BinanceSpot / BinancePerp / Hyperliquid adapters, dYdX constants |
| `src/writer/raw.rs` | Raw diff Parquet writer (hourly rotation) |
| `src/writer/rotation.rs` | Shared hourly `Bucket` open/rename lifecycle + `Clock` abstraction |
| `src/writer/snap_1s.rs` | 1s snapshot Parquet writer (hourly rotation) |
| `src/writer/trades.rs` | Trade tape Parquet writer (hourly rotation) |
| `src/writer/deriv.rs` | Derivatives Parquet writer (funding/OI/liquidations, hourly rotation + periodic force-rotate) |
| `src/nats_sink.rs` | NATS JetStream publisher (snapshots + depth diffs + trades + deriv) |
| `src/monitor.rs` | Reconnect/gap/liveness tracking |

## Important gotcha: perp vs spot gap check

**Binance USDM Futures** diff events carry a `pu` field (prev_final_update_id).
The `pu` field is used for **both initial sync and ongoing gap detection**:

```
Initial sync:  pu > last_update_id  → SnapshotRequired (gap)
               pu < last_update_id  → Ok(None) (stale, drop)
               pu == last_update_id → valid sync event

Ongoing:       pu > last_update_id  → OrderBookGap (missed events)
               pu < last_update_id  → Ok(None) (stale, drop)
               pu == last_update_id → normal sequence
```

**Not** `U == last_update_id + 1`, which is the spot rule. Both are handled in
`src/orderbook/mod.rs: apply_diff`. Getting this wrong causes spurious gap reconnects.

**Hyperliquid** sends full snapshots (no diffs) — no gap detection needed.
**dYdX v4** uses batched diffs after initial snapshot; the WS guarantees ordering.

## Data layout

Data is written to `{data_dir}/` (configured in `config.toml`). When `DATA_DIR` env is set, it overrides the config value (useful for blue-green deploys).

```
{data_dir}/
├── 1s/{exchange}/{symbol}/{date}/snap_HHMM_HHMM.parquet    # 1-second snapshots (1 row/sec)
├── raw/{exchange}/{symbol}/{date}/depth_HHMM_HHMM.parquet  # raw diffs
├── trades/{exchange}/{symbol}/{date}/trades_HHMM_HHMM.parquet  # raw trade tape
├── deriv/{exchange}/{symbol}/{date}/{funding|oi|liq}_HHMM_HHMM.parquet  # derivatives feeds
└── metadata/status.json                     # health, updated every 30s
```

All four writers rotate hourly (`raw_rotate_hours` in `config.toml`, default 1h) via a shared open-temp-file/rename-on-close lifecycle (`src/writer/rotation.rs::Bucket`) — this bounds restart data loss to at most one open bucket instead of a full day.

**Exchanges:** `binance_spot`, `binance_perp`, `hyperliquid`, `dydx` (22 symbols total)

## Deployment

CI builds and pushes Docker images to GHCR on every push to `main`. Deploy to your own infrastructure by pulling the image and running via Docker Compose or `docker run`.

See `docker-compose.yml` for local dev and `docker-compose.prod.yml` for production reference.

## Testing conventions

- Unit tests: `mod tests` inside `src/` files
- Integration / e2e tests: `tests/` directory (e2e uses an axum mock server, 7 scenarios)
- Smoke tests: `tests/smoke_test.rs` (Binance), `tests/smoke_hl_test.rs` (HL), `tests/smoke_dydx_test.rs` (dYdX) — all marked `#[ignore]`, run manually
- Never auto-commit without explicit user approval
