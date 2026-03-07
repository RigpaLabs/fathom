# fathom — Multi-Exchange LOB Collector

Collects Level-2 order book data from Binance (Spot + USDM Futures), Hyperliquid, and dYdX v4 via WebSocket and writes Parquet files.

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
  WebSocket combined stream
    → gap detection (per-symbol sequence validation)
    → apply diff to BTreeMap L2 book (src/orderbook/mod.rs)
    → OFI / churn / microprice accumulation (src/accumulator.rs)

Hyperliquid:
  WebSocket (single endpoint, subscribe after connect)
    → full L2 snapshots (~500ms) + trades
    → snapshot-to-snapshot OFI / churn (src/connection_hl.rs)
    → accumulation via WindowAccumulator::on_diff_from_levels

dYdX v4:
  WebSocket Indexer API (subscribe after connect)
    → initial snapshot + batched diffs (~250ms) + trades
    → local BTreeMap book (DydxBook in src/connection_dydx.rs)
    → accumulation via WindowAccumulator::on_diff_from_levels

All paths → two parallel writers:
  raw diff  → {data_dir}/raw/{exchange}/{symbol}/{date}/depth_HHMM_HHMM.parquet
  1s snap   → {data_dir}/1s/{exchange}/{symbol}/{date}.parquet  (64 columns, 1 row/sec)
```

**1s snapshot columns:** `ts_us`, `exchange`, `symbol`, `bid_px_0..9`, `ask_px_0..9`, `bid_sz_0..9`, `ask_sz_0..9`,
`mid_px`, `microprice`, `spread_bps`, `imbalance_l1`, `imbalance_l5`, `imbalance_l10`,
`bid_depth_l5`, `bid_depth_l10`, `ask_depth_l5`, `ask_depth_l10`,
`ofi_l1`, `churn_bid`, `churn_ask`, `intra_sigma`, `open_px`, `close_px`, `n_events`,
`volume_delta`, `buy_vol`, `sell_vol`, `trade_count`.

## Key source files

| File | Responsibility |
|------|---------------|
| `src/main.rs` | Load config, spawn connection tasks + writers + monitor |
| `src/connection.rs` | Binance WS connect → REST snapshot → event loop |
| `src/connection_hl.rs` | Hyperliquid WS: L2 snapshots + trades, OFI from snapshot diffs |
| `src/connection_dydx.rs` | dYdX v4 WS: snapshot + batched diffs + trades, local DydxBook |
| `src/orderbook/mod.rs` | BTreeMap L2 book, Binance sync protocol |
| `src/accumulator.rs` | 1s window stats (shared by all exchanges) |
| `src/exchange/` | BinanceSpot / BinancePerp / Hyperliquid adapters, dYdX constants |
| `src/writer/raw.rs` | Raw diff Parquet writer |
| `src/writer/snap_1s.rs` | 1s snapshot Parquet writer |
| `src/monitor.rs` | Reconnect/gap/liveness tracking |

## Important gotcha: perp vs spot gap check

**Binance USDM Futures** diff events carry a `pu` field (prev_final_update_id).
The ongoing sequence check for perps is:

```
pu == last_update_id   ← CORRECT for perp
```

**Not** `U == last_update_id + 1`, which is the spot rule. Both are handled in
`src/orderbook/mod.rs: apply_diff`. Getting this wrong causes spurious gap reconnects.

**Hyperliquid** sends full snapshots (no diffs) — no gap detection needed.
**dYdX v4** uses batched diffs after initial snapshot; the WS guarantees ordering.

## Data locations

**Production (Vultr Tokyo):** SSH `fathom-root`, data inside Docker volume mounted at `/app/data/`.
Each deploy creates a versioned dir: `/app/data/v{YYYYMMDD}-{sha7}/`.

**Local backup (iCloud):** `~/Library/Mobile Documents/com~apple~CloudDocs/fathom-data/`
- Contains all historical versions from v20260301 onward
- Auto-synced daily at 10:00 WITA via launchd (`com.fathom.sync-data`)
- Sync script: `scripts/sync-data.sh` (two-step: VPS → staging → iCloud)

**Local staging:** `~/.local/fathom-staging/` — intermediate rsync target (avoids iCloud path spaces)

**Data structure inside each version dir:**
```
v{tag}/
├── 1s/{exchange}/{symbol}/{date}.parquet    # 1-second snapshots (1 row/sec)
├── raw/{exchange}/{symbol}/{date}/depth_HHMM_HHMM.parquet  # raw diffs
└── metadata/status.json
```

**Exchanges:** `binance_spot`, `binance_perp`, `hyperliquid`, `dydx` (22 symbols total)

## Testing conventions

- Unit tests: `mod tests` inside `src/` files
- Integration / e2e tests: `tests/` directory (e2e uses an axum mock server, 7 scenarios)
- Smoke tests: `tests/smoke_test.rs` (Binance), `tests/smoke_hl_test.rs` (HL), `tests/smoke_dydx_test.rs` (dYdX) — all marked `#[ignore]`, run manually
- Never auto-commit without explicit user approval
