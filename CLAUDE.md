# fathom — Binance LOB Collector

Collects Level-2 order book data from Binance (Spot + USDM Futures) via WebSocket and writes Parquet files.

## Commands

```bash
cargo build                  # build
cargo test                   # unit + integration + e2e (no smoke)
cargo clippy -- -D warnings  # lint
cargo fmt --check            # format check

make run                     # cargo run (uses config.toml)
make smoke                   # smoke tests against live Binance (manual only)
```

## Architecture

```
WebSocket stream
  → gap detection (per-symbol sequence validation)
  → apply diff to BTreeMap L2 book (src/orderbook/mod.rs)
  → OFI / churn / microprice accumulation in 1s windows (src/accumulator.rs)
  → two parallel writers:
      raw diff  → {data_dir}/raw/{exchange}/{symbol}/{date}/depth_HHMM_HHMM.parquet  (flush every 5 min, rotate every 6h)
      1s snap   → {data_dir}/1s/{exchange}/{symbol}/{date}.parquet                    (60 columns, 1 row/sec)
```

**1s snapshot columns:** `ts_us`, `exchange`, `symbol`, `bid_px_0..9`, `ask_px_0..9`, `bid_sz_0..9`, `ask_sz_0..9`,
`mid_px`, `microprice`, `spread_bps`, `imbalance_l1`, `imbalance_l5`, `imbalance_l10`,
`bid_depth_l5`, `bid_depth_l10`, `ask_depth_l5`, `ask_depth_l10`,
`ofi_l1`, `churn_bid`, `churn_ask`, `intra_sigma`, `open_px`, `close_px`, `n_events`.

## Key source files

| File | Responsibility |
|------|---------------|
| `src/main.rs` | Load config, spawn connection tasks + writers + monitor |
| `src/connection.rs` | WS connect → REST snapshot → event loop |
| `src/orderbook/mod.rs` | BTreeMap L2 book, Binance sync protocol |
| `src/accumulator.rs` | 1s window stats |
| `src/exchange/` | BinanceSpot / BinancePerp adapters |
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

## Testing conventions

- Unit tests: `mod tests` inside `src/` files
- Integration / e2e tests: `tests/` directory (e2e uses an axum mock server, 7 scenarios)
- Smoke tests: `tests/smoke_test.rs`, all marked `#[ignore]` — run manually with `make smoke`
- Never auto-commit without explicit user approval
