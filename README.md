# fathom — Binance LOB Collector

fathom collects Level-2 order book data from Binance Spot and USDM Futures via WebSocket, maintains a BTreeMap-based L2 book with Binance's sequence-sync protocol, accumulates per-second OFI / churn / imbalance statistics, and writes both raw diffs and 1-second snapshots to Parquet files (Snappy-compressed).

## Quick start

```bash
cp config.toml.example config.toml
cargo run
```

**config.toml example:**

```toml
data_dir = "/data/fathom"

[[connections]]
name     = "spot_eth_btc"
exchange = "binance_spot"
symbols  = ["ETHUSDT", "BTCUSDT"]
depth_ms = 100

[[connections]]
name     = "perp_eth"
exchange = "binance_perp"
symbols  = ["ETHUSDT"]
depth_ms = 100
```

## Data output

Files are written under `{data_dir}/{exchange}/{symbol}/`:

```
/data/fathom/
  binance_spot/
    ETHUSDT/
      raw/   ← raw diff events, flushed every 5 minutes
      1s/    ← per-second snapshots
  binance_perp/
    ETHUSDT/
      raw/
      1s/
```

**Raw diff columns:** `ts_us`, `symbol`, `side`, `price`, `qty`, `first_update_id`, `last_update_id`

**1s snapshot columns (60 total):** `ts_us`, `bid_px_0..9`, `ask_px_0..9`, `bid_qty_0..9`, `ask_qty_0..9`, `mid_px`, `ofi_l1`, `churn_bid`, `churn_ask`, `spread_bps`, `imbalance_l1`, `imbalance_l5`, `imbalance_l10`, plus derived microprice and volume-weighted metrics.

## Tests

```bash
# Unit + integration + e2e (axum mock server, no network required)
cargo test

# Lint
cargo clippy -- -D warnings

# Smoke tests against live Binance (manual, requires network)
make smoke
# or:
cargo test --test smoke_test -- --include-ignored --test-threads 1 --nocapture
```

## Config reference

| Field | Type | Description |
|-------|------|-------------|
| `data_dir` | string | Root directory for Parquet output |
| `connections[].name` | string | Logical name for the connection group |
| `connections[].exchange` | string | `binance_spot` or `binance_perp` |
| `connections[].symbols` | string[] | List of trading pairs (e.g. `["ETHUSDT"]`) |
| `connections[].depth_ms` | integer | WebSocket stream update speed: `100` or `1000` ms |
