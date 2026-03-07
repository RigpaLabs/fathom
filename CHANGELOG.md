# Changelog

## [0.2.1] — 2026-03-07

### Fixed
- **Memory: ~500MB → ~60MB** — set `max_row_group_size` on Parquet ArrowWriters (was default 1M rows, now 8K raw / 4K snap)

### Changed
- Docker deploy: added `--memory 256m` limit

## [0.2.0] — 2026-03-06

### Added
- **Hyperliquid adapter** — L2 orderbook + trades via WS, snapshot-only protocol
- **dYdX v4 adapter** — L2 orderbook (snapshot + batched diffs) + trades via Indexer WS
- Trade stream accumulation: `volume_delta`, `buy_vol`, `sell_vol`, `trade_count` columns in 1s snapshots
- `flush_with_levels()` on WindowAccumulator for exchanges without OrderBook (HL, dYdX)
- `on_diff_from_levels()` for OFI computation from raw best bid/ask
- `accumulate_trade()` on WindowAccumulator
- Smoke tests for Hyperliquid (2 tests) and dYdX (2 tests)
- Config: HL (7 pairs), dYdX (3 pairs), Binance spot+perp (6 pairs each)

### Changed
- Exchange enum: added `Hyperliquid`, `Dydx` variants
- Parquet schema: 4 new columns (volume_delta, buy_vol, sell_vol, trade_count)
- Config: added DOGE to Binance, replaced standalone BNB-only pairs

## [0.1.0] — 2026-03-01

### Added
- Initial release: Binance Spot + USDM Futures L2 collector
- BTreeMap orderbook with gap detection and auto-resnapshot
- 1s snapshot Parquet writer (60 columns)
- Raw diff Parquet writer with configurable rotation
- OFI, churn, microprice, imbalance, intra_sigma accumulation
- Blue-green Docker deployment via GitHub Actions
- Health monitoring with status.json
