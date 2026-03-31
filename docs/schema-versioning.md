# Schema Versioning

## Current schema

**Version:** 1 (implicit — no version field in Parquet metadata yet)

### 1s snapshot columns (64)

`ts_us`, `exchange`, `symbol`, `bid_px_0..9`, `ask_px_0..9`, `bid_sz_0..9`, `ask_sz_0..9`, `mid_px`, `microprice`, `spread_bps`, `imbalance_l1`, `imbalance_l5`, `imbalance_l10`, `bid_depth_l5`, `bid_depth_l10`, `ask_depth_l5`, `ask_depth_l10`, `ofi_l1`, `churn_bid`, `churn_ask`, `intra_sigma`, `open_px`, `close_px`, `n_events`, `volume_delta`, `buy_vol`, `sell_vol`, `trade_count`

### Raw diff columns

`timestamp_us`, `exchange`, `symbol`, `seq_id`, `prev_seq_id`, `bids` (list of [price, qty]), `asks` (list of [price, qty])

## Backward compatibility policy

**Additive only.** Existing columns are never renamed, removed, or have their types changed. This ensures all downstream consumers (sigil, ar-quant, notebooks) continue to work without modification.

Allowed changes:
- **Add new columns** — always appended after existing columns
- **Add new Parquet metadata keys** — e.g. `fathom_schema_version`

Disallowed changes (require a major version bump):
- Removing columns
- Renaming columns
- Changing column types (e.g. `i64` → `f64`)
- Reordering columns

## Migration path when columns change

1. **Bump schema version** in code and document the new column(s) here
2. **Update `DATA_DIR`** version suffix (e.g. `v2/`) so the new schema writes to a separate directory tree — old and new data coexist without conflict
3. **Update consumers** (sigil, ar-quant) to handle the new columns. Since old columns are preserved, consumers can adopt new columns at their own pace
4. **Blue-green deploy** ensures both old and new fathom instances can run simultaneously with isolated data directories

## `DATA_DIR` versioning

When `DATA_DIR` is set (typically during deploy), data is written under `{DATA_DIR}/{version}/`. This isolates schema changes between deploys:

```
/data/
├── v1/          # old schema, old fathom instance
│   ├── 1s/
│   └── raw/
└── v2/          # new schema, new fathom instance
    ├── 1s/
    └── raw/
```

During blue-green deploy, both versions coexist. Once the new instance is healthy and the old one is stopped, the old version directory can be archived or left in place for historical queries.

This approach avoids in-place schema migrations entirely — each version is a clean, self-contained dataset.
