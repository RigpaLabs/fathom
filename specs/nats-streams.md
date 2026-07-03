# NATS Streams — JetStream Contract

Status: **stable** (trades/derivatives streams planned)

Optional: enabled via `[nats]` config section; fathom runs fine without it (Parquet writers are independent). Publisher: `src/nats_sink.rs`. Backpressure policy: drop, never block collection (ADR-002).

## Streams

| Stream | Subjects | Storage | Retention | Purpose |
|---|---|---|---|---|
| `FATHOM_SNAPSHOTS` | `fathom.v1.{exchange}.{symbol}.snapshot` | File | 24 h / 200 MB | 1s snapshots — the "critical" feed downstream signal engines consume |
| `FATHOM_DEPTH` | `fathom.v1.{exchange}.{symbol}.depth` | File | 1 h / 500 MB | Raw depth diffs — short replay window for warmup (e.g. consumer replays 30 min on start) |

Payloads: wire-encoded `Snapshot1s` / `RawDiff` (see [data-schema.md](data-schema.md)). Same data as Parquet — no truncation.

## Operational gotchas

- Stream creation is `get_or_create`: it does **not** update the storage type (or limits) of an existing stream. Changing stream config requires delete + recreate.
- If JetStream state is lost (e.g. NATS restart with volume wipe), consumers see "no stream found" — restart order matters: nats → fathom (recreates streams) → consumers.

## Planned

| Stream | Subjects | Notes |
|---|---|---|
| `FATHOM_TRADES` | `fathom.v1.{exchange}.{symbol}.trade` | Raw trade tape ([trades-feed.md](trades-feed.md)) |
| `FATHOM_DERIV` | `fathom.v1.{exchange}.{symbol}.{funding\|mark\|oi\|liq}` | Derivatives feeds ([derivatives-feeds.md](derivatives-feeds.md)); low volume, one stream for all four |
