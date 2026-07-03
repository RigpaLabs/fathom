# NATS Streams — JetStream Contract

Status: **stable**

Optional: enabled via `[nats]` config section; fathom runs fine without it (Parquet writers are independent). Publisher: `src/nats_sink.rs`. Backpressure policy: drop, never block collection (ADR-002).

## Streams

| Stream | Subjects | Storage | Retention | Purpose |
|---|---|---|---|---|
| `FATHOM_SNAPSHOTS` | `fathom.v1.{exchange}.{symbol}.snapshot` | File | 24 h / 200 MB | 1s snapshots — the "critical" feed downstream signal engines consume |
| `FATHOM_DEPTH` | `fathom.v1.{exchange}.{symbol}.depth` | File | 1 h / 500 MB | Raw depth diffs — short replay window for warmup (e.g. consumer replays 30 min on start) |
| `FATHOM_TRADES` | `fathom.v1.{exchange}.{symbol}.trade` | File | 24 h / 200 MB | Raw trade tape ([trades-feed.md](trades-feed.md)) — low volume relative to depth |
| `FATHOM_DERIV` | `fathom.v1.{exchange}.{symbol}.{funding\|oi\|liq}` | File | 24 h / 200 MB | Derivatives feeds ([derivatives-feeds.md](derivatives-feeds.md)) — `.funding` = `MarkFunding` (mark is folded in, no `.mark` subject), `.oi` = `OpenInterest`, `.liq` = `Liquidation` |

Payloads: wire-encoded `Snapshot1s` / `RawDiff` / `RawTrade` / `MarkFunding` / `OpenInterest` / `Liquidation` (see [data-schema.md](data-schema.md)). Same data as Parquet — no truncation.

**Consumer rule:** the wire format carries a version byte but no type discriminant — the subject is the only thing identifying the payload type. Bind your decoder to the subject you subscribed to; never decode a `.depth` payload as `Snapshot1s` etc. (wrong-type decodes fail loudly in practice, but don't rely on it).

## Operational gotchas

- Stream creation is `get_or_create`: it does **not** update the storage type (or limits) of an existing stream. Changing stream config requires delete + recreate.
- If JetStream state is lost (e.g. NATS restart with volume wipe), consumers see "no stream found" — restart order matters: nats → fathom (recreates streams) → consumers.
