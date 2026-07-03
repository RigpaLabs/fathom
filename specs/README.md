# Fathom Specs

Behavior contracts for fathom. A **spec** describes what fathom does (or will do); an **ADR** (`docs/adr/`) records why a decision was made. When code and spec disagree, one of them is a bug — fix whichever is wrong, in the same PR if possible.

## Status legend

| Status | Meaning |
|---|---|
| `stable` | Implemented, verified against code, covered by tests |
| `planned` | Agreed design, not yet implemented |
| `deprecated` | Being removed; do not build on it |

## Index

| Spec | Covers | Status |
|---|---|---|
| [collection.md](collection.md) | Exchanges, WS channels, capture matrix, gap semantics | stable |
| [data-schema.md](data-schema.md) | Raw diff + 1s snapshot Parquet schemas, wire format | stable |
| [nats-streams.md](nats-streams.md) | JetStream streams, subjects, retention | stable |
| [storage.md](storage.md) | Local layout, rotation, retention, object-storage upload | stable / planned |
| [trades-feed.md](trades-feed.md) | Raw trade tape with price | planned |
| [derivatives-feeds.md](derivatives-feeds.md) | Funding, mark/oracle price, open interest, liquidations | planned |
| [observability.md](observability.md) | Prometheus metrics, status.json, health semantics | stable / planned |

## Design principle: capture raw, compute later

Fathom's job is to persist **everything the exchange sends that has information content**, at the lowest level the API offers. Derived metrics (volume profile, VWAP, imbalance beyond the built-in 1s aggregates) belong to downstream consumers. If a field arrives over the wire and we drop it, that is a gap — either document it in the capture matrix or fix it.
