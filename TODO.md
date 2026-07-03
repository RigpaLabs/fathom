# fathom — TODO

Feature roadmap moved to `specs/` (trades feed, derivatives feeds, storage upload/retention, write-health metrics — each spec has a Planned section).

## Reliability

- [ ] **Per-symbol REST snapshot on gap** — instead of reconnecting the entire WS on a single symbol's sequence gap, fetch REST snapshot only for the gapped symbol and continue on the same WS. Reduces disruption to other symbols sharing the connection.

- [ ] **Per-symbol WS connections** — separate WS per symbol so a disconnect/gap on one doesn't affect others. Trade-off: 5x more connections, Binance rate limits apply (5 connections per IP for spot, separate for perp).
