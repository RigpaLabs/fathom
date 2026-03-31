# ADR-004: Order book invariants and gap semantics

Date: 2026-03-31
Status: Accepted

## Context

A Level-2 order book maintained from a WebSocket diff stream can diverge from the exchange's true state if events are missed (network hiccup, server-side drop, slow consumer). Publishing a diverged book would produce incorrect microstructure statistics — bad data is worse than no data for quantitative research.

Binance provides sequence IDs for gap detection. Hyperliquid and dYdX use different delivery guarantees (full snapshots and ordered batches, respectively).

## Decision

### Invariants maintained

1. **Sorted, no duplicates.** Enforced by `BTreeMap` — structurally impossible to violate.
2. **No zero-quantity levels.** Levels with qty 0.0 are removed on every diff application.
3. **Sequence continuity (Binance only).** Every applied diff must chain from the previous one:
   - **Spot:** `first_update_id == last_update_id + 1`
   - **Perp (USDM Futures):** `pu == last_update_id` (where `pu` is the prev_final_update_id field)
4. **Snapshot-gated processing.** No diffs are applied until a REST snapshot is loaded (`has_snapshot` flag). Diffs arriving before the snapshot are silently dropped.

### Gap detection rules

```
Spot (ongoing):   U != last_update_id + 1  → OrderBookGap → reconnect
Perp (ongoing):   pu > last_update_id      → OrderBookGap → reconnect
                  pu < last_update_id      → stale event  → drop silently
                  pu == last_update_id     → valid         → apply
```

The spot and perp rules differ because Binance USDM Futures provides the `pu` (prev_final_update_id) field that spot streams lack. The code branches on `Option<i64>` presence, making the check explicit and impossible to confuse.

### What happens on gap

**Full re-sync.** When `apply_diff()` returns `Err(OrderBookGap { .. })` or `Err(SnapshotRequired(..))`:

1. The connection task breaks out of the inner event loop.
2. All per-symbol order books and accumulators are cleared (`books.clear()`).
3. The WebSocket connection is torn down.
4. Exponential backoff (1s → 2s → ... → 60s max, with 25% jitter).
5. Fresh WS connection + REST snapshot + full re-sync from scratch.

There is no partial recovery — no "re-snapshot just the affected symbol." The entire connection group reconnects. This is simpler and avoids the risk of one symbol's gap state leaking into another's processing.

### Partial state is never published

During initial sync, diffs are buffered in a forwarder channel while REST snapshots are fetched (200–500ms per symbol). Diffs that arrive before the snapshot bridges are dropped (`u <= last_update_id → Ok(None)`). The first published snapshot only occurs after the book is fully synced.

If a gap is detected at any point, the entire connection resets — no partial book is ever sent to the broadcast channel.

### Exchange-specific behavior

| Exchange | Gap detection | Recovery |
|----------|--------------|----------|
| Binance Spot | Sequence ID chain | Full reconnect |
| Binance Perp | `pu` chain | Full reconnect |
| Hyperliquid | N/A (full snapshots) | N/A — each message is self-contained |
| dYdX v4 | N/A (ordered batches) | WS guarantees ordering; reconnect on disconnect |

## Consequences

**Benefits:**

- Downstream consumers (writers, NATS, analytics) can trust that every published snapshot represents a consistent book state.
- The gap detection code is thoroughly tested (39 order book tests covering spot gaps, perp `pu` chain, initial sync, stale events).

**Costs:**

- A gap in one symbol causes all symbols in the connection group to reconnect. This loses 5–60 seconds of data across all symbols in the group. Acceptable because gaps are rare (<1/day in production) and the alternative (per-symbol recovery) adds significant complexity with minimal benefit.
- No partial state means a slow REST snapshot (429 rate limit) delays sync for all symbols. Mitigated by rate-limit detection with an extended 300-second backoff.

## Alternatives considered

**Per-symbol re-snapshot without full reconnect.** Would preserve data for unaffected symbols during a single-symbol gap. Rejected because: (a) the WS combined stream is shared across all symbols — you can't selectively pause one, and (b) partial recovery logic is error-prone and hard to test.

**Publish best-effort data on gap (with a quality flag).** Would allow downstream consumers to decide. Rejected because for quant research, the quality flag would always be "discard this" — so we discard at the source instead of pushing the decision downstream.

**Buffering and reordering.** Hold out-of-order events and reorder. Rejected because Binance guarantees in-order delivery within a single stream — gaps mean missed events, not reordered ones.
