# ADR-003: BTreeMap for order book

Date: 2026-03-31
Status: Accepted

## Context

Fathom maintains Level-2 order books for Binance symbols (spot + perp). The book must support:

1. **Insert/update/remove** a price level on every diff event (~10–100 updates/sec per symbol).
2. **Sorted iteration** for top-N level extraction (10 best bids descending, 10 best asks ascending) — done every second for the 1s snapshot.
3. **Best bid/ask lookup** — done on every diff event for OFI calculation.
4. **Depth aggregation** across top N levels — for imbalance metrics.

The book is keyed by `OrderedFloat<f64>` (price) with `f64` values (quantity). Typical book depth: 500–2,000 levels per side.

## Decision

Use `BTreeMap<OrderedFloat<f64>, f64>` for both bids and asks. Bids use natural BTreeMap ordering (ascending) with reverse iteration (`iter().rev()`) for descending access. Asks use natural ascending iteration.

```rust
pub struct OrderBook {
    bids: BTreeMap<OrderedFloat<f64>, f64>,  // best bid via .keys().next_back()
    asks: BTreeMap<OrderedFloat<f64>, f64>,  // best ask via .keys().next()
    // ...
}
```

Top-N extraction:

```rust
pub fn bids_top_n(&self, n: usize) -> Vec<(f64, f64)> {
    self.bids.iter().rev().take(n).map(|(k, v)| (k.0, *v)).collect()
}
pub fn asks_top_n(&self, n: usize) -> Vec<(f64, f64)> {
    self.asks.iter().take(n).map(|(k, v)| (k.0, *v)).collect()
}
```

## Consequences

**Benefits:**

- **O(log n) insert/update/remove.** For 1,000 levels, ~10 comparisons per operation.
- **O(1) best bid/ask.** `next_back()` and `next()` on the iterator are constant-time for BTreeMap extremes.
- **O(k) top-k extraction.** Sorted iteration means we stop after k elements — no sorting step needed.
- **Automatic deduplication.** BTreeMap replaces the value at an existing key, which is exactly the Binance diff semantic (new qty replaces old).
- **Zero-quantity removal is natural.** `remove(&key)` when `qty == 0.0` — no tombstone management.
- **Standard library, zero dependencies.** No external crate needed.

**Performance envelope:**

At ~100 updates/sec per symbol × 22 symbols = ~2,200 BTreeMap operations/sec globally. Each operation is ~10 comparisons of `OrderedFloat<f64>` (essentially f64 comparison). This is well within the budget — the bottleneck is WebSocket I/O, not book operations.

**Costs:**

- BTreeMap nodes are heap-allocated and not cache-friendly compared to a contiguous array. For our scale (1,000 levels, 2,200 ops/sec), this is not measurable.
- `OrderedFloat<f64>` adds a wrapper. The cost is a single comparison instruction — negligible.

## Alternatives considered

**Sorted `Vec<(f64, f64)>`.** O(log n) search via `binary_search`, but O(n) insert/remove due to shifting. At 1,000 levels, each insert shifts ~500 elements on average. Better cache locality doesn't compensate for the linear cost at this depth.

**`HashMap<OrderedFloat<f64>, f64>` + sort on read.** O(1) insert, but top-N extraction requires sorting all keys — O(n log n) per snapshot. With 1,000 levels × 22 symbols × 1 snapshot/sec, that's 22,000 sorts/sec. Wasteful when BTreeMap gives sorted iteration for free.

**Skip list.** Similar O(log n) guarantees to BTreeMap with potentially better concurrent access. Not in std, requires a third-party crate, and we don't need concurrent access — each book is owned by a single connection task.

**Fixed-size array `[(f64, f64); N]`.** Only works if we cap book depth. Binance books can have thousands of levels, and we need all of them for depth aggregation. A fixed array either wastes memory or truncates data.
