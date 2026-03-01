use std::collections::HashMap;

use tokio::sync::mpsc;
use tracing::{info, warn};

use super::{DepthDiff, DiffApplied, OrderBook, SnapshotMsg};
use crate::error::AppError;

/// Messages sent to the OrderBookManager task
pub enum BookManagerMsg {
    Snapshot(SnapshotMsg),
    Diff(DepthDiff),
    Shutdown,
}

/// Events emitted by the OrderBookManager after processing each diff
#[derive(Debug, Clone)]
pub struct BookEvent {
    pub diff: DepthDiff,
    pub applied: DiffApplied,
}

/// Manages per-symbol OrderBooks, routing diffs and emitting BookEvents.
pub struct OrderBookManager {
    books: HashMap<String, OrderBook>,
    /// Diffs buffered before snapshot arrives (per symbol)
    pending: HashMap<String, Vec<DepthDiff>>,
}

impl OrderBookManager {
    pub fn new() -> Self {
        Self {
            books: HashMap::new(),
            pending: HashMap::new(),
        }
    }

    pub fn apply_snapshot(&mut self, snap: SnapshotMsg) {
        let symbol = snap.symbol.clone();
        info!(symbol = %symbol, last_update_id = snap.last_update_id, "applying snapshot");
        let book = self.books.entry(symbol.clone()).or_default();
        book.apply_snapshot(snap);

        // Replay any buffered diffs
        if let Some(pending) = self.pending.remove(&symbol) {
            info!(symbol = %symbol, count = pending.len(), "replaying buffered diffs");
            for diff in pending {
                let _ = book.apply_diff(&diff); // best-effort during replay
            }
        }
    }

    /// Returns `Ok(Some(event))` on success, `Ok(None)` if diff was dropped (pre-sync),
    /// `Err(SnapshotRequired)` if a gap forces re-snapshot.
    pub fn apply_diff(&mut self, diff: DepthDiff) -> Result<Option<BookEvent>, AppError> {
        let book = self.books.entry(diff.symbol.clone()).or_insert_with(|| {
            // Book not yet initialized — buffer until snapshot arrives
            OrderBook::new()
        });

        // If book has no snapshot yet (last_update_id == 0 and !synced), buffer
        if book.last_update_id == 0 && !book.synced {
            self.pending
                .entry(diff.symbol.clone())
                .or_default()
                .push(diff);
            return Ok(None);
        }

        match book.apply_diff(&diff)? {
            None => Ok(None),
            Some(applied) => Ok(Some(BookEvent { diff, applied })),
        }
    }
}

impl Default for OrderBookManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Spawn the OrderBookManager as a Tokio task.
/// Returns (msg_tx, event_rx).
pub fn spawn_manager(
    buffer: usize,
) -> (mpsc::Sender<BookManagerMsg>, mpsc::Receiver<BookEvent>) {
    let (msg_tx, mut msg_rx) = mpsc::channel::<BookManagerMsg>(buffer);
    let (event_tx, event_rx) = mpsc::channel::<BookEvent>(buffer);

    tokio::spawn(async move {
        let mut mgr = OrderBookManager::new();
        loop {
            match msg_rx.recv().await {
                None | Some(BookManagerMsg::Shutdown) => {
                    info!("book_manager shutting down");
                    break;
                }
                Some(BookManagerMsg::Snapshot(snap)) => {
                    mgr.apply_snapshot(snap);
                }
                Some(BookManagerMsg::Diff(diff)) => {
                    let symbol = diff.symbol.clone();
                    match mgr.apply_diff(diff) {
                        Ok(None) => {} // pre-sync drop
                        Ok(Some(event)) => {
                            if event_tx.send(event).await.is_err() {
                                break;
                            }
                        }
                        Err(AppError::SnapshotRequired(sym)) => {
                            warn!(symbol = %sym, "gap detected — snapshot required");
                            // Signal caller by sending a special event? For now just log.
                            // The connection_task handles reconnect/re-snapshot.
                        }
                        Err(AppError::OrderBookGap { expected, got }) => {
                            warn!(
                                symbol = %symbol,
                                expected,
                                got,
                                "order book gap — triggering re-snapshot"
                            );
                        }
                        Err(e) => {
                            warn!(error = %e, "book_manager error");
                        }
                    }
                }
            }
        }
    });

    (msg_tx, event_rx)
}
