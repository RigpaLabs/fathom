pub mod parquet;

/// Mock Binance HTTP + WebSocket server for e2e tests.
///
/// Uses axum to properly handle:
///   GET /depth?symbol=X         → snapshot JSON or queued HTTP errors
///   GET /stream  (WS upgrade)   → one "round" of messages per connection
///
/// Both routes share the same TCP port so the same address works for both
/// `ws_url_override` and `snapshot_url_override` in ConnectionConfig.
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use axum::{
    Router,
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::get,
};

// ── Internal state ─────────────────────────────────────────────────────────────

struct SymbolSnap {
    /// HTTP error codes to return first (consumed one by one).
    errors: VecDeque<u16>,
    /// Snapshot JSON to return after all errors are consumed.
    /// Kept persistently — not consumed — so reconnects re-use the same snapshot.
    json: Option<String>,
    /// Extra delay (ms) to add before returning the snapshot response.
    delay_ms: u64,
}

#[derive(Clone)]
struct ServerState {
    snapshots: Arc<Mutex<HashMap<String, SymbolSnap>>>,
    /// Each element = one WS connection's messages.  Consumed round-robin.
    ws_rounds: Arc<Mutex<VecDeque<Vec<String>>>>,
    connect_count: Arc<AtomicUsize>,
}

// ── Axum handlers ──────────────────────────────────────────────────────────────

async fn depth_handler(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    let symbol = params
        .get("symbol")
        .map(|s| s.to_uppercase())
        .unwrap_or_default();

    // Decide response while holding the lock, clone what we need, then drop lock.
    let (result, delay_ms) = {
        let mut snaps = state.snapshots.lock().unwrap();
        if let Some(entry) = snaps.get_mut(&symbol) {
            let delay = entry.delay_ms;
            if let Some(code) = entry.errors.pop_front() {
                (Some(Err(code)), delay)
            } else if let Some(json) = entry.json.clone() {
                (Some(Ok(json)), delay)
            } else {
                (None, delay)
            }
        } else {
            (None, 0)
        }
    };
    if delay_ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
    }
    let result = result;

    match result {
        None => (
            axum::http::StatusCode::NOT_FOUND,
            "symbol not found".to_string(),
        )
            .into_response(),
        Some(Err(code)) => {
            let status = axum::http::StatusCode::from_u16(code)
                .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
            (status, "mock http error".to_string()).into_response()
        }
        Some(Ok(json)) => (
            axum::http::StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            json,
        )
            .into_response(),
    }
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<ServerState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state))
}

async fn handle_ws(mut socket: WebSocket, state: ServerState) {
    state.connect_count.fetch_add(1, Ordering::Relaxed);

    let round = {
        let mut rounds = state.ws_rounds.lock().unwrap();
        rounds.pop_front()
    };

    if let Some(messages) = round {
        for msg in messages {
            // If client closed early (e.g. snapshot failed), send will error — just return.
            if socket.send(Message::Text(msg.into())).await.is_err() {
                return;
            }
        }
    }
    // Dropping `socket` here closes the WS connection cleanly.
    // connection_task will see `Ok(None)` from ws_stream.next() → break inner loop.
}

// ── Public API ─────────────────────────────────────────────────────────────────

/// Mock Binance HTTP + WebSocket server.
///
/// **Snapshot semantics:**
/// - Errors are queued per symbol and consumed one at a time.
/// - The JSON snapshot is persistent — once set, every successful request returns it.
///   Call `push_snapshot` again to update it.
///
/// **WS round semantics:**
/// - Each `push_ws_round` call adds one "round" to the queue.
/// - Each new WS connection consumes one round (sends its messages then closes).
/// - If no round is queued, the WS connection closes immediately (empty round).
pub struct MockBinanceServer {
    addr: SocketAddr,
    state: ServerState,
}

#[allow(dead_code)]
impl MockBinanceServer {
    /// Spin up the mock server and return a handle.
    pub async fn new() -> Self {
        let state = ServerState {
            snapshots: Arc::new(Mutex::new(HashMap::new())),
            ws_rounds: Arc::new(Mutex::new(VecDeque::new())),
            connect_count: Arc::new(AtomicUsize::new(0)),
        };

        let app = Router::new()
            .route("/depth", get(depth_handler))
            .route("/stream", get(ws_handler))
            .with_state(state.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            // Ignore error on test teardown (runtime drops the task).
            let _ = axum::serve(listener, app).await;
        });

        Self { addr, state }
    }

    /// Set (or replace) the snapshot JSON for `symbol`.
    /// The response will be served persistently after all queued errors are consumed.
    pub fn push_snapshot(
        &self,
        symbol: &str,
        last_update_id: i64,
        bids: Vec<(&str, &str)>,
        asks: Vec<(&str, &str)>,
    ) {
        let bids_json: Vec<[&str; 2]> = bids.iter().map(|(p, q)| [*p, *q]).collect();
        let asks_json: Vec<[&str; 2]> = asks.iter().map(|(p, q)| [*p, *q]).collect();
        let json = serde_json::json!({
            "lastUpdateId": last_update_id,
            "bids": bids_json,
            "asks": asks_json,
        })
        .to_string();

        let mut snaps = self.state.snapshots.lock().unwrap();
        let entry = snaps
            .entry(symbol.to_uppercase())
            .or_insert_with(|| SymbolSnap {
                errors: VecDeque::new(),
                json: None,
                delay_ms: 0,
            });
        entry.json = Some(json);
    }

    /// Queue an HTTP error status for the next snapshot request for `symbol`.
    pub fn push_http_error(&self, symbol: &str, status: u16) {
        let mut snaps = self.state.snapshots.lock().unwrap();
        let entry = snaps
            .entry(symbol.to_uppercase())
            .or_insert_with(|| SymbolSnap {
                errors: VecDeque::new(),
                json: None,
                delay_ms: 0,
            });
        entry.errors.push_back(status);
    }

    /// Add an artificial delay (milliseconds) before snapshot responses for `symbol`.
    /// Useful for testing that WS events buffered during slow snapshot fetch are processed.
    pub fn set_snapshot_delay_ms(&self, symbol: &str, delay_ms: u64) {
        let mut snaps = self.state.snapshots.lock().unwrap();
        let entry = snaps
            .entry(symbol.to_uppercase())
            .or_insert_with(|| SymbolSnap {
                errors: VecDeque::new(),
                json: None,
                delay_ms: 0,
            });
        entry.delay_ms = delay_ms;
    }

    /// Queue a batch of WS messages to send on the next incoming WS connection.
    pub fn push_ws_round(&self, messages: Vec<String>) {
        let mut rounds = self.state.ws_rounds.lock().unwrap();
        rounds.push_back(messages);
    }

    /// Full WebSocket URL (use as `ws_url_override`).
    pub fn ws_url(&self) -> String {
        format!("ws://{}/stream", self.addr)
    }

    /// HTTP snapshot URL template (use as `snapshot_url_override`).
    /// The placeholder `{symbol}` is substituted by connection_task.
    pub fn snapshot_url_template(&self) -> String {
        format!("http://{}/depth?symbol={{symbol}}&limit=5000", self.addr)
    }

    /// Number of WebSocket connections established so far.
    pub fn connected_count(&self) -> usize {
        self.state.connect_count.load(Ordering::Relaxed)
    }
}
