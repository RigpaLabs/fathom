/// Mock Bybit v5 WebSocket server for e2e tests.
///
/// Unlike `MockBinanceServer` (HTTP snapshot + WS diffs), Bybit is entirely
/// WS-native (`specs/bybit-collection.md`'s "Architecturally different"
/// section) — one endpoint, subscription via an `{"op":"subscribe",...}`
/// frame sent *after* connecting, initial book delivered as a `type:
/// "snapshot"` message over that same socket. So this mock has no HTTP route
/// at all: just a single WS endpoint that drains the client's subscribe
/// (and ping) frames, acks them, then plays back one queued "round" of
/// scripted topic messages before closing — same round semantics as
/// `MockBinanceServer::push_ws_round` (one round consumed per connection,
/// letting a test script a reconnect scenario across multiple rounds).
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::get,
};

#[derive(Clone)]
struct BybitServerState {
    /// Each element = one WS connection's scripted messages. Consumed
    /// round-robin, like `MockBinanceServer::ws_rounds`.
    ws_rounds: Arc<Mutex<VecDeque<Vec<String>>>>,
    connect_count: Arc<AtomicUsize>,
    /// Subscribe topic batches seen so far, one `Vec` per `subscribe` WS
    /// frame (not per connection) — lets a test assert the spot 10-arg-cap
    /// split into multiple frames actually happened on the wire, not just
    /// in `bybit.rs`'s own unit tests.
    subscribed_batches: Arc<Mutex<Vec<Vec<String>>>>,
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<BybitServerState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state))
}

async fn handle_ws(mut socket: WebSocket, state: BybitServerState) {
    state.connect_count.fetch_add(1, Ordering::Relaxed);

    // Drain the client's op:subscribe (and any op:ping) frames for a short
    // window, replying with the ack shape `src/connection/bybit.rs`'s
    // `BybitAck` expects. Spot's 10-arg cap means the client may send more
    // than one subscribe batch on the same socket — keep reading until it
    // goes quiet (short per-read timeout) rather than assuming exactly one
    // frame.
    loop {
        match tokio::time::timeout(Duration::from_millis(150), socket.recv()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) else {
                    continue;
                };
                match v.get("op").and_then(|o| o.as_str()) {
                    Some("subscribe") => {
                        if let Some(args) = v.get("args").and_then(|a| a.as_array()) {
                            let batch: Vec<String> = args
                                .iter()
                                .filter_map(|a| a.as_str().map(str::to_string))
                                .collect();
                            state.subscribed_batches.lock().unwrap().push(batch);
                        }
                        let ack = serde_json::json!({
                            "success": true,
                            "ret_msg": "",
                            "conn_id": "mock",
                            "req_id": "",
                            "op": "subscribe"
                        })
                        .to_string();
                        if socket.send(Message::Text(ack.into())).await.is_err() {
                            return;
                        }
                    }
                    Some("ping") => {
                        let pong = serde_json::json!({
                            "success": true,
                            "ret_msg": "pong",
                            "conn_id": "mock",
                            "op": "ping"
                        })
                        .to_string();
                        if socket.send(Message::Text(pong.into())).await.is_err() {
                            return;
                        }
                    }
                    _ => {}
                }
            }
            Ok(Some(Ok(_))) => {} // ignore non-text frames (ping/pong/binary)
            _ => break,           // timeout, close, or error — stop draining
        }
    }

    let round = { state.ws_rounds.lock().unwrap().pop_front() };
    if let Some(messages) = round {
        for msg in messages {
            // If client already closed (e.g. reacted to a gap and reconnected
            // before we finished writing), send will error — just return.
            if socket.send(Message::Text(msg.into())).await.is_err() {
                return;
            }
        }
    }
    // Dropping `socket` here closes the connection — connection_task_bybit
    // sees the stream end and reconnects.
}

/// Mock Bybit v5 WS server.
///
/// **Round semantics:** each `push_ws_round` call queues one round; each new
/// WS connection consumes one round (acks subscribes, sends its messages,
/// then closes). If no round is queued, the connection closes immediately
/// after the subscribe-drain phase.
pub struct MockBybitServer {
    addr: SocketAddr,
    state: BybitServerState,
}

#[allow(dead_code)]
impl MockBybitServer {
    /// Spin up the mock server and return a handle.
    pub async fn new() -> Self {
        let state = BybitServerState {
            ws_rounds: Arc::new(Mutex::new(VecDeque::new())),
            connect_count: Arc::new(AtomicUsize::new(0)),
            subscribed_batches: Arc::new(Mutex::new(Vec::new())),
        };

        let app = Router::new()
            .route("/v5/public/linear", get(ws_handler))
            .with_state(state.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Self { addr, state }
    }

    /// Queue a batch of WS messages to send on the next incoming connection.
    pub fn push_ws_round(&self, messages: Vec<String>) {
        self.state.ws_rounds.lock().unwrap().push_back(messages);
    }

    /// Full WebSocket URL (use as `ws_url_override`) — Bybit has no separate
    /// snapshot URL, so there is no `snapshot_url_template` here.
    pub fn ws_url(&self) -> String {
        format!("ws://{}/v5/public/linear", self.addr)
    }

    /// Number of WebSocket connections established so far.
    pub fn connected_count(&self) -> usize {
        self.state.connect_count.load(Ordering::Relaxed)
    }

    /// Subscribe topic batches seen so far, one `Vec` per `subscribe` frame.
    pub fn subscribed_batches(&self) -> Vec<Vec<String>> {
        self.state.subscribed_batches.lock().unwrap().clone()
    }
}
