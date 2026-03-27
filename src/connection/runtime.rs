use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::monitor::{MonitorState, lock_state};

pub const BACKOFF_START_MS: u64 = 1_000;
pub const BACKOFF_MAX_MS: u64 = 60_000;
pub const RATE_LIMIT_BACKOFF_S: u64 = 300;
pub const DEFAULT_HEARTBEAT_TIMEOUT_S: u64 = 30;

// ── Backoff ─────────────────────────────────────────────────────────────────

pub async fn sleep_backoff(backoff_ms: &mut u64) {
    let jitter = rand::thread_rng().gen_range(0..*backoff_ms / 4 + 1);
    let sleep_ms = (*backoff_ms + jitter).min(BACKOFF_MAX_MS);
    info!("reconnect backoff {}ms", sleep_ms);
    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    *backoff_ms = (*backoff_ms * 2).min(BACKOFF_MAX_MS);
}

// ── WS connect ──────────────────────────────────────────────────────────────

type WsResult =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Connect to a WS endpoint. Returns the stream on success, or None after
/// logging the error. Resets backoff on success.
pub async fn connect_ws(url: &str, name: &str, backoff_ms: &mut u64) -> Option<WsResult> {
    match tokio_tungstenite::connect_async(url).await {
        Ok((ws, _)) => {
            info!(conn = %name, url = %url, "WS connected");
            *backoff_ms = BACKOFF_START_MS;
            Some(ws)
        }
        Err(e) => {
            warn!(conn = %name, error = %e, "WS connect failed");
            sleep_backoff(backoff_ms).await;
            None
        }
    }
}

// ── WS forwarder ────────────────────────────────────────────────────────────

type WsStream = futures_util::stream::SplitStream<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
>;
type WsSink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Message,
>;

/// Spawn a unidirectional forwarder: reads WS frames, answers Ping/Pong,
/// forwards Text/Binary as String to `fwd_tx`. Dropping `fwd_tx` on exit
/// signals the caller that the stream is done.
pub fn spawn_forwarder(
    name: String,
    sink: WsSink,
    stream: WsStream,
    heartbeat_timeout_s: u64,
    fwd_tx: mpsc::Sender<String>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let hb_dur = Duration::from_secs(heartbeat_timeout_s);
        let mut sink = sink;
        let mut stream = stream;
        loop {
            match tokio::time::timeout(hb_dur, stream.next()).await {
                Err(_) => {
                    warn!(conn = %name, "heartbeat timeout");
                    break;
                }
                Ok(None) => {
                    info!(conn = %name, "WS stream closed");
                    break;
                }
                Ok(Some(Err(e))) => {
                    warn!(conn = %name, error = %e, "WS error");
                    break;
                }
                Ok(Some(Ok(msg))) => match msg {
                    Message::Text(t) => {
                        if fwd_tx.send(t.to_string()).await.is_err() {
                            break;
                        }
                    }
                    Message::Binary(b) => {
                        if let Ok(s) = String::from_utf8(b.into())
                            && fwd_tx.send(s).await.is_err()
                        {
                            break;
                        }
                    }
                    Message::Ping(p) => {
                        let _ = sink.send(Message::Pong(p)).await;
                    }
                    Message::Close(_) => break,
                    _ => {}
                },
            }
        }
    })
}

/// Spawn a bidirectional forwarder: in addition to the unidirectional behavior,
/// also reads outgoing messages from `send_rx` and sends them over the WS sink.
pub fn spawn_bidi_forwarder(
    name: String,
    sink: WsSink,
    stream: WsStream,
    heartbeat_timeout_s: u64,
    fwd_tx: mpsc::Sender<String>,
    mut send_rx: mpsc::Receiver<String>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let hb_dur = Duration::from_secs(heartbeat_timeout_s);
        let mut sink = sink;
        let mut stream = stream;
        loop {
            tokio::select! {
                msg = tokio::time::timeout(hb_dur, stream.next()) => {
                    match msg {
                        Err(_) => {
                            warn!(conn = %name, "heartbeat timeout");
                            break;
                        }
                        Ok(None) => {
                            info!(conn = %name, "WS stream closed");
                            break;
                        }
                        Ok(Some(Err(e))) => {
                            warn!(conn = %name, error = %e, "WS error");
                            break;
                        }
                        Ok(Some(Ok(msg))) => match msg {
                            Message::Text(t) => {
                                if fwd_tx.send(t.to_string()).await.is_err() {
                                    break;
                                }
                            }
                            Message::Binary(b) => {
                                if let Ok(s) = String::from_utf8(b.into())
                                    && fwd_tx.send(s).await.is_err()
                                {
                                    break;
                                }
                            }
                            Message::Ping(p) => {
                                let _ = sink.send(Message::Pong(p)).await;
                            }
                            Message::Close(_) => break,
                            _ => {}
                        },
                    }
                }
                outgoing = send_rx.recv() => {
                    match outgoing {
                        None => break,
                        Some(text) => {
                            if sink.send(Message::Text(text.into())).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        }
    })
}

// ── Monitor helpers ─────────────────────────────────────────────────────────

pub fn init_monitor(monitor: &MonitorState, name: &str, symbols: &[String]) {
    let mut state = lock_state(monitor);
    let cs = state.entry(name.to_string()).or_default();
    cs.connected = false;
    for sym in symbols {
        cs.symbols.entry(sym.clone()).or_default();
    }
}

pub fn mark_connected(monitor: &MonitorState, name: &str) {
    let mut state = lock_state(monitor);
    if let Some(cs) = state.get_mut(name) {
        cs.connected = true;
    }
}

pub fn mark_disconnected(monitor: &MonitorState, name: &str) {
    let mut state = lock_state(monitor);
    if let Some(cs) = state.get_mut(name) {
        cs.connected = false;
        cs.reconnects_today += 1;
    }
}

pub fn record_event(monitor: &MonitorState, name: &str, symbol: &str) {
    let mut state = lock_state(monitor);
    if let Some(cs) = state.get_mut(name)
        && let Some(ss) = cs.symbols.get_mut(symbol)
    {
        ss.last_event_at = Some(Instant::now());
    }
}

pub fn record_gap(monitor: &MonitorState, name: &str, symbol: &str) {
    let mut state = lock_state(monitor);
    if let Some(cs) = state.get_mut(name)
        && let Some(ss) = cs.symbols.get_mut(symbol)
    {
        ss.gaps_today += 1;
    }
}

// ── Metrics helpers ─────────────────────────────────────────────────────────

pub fn inc_event_metrics(metrics: &crate::metrics::Metrics, conn_name: &str, symbol: &str) {
    metrics
        .events_total
        .get_or_create(&crate::metrics::ConnLabel {
            conn: conn_name.to_string(),
        })
        .inc();
    metrics
        .events_by_symbol
        .get_or_create(&crate::metrics::SymbolLabel {
            conn: conn_name.to_string(),
            symbol: symbol.to_string(),
        })
        .inc();
}

// ── Stats tracker ───────────────────────────────────────────────────────────

pub struct StatsTracker {
    pub event_count: u64,
    start: Instant,
    pub ticker: tokio::time::Interval,
}

impl Default for StatsTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl StatsTracker {
    pub fn new() -> Self {
        let ticker = tokio::time::interval(Duration::from_secs(60));
        Self {
            event_count: 0,
            start: Instant::now(),
            ticker,
        }
    }

    /// Consume the immediate first tick. Call this in the event loop setup.
    pub async fn skip_first_tick(&mut self) {
        self.ticker.tick().await;
    }

    pub fn inc(&mut self) {
        self.event_count += 1;
    }

    pub fn log(&self, name: &str, symbol_count: usize) {
        let elapsed = self.start.elapsed().as_secs();
        let rate = if elapsed > 0 {
            self.event_count / elapsed
        } else {
            0
        };
        info!(
            conn = %name,
            events = self.event_count,
            uptime_s = elapsed,
            events_per_sec = rate,
            symbols = symbol_count,
            "periodic stats"
        );
    }
}

/// Create a 1s snap ticker with the standard configuration.
pub fn snap_ticker() -> tokio::time::Interval {
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker
}
