use std::{
    sync::{Arc, Mutex, atomic::AtomicU64},
    time::Instant,
};

use axum::{Router, routing::get};
use prometheus_client::{
    encoding::text::encode,
    metrics::{counter::Counter, family::Family, gauge::Gauge},
    registry::Registry,
};
use tracing::{info, warn};

use crate::monitor::{MonitorState, lock_state};

// ── Type aliases ────────────────────────────────────────────────────────────

type GaugeF64 = Gauge<f64, AtomicU64>;

// ── Label type ──────────────────────────────────────────────────────────────

/// Single "conn" label for per-connection metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, prometheus_client::encoding::EncodeLabelSet)]
pub struct ConnLabel {
    pub conn: String,
}

// ── Metrics registry ────────────────────────────────────────────────────────

/// All Prometheus metrics exposed by Fathom.
pub struct Metrics {
    pub events_total: Family<ConnLabel, Counter>,
    pub events_per_sec: Family<ConnLabel, GaugeF64>,
    pub uptime_seconds: Family<ConnLabel, GaugeF64>,
    pub symbols_active: Family<ConnLabel, Gauge>,
    pub ws_connected: Family<ConnLabel, Gauge>,
    pub ws_reconnects_total: Family<ConnLabel, Counter>,
    pub parquet_writes_total: Counter,
    pub parquet_write_errors_total: Counter,
}

impl Metrics {
    pub fn new(registry: &mut Registry) -> Self {
        let events_total = Family::<ConnLabel, Counter>::default();
        registry.register(
            "fathom_events_total",
            "Total events received per connection",
            events_total.clone(),
        );

        let events_per_sec = Family::<ConnLabel, GaugeF64>::default();
        registry.register(
            "fathom_events_per_sec",
            "Current event rate per connection",
            events_per_sec.clone(),
        );

        let uptime_seconds = Family::<ConnLabel, GaugeF64>::default();
        registry.register(
            "fathom_uptime_seconds",
            "Connection uptime in seconds",
            uptime_seconds.clone(),
        );

        let symbols_active = Family::<ConnLabel, Gauge>::default();
        registry.register(
            "fathom_symbols_active",
            "Number of active symbols per connection",
            symbols_active.clone(),
        );

        let ws_connected = Family::<ConnLabel, Gauge>::default();
        registry.register(
            "fathom_ws_connected",
            "WebSocket connection status (1=connected, 0=disconnected)",
            ws_connected.clone(),
        );

        let ws_reconnects_total = Family::<ConnLabel, Counter>::default();
        registry.register(
            "fathom_ws_reconnects_total",
            "Total WebSocket reconnections per connection",
            ws_reconnects_total.clone(),
        );

        let parquet_writes_total = Counter::default();
        registry.register(
            "fathom_parquet_writes_total",
            "Total parquet file writes",
            parquet_writes_total.clone(),
        );

        let parquet_write_errors_total = Counter::default();
        registry.register(
            "fathom_parquet_write_errors_total",
            "Total parquet write errors",
            parquet_write_errors_total.clone(),
        );

        Self {
            events_total,
            events_per_sec,
            uptime_seconds,
            symbols_active,
            ws_connected,
            ws_reconnects_total,
            parquet_writes_total,
            parquet_write_errors_total,
        }
    }
}

/// Shared handle to the metrics registry + metrics.
pub struct MetricsHandle {
    pub registry: Arc<Mutex<Registry>>,
    pub metrics: Arc<Metrics>,
}

/// Create a new metrics handle with a fresh registry.
pub fn new_metrics() -> MetricsHandle {
    let mut registry = Registry::default();
    let metrics = Arc::new(Metrics::new(&mut registry));
    MetricsHandle {
        registry: Arc::new(Mutex::new(registry)),
        metrics,
    }
}

// ── Sync task: MonitorState → Prometheus gauges ─────────────────────────────

/// Background task that periodically syncs MonitorState into Prometheus gauges.
/// Runs every 15s so /metrics always has fresh data without coupling monitor to prometheus.
pub async fn sync_monitor_to_metrics(monitor: MonitorState, metrics: Arc<Metrics>, start: Instant) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));

    loop {
        interval.tick().await;

        let conns_snap = {
            let guard = lock_state(&monitor);
            guard.clone()
        };

        let uptime = start.elapsed().as_secs_f64();

        for (conn_name, conn) in &conns_snap {
            let label = ConnLabel {
                conn: conn_name.clone(),
            };

            metrics
                .ws_connected
                .get_or_create(&label)
                .set(i64::from(conn.connected));

            // Reconnects: counter can only go up, so set to max of current
            // We track reconnects_today in MonitorState as a running total already,
            // so we use it directly. Counter::inner returns current value.
            let current_reconnects = metrics.ws_reconnects_total.get_or_create(&label).get();
            let diff = conn.reconnects_today.saturating_sub(current_reconnects);
            if diff > 0 {
                metrics
                    .ws_reconnects_total
                    .get_or_create(&label)
                    .inc_by(diff);
            }

            // Active symbols: count those with a recent event (< 120s)
            let now = Instant::now();
            let active = conn
                .symbols
                .values()
                .filter(|s| {
                    s.last_event_at
                        .is_some_and(|t| now.duration_since(t).as_secs() < 120)
                })
                .count() as i64;
            metrics.symbols_active.get_or_create(&label).set(active);

            // Uptime: same for all connections (process uptime)
            metrics.uptime_seconds.get_or_create(&label).set(uptime);
        }
    }
}

// ── HTTP server ─────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    registry: Arc<Mutex<Registry>>,
}

async fn metrics_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> (
    axum::http::StatusCode,
    [(axum::http::header::HeaderName, &'static str); 1],
    String,
) {
    let mut buf = String::new();
    let registry = state
        .registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match encode(&mut buf, &registry) {
        Ok(()) => (
            axum::http::StatusCode::OK,
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            buf,
        ),
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; charset=utf-8",
            )],
            format!("encoding error: {e}"),
        ),
    }
}

async fn health_handler() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({"status": "ok"}))
}

/// Build the axum router (public for testing).
pub fn build_router(registry: Arc<Mutex<Registry>>) -> Router {
    let state = AppState { registry };
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .with_state(state)
}

/// Start the metrics HTTP server. Returns when the server shuts down.
pub async fn run_metrics_server(registry: Arc<Mutex<Registry>>) {
    let port: u16 = std::env::var("FATHOM_METRICS_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(9090);

    let app = build_router(registry);

    let listener = match tokio::net::TcpListener::bind(("0.0.0.0", port)).await {
        Ok(l) => l,
        Err(e) => {
            warn!(error = %e, port, "failed to bind metrics server");
            return;
        }
    };

    info!(port, "metrics server listening");
    if let Err(e) = axum::serve(listener, app).await {
        warn!(error = %e, "metrics server error");
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_metrics_creates_registry() {
        let handle = new_metrics();
        let mut buf = String::new();
        let registry = handle.registry.lock().unwrap();
        encode(&mut buf, &registry).unwrap();
        // Should contain our metric names
        assert!(buf.contains("fathom_events_total"));
        assert!(buf.contains("fathom_ws_connected"));
        assert!(buf.contains("fathom_parquet_writes_total"));
    }

    #[test]
    fn test_counter_increment() {
        let handle = new_metrics();
        let label = ConnLabel {
            conn: "test".to_string(),
        };

        handle.metrics.events_total.get_or_create(&label).inc();
        handle.metrics.events_total.get_or_create(&label).inc();

        assert_eq!(handle.metrics.events_total.get_or_create(&label).get(), 2);
    }

    #[test]
    fn test_gauge_set() {
        let handle = new_metrics();
        let label = ConnLabel {
            conn: "test".to_string(),
        };

        handle.metrics.ws_connected.get_or_create(&label).set(1);
        assert_eq!(handle.metrics.ws_connected.get_or_create(&label).get(), 1);

        handle.metrics.ws_connected.get_or_create(&label).set(0);
        assert_eq!(handle.metrics.ws_connected.get_or_create(&label).get(), 0);
    }

    #[test]
    fn test_parquet_counters() {
        let handle = new_metrics();
        handle.metrics.parquet_writes_total.inc();
        handle.metrics.parquet_writes_total.inc();
        handle.metrics.parquet_write_errors_total.inc();

        assert_eq!(handle.metrics.parquet_writes_total.get(), 2);
        assert_eq!(handle.metrics.parquet_write_errors_total.get(), 1);
    }

    #[test]
    fn test_prometheus_text_format() {
        let handle = new_metrics();
        let label = ConnLabel {
            conn: "binance_spot".to_string(),
        };

        handle.metrics.events_total.get_or_create(&label).inc_by(42);
        handle.metrics.ws_connected.get_or_create(&label).set(1);

        let mut buf = String::new();
        let registry = handle.registry.lock().unwrap();
        encode(&mut buf, &registry).unwrap();

        // Verify Prometheus text exposition format
        assert!(buf.contains("# HELP fathom_events_total"));
        assert!(buf.contains("# TYPE fathom_events_total"));
        assert!(buf.contains(r#"conn="binance_spot""#));
        assert!(buf.contains("42"));
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let handle = new_metrics();
        let app = build_router(handle.registry.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let resp = reqwest::get(format!("http://{addr}/health")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "ok");

        server.abort();
    }

    #[tokio::test]
    async fn test_metrics_endpoint_returns_prometheus_format() {
        let handle = new_metrics();
        let label = ConnLabel {
            conn: "test_conn".to_string(),
        };
        handle
            .metrics
            .events_total
            .get_or_create(&label)
            .inc_by(100);
        handle.metrics.parquet_writes_total.inc_by(5);

        let app = build_router(handle.registry.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let resp = reqwest::get(format!("http://{addr}/metrics"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        let content_type = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(content_type.contains("text/plain"));

        let body = resp.text().await.unwrap();
        assert!(body.contains("fathom_events_total"));
        assert!(body.contains("fathom_parquet_writes_total"));
        assert!(body.contains(r#"conn="test_conn""#));
        assert!(body.contains("100"));

        server.abort();
    }

    #[tokio::test]
    async fn test_sync_monitor_to_metrics() {
        let monitor = crate::monitor::new_state();
        let handle = new_metrics();
        let start = Instant::now();

        // Populate monitor state
        {
            let mut guard = monitor.lock().unwrap();
            let cs = guard.entry("spot".to_string()).or_default();
            cs.connected = true;
            cs.reconnects_today = 5;
            let ss = cs.symbols.entry("ETHUSDT".to_string()).or_default();
            ss.last_event_at = Some(Instant::now());
        }

        // Run sync once (spawn + short sleep + abort)
        let metrics = handle.metrics.clone();
        let mon = monitor.clone();
        let sync_handle = tokio::spawn(sync_monitor_to_metrics(mon, metrics, start));
        // First tick of interval is immediate
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        sync_handle.abort();

        let label = ConnLabel {
            conn: "spot".to_string(),
        };
        assert_eq!(
            handle.metrics.ws_connected.get_or_create(&label).get(),
            1,
            "ws_connected should be 1 for connected conn"
        );
        assert_eq!(
            handle
                .metrics
                .ws_reconnects_total
                .get_or_create(&label)
                .get(),
            5,
            "reconnects should match monitor state"
        );
        assert_eq!(
            handle.metrics.symbols_active.get_or_create(&label).get(),
            1,
            "one active symbol"
        );
    }
}
