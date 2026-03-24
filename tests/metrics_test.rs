use std::time::Instant;

use fathom::metrics::{ConnLabel, build_router, new_metrics, sync_monitor_to_metrics};
use fathom::monitor::new_state;
use prometheus_client::encoding::text::encode;

// ── Unit tests ──────────────────────────────────────────────────────────────

#[test]
fn test_metrics_registry_contains_all_metrics() {
    let handle = new_metrics();
    let mut buf = String::new();
    let registry = handle.registry.lock().unwrap();
    encode(&mut buf, &registry).unwrap();

    let expected = [
        "fathom_events_total",
        "fathom_events_per_sec",
        "fathom_uptime_seconds",
        "fathom_symbols_active",
        "fathom_ws_connected",
        "fathom_ws_reconnects_total",
        "fathom_parquet_writes_total",
        "fathom_parquet_write_errors_total",
    ];
    for name in expected {
        assert!(buf.contains(name), "missing metric: {name}");
    }
}

#[test]
fn test_events_total_increments_per_connection() {
    let handle = new_metrics();
    let spot = ConnLabel {
        conn: "spot".to_string(),
    };
    let perp = ConnLabel {
        conn: "perp".to_string(),
    };

    handle.metrics.events_total.get_or_create(&spot).inc_by(10);
    handle.metrics.events_total.get_or_create(&perp).inc_by(20);

    assert_eq!(handle.metrics.events_total.get_or_create(&spot).get(), 10);
    assert_eq!(handle.metrics.events_total.get_or_create(&perp).get(), 20);
}

#[test]
fn test_parquet_counters_independent() {
    let handle = new_metrics();
    handle.metrics.parquet_writes_total.inc_by(100);
    handle.metrics.parquet_write_errors_total.inc_by(3);

    assert_eq!(handle.metrics.parquet_writes_total.get(), 100);
    assert_eq!(handle.metrics.parquet_write_errors_total.get(), 3);
}

#[test]
fn test_gauge_f64_precision() {
    let handle = new_metrics();
    let label = ConnLabel {
        conn: "test".to_string(),
    };

    handle
        .metrics
        .events_per_sec
        .get_or_create(&label)
        .set(42.5);
    let val = handle.metrics.events_per_sec.get_or_create(&label).get();
    assert!((val - 42.5).abs() < f64::EPSILON);
}

// ── HTTP endpoint tests ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_health_returns_ok() {
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
async fn test_metrics_endpoint_content_type() {
    let handle = new_metrics();
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
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        ct.contains("text/plain"),
        "content-type should be text/plain, got: {ct}"
    );

    server.abort();
}

#[tokio::test]
async fn test_metrics_endpoint_reflects_counter_values() {
    let handle = new_metrics();
    let label = ConnLabel {
        conn: "binance_perp".to_string(),
    };
    handle
        .metrics
        .events_total
        .get_or_create(&label)
        .inc_by(999);

    let app = build_router(handle.registry.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let body = reqwest::get(format!("http://{addr}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert!(
        body.contains("999"),
        "metrics body should contain counter value 999"
    );
    assert!(
        body.contains(r#"conn="binance_perp""#),
        "metrics body should contain conn label"
    );

    server.abort();
}

// ── Monitor sync tests ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_sync_updates_ws_connected() {
    let monitor = new_state();
    let handle = new_metrics();
    let start = Instant::now();

    {
        let mut guard = monitor.lock().unwrap();
        let cs = guard.entry("hl".to_string()).or_default();
        cs.connected = true;
        cs.symbols
            .entry("ETH".to_string())
            .or_default()
            .last_event_at = Some(Instant::now());
    }

    let metrics = handle.metrics.clone();
    let sync = tokio::spawn(sync_monitor_to_metrics(monitor.clone(), metrics, start));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    sync.abort();

    let label = ConnLabel {
        conn: "hl".to_string(),
    };
    assert_eq!(handle.metrics.ws_connected.get_or_create(&label).get(), 1);
    assert_eq!(handle.metrics.symbols_active.get_or_create(&label).get(), 1);
}

#[tokio::test]
async fn test_sync_disconnected_sets_zero() {
    let monitor = new_state();
    let handle = new_metrics();
    let start = Instant::now();

    {
        let mut guard = monitor.lock().unwrap();
        let cs = guard.entry("dydx".to_string()).or_default();
        cs.connected = false;
    }

    let metrics = handle.metrics.clone();
    let sync = tokio::spawn(sync_monitor_to_metrics(monitor.clone(), metrics, start));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    sync.abort();

    let label = ConnLabel {
        conn: "dydx".to_string(),
    };
    assert_eq!(handle.metrics.ws_connected.get_or_create(&label).get(), 0);
    assert_eq!(handle.metrics.symbols_active.get_or_create(&label).get(), 0);
}
