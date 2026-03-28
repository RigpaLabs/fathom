#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::{path::PathBuf, time::Instant};

use fathom::{
    CHANNEL_BUFFER,
    config::{Config, Exchange},
    connection::{connection_task, connection_task_dydx, connection_task_hl},
    exchange::{BinancePerp, BinanceSpot, Hyperliquid},
    metrics, monitor, nats_sink,
    writer::{raw::RawDiff, snap_1s::run_snap_writer},
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::info;

use fathom::accumulator::Snapshot1s;

const RAW_FLUSH_INTERVAL_S: u64 = 5;

/// Task supervision policy
///
/// | Task                | Failure policy                                       |
/// |---------------------|------------------------------------------------------|
/// | Connection tasks    | Restart with backoff (handled inside connection_task)|
/// | Raw Parquet writer  | **Fatal** — process exit                             |
/// | Snap Parquet writer | **Fatal** — process exit                             |
/// | NATS sink           | Warn + continue                                      |
/// | Metrics server      | Warn + continue                                      |
/// | Monitor             | Warn + continue                                      |
///
/// Writers are the core data durability path. If either dies unexpectedly
/// (e.g. disk full, IO error, panic), we cannot silently lose data — so we
/// treat writer exit as fatal and let the process restart via Docker/systemd.
///
/// NATS, metrics, and monitor are best-effort ancillaries. Their failure does
/// not compromise data integrity, so we log a warning and keep running.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "fathom=info".into());

    if std::env::var("FATHOM_JSON_LOG").is_ok() {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .init();
    }

    let cfg = Config::load("config.toml")?;
    let data_dir = std::env::var("DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or(cfg.data_dir);
    std::fs::create_dir_all(&data_dir)?;

    let cancel = CancellationToken::new();
    let monitor_state = monitor::new_state();
    let start = Instant::now();

    let (raw_tx, _) = broadcast::channel::<RawDiff>(CHANNEL_BUFFER);
    let (snap_tx, _) = broadcast::channel::<Snapshot1s>(CHANNEL_BUFFER);

    let raw_rx_parquet = raw_tx.subscribe();
    let snap_rx_parquet = snap_tx.subscribe();

    // Metrics: Prometheus /metrics + /health HTTP server
    // Failure policy: Warn + continue (ancillary, does not affect data integrity)
    let metrics_handle_data = metrics::new_metrics();
    let metrics_server_handle = tokio::spawn(metrics::run_metrics_server(
        metrics_handle_data.registry.clone(),
    ));
    let metrics_sync_handle = tokio::spawn(metrics::sync_monitor_to_metrics(
        monitor_state.clone(),
        metrics_handle_data.metrics.clone(),
        start,
    ));

    // Writers: Fatal on unexpected exit — losing a writer means silent data loss.
    let mut raw_handle = tokio::spawn(fathom::writer::raw::run_raw_writer(
        data_dir.clone(),
        raw_rx_parquet,
        RAW_FLUSH_INTERVAL_S,
        cfg.raw_rotate_hours,
        metrics_handle_data.metrics.clone(),
    ));
    let mut snap_handle = tokio::spawn(run_snap_writer(
        data_dir.clone(),
        snap_rx_parquet,
        cancel.clone(),
        metrics_handle_data.metrics.clone(),
    ));

    // Monitor: Warn + continue (health tracking only, not on the write path)
    let mon_handle = tokio::spawn(monitor::run_monitor(
        data_dir.clone(),
        monitor_state.clone(),
        start,
    ));

    // NATS sink: Warn + continue (best-effort streaming, not required for durability)
    let nats_handle = if let Some(nats_cfg) = cfg.nats.as_ref().filter(|c| c.enabled) {
        let raw_rx_nats = raw_tx.subscribe();
        let snap_rx_nats = snap_tx.subscribe();
        Some(tokio::spawn(nats_sink::run(
            nats_cfg.clone(),
            snap_rx_nats,
            raw_rx_nats,
        )))
    } else {
        None
    };

    let mut handles = Vec::new();
    for conn in cfg.connections {
        let data_dir = data_dir.clone();
        let mon = monitor_state.clone();
        let rtx = raw_tx.clone();
        let stx = snap_tx.clone();
        let ct = cancel.clone();
        let m = metrics_handle_data.metrics.clone();
        match conn.exchange {
            Exchange::BinanceSpot => {
                handles.push(tokio::spawn(connection_task(
                    conn,
                    Box::new(BinanceSpot),
                    data_dir,
                    mon,
                    rtx,
                    stx,
                    ct,
                    m,
                )));
            }
            Exchange::BinancePerp => {
                handles.push(tokio::spawn(connection_task(
                    conn,
                    Box::new(BinancePerp),
                    data_dir,
                    mon,
                    rtx,
                    stx,
                    ct,
                    m,
                )));
            }
            Exchange::Hyperliquid => {
                handles.push(tokio::spawn(connection_task_hl(
                    conn,
                    Box::new(Hyperliquid),
                    data_dir,
                    mon,
                    rtx,
                    stx,
                    ct,
                    m,
                )));
            }
            Exchange::Dydx => {
                handles.push(tokio::spawn(connection_task_dydx(
                    conn, data_dir, mon, rtx, stx, ct, m,
                )));
            }
        }
    }

    // Supervision loop: run until a shutdown signal OR a fatal writer exit.
    //
    // Writers (raw + snap Parquet) are watched directly. If either exits before
    // a clean shutdown signal, we log FATAL and call `process::exit(1)`.
    // The container orchestrator (Docker restart / systemd) will restart us.
    //
    // On a clean signal we fall through to the graceful drain below.
    //
    // NATS, metrics, and monitor are intentionally NOT supervised here;
    // their failure is non-fatal. Connection tasks manage their own restart loop.
    wait_for_shutdown_or_writer_exit(&mut raw_handle, &mut snap_handle, &cancel).await?;

    info!("shutting down fathom...");

    // Signal all connection tasks to exit cooperatively.
    cancel.cancel();

    // Wait for connection tasks to drain and exit (with timeout).
    for handle in handles {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
    }

    // Dropping senders closes the broadcast channel, signaling writers to finish.
    drop(raw_tx);
    drop(snap_tx);

    // Await writers so all buffered data is flushed before exit.
    let _ = raw_handle.await;
    let _ = snap_handle.await;
    // Await NATS sink so in-flight JetStream publishes complete.
    if let Some(h) = nats_handle {
        let _ = h.await;
    }
    mon_handle.abort();
    metrics_server_handle.abort();
    metrics_sync_handle.abort();

    info!("shutdown complete");

    Ok(())
}

/// Watches Parquet writer tasks and the OS shutdown signal concurrently.
///
/// Returns `Ok(())` on a clean shutdown signal (SIGINT / SIGTERM / Ctrl-C),
/// allowing the caller to proceed with the graceful buffer flush.
///
/// If either writer exits before a shutdown signal, this function logs a FATAL
/// message and calls [`std::process::exit`]`(1)`. Writers are the sole
/// mechanism for data durability; continuing without a working writer would
/// silently lose data, which is worse than a visible crash-restart cycle.
/// The container orchestrator (Docker `restart: unless-stopped` / systemd)
/// will restart the process automatically.
async fn wait_for_shutdown_or_writer_exit(
    raw_handle: &mut tokio::task::JoinHandle<()>,
    snap_handle: &mut tokio::task::JoinHandle<()>,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::select! {
            result = &mut *raw_handle => {
                tracing::error!("FATAL: raw parquet writer exited unexpectedly: {:?}", result);
                std::process::exit(1);
            }
            result = &mut *snap_handle => {
                tracing::error!("FATAL: snap parquet writer exited unexpectedly: {:?}", result);
                std::process::exit(1);
            }
            _ = cancel.cancelled() => {}
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        tokio::select! {
            result = &mut *raw_handle => {
                tracing::error!("FATAL: raw parquet writer exited unexpectedly: {:?}", result);
                std::process::exit(1);
            }
            result = &mut *snap_handle => {
                tracing::error!("FATAL: snap parquet writer exited unexpectedly: {:?}", result);
                std::process::exit(1);
            }
            _ = cancel.cancelled() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }

    Ok(())
}
