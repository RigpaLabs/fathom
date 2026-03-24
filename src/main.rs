#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::{path::PathBuf, time::Instant};

use fathom::{
    CHANNEL_BUFFER,
    config::{Config, Exchange},
    connection::connection_task,
    connection_dydx::connection_task_dydx,
    connection_hl::connection_task_hl,
    exchange::{BinancePerp, BinanceSpot, Hyperliquid},
    monitor, nats_sink,
    writer::{raw::RawDiff, snap_1s::run_snap_writer},
};
use tokio::sync::broadcast;
use tracing::info;

use fathom::accumulator::Snapshot1s;

const RAW_FLUSH_INTERVAL_S: u64 = 5;

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

    let monitor_state = monitor::new_state();
    let start = Instant::now();

    let (raw_tx, _) = broadcast::channel::<RawDiff>(CHANNEL_BUFFER);
    let (snap_tx, _) = broadcast::channel::<Snapshot1s>(CHANNEL_BUFFER);

    let raw_rx_parquet = raw_tx.subscribe();
    let snap_rx_parquet = snap_tx.subscribe();

    let raw_handle = tokio::spawn(fathom::writer::raw::run_raw_writer(
        data_dir.clone(),
        raw_rx_parquet,
        RAW_FLUSH_INTERVAL_S,
        cfg.raw_rotate_hours,
    ));
    let snap_handle = tokio::spawn(run_snap_writer(data_dir.clone(), snap_rx_parquet));
    let mon_handle = tokio::spawn(monitor::run_monitor(
        data_dir.clone(),
        monitor_state.clone(),
        start,
    ));

    // Optionally start the NATS sink
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
        match conn.exchange {
            Exchange::BinanceSpot => {
                handles.push(tokio::spawn(connection_task(
                    conn,
                    Box::new(BinanceSpot),
                    data_dir,
                    mon,
                    rtx,
                    stx,
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
                )));
            }
            Exchange::Dydx => {
                handles.push(tokio::spawn(connection_task_dydx(
                    conn, data_dir, mon, rtx, stx,
                )));
            }
        }
    }

    // Handle both SIGINT (Ctrl-C) and SIGTERM (Docker stop).
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = sigterm.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;

    info!("shutting down fathom...");

    // Abort connection tasks (they loop forever and have no shutdown channel).
    for handle in &handles {
        handle.abort();
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

    info!("shutdown complete");

    Ok(())
}
