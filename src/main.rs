use std::time::Instant;

use fathom::{
    config::{Config, Exchange},
    connection::connection_task,
    exchange::{BinancePerp, BinanceSpot, ExchangeAdapter},
    monitor,
    writer::{raw::RawDiff, snap_1s::run_snap_writer},
};
use tokio::sync::mpsc;
use tracing::info;

use fathom::accumulator::Snapshot1s;

const RAW_FLUSH_INTERVAL_S: u64 = 300;
const CHANNEL_BUFFER: usize = 8_192;

fn make_adapter(exchange: &Exchange) -> Box<dyn ExchangeAdapter> {
    match exchange {
        Exchange::BinanceSpot => Box::new(BinanceSpot),
        Exchange::BinancePerp => Box::new(BinancePerp),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "fathom=info".into()),
        )
        .json()
        .init();

    let cfg = Config::load("config.toml")?;
    std::fs::create_dir_all(&cfg.data_dir)?;

    let monitor_state = monitor::new_state();
    let start = Instant::now();

    let (raw_tx, raw_rx) = mpsc::channel::<RawDiff>(CHANNEL_BUFFER);
    let (snap_tx, snap_rx) = mpsc::channel::<Snapshot1s>(CHANNEL_BUFFER);

    let raw_handle = tokio::spawn(fathom::writer::raw::run_raw_writer(
        cfg.data_dir.clone(),
        raw_rx,
        RAW_FLUSH_INTERVAL_S,
    ));
    let snap_handle = tokio::spawn(run_snap_writer(cfg.data_dir.clone(), snap_rx));
    let mon_handle = tokio::spawn(monitor::run_monitor(
        cfg.data_dir.clone(),
        monitor_state.clone(),
        start,
    ));

    let mut handles = Vec::new();
    for conn in cfg.connections {
        let adapter = make_adapter(&conn.exchange);
        let data_dir = cfg.data_dir.clone();
        let mon = monitor_state.clone();
        let rtx = raw_tx.clone();
        let stx = snap_tx.clone();
        handles.push(tokio::spawn(connection_task(
            conn, adapter, data_dir, mon, rtx, stx,
        )));
    }

    tokio::signal::ctrl_c().await?;
    info!("shutting down fathom...");

    // Abort connection tasks (they loop forever and have no shutdown channel).
    for handle in &handles {
        handle.abort();
    }

    // Closing senders signals writers to drain their channels and finalize files.
    drop(raw_tx);
    drop(snap_tx);

    // Await writers so all buffered data is flushed to disk before exit.
    let _ = raw_handle.await;
    let _ = snap_handle.await;
    mon_handle.abort();

    info!("shutdown complete");

    Ok(())
}
