use async_nats::jetstream::{self, stream};
use fathom_types::{RawDiff, Snapshot1s, wire_encode};
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::config::NatsConfig;

fn snapshot_subject(exchange: &str, symbol: &str) -> String {
    format!("fathom.v1.{exchange}.{symbol}.snapshot")
}

fn depth_subject(exchange: &str, symbol: &str) -> String {
    format!("fathom.v1.{exchange}.{symbol}.depth")
}

pub async fn run(
    config: NatsConfig,
    snap_rx: broadcast::Receiver<Snapshot1s>,
    raw_rx: broadcast::Receiver<RawDiff>,
) {
    let client = match async_nats::connect(&config.url).await {
        Ok(c) => c,
        Err(e) => {
            warn!("NATS connect failed: {e}. Running without NATS.");
            return;
        }
    };

    let js = jetstream::new(client.clone());
    ensure_streams(&js).await;

    info!("NATS sink connected to {}", config.url);

    let snap_handle = tokio::spawn(publish_snapshots(js, snap_rx));
    let raw_handle = tokio::spawn(publish_depth(client, raw_rx));

    let _ = tokio::join!(snap_handle, raw_handle);
    info!("NATS sink stopped");
}

async fn ensure_streams(js: &jetstream::Context) {
    // 1s snapshots: file storage, 24h retention, 200 MB limit
    if let Err(e) = js
        .get_or_create_stream(stream::Config {
            name: "FATHOM_SNAPSHOTS".into(),
            subjects: vec!["fathom.v1.*.*.snapshot".into()],
            storage: stream::StorageType::File,
            max_age: std::time::Duration::from_secs(24 * 3600),
            max_bytes: 200 * 1024 * 1024,
            ..Default::default()
        })
        .await
    {
        warn!("failed to ensure FATHOM_SNAPSHOTS stream: {e}");
    }

    // Raw depth diffs: memory storage, 1h retention, 500 MB limit
    if let Err(e) = js
        .get_or_create_stream(stream::Config {
            name: "FATHOM_DEPTH".into(),
            subjects: vec!["fathom.v1.*.*.depth".into()],
            storage: stream::StorageType::Memory,
            max_age: std::time::Duration::from_secs(3600),
            max_bytes: 500 * 1024 * 1024,
            ..Default::default()
        })
        .await
    {
        warn!("failed to ensure FATHOM_DEPTH stream: {e}");
    }
}

async fn publish_snapshots(js: jetstream::Context, mut rx: broadcast::Receiver<Snapshot1s>) {
    loop {
        match rx.recv().await {
            Ok(snap) => {
                let subject = snapshot_subject(&snap.exchange, &snap.symbol);
                match wire_encode(&snap) {
                    Ok(payload) => {
                        if let Err(e) = js.publish(subject, payload.into()).await {
                            warn!("NATS snapshot publish error: {e}");
                        }
                    }
                    Err(e) => warn!("snapshot encode error: {e}"),
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("NATS snap sink lagged by {n} messages");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

async fn publish_depth(client: async_nats::Client, mut rx: broadcast::Receiver<RawDiff>) {
    loop {
        match rx.recv().await {
            Ok(diff) => {
                let subject = depth_subject(&diff.exchange, &diff.symbol);
                match wire_encode(&diff) {
                    Ok(payload) => {
                        if let Err(e) = client.publish(subject, payload.into()).await {
                            warn!("NATS depth publish error: {e}");
                        }
                    }
                    Err(e) => warn!("depth encode error: {e}"),
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("NATS depth sink lagged by {n} messages");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}
