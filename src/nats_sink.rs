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

    let js = jetstream::new(client);

    if let Err(e) = ensure_streams(&js).await {
        warn!("NATS stream setup failed: {e}. Running without NATS.");
        return;
    }

    info!("NATS sink connected to {}", config.url);

    let snap_handle = tokio::spawn(publish_snapshots(js.clone(), snap_rx));
    let raw_handle = tokio::spawn(publish_depth(js, raw_rx));

    let _ = tokio::join!(snap_handle, raw_handle);
    info!("NATS sink stopped");
}

/// Ensure JetStream streams exist. Returns error if either fails — caller should
/// abort the sink rather than publish into void.
async fn ensure_streams(js: &jetstream::Context) -> Result<(), async_nats::Error> {
    // 1s snapshots: file storage, 24h retention, 200 MB limit
    js.get_or_create_stream(stream::Config {
        name: "FATHOM_SNAPSHOTS".into(),
        subjects: vec!["fathom.v1.*.*.snapshot".into()],
        storage: stream::StorageType::File,
        max_age: std::time::Duration::from_secs(24 * 3600),
        max_bytes: 200 * 1024 * 1024,
        ..Default::default()
    })
    .await?;

    // Raw depth diffs: file storage, 1h retention, 500 MB limit
    js.get_or_create_stream(stream::Config {
        name: "FATHOM_DEPTH".into(),
        subjects: vec!["fathom.v1.*.*.depth".into()],
        storage: stream::StorageType::File,
        max_age: std::time::Duration::from_secs(3600),
        max_bytes: 500 * 1024 * 1024,
        ..Default::default()
    })
    .await?;

    Ok(())
}

async fn publish_snapshots(js: jetstream::Context, mut rx: broadcast::Receiver<Snapshot1s>) {
    loop {
        match rx.recv().await {
            Ok(snap) => {
                let subject = snapshot_subject(&snap.exchange, &snap.symbol);
                match wire_encode(&snap) {
                    Ok(payload) => {
                        // Double-await: first sends the publish request, second
                        // awaits the JetStream ACK confirming durable storage.
                        match js.publish(subject, payload.into()).await {
                            Ok(ack_future) => {
                                if let Err(e) = ack_future.await {
                                    warn!("NATS snapshot ACK error: {e}");
                                }
                            }
                            Err(e) => warn!("NATS snapshot publish error: {e}"),
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

async fn publish_depth(js: jetstream::Context, mut rx: broadcast::Receiver<RawDiff>) {
    loop {
        match rx.recv().await {
            Ok(diff) => {
                let subject = depth_subject(&diff.exchange, &diff.symbol);
                match wire_encode(&diff) {
                    Ok(payload) => match js.publish(subject, payload.into()).await {
                        Ok(ack_future) => {
                            if let Err(e) = ack_future.await {
                                warn!("NATS depth ACK error: {e}");
                            }
                        }
                        Err(e) => warn!("NATS depth publish error: {e}"),
                    },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_subject_format() {
        assert_eq!(
            snapshot_subject("binance_perp", "ETHUSDT"),
            "fathom.v1.binance_perp.ETHUSDT.snapshot"
        );
        assert_eq!(
            snapshot_subject("hyperliquid", "ETH"),
            "fathom.v1.hyperliquid.ETH.snapshot"
        );
    }

    #[test]
    fn depth_subject_format() {
        assert_eq!(
            depth_subject("binance_spot", "BTCUSDT"),
            "fathom.v1.binance_spot.BTCUSDT.depth"
        );
        assert_eq!(
            depth_subject("dydx", "ETH-USD"),
            "fathom.v1.dydx.ETH-USD.depth"
        );
    }
}
