use async_nats::jetstream::{self, stream};
use fathom_types::{RawDiff, RawTrade, Snapshot1s, wire_encode};
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::{config::NatsConfig, writer::deriv::DerivEvent};

fn snapshot_subject(exchange: &str, symbol: &str) -> String {
    format!("fathom.v1.{exchange}.{symbol}.snapshot")
}

/// Subject for a derivatives event. The wire payload carries no type
/// discriminant, so the feed suffix (`funding` / `oi` / `liq`) is the only
/// thing telling consumers which struct to decode.
fn deriv_subject(event: &DerivEvent) -> String {
    format!(
        "fathom.v1.{}.{}.{}",
        event.exchange(),
        event.symbol(),
        event.feed()
    )
}

fn depth_subject(exchange: &str, symbol: &str) -> String {
    format!("fathom.v1.{exchange}.{symbol}.depth")
}

fn trade_subject(exchange: &str, symbol: &str) -> String {
    format!("fathom.v1.{exchange}.{symbol}.trade")
}

pub async fn run(
    config: NatsConfig,
    snap_rx: broadcast::Receiver<Snapshot1s>,
    raw_rx: broadcast::Receiver<RawDiff>,
    trade_rx: broadcast::Receiver<RawTrade>,
    deriv_rx: broadcast::Receiver<DerivEvent>,
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
    let trade_handle = tokio::spawn(publish_trades(js.clone(), trade_rx));
    let deriv_handle = tokio::spawn(publish_deriv(js.clone(), deriv_rx));
    let raw_handle = tokio::spawn(publish_depth(js, raw_rx));

    let _ = tokio::join!(snap_handle, raw_handle, trade_handle, deriv_handle);
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

    // Raw trade tape: file storage, 24h retention, 200 MB limit
    // (low volume relative to depth — sized like FATHOM_SNAPSHOTS)
    js.get_or_create_stream(stream::Config {
        name: "FATHOM_TRADES".into(),
        subjects: vec!["fathom.v1.*.*.trade".into()],
        storage: stream::StorageType::File,
        max_age: std::time::Duration::from_secs(24 * 3600),
        max_bytes: 200 * 1024 * 1024,
        ..Default::default()
    })
    .await?;

    // Derivatives feeds (funding / open interest / liquidations): file storage,
    // 24h retention, 200 MB limit — tiny volume, sized like FATHOM_TRADES
    js.get_or_create_stream(stream::Config {
        name: "FATHOM_DERIV".into(),
        subjects: vec![
            "fathom.v1.*.*.funding".into(),
            "fathom.v1.*.*.oi".into(),
            "fathom.v1.*.*.liq".into(),
        ],
        storage: stream::StorageType::File,
        max_age: std::time::Duration::from_secs(24 * 3600),
        max_bytes: 200 * 1024 * 1024,
        ..Default::default()
    })
    .await?;

    Ok(())
}

/// Publish derivatives events. One channel carries all three structs; each
/// variant is wire-encoded as its *inner* struct on its own subject —
/// `DerivEvent` never crosses the wire (no type discriminant in the format).
async fn publish_deriv(js: jetstream::Context, mut rx: broadcast::Receiver<DerivEvent>) {
    loop {
        match rx.recv().await {
            Ok(event) => {
                let subject = deriv_subject(&event);
                let encoded = match &event {
                    DerivEvent::MarkFunding(m) => wire_encode(m),
                    DerivEvent::OpenInterest(o) => wire_encode(o),
                    DerivEvent::Liquidation(l) => wire_encode(l),
                };
                match encoded {
                    Ok(payload) => match js.publish(subject, payload.into()).await {
                        Ok(ack_future) => {
                            if let Err(e) = ack_future.await {
                                warn!("NATS deriv ACK error: {e}");
                            }
                        }
                        Err(e) => warn!("NATS deriv publish error: {e}"),
                    },
                    Err(e) => warn!("deriv encode error: {e}"),
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("NATS deriv sink lagged by {n} messages");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

async fn publish_trades(js: jetstream::Context, mut rx: broadcast::Receiver<RawTrade>) {
    loop {
        match rx.recv().await {
            Ok(trade) => {
                let subject = trade_subject(&trade.exchange, &trade.symbol);
                match wire_encode(&trade) {
                    Ok(payload) => match js.publish(subject, payload.into()).await {
                        Ok(ack_future) => {
                            if let Err(e) = ack_future.await {
                                warn!("NATS trade ACK error: {e}");
                            }
                        }
                        Err(e) => warn!("NATS trade publish error: {e}"),
                    },
                    Err(e) => warn!("trade encode error: {e}"),
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("NATS trade sink lagged by {n} messages");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
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
    fn trade_subject_format() {
        assert_eq!(
            trade_subject("binance_perp", "ETHUSDT"),
            "fathom.v1.binance_perp.ETHUSDT.trade"
        );
        assert_eq!(
            trade_subject("hyperliquid", "ETH"),
            "fathom.v1.hyperliquid.ETH.trade"
        );
    }

    #[test]
    fn deriv_subject_routes_each_variant_to_its_own_feed_suffix() {
        // The wire format has no type discriminant — the subject suffix is the
        // only thing identifying the payload type, so the mapping is pinned.
        let mf = DerivEvent::MarkFunding(fathom_types::MarkFunding {
            timestamp_us: 0,
            exchange: "binance_perp".into(),
            symbol: "ETHUSDT".into(),
            mark_px: 3000.0,
            index_px: None,
            funding_rate: 0.0001,
            next_funding_ts: None,
        });
        assert_eq!(deriv_subject(&mf), "fathom.v1.binance_perp.ETHUSDT.funding");

        let oi = DerivEvent::OpenInterest(fathom_types::OpenInterest {
            timestamp_us: 0,
            exchange: "hyperliquid".into(),
            symbol: "ETH".into(),
            oi_base: 1.0,
            oi_quote: None,
        });
        assert_eq!(deriv_subject(&oi), "fathom.v1.hyperliquid.ETH.oi");

        let liq = DerivEvent::Liquidation(fathom_types::Liquidation {
            timestamp_us: 0,
            exchange: "binance_perp".into(),
            symbol: "BTCUSDT".into(),
            side: "SELL".into(),
            price: 1.0,
            qty: 1.0,
        });
        assert_eq!(deriv_subject(&liq), "fathom.v1.binance_perp.BTCUSDT.liq");
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
