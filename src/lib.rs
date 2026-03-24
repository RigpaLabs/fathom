// AppError carries tokio_tungstenite::tungstenite::Error (136 bytes). Boxing it
// would add noise at every use-site; suppress the lint crate-wide instead.
#![allow(clippy::result_large_err)]

/// Channel buffer size for all mpsc channels (forwarder, raw writer, snap writer).
pub const CHANNEL_BUFFER: usize = 4_096;

pub mod accumulator;
pub mod config;
pub mod connection;
pub mod connection_dydx;
pub mod connection_hl;
pub mod error;
pub mod exchange;
pub mod monitor;
pub mod nats_sink;
pub mod orderbook;
pub mod schema;
pub mod writer;
