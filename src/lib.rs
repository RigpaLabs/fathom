// AppError carries tokio_tungstenite::tungstenite::Error (136 bytes). Boxing it
// would add noise at every use-site; suppress the lint crate-wide instead.
#![allow(clippy::result_large_err)]

pub mod accumulator;
pub mod config;
pub mod connection;
pub mod error;
pub mod exchange;
pub mod monitor;
pub mod orderbook;
pub mod schema;
pub mod writer;
