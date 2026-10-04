//! Binance Spot REST adapter. The work is split across private sibling
//! modules — `client` owns the HTTP transport, `venue` implements the
//! `MarketDataSource` and `TradingVenue` ports on top of it, `signing`
//! computes the HMAC-SHA256 query signature, `credentials` resolves the API
//! keys, and `types` and `convert` hold the wire DTOs and their mapping onto
//! domain types. Only [`BinanceSpotClient`] and [`BinanceCredentials`] leave
//! this namespace.

mod client;
mod convert;
mod credentials;
mod signing;
mod types;
mod venue;

pub use client::BinanceSpotClient;
pub use credentials::BinanceCredentials;
