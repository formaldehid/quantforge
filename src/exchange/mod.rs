//! Exchange adapters. Binance Spot is the only venue today; each venue lives
//! under its own namespace ([`binance`]) so a second one can be added without
//! restructuring the tree.

pub mod binance;

pub use binance::{BinanceCredentials, BinanceSpotClient};
