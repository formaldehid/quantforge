//! End-to-end CLI tests, tiered by external dependencies.
//!
//! - `offline`: driven against the scriptable mock Binance server in
//!   [`mock_binance`]; always runs, zero network
//! - `testnet`: real Binance Spot testnet; skips with a stderr marker
//!   unless `QF_BINANCE_API_KEY`/`QF_BINANCE_API_SECRET` are exported
//!
//! Shared infrastructure lives in [`harness`]: the env-isolated
//! `assert_cmd` runner, per-test temp databases, `key: value` output
//! parsing, and the tier gate.
//!
//! Run one tier alone: `cargo test --test e2e offline::` or
//! `cargo test --test e2e testnet::`.

mod harness;
mod mock_binance;
mod offline;
mod testnet;
