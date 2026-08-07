//! Testnet tier: read-only smoke against the real Binance Spot testnet.
//!
//! Gated by [`crate::harness::skip_unless_testnet!`]: without
//! `QF_BINANCE_API_KEY`/`QF_BINANCE_API_SECRET` in the test runner's
//! environment, each test returns early with a `SKIP (testnet tier)`
//! marker on stderr. The gate keys on credentials even for public
//! endpoints — exported testnet keys are the operator's opt-in to network
//! tests, and the gate stays stable when signed read-only tests join this
//! tier.
//!
//! Fixtures use relative time windows only: testnet history is reset
//! periodically, so absolute dates would silently return zero candles.
//! Every command carries the harness's 30-second timeout because the HTTP
//! client has none of its own.

use crate::harness::{CliOutput, TestBed, cli_cmd, skip_unless_testnet, testnet_cmd};
use quantforge::ms_to_rfc3339;

#[test]
fn testnet_smoke_bounded_sync_then_validate_reports_no_issues() {
    let testnet = skip_unless_testnet!();
    let bed = TestBed::new();

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as i64;
    let start = ms_to_rfc3339(now_ms - 5 * 60_000);
    let end = ms_to_rfc3339(now_ms);

    let mut sync = testnet_cmd(&bed, &testnet);
    sync.args([
        "data",
        "sync",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "1m",
        "--start",
        &start,
        "--end",
        &end,
        "--limit",
        "5",
        "--max-loops",
        "1",
    ]);
    let assert = sync.assert().success();

    let output = CliOutput::from_assert(&assert);
    assert_eq!(output.get("iterations"), Some("1"));
    let written: u64 = output
        .get("written")
        .expect("written key")
        .parse()
        .expect("written count");
    assert!(written >= 1, "expected candles written, got {written}");
    let first_synced = output
        .get("first_synced_open_time")
        .expect("first synced key");
    assert_ne!(first_synced, "none");

    // Validation runs offline over the same bed's database.
    let mut validate = cli_cmd(&bed, "error");
    validate.args([
        "data",
        "validate",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "1m",
    ]);
    let assert = validate.assert().success();

    let output = CliOutput::from_assert(&assert);
    assert_eq!(output.get("market"), Some("binance_spot BTCUSDT 1m"));
    assert_eq!(output.get("issues"), Some("0"));
}
