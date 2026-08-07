//! Offline tier: every command talks HTTP to the local scriptable mock
//! Binance server ([`crate::mock_binance`]) with canned testnet-shaped
//! payloads — zero real network, fake credentials, deterministic outcomes.
//! Always runs.

use crate::harness::{
    CliOutput, FAKE_API_KEY, FAKE_API_SECRET, TestBed, cli_cmd, offline_cmd, offline_cmd_with_log,
};
use crate::mock_binance::{
    CannedResponse, MockBinance, Scenario, account_response, exchange_info_btcusdt, kline_row,
    my_trade_row, order_ack_response, order_full_response, order_query_response,
};
use assert_cmd::Command;
use predicates::prelude::*;
use quantforge::{RunJournalStore, RunStatus};
use rust_decimal::Decimal;
use std::str::FromStr;

// The engine reads the wall clock internally (bootstrap window, poll range,
// closed-bar filter), so kline fixtures anchor to the current minute, far
// enough in the past that every bar is closed for any test runtime. All
// offsets are exact step multiples; only the anchor itself is wall-clock.
fn anchor_ms() -> i64 {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as i64;
    now_ms / 60_000 * 60_000 - 10 * 60_000
}

fn trade_run_live(mock: &MockBinance, bed: &TestBed, run_id: &str, log_level: &str) -> Command {
    let mut cmd = offline_cmd_with_log(bed, mock, log_level);
    cmd.args([
        "trade",
        "run",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "1m",
        "--mode",
        "live",
        "--yes",
        "--fast",
        "1",
        "--slow",
        "2",
        "--bootstrap-bars",
        "5",
        "--poll-secs",
        "1",
        "--max-loops",
        "2",
        "--quote-order-qty",
        "100",
        "--run-id",
        run_id,
    ]);
    cmd
}

// The acceptance scenario: a live-mode run, entry to exit, fully offline.
// Loop 1 delivers a rising close (fast crosses above slow -> market BUY);
// loop 2 delivers a falling close (cross down -> account lookup + market
// SELL); the run then stops via --max-loops with a closed trade journaled.
#[test]
fn live_trade_run_completes_an_entry_exit_round_trip_fully_offline() {
    let anchor = anchor_ms();
    let mut scenario = Scenario::new();
    scenario.exchange_info(exchange_info_btcusdt());
    // Bootstrap sync makes exactly ONE fetch: the mock ignores requested
    // ranges, so serving bars anchored below the bootstrap window leaves
    // the sync cursor non-advancing and the loop stops after this page.
    // The two warm-up bars have equal closes (signal-silent under
    // fast=1/slow=2).
    scenario.klines_page(vec![
        kline_row(anchor, "100.00000000"),
        kline_row(anchor + 60_000, "100.00000000"),
    ]);
    // Poll loop 1: the cross-up bar advances the cursor, so a second fetch
    // follows; the empty page ends it.
    scenario.klines_page(vec![kline_row(anchor + 120_000, "110.00000000")]);
    scenario.klines_page(vec![]);
    // Poll loop 2: the cross-down bar; the drained queue then serves [] by
    // default, ending the final sync.
    scenario.klines_page(vec![kline_row(anchor + 180_000, "90.00000000")]);
    // Entry fill: 0.01 BTC for 100 USDT at 10_000, 0.00001 BTC commission
    // -> net position 0.00999.
    scenario.order_response(order_full_response(
        "BUY",
        1_001,
        "0.01000000",
        "100.00000000",
        "10000.00000000",
        "0.00001000",
    ));
    // Exit path: the venue reports the free base balance, then fills the
    // sell of the full net position at 9_000.
    scenario.account(account_response("BTC", "0.00999000"));
    scenario.order_response(order_full_response(
        "SELL",
        1_002,
        "0.00999000",
        "89.91000000",
        "9000.00000000",
        "0.08991000",
    ));
    let mock = MockBinance::start(scenario);

    let bed = TestBed::new();

    trade_run_live(&mock, &bed, "e2e-live-1", "error")
        .assert()
        .success()
        .stdout(predicate::str::contains("run_id: e2e-live-1\n"))
        .stdout(predicate::str::contains("processed_bars: 2\n"))
        .stdout(predicate::str::contains("submitted_orders: 2\n"))
        .stdout(predicate::str::contains("closed_trades: 1\n"));

    // The journal agrees: run stopped flat with one closed trade at the
    // venue-reported prices.
    let store = bed.store();
    let state = store
        .load_run_state("e2e-live-1")
        .expect("load run")
        .expect("run state");
    assert_eq!(state.status, RunStatus::Stopped);
    assert_eq!(state.position.qty, Decimal::ZERO);

    let trades = store.list_closed_trades("e2e-live-1", 10).expect("trades");
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].entry_price, Decimal::from(10_000));
    assert_eq!(trades[0].exit_price, Decimal::from(9_000));
    assert_eq!(
        trades[0].qty,
        Decimal::from_str("0.00999").expect("decimal")
    );
    assert_eq!(
        trades[0].gross_quote_pnl,
        Decimal::from_str("-9.99").expect("decimal")
    );
    assert_eq!(trades[0].entry_order_id, Some(1_001));
    assert_eq!(trades[0].exit_order_id, Some(1_002));

    let events = store.list_order_events("e2e-live-1", 10).expect("events");
    assert_eq!(events.len(), 2);

    // The wire agrees: public endpoints unsigned, signed endpoints carry
    // the API key and signature, entry sizes by quote, exit by quantity.
    let requests = mock.requests();
    let exchange_info = &requests[0];
    assert_eq!(exchange_info.path, "/api/v3/exchangeInfo");
    assert!(exchange_info.has_param("symbol", "BTCUSDT"));
    assert_eq!(exchange_info.api_key, None);

    let klines: Vec<_> = requests
        .iter()
        .filter(|request| request.path == "/api/v3/klines")
        .collect();
    assert!(klines.len() >= 5, "got {}", klines.len());
    assert!(klines.iter().all(|request| {
        request.has_param("symbol", "BTCUSDT")
            && request.has_param("interval", "1m")
            && request.api_key.is_none()
    }));

    let orders: Vec<_> = requests
        .iter()
        .filter(|request| request.method == "POST" && request.path == "/api/v3/order")
        .collect();
    assert_eq!(orders.len(), 2);
    let entry = orders[0];
    assert!(entry.has_param("side", "BUY"));
    assert!(entry.has_param("type", "MARKET"));
    assert!(entry.has_param("newOrderRespType", "FULL"));
    assert!(entry.has_param("quoteOrderQty", "100"));
    assert!(!entry.has_param_named("quantity"));
    assert!(entry.has_param_named("signature"));
    assert!(entry.has_param_named("timestamp"));
    assert_eq!(entry.api_key.as_deref(), Some(FAKE_API_KEY));
    let exit = orders[1];
    assert!(exit.has_param("side", "SELL"));
    assert!(
        exit.has_param("quantity", "0.00999000"),
        "exit query: {}",
        exit.query
    );
    assert!(!exit.has_param_named("quoteOrderQty"));
    assert_eq!(exit.api_key.as_deref(), Some(FAKE_API_KEY));

    let account: Vec<_> = requests
        .iter()
        .filter(|request| request.path == "/api/v3/account")
        .collect();
    assert_eq!(account.len(), 1);
    assert!(account[0].has_param("omitZeroBalances", "true"));
    assert_eq!(account[0].api_key.as_deref(), Some(FAKE_API_KEY));
}

#[test]
fn live_entry_rejected_with_a_binance_error_body_fails_the_run() {
    let anchor = anchor_ms();
    let mut scenario = Scenario::new();
    scenario.exchange_info(exchange_info_btcusdt());
    scenario.klines_page(vec![
        kline_row(anchor, "100.00000000"),
        kline_row(anchor + 60_000, "100.00000000"),
    ]);
    scenario.klines_page(vec![]);
    scenario.klines_page(vec![kline_row(anchor + 120_000, "110.00000000")]);
    scenario.klines_page(vec![]);
    scenario.enqueue(
        "POST",
        "/api/v3/order",
        CannedResponse::api_error(
            400,
            -2010,
            "Account has insufficient balance for requested action.",
        ),
    );
    let mock = MockBinance::start(scenario);

    let bed = TestBed::new();

    trade_run_live(&mock, &bed, "e2e-reject-1", "error")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Account has insufficient balance for requested action.",
        ))
        .stderr(predicate::str::contains("-2010"));

    // The failed entry is journaled as a failed run with no position.
    let store = bed.store();
    let state = store
        .load_run_state("e2e-reject-1")
        .expect("load run")
        .expect("run state");
    assert_eq!(state.status, RunStatus::Failed);
    assert_eq!(state.position.qty, Decimal::ZERO);
}

#[test]
fn rate_limited_klines_surface_the_http_status_as_an_api_error() {
    let mut scenario = Scenario::new();
    scenario.exchange_info(exchange_info_btcusdt());
    // A bare 429 without a Binance error body: the client reports the HTTP
    // status itself as the error code.
    scenario.enqueue(
        "GET",
        "/api/v3/klines",
        CannedResponse::body(429, "slow down"),
    );
    let mock = MockBinance::start(scenario);

    let bed = TestBed::new();

    trade_run_live(&mock, &bed, "e2e-limited-1", "error")
        .assert()
        .failure()
        .stderr(predicate::str::contains("code=Some(429)"))
        .stderr(predicate::str::contains("slow down"));
}

#[test]
fn teapot_from_an_ip_ban_surfaces_the_binance_error_body() {
    let mut scenario = Scenario::new();
    // Binance's 418 carries a {code, msg} body; it must surface verbatim.
    scenario.enqueue(
        "GET",
        "/api/v3/exchangeInfo",
        CannedResponse::api_error(418, -1003, "Way too much request weight used; IP banned."),
    );
    let mock = MockBinance::start(scenario);

    let bed = TestBed::new();

    trade_run_live(&mock, &bed, "e2e-teapot-1", "error")
        .assert()
        .failure()
        .stderr(predicate::str::contains("code=Some(-1003)"))
        .stderr(predicate::str::contains("IP banned"));
}

#[test]
fn ack_only_order_response_aborts_the_entry_loudly() {
    let anchor = anchor_ms();
    let mut scenario = Scenario::new();
    scenario.exchange_info(exchange_info_btcusdt());
    scenario.klines_page(vec![
        kline_row(anchor, "100.00000000"),
        kline_row(anchor + 60_000, "100.00000000"),
    ]);
    scenario.klines_page(vec![]);
    scenario.klines_page(vec![kline_row(anchor + 120_000, "110.00000000")]);
    scenario.klines_page(vec![]);
    // An ACK response has no executedQty: the engine must refuse to update
    // position state rather than assume a fill.
    scenario.order_response(order_ack_response("BUY", 2_001));
    let mock = MockBinance::start(scenario);

    let bed = TestBed::new();

    trade_run_live(&mock, &bed, "e2e-ack-1", "error")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "did not report an executed quantity",
        ));
}

#[test]
fn malformed_filters_degrade_loudly_and_the_run_still_completes() {
    let mut scenario = Scenario::new();
    // Filters the client must skip: one with no filterType, one with
    // numeric values where strings are required. With no usable rules the
    // engine warns about missing step size and min-notional but proceeds.
    scenario.exchange_info(serde_json::json!({
        "symbols": [{
            "symbol": "BTCUSDT",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "filters": [
                { "minQty": "9.9" },
                { "filterType": "LOT_SIZE", "minQty": 1, "maxQty": 9000, "stepSize": 1 }
            ]
        }]
    }));
    let mock = MockBinance::start(scenario);

    let bed = TestBed::new();

    let mut cmd = trade_run_live(&mock, &bed, "e2e-malformed-1", "warn");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("no lot-size step"))
        .stdout(predicate::str::contains("no min-notional rule"))
        .stdout(predicate::str::contains("submitted_orders: 0\n"));
}

#[test]
fn monitor_status_reads_account_orders_and_trades_offline() {
    let mut scenario = Scenario::new();
    scenario.exchange_info(exchange_info_btcusdt());
    scenario.account(account_response("BTC", "0.05000000"));
    scenario.open_orders(serde_json::json!([]));
    scenario.my_trades(serde_json::json!([
        my_trade_row(9_001, 1_001, true),
        my_trade_row(9_002, 1_002, false)
    ]));
    let mock = MockBinance::start(scenario);

    let bed = TestBed::new();

    let mut cmd = offline_cmd(&bed, &mock);
    cmd.args(["monitor", "status", "--symbol", "BTCUSDT"]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("BTC free=0.05000000"))
        .stdout(predicate::str::contains("open_orders: 0"));

    let requests = mock.requests();
    for path in ["/api/v3/account", "/api/v3/openOrders", "/api/v3/myTrades"] {
        let request = requests
            .iter()
            .find(|request| request.path == path)
            .unwrap_or_else(|| panic!("missing request to {path}"));
        assert_eq!(request.api_key.as_deref(), Some(FAKE_API_KEY));
        assert!(request.has_param_named("signature"));
    }
}

#[test]
fn monitor_cancel_order_sends_a_delete_when_confirmed() {
    let mut scenario = Scenario::new();
    scenario.order_cancel_response(order_query_response("SELL", 42, "0.00000000"));
    let mock = MockBinance::start(scenario);

    let bed = TestBed::new();

    let mut cmd = offline_cmd(&bed, &mock);
    cmd.args([
        "monitor",
        "cancel-order",
        "--symbol",
        "BTCUSDT",
        "--order-id",
        "42",
        "--yes",
    ]);
    cmd.assert().success();

    let requests = mock.requests();
    let cancel = requests
        .iter()
        .find(|request| request.method == "DELETE" && request.path == "/api/v3/order")
        .expect("delete request");
    assert!(cancel.has_param("symbol", "BTCUSDT"));
    assert!(cancel.has_param("orderId", "42"));
    assert_eq!(cancel.api_key.as_deref(), Some(FAKE_API_KEY));
}

// The mock's under-scripting rail: an endpoint with no queued response
// (other than klines) must answer with the loud unscripted marker so a
// missing script line fails the test immediately instead of passing on a
// silently-invented payload.
#[test]
fn unscripted_endpoints_fail_loudly_instead_of_inventing_payloads() {
    let mut scenario = Scenario::new();
    scenario.exchange_info(exchange_info_btcusdt());
    // account/openOrders/myTrades deliberately left unscripted.
    let mock = MockBinance::start(scenario);

    let bed = TestBed::new();

    let mut cmd = offline_cmd(&bed, &mock);
    cmd.args(["monitor", "status", "--symbol", "BTCUSDT"]);
    cmd.assert().failure().stderr(predicate::str::contains(
        "unscripted mock endpoint: GET /api/v3/account",
    ));
}

// Drives the library client directly (no CLI consumer calls GET
// /api/v3/order today): a query-shaped response without fills must parse
// with `fills: None` — missing fill data, not an empty fill list.
#[tokio::test]
async fn order_query_without_fills_parses_as_missing_fill_data() {
    let mut scenario = Scenario::new();
    scenario.order_query_response(order_query_response("SELL", 77, "0.01000000"));
    let mock = MockBinance::start(scenario);

    let client =
        quantforge::BinanceSpotClient::new(url::Url::parse(&mock.base_url()).expect("mock url"))
            .with_credentials(quantforge::BinanceCredentials {
                api_key: FAKE_API_KEY.to_string(),
                secret: FAKE_API_SECRET.to_string(),
            });

    let order = quantforge::TradingVenue::query_order(
        &client,
        &quantforge::OrderQueryRequest {
            symbol: quantforge::Symbol::new("BTCUSDT").expect("symbol"),
            order_id: Some(77),
            client_order_id: None,
        },
    )
    .await
    .expect("query order");

    assert_eq!(order.order_id, Some(77));
    assert_eq!(order.fills, None);
    assert_eq!(
        order.executed_qty,
        Some(Decimal::from_str("0.01").expect("decimal"))
    );
    assert!(order.average_price().is_some());
}

// The named trivial offline-tier e2e: TestBed + isolation + parser, no
// mock, no network.
#[test]
fn offline_smoke_data_validate_runs_green_on_an_isolated_fresh_db() {
    let bed = TestBed::new();
    let mut cmd = cli_cmd(&bed, "error");
    cmd.args([
        "data",
        "validate",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "1m",
    ]);
    let assert = cmd.assert().success();

    let output = CliOutput::from_assert(&assert);
    assert_eq!(output.get("market"), Some("binance_spot BTCUSDT 1m"));
    assert_eq!(output.get("candles"), Some("0"));
    assert_eq!(output.get("issues"), Some("0"));
}

// Isolation proofs are behavioral: a developer shell that sources .env
// exports a base-URL override and real credentials, so if env_clear ever
// regresses these fail locally while staying trivially green in CI.

#[test]
fn isolated_commands_use_the_default_base_url_ignoring_the_shell_environment() {
    // The startup log (info level, stdout) names the effective base URL;
    // data validate never touches the network.
    let bed = TestBed::new();
    let mut cmd = cli_cmd(&bed, "info");
    cmd.args([
        "data",
        "validate",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "1m",
    ]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("https://api.binance.com/"));
}

#[test]
fn isolated_commands_do_not_inherit_shell_credentials() {
    // Unroutable URL backstop: if isolation regressed AND the credential
    // gate somehow passed, the run would still die on a refused connection
    // instead of reaching a real exchange.
    let bed = TestBed::new();
    let mut cmd = cli_cmd(&bed, "error");
    cmd.arg("--binance-base-url")
        .arg("http://127.0.0.1:9/")
        .args([
            "trade", "run", "--symbol", "BTCUSDT", "--mode", "live", "--yes",
        ]);
    cmd.assert().failure().stderr(predicate::str::contains(
        "trade run --mode live requires Binance credentials",
    ));
}
