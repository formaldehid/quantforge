use assert_cmd::Command;
use predicates::prelude::*;
use quantforge::{Candle, CandleStore, ExchangeId, Interval, MarketId, SqliteStore, Symbol};
use rust_decimal::Decimal;
use tempfile::tempdir;

#[test]
fn help_lists_v020_command_groups() {
    let mut cmd = Command::cargo_bin("quantforge").expect("binary");
    cmd.arg("--help");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("data"))
        .stdout(predicate::str::contains("backtest"))
        .stdout(predicate::str::contains("trade"))
        .stdout(predicate::str::contains("monitor"));
}

#[test]
fn data_validate_reports_the_requested_interval() {
    let tempdir = tempdir().expect("tempdir");
    let db_path = tempdir.path().join("market.sqlite");

    let mut cmd = Command::cargo_bin("quantforge").expect("binary");
    cmd.env_remove("QF_BINANCE_BASE_URL")
        .arg("--db")
        .arg(&db_path)
        .arg("--log-level")
        .arg("error")
        .args([
            "data",
            "validate",
            "--symbol",
            "BTCUSDT",
            "--interval",
            "8h",
        ]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("market: binance_spot BTCUSDT 8h"))
        .stdout(predicate::str::contains("candles: 0"))
        .stdout(predicate::str::contains("issues: 0"));
}

#[test]
fn data_validate_exits_non_zero_when_issues_found() {
    let tempdir = tempdir().expect("tempdir");
    let db_path = tempdir.path().join("market.sqlite");

    let store = SqliteStore::new(&db_path);
    CandleStore::init(&store).expect("init store");
    let market = MarketId::new(
        ExchangeId::BinanceSpot,
        Symbol::new("BTCUSDT").expect("symbol"),
        Interval::M1,
    );
    // Two otherwise-valid 1m candles with one missing bar between them:
    // exactly one Gap issue, deterministically.
    let candles: Vec<Candle> = [0i64, 120_000]
        .iter()
        .map(|&open_time_ms| Candle {
            open_time_ms,
            close_time_ms: open_time_ms + 59_999,
            open: Decimal::ONE,
            high: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::ONE,
            volume: Decimal::ONE,
            trades: Some(1),
        })
        .collect();
    store
        .upsert_candles(&market, &candles)
        .expect("seed candles");

    let mut cmd = Command::cargo_bin("quantforge").expect("binary");
    cmd.env_remove("QF_BINANCE_BASE_URL")
        .arg("--db")
        .arg(&db_path)
        .arg("--log-level")
        .arg("error")
        .args([
            "data",
            "validate",
            "--symbol",
            "BTCUSDT",
            "--interval",
            "1m",
        ]);
    cmd.assert()
        .failure()
        .stdout(predicate::str::contains("candles: 2"))
        .stdout(predicate::str::contains("issues: 1"))
        .stdout(predicate::str::contains("Gap"))
        .stderr(predicate::str::contains("data validate found 1 issue(s)"));
}

#[test]
fn backtest_rejects_zero_cash() {
    let tempdir = tempdir().expect("tempdir");
    let db_path = tempdir.path().join("market.sqlite");

    let mut cmd = Command::cargo_bin("quantforge").expect("binary");
    cmd.env_remove("QF_BINANCE_BASE_URL")
        .arg("--db")
        .arg(&db_path)
        .arg("--log-level")
        .arg("error")
        .args([
            "backtest",
            "--symbol",
            "BTCUSDT",
            "--interval",
            "1m",
            "--cash",
            "0",
        ]);
    cmd.assert().failure().stderr(predicate::str::contains(
        "--cash must be greater than 0, got 0",
    ));
}

#[test]
fn backtest_rejects_negative_fee_bps() {
    let tempdir = tempdir().expect("tempdir");
    let db_path = tempdir.path().join("market.sqlite");

    let mut cmd = Command::cargo_bin("quantforge").expect("binary");
    cmd.env_remove("QF_BINANCE_BASE_URL")
        .arg("--db")
        .arg(&db_path)
        .arg("--log-level")
        .arg("error")
        .args([
            "backtest",
            "--symbol",
            "BTCUSDT",
            "--interval",
            "1m",
            "--fee-bps=-1",
        ]);
    cmd.assert().failure().stderr(predicate::str::contains(
        "--fee-bps must be zero or greater, got -1",
    ));
}

#[test]
fn trade_run_rejects_zero_quote_order_qty_before_any_network() {
    let tempdir = tempdir().expect("tempdir");
    let db_path = tempdir.path().join("market.sqlite");

    let mut cmd = Command::cargo_bin("quantforge").expect("binary");
    // Unroutable base URL: if argument validation ever regresses, this test
    // fails on a refused connection instead of touching the real API.
    cmd.env_remove("QF_BINANCE_BASE_URL")
        .arg("--db")
        .arg(&db_path)
        .arg("--log-level")
        .arg("error")
        .arg("--binance-base-url")
        .arg("http://127.0.0.1:9/")
        .args([
            "trade",
            "run",
            "--symbol",
            "BTCUSDT",
            "--interval",
            "1m",
            "--quote-order-qty",
            "0",
        ]);
    cmd.assert().failure().stderr(predicate::str::contains(
        "--quote-order-qty must be greater than 0, got 0",
    ));
}

#[test]
fn data_sync_rejects_zero_poll_secs() {
    let tempdir = tempdir().expect("tempdir");
    let db_path = tempdir.path().join("market.sqlite");

    let mut cmd = Command::cargo_bin("quantforge").expect("binary");
    cmd.env_remove("QF_BINANCE_BASE_URL")
        .arg("--db")
        .arg(&db_path)
        .arg("--log-level")
        .arg("error")
        .arg("--binance-base-url")
        .arg("http://127.0.0.1:9/")
        .args(["data", "sync", "--symbol", "BTCUSDT", "--poll-secs", "0"]);
    cmd.assert().failure().stderr(predicate::str::contains(
        "invalid value '0' for '--poll-secs",
    ));
}

#[test]
fn monitor_watch_rejects_zero_poll_secs_without_credentials() {
    let tempdir = tempdir().expect("tempdir");
    let db_path = tempdir.path().join("market.sqlite");

    let mut cmd = Command::cargo_bin("quantforge").expect("binary");
    cmd.env_remove("QF_BINANCE_BASE_URL")
        .arg("--db")
        .arg(&db_path)
        .arg("--log-level")
        .arg("error")
        .args([
            "monitor",
            "watch",
            "--symbol",
            "BTCUSDT",
            "--poll-secs",
            "0",
        ]);
    cmd.assert().failure().stderr(predicate::str::contains(
        "invalid value '0' for '--poll-secs",
    ));
}

#[test]
fn data_validate_rejects_invalid_interval_with_clear_error() {
    let tempdir = tempdir().expect("tempdir");
    let db_path = tempdir.path().join("market.sqlite");

    let mut cmd = Command::cargo_bin("quantforge").expect("binary");
    cmd.env_remove("QF_BINANCE_BASE_URL")
        .arg("--db")
        .arg(&db_path)
        .arg("--log-level")
        .arg("error")
        .args([
            "data",
            "validate",
            "--symbol",
            "BTCUSDT",
            "--interval",
            "7m",
        ]);
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("invalid interval: 7m"));
}
