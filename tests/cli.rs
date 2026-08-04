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
fn trade_run_live_without_yes_prints_preview_and_exits_zero_before_any_network() {
    let tempdir = tempdir().expect("tempdir");
    let db_path = tempdir.path().join("market.sqlite");

    let mut cmd = Command::cargo_bin("quantforge").expect("binary");
    // Credentials are removed so a gate regression degrades to the offline
    // credentials error instead of reaching the network; the unroutable URL
    // backstops that. Piped stdio means the non-interactive preview branch.
    cmd.env_remove("QF_BINANCE_BASE_URL")
        .env_remove("QF_BINANCE_API_KEY")
        .env_remove("QF_BINANCE_API_SECRET")
        .arg("--db")
        .arg(&db_path)
        .arg("--log-level")
        .arg("error")
        .arg("--binance-base-url")
        .arg("http://127.0.0.1:9/")
        .args(["trade", "run", "--symbol", "BTCUSDT", "--mode", "live"]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains(
            "refusing to start live trading without --yes",
        ))
        .stdout(predicate::str::contains(
            "would run strategy sma_cross on binance_spot BTCUSDT 1m",
        ))
        .stdout(predicate::str::contains(
            "with REAL orders via http://127.0.0.1:9/",
        ))
        .stdout(predicate::str::contains(
            "re-run with --yes to confirm, or use --mode dry-run",
        ))
        .stdout(predicate::str::contains("run_id:").not());
}

#[test]
fn trade_run_live_preview_marks_production_endpoints() {
    let tempdir = tempdir().expect("tempdir");
    let db_path = tempdir.path().join("market.sqlite");

    let mut cmd = Command::cargo_bin("quantforge").expect("binary");
    // The preview returns before any client use, so the production URL is
    // never contacted.
    cmd.env_remove("QF_BINANCE_BASE_URL")
        .env_remove("QF_BINANCE_API_KEY")
        .env_remove("QF_BINANCE_API_SECRET")
        .arg("--db")
        .arg(&db_path)
        .arg("--log-level")
        .arg("error")
        .arg("--binance-base-url")
        .arg("https://api.binance.com/")
        .args(["trade", "run", "--symbol", "BTCUSDT", "--mode", "live"]);
    cmd.assert().success().stdout(predicate::str::contains(
        "with REAL orders via https://api.binance.com/ (PRODUCTION)",
    ));
}

#[test]
fn trade_run_live_with_yes_requires_credentials_before_any_network() {
    let tempdir = tempdir().expect("tempdir");
    let db_path = tempdir.path().join("market.sqlite");

    let mut cmd = Command::cargo_bin("quantforge").expect("binary");
    cmd.env_remove("QF_BINANCE_BASE_URL")
        .env_remove("QF_BINANCE_API_KEY")
        .env_remove("QF_BINANCE_API_SECRET")
        .arg("--db")
        .arg(&db_path)
        .arg("--log-level")
        .arg("error")
        .arg("--binance-base-url")
        .arg("http://127.0.0.1:9/")
        .args([
            "trade", "run", "--symbol", "BTCUSDT", "--mode", "live", "--yes",
        ]);
    cmd.assert().failure().stderr(predicate::str::contains(
        "trade run --mode live requires Binance credentials",
    ));
}

#[test]
fn trade_run_live_with_yes_warns_on_production_endpoint_and_still_requires_credentials() {
    let tempdir = tempdir().expect("tempdir");
    let db_path = tempdir.path().join("market.sqlite");

    let mut cmd = Command::cargo_bin("quantforge").expect("binary");
    // The credentials error fires while constructing the engine, before any
    // request, so the production URL is never contacted.
    cmd.env_remove("QF_BINANCE_BASE_URL")
        .env_remove("QF_BINANCE_API_KEY")
        .env_remove("QF_BINANCE_API_SECRET")
        .arg("--db")
        .arg(&db_path)
        .arg("--log-level")
        .arg("warn")
        .arg("--binance-base-url")
        .arg("https://api.binance.com/")
        .args([
            "trade", "run", "--symbol", "BTCUSDT", "--mode", "live", "--yes",
        ]);
    // The tracing fmt subscriber writes to stdout; the anyhow error goes to
    // stderr.
    cmd.assert()
        .failure()
        .stdout(predicate::str::contains(
            "live trading against PRODUCTION Binance",
        ))
        .stderr(predicate::str::contains(
            "trade run --mode live requires Binance credentials",
        ));
}

#[test]
fn trade_run_defaults_to_dry_run_and_never_engages_live_gates() {
    let tempdir = tempdir().expect("tempdir");
    let db_path = tempdir.path().join("market.sqlite");

    let mut cmd = Command::cargo_bin("quantforge").expect("binary");
    // Dry-run proceeds to the bootstrap sync and dies on the unroutable URL;
    // the connection-error text differs per OS, so only the absence of the
    // live gates is asserted.
    cmd.env_remove("QF_BINANCE_BASE_URL")
        .env_remove("QF_BINANCE_API_KEY")
        .env_remove("QF_BINANCE_API_SECRET")
        .arg("--db")
        .arg(&db_path)
        .arg("--log-level")
        .arg("error")
        .arg("--binance-base-url")
        .arg("http://127.0.0.1:9/")
        .args(["trade", "run", "--symbol", "BTCUSDT", "--max-loops", "1"]);
    cmd.assert()
        .failure()
        .stdout(predicate::str::contains("refusing to start live trading").not())
        .stderr(predicate::str::contains("requires Binance credentials").not());
}

#[test]
fn trade_run_help_documents_dry_run_as_the_default_mode() {
    let mut cmd = Command::cargo_bin("quantforge").expect("binary");
    cmd.env_remove("QF_BINANCE_BASE_URL")
        .args(["trade", "run", "--help"]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("[default: dry-run]"))
        .stdout(predicate::str::contains("--yes"));
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
