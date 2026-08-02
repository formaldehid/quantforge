use assert_cmd::Command;
use predicates::prelude::*;
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
        .stdout(predicate::str::contains("candles: 0"));
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
