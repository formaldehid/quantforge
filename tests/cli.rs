use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn help_lists_core_commands() {
    let mut cmd = Command::cargo_bin("quantforge").expect("binary");
    cmd.arg("--help");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("download"))
        .stdout(predicate::str::contains("validate"))
        .stdout(predicate::str::contains("backtest"));
}
