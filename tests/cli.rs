use assert_cmd::Command;
use predicates::prelude::*;

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
