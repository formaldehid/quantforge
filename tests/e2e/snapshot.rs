//! Locks the CLI surface: committed `--help` snapshots for every command,
//! and a check that the module tree keeps mirroring the command tree.
//!
//! Snapshots are plain text under `tests/snapshots/`. They are compared
//! byte for byte, so a renamed flag, a changed default, or a reordered
//! argument fails CI. After an *intentional* surface change, refresh them
//! with:
//!
//! ```text
//! UPDATE_SNAPSHOTS=1 cargo test --test e2e snapshot::
//! ```
//!
//! and review the resulting diff — the snapshots are the CLI contract.
//!
//! Commands run through [`crate::harness::isolated_cmd`], whose cleared
//! environment is what makes the snapshots machine-independent: clap
//! renders `[env: QF_DB=<value>]` in help, so a shell with the repo `.env`
//! sourced would otherwise produce different bytes than CI.

use crate::harness::isolated_cmd;
use std::path::{Path, PathBuf};

/// Every command path in the CLI, paired with the snapshot file name and
/// the module path expected to implement it. One list so that adding a
/// subcommand fails in exactly one obvious place.
///
/// The module path is relative to `src/cli/`; a leaf command may live
/// either in `<name>.rs` or in `<name>/mod.rs` (the directory form leaves
/// room for future subcommands).
const COMMANDS: &[(&str, &[&str], &str)] = &[
    ("root", &[], ""),
    ("data", &["data"], "data"),
    ("data-sync", &["data", "sync"], "data/sync"),
    ("data-validate", &["data", "validate"], "data/validate"),
    ("backtest", &["backtest"], "backtest"),
    ("trade", &["trade"], "trade"),
    ("trade-run", &["trade", "run"], "trade/run"),
    ("trade-close", &["trade", "close"], "trade/close"),
    ("monitor", &["monitor"], "monitor"),
    ("monitor-status", &["monitor", "status"], "monitor/status"),
    ("monitor-watch", &["monitor", "watch"], "monitor/watch"),
    ("monitor-orders", &["monitor", "orders"], "monitor/orders"),
    ("monitor-trades", &["monitor", "trades"], "monitor/trades"),
    (
        "monitor-cancel-order",
        &["monitor", "cancel-order"],
        "monitor/cancel_order",
    ),
    (
        "monitor-close-position",
        &["monitor", "close-position"],
        "monitor/close_position",
    ),
];

fn snapshot_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("snapshots")
        .join(format!("{name}.txt"))
}

// Deliberately value-based: `UPDATE_SNAPSHOTS=0` in a leftover shell
// export must not silently disable every guard in this file.
fn updating() -> bool {
    matches!(
        std::env::var("UPDATE_SNAPSHOTS").ok().as_deref(),
        Some("1") | Some("true")
    )
}

/// Compares `actual` against the committed snapshot, or rewrites it when
/// `UPDATE_SNAPSHOTS` is set.
fn assert_snapshot(name: &str, actual: &str) {
    let path = snapshot_path(name);

    if updating() {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("snapshot dir");
        }
        std::fs::write(&path, actual).expect("write snapshot");
        return;
    }

    let expected = std::fs::read_to_string(&path).unwrap_or_else(|err| {
        panic!(
            "missing snapshot {}: {err}; run `UPDATE_SNAPSHOTS=1 cargo test --test e2e snapshot::`",
            path.display()
        )
    });

    if expected != actual {
        let line = expected
            .lines()
            .zip(actual.lines())
            .position(|(want, got)| want != got)
            .map(|index| index + 1);
        panic!(
            "cli surface changed for `{name}`{}\n\
             expected ({} lines):\n{expected}\n\
             actual ({} lines):\n{actual}\n\
             if this change is intentional, refresh with \
             `UPDATE_SNAPSHOTS=1 cargo test --test e2e snapshot::`",
            line.map(|line| format!(" at line {line}"))
                .unwrap_or_else(|| " in trailing lines".to_string()),
            expected.lines().count(),
            actual.lines().count(),
        );
    }
}

/// Windows renders the usage line from `argv[0]`, which carries the `.exe`
/// suffix (`Usage: quantforge.exe ...`). The executable's file extension is
/// not part of the CLI surface these snapshots lock, so it is normalized
/// away — on the capture path, so snapshots regenerated on Windows stay
/// platform-neutral too.
fn normalize(output: &str) -> String {
    output.replace("quantforge.exe", "quantforge")
}

/// `--help` for one command path: stdout, asserted to exit 0.
fn help_output(args: &[&str]) -> String {
    let output = isolated_cmd()
        .args(args)
        .arg("--help")
        .output()
        .expect("run help");
    assert!(
        output.status.success(),
        "`{} --help` exited with {:?}",
        args.join(" "),
        output.status.code()
    );
    assert!(
        output.stderr.is_empty(),
        "`{} --help` wrote to stderr",
        args.join(" ")
    );
    normalize(&String::from_utf8(output.stdout).expect("utf-8 help"))
}

#[test]
fn help_snapshots_cover_every_command() {
    for (name, args, _) in COMMANDS {
        assert_snapshot(name, &help_output(args));
    }
}

#[test]
fn version_snapshot_matches() {
    let output = isolated_cmd()
        .arg("--version")
        .output()
        .expect("run version");
    assert!(output.status.success(), "--version exited non-zero");
    let version = String::from_utf8(output.stdout).expect("utf-8");
    assert_snapshot("version", &normalize(&version));
}

// Committed snapshots must be identical on every CI leg: LF endings
// (`.gitattributes` pins them) and no `.exe` suffix, which would appear if
// someone regenerated them on Windows without the normalization above.
#[test]
fn committed_snapshots_are_platform_neutral() {
    if updating() {
        // The snapshot-writing tests run concurrently with this one; the
        // files they rewrite are checked on the next ordinary run.
        return;
    }
    // Walks the directory rather than COMMANDS so that every committed
    // snapshot is covered, including `version.txt` and any added later.
    let dir = snapshot_path("root")
        .parent()
        .expect("snapshot dir")
        .to_path_buf();
    let mut checked = 0usize;
    for entry in std::fs::read_dir(&dir).expect("read snapshot dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().is_none_or(|ext| ext != "txt") {
            continue;
        }
        let raw = std::fs::read(&path).expect("read snapshot");
        assert!(
            !raw.windows(2).any(|pair| pair == b"\r\n"),
            "snapshot {} contains CRLF line endings",
            path.display()
        );
        assert!(
            !String::from_utf8_lossy(&raw).contains("quantforge.exe"),
            "snapshot {} carries the windows .exe suffix; regenerate on any \
             platform (the capture path normalizes it away)",
            path.display()
        );
        checked += 1;
    }
    assert!(
        checked > COMMANDS.len(),
        "expected every snapshot to be read"
    );
}

/// Subcommand names listed in a `Commands:` block of captured help.
fn subcommands_in(help: &str) -> Vec<&str> {
    help.lines()
        .skip_while(|line| *line != "Commands:")
        .skip(1)
        .take_while(|line| !line.trim().is_empty())
        .filter_map(|line| line.strip_prefix("  "))
        .filter_map(|line| line.split_whitespace().next())
        .filter(|name| *name != "help")
        .collect()
}

// Completeness: the locked group snapshots decide which commands exist, so
// a new subcommand cannot be absorbed by a snapshot refresh without also
// gaining its own snapshot and module entry here.
#[test]
fn every_subcommand_in_help_is_covered_by_this_file() {
    for (name, args, _) in COMMANDS {
        let help = std::fs::read_to_string(snapshot_path(name)).expect("read snapshot");
        for child in subcommands_in(&help) {
            let mut path: Vec<&str> = args.to_vec();
            path.push(child);
            assert!(
                COMMANDS
                    .iter()
                    .any(|(_, covered, _)| *covered == path.as_slice()),
                "`quantforge {}` appears in the {name} help but has no COMMANDS entry; \
                 add it so its help is snapshotted and its module is checked",
                path.join(" ")
            );
        }
    }
}

// The restructure invariant: every command path in COMMANDS (kept
// complete by `every_subcommand_in_help_is_covered_by_this_file`) has a
// module at the mirroring file path. One-way — shared modules
// (`common.rs`, `context.rs`) and the root `mod.rs` are legal extras.
#[test]
fn module_tree_mirrors_the_command_tree() {
    let cli_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("cli");

    for (name, args, module) in COMMANDS {
        if module.is_empty() {
            continue; // the root parser lives in src/cli/mod.rs
        }

        let mut leaf = cli_root.clone();
        for segment in module.split('/') {
            leaf = leaf.join(segment);
        }
        let flat = leaf.with_extension("rs");
        let directory = leaf.join("mod.rs");

        assert!(
            flat.is_file() || directory.is_file(),
            "command `quantforge {}` ({name}) has no module: expected {} or {}",
            args.join(" "),
            flat.display(),
            directory.display()
        );
    }
}
