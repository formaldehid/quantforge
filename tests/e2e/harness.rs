//! Shared e2e infrastructure: an env-isolated CLI runner, per-test
//! databases, `key: value` output parsing, and tier gating.
//!
//! # Isolation guarantees
//!
//! - Every [`TestBed`] owns a fresh temp directory and database path;
//!   nothing is shared between tests and everything is deleted on drop.
//! - Every command starts from [`isolated_cmd`]: `env_clear()` plus an
//!   explicit allowlist, so the parent shell's environment (notably a
//!   sourced `.env` with real credentials and a base-URL override) can
//!   never leak into a test. The only `QF_*` values a child sees are the
//!   ones a builder injects explicitly.
//! - `--db` and `--log-level` are always passed as flags, so even a
//!   hypothetical `QF_DB`/`QF_LOG` leak would be inert (flags win).
//!
//! # Tiers
//!
//! - Offline commands use [`offline_cmd`]: the mock server's base URL plus
//!   fake credentials ([`FAKE_API_KEY`]/[`FAKE_API_SECRET`]) — inert for
//!   `data`/`backtest`, and they satisfy the credential gate for
//!   `monitor`/`trade` without any real secret.
//! - Testnet commands use [`testnet_cmd`]: the public Spot testnet URL,
//!   the operator's real testnet credentials, and a 30-second command
//!   timeout (the HTTP client itself has none). Gate tests with
//!   [`skip_unless_testnet!`]; without credentials they return early and
//!   write a `SKIP (testnet tier)` marker straight to stderr — direct
//!   `io::stderr()` writes bypass libtest's output capture, so the marker
//!   is visible on a plain `cargo test` even though the test reports `ok`
//!   (runtime gating cannot change the summary line). Runners that
//!   capture at the file-descriptor level (cargo-nextest) swallow the
//!   marker; tests still pass there, only skip visibility is lost.
//!
//! # Output parsing
//!
//! The CLI prints `key: value` lines on stdout; [`CliOutput`] extracts
//! them for value assertions. Parse only `--log-level error` output — the
//! tracing subscriber also writes to stdout at higher levels. Indented
//! detail rows, bare section headers, and `---` separators are skipped;
//! repeated keys (backtest `trade:` lines) are kept in order.
//!
//! ```text
//! let bed = TestBed::new();
//! let mut cmd = cli_cmd(&bed, "error");
//! cmd.args(["data", "validate", "--symbol", "BTCUSDT", "--interval", "1m"]);
//! let assert = cmd.assert().success();
//! let output = CliOutput::from_assert(&assert);
//! assert_eq!(output.get("issues"), Some("0"));
//! ```

use assert_cmd::Command;
use quantforge::SqliteStore;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;

pub const FAKE_API_KEY: &str = "test-key";
pub const FAKE_API_SECRET: &str = "test-secret";
pub const TESTNET_BASE_URL: &str = "https://testnet.binance.vision/";

/// Per-test isolated working directory and database.
#[derive(Debug)]
pub struct TestBed {
    tempdir: TempDir,
}

impl TestBed {
    pub fn new() -> Self {
        Self {
            tempdir: tempfile::tempdir().expect("tempdir"),
        }
    }

    pub fn db_path(&self) -> PathBuf {
        self.tempdir.path().join("market.sqlite")
    }

    /// Open the bed's database for direct state assertions. Does not
    /// create the schema — the binary does on first run; when seeding
    /// before any CLI invocation, call `CandleStore::init` first.
    pub fn store(&self) -> SqliteStore {
        SqliteStore::new(self.db_path())
    }
}

impl Default for TestBed {
    fn default() -> Self {
        Self::new()
    }
}

/// Level 0: the binary with a cleared environment. No `QF_*` values, no
/// database, no log level — build on this only when a builder below does
/// not fit.
pub fn isolated_cmd() -> Command {
    let mut cmd = Command::cargo_bin("quantforge").expect("binary");
    cmd.env_clear();
    // Windows children need SystemRoot for winsock and PATH/TEMP for the
    // CRT (rust-lang/rust#114737; the empty-env spawn failure itself was
    // #31259): re-add a minimal allowlist. If the windows CI leg still
    // fails, extend with SYSTEMDRIVE, USERPROFILE, COMSPEC, PATHEXT.
    #[cfg(windows)]
    for key in ["SystemRoot", "PATH", "TEMP", "TMP"] {
        if let Some(value) = std::env::var_os(key) {
            cmd.env(key, value);
        }
    }
    cmd
}

/// Level 1: an isolated command bound to the bed's database with an
/// explicit log level. Use `"error"` unless a test asserts log output.
pub fn cli_cmd(bed: &TestBed, log_level: &str) -> Command {
    let mut cmd = isolated_cmd();
    cmd.arg("--db")
        .arg(bed.db_path())
        .arg("--log-level")
        .arg(log_level);
    cmd
}

/// Level 2, offline tier: bed + mock base URL + fake credentials.
pub fn offline_cmd(bed: &TestBed, mock: &crate::mock_binance::MockBinance) -> Command {
    offline_cmd_with_log(bed, mock, "error")
}

pub fn offline_cmd_with_log(
    bed: &TestBed,
    mock: &crate::mock_binance::MockBinance,
    log_level: &str,
) -> Command {
    let mut cmd = cli_cmd(bed, log_level);
    cmd.env("QF_BINANCE_API_KEY", FAKE_API_KEY)
        .env("QF_BINANCE_API_SECRET", FAKE_API_SECRET)
        .arg("--binance-base-url")
        .arg(mock.base_url());
    cmd
}

/// Level 2, testnet tier: bed + Spot testnet URL + the operator's real
/// testnet credentials + a 30s wall-clock guard (the HTTP client has no
/// timeouts of its own).
pub fn testnet_cmd(bed: &TestBed, testnet: &TestnetEnv) -> Command {
    let mut cmd = cli_cmd(bed, "error");
    cmd.env("QF_BINANCE_API_KEY", &testnet.api_key)
        .env("QF_BINANCE_API_SECRET", &testnet.api_secret)
        .arg("--binance-base-url")
        .arg(TESTNET_BASE_URL)
        .timeout(Duration::from_secs(30));
    cmd
}

/// The operator's testnet credentials, read from the real (test-runner)
/// environment. Both-or-nothing, mirroring `BinanceCredentials::from_env`.
#[derive(Debug)]
pub struct TestnetEnv {
    pub api_key: String,
    pub api_secret: String,
}

pub fn testnet_env() -> Option<TestnetEnv> {
    let api_key = std::env::var("QF_BINANCE_API_KEY").ok()?;
    let api_secret = std::env::var("QF_BINANCE_API_SECRET").ok()?;
    Some(TestnetEnv {
        api_key,
        api_secret,
    })
}

/// Evaluates to the [`TestnetEnv`] or returns from the test with a visible
/// skip marker on stderr (see the module docs for why the marker stays
/// visible under libtest capture).
macro_rules! skip_unless_testnet {
    () => {
        match crate::harness::testnet_env() {
            Some(testnet) => testnet,
            None => {
                use std::io::Write as _;
                let _ = writeln!(
                    std::io::stderr(),
                    "SKIP (testnet tier) {}:{}: QF_BINANCE_API_KEY/QF_BINANCE_API_SECRET not set; \
                     export Spot testnet keys to run",
                    module_path!(),
                    line!(),
                );
                return;
            }
        }
    };
}
pub(crate) use skip_unless_testnet;

/// Parsed `key: value` stdout lines for value assertions.
#[derive(Debug)]
pub struct CliOutput {
    pairs: Vec<(String, String)>,
}

impl CliOutput {
    /// Keeps only non-indented lines containing `": "`: indented detail
    /// rows (`  00: Gap...`, balance/order/trade rows), bare section
    /// headers (`balances:`), `---` separators, and any other line without
    /// a `": "` (monitor trade rows) are all dropped.
    pub fn parse(stdout: &[u8]) -> Self {
        let text = String::from_utf8_lossy(stdout);
        let pairs = text
            .lines()
            .filter(|line| !line.starts_with([' ', '\t']))
            .filter_map(|line| line.split_once(": "))
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        Self { pairs }
    }

    pub fn from_assert(assert: &assert_cmd::assert::Assert) -> Self {
        Self::parse(&assert.get_output().stdout)
    }

    /// First value printed for `key`.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.pairs
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    }

    /// Every value printed for `key`, in output order (the CLI repeats
    /// keys, e.g. backtest `trade:` lines).
    pub fn get_all(&self, key: &str) -> Vec<&str> {
        self.pairs
            .iter()
            .filter(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_output_parser_skips_detail_lines_and_collects_repeated_keys() {
        let stdout = b"market: binance_spot BTCUSDT 1m\n\
            candles: 2\n\
            issues: 1\n\
            \x20 00: Gap { expected_open_time_ms: 60000, open_time_ms: 120000 }\n\
            balances:\n\
            \x20 BTC free=0.05 locked=0.00\n\
            ---\n\
            trade: entry=0 @ 100 exit=1 @ 110\n\
            trade: entry=2 @ 110 exit=3 @ 90\n";

        let output = CliOutput::parse(stdout);
        assert_eq!(output.get("market"), Some("binance_spot BTCUSDT 1m"));
        assert_eq!(output.get("candles"), Some("2"));
        assert_eq!(output.get("issues"), Some("1"));
        // Indented detail rows, bare headers, and separators are dropped —
        // dropped entirely, not kept under a whitespace-padded key.
        assert_eq!(output.get("00"), None);
        assert_eq!(output.get("  00"), None);
        assert_eq!(output.get("  BTC free=0.05 locked=0.00"), None);
        assert_eq!(output.get("balances"), None);
        assert_eq!(output.get("---"), None);
        // Repeated keys stay in order.
        assert_eq!(
            output.get_all("trade"),
            vec!["entry=0 @ 100 exit=1 @ 110", "entry=2 @ 110 exit=3 @ 90"]
        );
        assert_eq!(output.get("trade"), Some("entry=0 @ 100 exit=1 @ 110"));
    }
}
