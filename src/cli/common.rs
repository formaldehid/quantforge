//! Shared argument structs for `#[command(flatten)]`, plus the CLI
//! execution-mode enum.
//!
//! clap splices a flattened struct's args into the parent at the flatten
//! position, in declaration order — so a group is only flattened where its
//! fields were already adjacent and in this order, keeping every command's
//! `--help` byte-identical to the pre-split output.

use anyhow::{Context, Result, bail};
use clap::{Args, ValueEnum};
use quantforge::{BuiltInStrategyConfig, ExchangeId, ExecutionMode, Interval, MarketId, Symbol};
use rust_decimal::Decimal;

/// `--symbol` for commands without an interval.
#[derive(Args, Debug)]
pub(crate) struct SymbolArgs {
    #[arg(long)]
    pub(crate) symbol: String,
}

/// `--symbol` and `--interval` for market-scoped commands.
#[derive(Args, Debug)]
pub(crate) struct MarketArgs {
    #[arg(long)]
    pub(crate) symbol: String,
    #[arg(long, default_value = "1m")]
    pub(crate) interval: String,
}

/// `--yes` for commands that mutate exchange state.
#[derive(Args, Debug)]
pub(crate) struct ConfirmArgs {
    #[arg(long, default_value_t = false)]
    pub(crate) yes: bool,
}

/// `--poll-secs` and `--max-loops` for polling loops. `--poll-secs` keeps
/// its clap-level minimum of 1.
#[derive(Args, Debug)]
pub(crate) struct PollArgs {
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) poll_secs: u64,
    #[arg(long)]
    pub(crate) max_loops: Option<usize>,
}

/// `--strategy-name` for commands that select a run by strategy.
#[derive(Args, Debug)]
pub(crate) struct StrategyArgs {
    #[arg(long, default_value = "sma_cross")]
    pub(crate) strategy_name: String,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum CliExecutionMode {
    #[value(name = "dry-run")]
    DryRun,
    #[value(name = "live")]
    Live,
}

impl From<CliExecutionMode> for ExecutionMode {
    fn from(value: CliExecutionMode) -> Self {
        match value {
            CliExecutionMode::DryRun => ExecutionMode::DryRun,
            CliExecutionMode::Live => ExecutionMode::Live,
        }
    }
}

pub(crate) fn parse_market(symbol: String, interval: String) -> Result<MarketId> {
    let symbol = Symbol::new(symbol)?;
    let interval = interval.parse::<Interval>()?;
    Ok(MarketId::new(ExchangeId::BinanceSpot, symbol, interval))
}

pub(crate) fn parse_positive_decimal(flag: &str, raw: &str) -> Result<Decimal> {
    let value = raw
        .trim()
        .parse::<Decimal>()
        .with_context(|| format!("failed to parse {flag}: {raw}"))?;
    if value <= Decimal::ZERO {
        bail!("{flag} must be greater than 0, got {raw}");
    }
    Ok(value)
}

pub(crate) fn parse_non_negative_decimal(flag: &str, raw: &str) -> Result<Decimal> {
    let value = raw
        .trim()
        .parse::<Decimal>()
        .with_context(|| format!("failed to parse {flag}: {raw}"))?;
    if value < Decimal::ZERO {
        bail!("{flag} must be zero or greater, got {raw}");
    }
    Ok(value)
}

pub(crate) fn strategy_config(fast: usize, slow: usize) -> BuiltInStrategyConfig {
    BuiltInStrategyConfig::SmaCross { fast, slow }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positive_decimal_parses_valid_values() {
        for (raw, expected) in [("100", "100"), ("0.001", "0.001"), (" 10 ", "10")] {
            let value = parse_positive_decimal("--cash", raw).expect("decimal");
            assert_eq!(value.to_string(), expected, "for input {raw:?}");
        }
    }

    #[test]
    fn positive_decimal_rejects_zero_and_negative_with_exact_message() {
        for raw in ["0", "-5"] {
            let error = parse_positive_decimal("--quote-order-qty", raw).expect_err("error");
            assert_eq!(
                error.to_string(),
                format!("--quote-order-qty must be greater than 0, got {raw}"),
                "for input {raw:?}"
            );
        }
    }

    #[test]
    fn non_negative_decimal_accepts_zero_and_rejects_negative() {
        let zero = parse_non_negative_decimal("--fee-bps", "0").expect("decimal");
        assert_eq!(zero, Decimal::ZERO);

        let error = parse_non_negative_decimal("--fee-bps", "-1").expect_err("error");
        assert_eq!(
            error.to_string(),
            "--fee-bps must be zero or greater, got -1"
        );
    }

    #[test]
    fn decimal_helpers_name_the_flag_on_parse_failure() {
        let error = parse_positive_decimal("--cash", "abc").expect_err("error");
        assert!(
            error.to_string().contains("failed to parse --cash: abc"),
            "got {error:#}"
        );
    }
}
