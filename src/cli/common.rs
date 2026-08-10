//! Shared argument structs for `#[command(flatten)]`, plus the CLI
//! execution-mode enum.
//!
//! clap splices a flattened struct's args into the parent at the flatten
//! position, in declaration order — so a group is only flattened where its
//! fields were already adjacent and in this order, keeping every command's
//! `--help` byte-identical to the pre-split output.

use clap::{Args, ValueEnum};
use quantforge::ExecutionMode;

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
