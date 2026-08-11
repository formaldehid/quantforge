//! Command-line interface: the root parser, the command tree, and the
//! `run()` dispatch. Each command group owns a module mirroring its
//! command path (`backtest`, `data`, `monitor`, `trade`).

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod backtest;
mod common;
mod context;
mod data;
mod monitor;
mod trade;

use context::AppContext;

// Args types are only named by the Command enum below; the handlers are
// dispatched by run().
use backtest::{BacktestArgs, handle_backtest};
use data::DataCommand;
use monitor::MonitorCommand;
use trade::TradeCommand;

/// Build the per-invocation context and dispatch the parsed command.
pub(crate) async fn run(cli: Cli) -> Result<()> {
    let ctx = AppContext::init(&cli)?;

    match cli.command {
        Command::Data { command } => data::dispatch(&ctx, command).await?,
        Command::Backtest(args) => handle_backtest(&ctx, args)?,
        Command::Trade { command } => trade::dispatch(&ctx, command).await?,
        Command::Monitor { command } => {
            let private_client = ctx.require_private_client(
                "monitor commands require QF_BINANCE_API_KEY and QF_BINANCE_API_SECRET",
            )?;
            monitor::dispatch(&ctx.store, private_client, command).await?
        }
    }

    Ok(())
}

/// Production Binance Spot endpoint — the default when neither
/// `--binance-base-url` nor `QF_BINANCE_BASE_URL` is set.
const DEFAULT_BINANCE_BASE_URL: &str = "https://api.binance.com/";

#[derive(Parser, Debug)]
#[command(
    name = "quantforge",
    version,
    about = "CLI-first market data ingestion, research, and controlled live trading framework."
)]
pub(crate) struct Cli {
    /// SQLite database path.
    #[arg(
        long,
        global = true,
        env = "QF_DB",
        default_value = "data/market.sqlite"
    )]
    pub(crate) db: PathBuf,

    /// Binance API base URL. Use https://testnet.binance.vision/ for Spot testnet.
    #[arg(
        long,
        global = true,
        env = "QF_BINANCE_BASE_URL",
        default_value = DEFAULT_BINANCE_BASE_URL
    )]
    pub(crate) binance_base_url: String,

    /// Log filter (trace, debug, info, warn, error or a full tracing filter expression).
    #[arg(long, global = true, env = "QF_LOG", default_value = "info")]
    pub(crate) log_level: String,

    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Command {
    /// Historical and incremental data ingestion into SQLite.
    Data {
        #[command(subcommand)]
        command: DataCommand,
    },

    /// Deterministic backtest using locally stored candles.
    Backtest(BacktestArgs),

    /// Run or manually close the strategy bot.
    Trade {
        #[command(subcommand)]
        command: TradeCommand,
    },

    /// Observe Binance state and manage orders manually when needed.
    Monitor {
        #[command(subcommand)]
        command: MonitorCommand,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    // clap's own consistency self-check: duplicate flags or ids, broken
    // defaults, and malformed value parsers panic here rather than at the
    // first user invocation.
    #[test]
    fn cli_definition_is_internally_consistent() {
        Cli::command().debug_assert();
    }
}
