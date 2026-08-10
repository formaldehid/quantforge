//! Command-line interface: the root parser, the command tree, and the
//! `run()` dispatch. Each command group owns a module mirroring its
//! command path (`data`, `trade`); `commands` is the temporary home for
//! the groups whose splits are still pending.

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod commands;
mod common;
mod context;
mod data;
mod trade;

use context::AppContext;

// Args types are only named by the Command enum below; the handlers are
// dispatched by run().
use commands::{
    BacktestArgs, MonitorCancelOrderArgs, MonitorClosePositionArgs, MonitorOrdersArgs,
    MonitorStatusArgs, MonitorTradesArgs, MonitorWatchArgs,
};
use commands::{
    handle_backtest, handle_monitor_cancel_order, handle_monitor_close_position,
    handle_monitor_orders, handle_monitor_status, handle_monitor_trades, handle_monitor_watch,
};
use data::DataCommand;
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
            match command {
                MonitorCommand::Status(args) => {
                    handle_monitor_status(&ctx.store, private_client, args).await?
                }
                MonitorCommand::Watch(args) => {
                    handle_monitor_watch(&ctx.store, private_client, args).await?
                }
                MonitorCommand::Orders(args) => handle_monitor_orders(private_client, args).await?,
                MonitorCommand::Trades(args) => handle_monitor_trades(private_client, args).await?,
                MonitorCommand::CancelOrder(args) => {
                    handle_monitor_cancel_order(private_client, args).await?
                }
                MonitorCommand::ClosePosition(args) => {
                    handle_monitor_close_position(private_client, args).await?
                }
            }
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

#[derive(Subcommand, Debug)]
pub(crate) enum MonitorCommand {
    /// Print balances, latest run state, open orders, and recent trades.
    Status(MonitorStatusArgs),

    /// Poll status repeatedly.
    Watch(MonitorWatchArgs),

    /// List current open orders on the symbol.
    Orders(MonitorOrdersArgs),

    /// List recent Binance trades on the symbol.
    Trades(MonitorTradesArgs),

    /// Cancel a specific order manually.
    CancelOrder(MonitorCancelOrderArgs),

    /// Close the current free base-asset balance for the symbol.
    ClosePosition(MonitorClosePositionArgs),
}
