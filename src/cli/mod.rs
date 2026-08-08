//! Command-line interface: the root parser, the command tree, and (until
//! the per-command split) every argument struct and handler.

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use quantforge::ExecutionMode;
use std::path::PathBuf;

mod commands;
mod context;

use context::AppContext;

// Args types are only named by the Command enum below; the handlers are
// dispatched by run(), and display_url is shared with context.rs.
use commands::{
    BacktestArgs, DataSyncArgs, DataValidateArgs, MonitorCancelOrderArgs, MonitorClosePositionArgs,
    MonitorOrdersArgs, MonitorStatusArgs, MonitorTradesArgs, MonitorWatchArgs, TradeCloseArgs,
    TradeRunArgs,
};
use commands::{
    display_url, handle_backtest, handle_data_sync, handle_data_validate,
    handle_monitor_cancel_order, handle_monitor_close_position, handle_monitor_orders,
    handle_monitor_status, handle_monitor_trades, handle_monitor_watch, handle_trade_close,
    handle_trade_run,
};

/// Build the per-invocation context and dispatch the parsed command.
pub(crate) async fn run(cli: Cli) -> Result<()> {
    let ctx = AppContext::init(&cli)?;

    match cli.command {
        Command::Data { command } => match command {
            DataCommand::Sync(args) => handle_data_sync(&ctx, args).await?,
            DataCommand::Validate(args) => handle_data_validate(&ctx, args)?,
        },
        Command::Backtest(args) => handle_backtest(&ctx, args)?,
        Command::Trade { command } => match command {
            TradeCommand::Run(args) => handle_trade_run(&ctx, args).await?,
            TradeCommand::Close(args) => handle_trade_close(&ctx, args).await?,
        },
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
pub(crate) enum DataCommand {
    /// Sync candles from Binance into SQLite.
    Sync(DataSyncArgs),

    /// Validate stored candles for duplicates, gaps, ordering, and OHLC sanity.
    Validate(DataValidateArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum TradeCommand {
    /// Run the polling strategy bot against SQLite-backed live candles.
    Run(TradeRunArgs),

    /// Close the bot-managed position with a market sell.
    Close(TradeCloseArgs),
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

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum CliExecutionMode {
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
