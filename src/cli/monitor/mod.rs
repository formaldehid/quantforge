//! `quantforge monitor` — observe Binance state and manage orders
//! manually when needed. Every subcommand requires credentials, gated by
//! the caller before dispatch.

use anyhow::Result;
use clap::Subcommand;
use quantforge::{BinanceSpotClient, SqliteStore};

mod cancel_order;
mod close_position;
mod orders;
mod status;
mod trades;
mod watch;

use cancel_order::{MonitorCancelOrderArgs, handle_monitor_cancel_order};
use close_position::{MonitorClosePositionArgs, handle_monitor_close_position};
use orders::{MonitorOrdersArgs, handle_monitor_orders};
use status::{MonitorStatusArgs, handle_monitor_status};
use trades::{MonitorTradesArgs, handle_monitor_trades};
use watch::{MonitorWatchArgs, handle_monitor_watch};

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

pub(crate) async fn dispatch(
    store: &SqliteStore,
    private_client: &BinanceSpotClient,
    command: MonitorCommand,
) -> Result<()> {
    match command {
        MonitorCommand::Status(args) => handle_monitor_status(store, private_client, args).await,
        MonitorCommand::Watch(args) => handle_monitor_watch(store, private_client, args).await,
        MonitorCommand::Orders(args) => handle_monitor_orders(private_client, args).await,
        MonitorCommand::Trades(args) => handle_monitor_trades(private_client, args).await,
        MonitorCommand::CancelOrder(args) => {
            handle_monitor_cancel_order(private_client, args).await
        }
        MonitorCommand::ClosePosition(args) => {
            handle_monitor_close_position(private_client, args).await
        }
    }
}
