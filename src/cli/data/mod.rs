//! `quantforge data` — historical and incremental data ingestion into
//! SQLite, plus stored-candle validation.

use super::context::AppContext;
use anyhow::Result;
use clap::Subcommand;

mod sync;
mod validate;

use sync::{DataSyncArgs, handle_data_sync};
use validate::{DataValidateArgs, handle_data_validate};

#[derive(Subcommand, Debug)]
pub(crate) enum DataCommand {
    /// Sync candles from Binance into SQLite.
    Sync(DataSyncArgs),

    /// Validate stored candles for duplicates, gaps, ordering, and OHLC sanity.
    Validate(DataValidateArgs),
}

pub(crate) async fn dispatch(ctx: &AppContext, command: DataCommand) -> Result<()> {
    match command {
        DataCommand::Sync(args) => handle_data_sync(ctx, args).await,
        DataCommand::Validate(args) => handle_data_validate(ctx, args),
    }
}
