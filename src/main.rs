use anyhow::{Context, Result, anyhow};
use clap::Parser;
use cli::{
    Cli, Command, DataCommand, MonitorCommand, TradeCommand, display_url, handle_backtest,
    handle_data_sync, handle_data_validate, handle_monitor_cancel_order,
    handle_monitor_close_position, handle_monitor_orders, handle_monitor_status,
    handle_monitor_trades, handle_monitor_watch, handle_trade_close, handle_trade_run,
};
use quantforge::{BinanceCredentials, BinanceSpotClient, CandleStore, SqliteStore};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use url::Url;

mod cli;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log_level)?;

    let db_existed = cli.db.exists();
    if let Some(parent) = cli.db.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create parent directory {}", parent.display()))?;
    }

    let store = SqliteStore::new(&cli.db);
    CandleStore::init(&store).context("failed to initialize sqlite store")?;
    if !db_existed {
        warn!(
            db_path = %cli.db.display(),
            "database did not exist and was created empty; if you expected existing data \
             (run state, candles), check the --db path or QF_DB"
        );
    }

    let base_url =
        Url::parse(&cli.binance_base_url).context("failed to parse --binance-base-url")?;
    info!(base_url = %display_url(&base_url), "using Binance base URL");
    let public_client = BinanceSpotClient::new(base_url.clone());
    let private_client = BinanceCredentials::from_env()
        .map(|credentials| BinanceSpotClient::new(base_url.clone()).with_credentials(credentials));

    match cli.command {
        Command::Data { command } => match command {
            DataCommand::Sync(args) => handle_data_sync(&store, &public_client, args).await?,
            DataCommand::Validate(args) => handle_data_validate(&store, args)?,
        },
        Command::Backtest(args) => handle_backtest(&store, args)?,
        Command::Trade { command } => match command {
            TradeCommand::Run(args) => {
                handle_trade_run(
                    &store,
                    &public_client,
                    private_client.as_ref(),
                    &base_url,
                    args,
                )
                .await?
            }
            TradeCommand::Close(args) => {
                let private_client = private_client.as_ref().ok_or_else(|| {
                    anyhow!("trade close requires QF_BINANCE_API_KEY and QF_BINANCE_API_SECRET")
                })?;
                handle_trade_close(&store, private_client, args).await?
            }
        },
        Command::Monitor { command } => {
            let private_client = private_client.as_ref().ok_or_else(|| {
                anyhow!("monitor commands require QF_BINANCE_API_KEY and QF_BINANCE_API_SECRET")
            })?;
            match command {
                MonitorCommand::Status(args) => {
                    handle_monitor_status(&store, private_client, args).await?
                }
                MonitorCommand::Watch(args) => {
                    handle_monitor_watch(&store, private_client, args).await?
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

fn init_tracing(level: &str) -> Result<()> {
    let env_filter = EnvFilter::try_new(level)
        .with_context(|| format!("invalid log level/filter expression: {level}"))?;

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .init();
    Ok(())
}
