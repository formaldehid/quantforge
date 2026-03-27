use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use quantforge::SqliteStore;
use quantforge::{
    BacktestConfig, BacktestEngine, DataSyncConfig, DataSyncEngine, LiveTradeConfig,
    LiveTradeEngine,
};
use quantforge::{BinanceCredentials, BinanceSpotClient};
use quantforge::{
    BuiltInStrategyConfig, CandleQuery, CandleStore, ClosedTrade, ExchangeId, ExecutionMode,
    Interval, MarketDataSource, MarketId, PositionState, RunJournalStore, Side, Symbol,
    TradingVenue, ms_to_rfc3339, now_utc_ms, parse_rfc3339_to_ms, round_down_to_step,
    validate_candles,
};
use rust_decimal::Decimal;
use std::{path::PathBuf, time::Duration};
use tracing_subscriber::EnvFilter;
use url::Url;

#[derive(Parser, Debug)]
#[command(
    name = "quantforge",
    version,
    about = "CLI-first market data ingestion, research, and controlled live trading framework."
)]
struct Cli {
    /// SQLite database path.
    #[arg(
        long,
        global = true,
        env = "QF_DB",
        default_value = "data/market.sqlite"
    )]
    db: PathBuf,

    /// Binance API base URL. Use https://testnet.binance.vision/ for Spot testnet.
    #[arg(
        long,
        global = true,
        env = "QF_BINANCE_BASE_URL",
        default_value = "https://api.binance.com/"
    )]
    binance_base_url: String,

    /// Log filter (trace, debug, info, warn, error or a full tracing filter expression).
    #[arg(long, global = true, env = "QF_LOG", default_value = "info")]
    log_level: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
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
enum DataCommand {
    /// Sync candles from Binance into SQLite.
    Sync(DataSyncArgs),

    /// Validate stored candles for duplicates, gaps, ordering, and OHLC sanity.
    Validate(DataValidateArgs),
}

#[derive(Args, Debug)]
struct DataSyncArgs {
    #[arg(long)]
    symbol: String,
    #[arg(long, default_value = "1m")]
    interval: String,
    /// RFC3339 start time. Omit to begin syncing from the current time.
    #[arg(long)]
    start: Option<String>,
    /// RFC3339 end time. Omit to keep syncing indefinitely.
    #[arg(long)]
    end: Option<String>,
    #[arg(long, default_value_t = 1000)]
    limit: u16,
    /// When --end is set, keep polling until that end boundary is reached.
    #[arg(long, default_value_t = false)]
    follow: bool,
    #[arg(long, default_value_t = 5)]
    poll_secs: u64,
    #[arg(long)]
    max_loops: Option<usize>,
}

#[derive(Args, Debug)]
struct DataValidateArgs {
    #[arg(long)]
    symbol: String,
    #[arg(long, default_value = "1m")]
    interval: String,
    #[arg(long)]
    start: Option<String>,
    #[arg(long)]
    end: Option<String>,
}

#[derive(Args, Debug)]
struct BacktestArgs {
    #[arg(long)]
    symbol: String,
    #[arg(long, default_value = "1m")]
    interval: String,
    #[arg(long)]
    start: Option<String>,
    #[arg(long)]
    end: Option<String>,
    #[arg(long, default_value_t = 20)]
    fast: usize,
    #[arg(long, default_value_t = 50)]
    slow: usize,
    #[arg(long, default_value = "10000")]
    cash: String,
    #[arg(long, default_value = "10")]
    fee_bps: String,
}

#[derive(Subcommand, Debug)]
enum TradeCommand {
    /// Run the polling strategy bot against SQLite-backed live candles.
    Run(TradeRunArgs),

    /// Close the bot-managed position with a market sell.
    Close(TradeCloseArgs),
}

#[derive(Args, Debug)]
struct TradeRunArgs {
    #[arg(long)]
    symbol: String,
    #[arg(long, default_value = "1m")]
    interval: String,
    #[arg(long, default_value_t = 20)]
    fast: usize,
    #[arg(long, default_value_t = 50)]
    slow: usize,
    #[arg(long, default_value = "100")]
    quote_order_qty: String,
    #[arg(long, value_enum, default_value_t = CliExecutionMode::DryRun)]
    mode: CliExecutionMode,
    #[arg(long, default_value_t = 5)]
    poll_secs: u64,
    #[arg(long, default_value_t = 300)]
    bootstrap_bars: usize,
    #[arg(long, default_value_t = false)]
    bootstrap_enter: bool,
    #[arg(long, default_value_t = 1000)]
    limit: u16,
    #[arg(long)]
    run_id: Option<String>,
    #[arg(long)]
    max_loops: Option<usize>,
}

#[derive(Args, Debug)]
struct TradeCloseArgs {
    #[arg(long)]
    symbol: String,
    #[arg(long, default_value = "1m")]
    interval: String,
    #[arg(long, default_value = "sma_cross")]
    strategy_name: String,
    #[arg(long)]
    run_id: Option<String>,
    #[arg(long, default_value_t = false)]
    yes: bool,
}

#[derive(Subcommand, Debug)]
enum MonitorCommand {
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

#[derive(Args, Debug)]
struct MonitorStatusArgs {
    #[arg(long)]
    symbol: String,
    #[arg(long, default_value = "1m")]
    interval: String,
    #[arg(long, default_value = "sma_cross")]
    strategy_name: String,
    #[arg(long, default_value_t = 10)]
    recent_trades: usize,
}

#[derive(Args, Debug)]
struct MonitorWatchArgs {
    #[arg(long)]
    symbol: String,
    #[arg(long, default_value = "1m")]
    interval: String,
    #[arg(long, default_value = "sma_cross")]
    strategy_name: String,
    #[arg(long, default_value_t = 10)]
    recent_trades: usize,
    #[arg(long, default_value_t = 5)]
    poll_secs: u64,
    #[arg(long)]
    max_loops: Option<usize>,
}

#[derive(Args, Debug)]
struct MonitorOrdersArgs {
    #[arg(long)]
    symbol: String,
}

#[derive(Args, Debug)]
struct MonitorTradesArgs {
    #[arg(long)]
    symbol: String,
    #[arg(long, default_value_t = 20)]
    limit: usize,
}

#[derive(Args, Debug)]
struct MonitorCancelOrderArgs {
    #[arg(long)]
    symbol: String,
    #[arg(long)]
    order_id: Option<i64>,
    #[arg(long)]
    client_order_id: Option<String>,
    #[arg(long, default_value_t = false)]
    yes: bool,
}

#[derive(Args, Debug)]
struct MonitorClosePositionArgs {
    #[arg(long)]
    symbol: String,
    #[arg(long, default_value_t = false)]
    yes: bool,
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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log_level)?;

    if let Some(parent) = cli.db.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create parent directory {}", parent.display()))?;
    }

    let store = SqliteStore::new(&cli.db);
    CandleStore::init(&store).context("failed to initialize sqlite store")?;

    let base_url =
        Url::parse(&cli.binance_base_url).context("failed to parse --binance-base-url")?;
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
                handle_trade_run(&store, &public_client, private_client.as_ref(), args).await?
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

fn parse_market(symbol: String, interval: String) -> Result<MarketId> {
    let symbol = Symbol::new(symbol)?;
    let interval = interval.parse::<Interval>()?;
    Ok(MarketId::new(ExchangeId::BinanceSpot, symbol, interval))
}

fn strategy_config(fast: usize, slow: usize) -> BuiltInStrategyConfig {
    BuiltInStrategyConfig::SmaCross { fast, slow }
}

async fn handle_data_sync(
    store: &SqliteStore,
    client: &BinanceSpotClient,
    args: DataSyncArgs,
) -> Result<()> {
    let market = parse_market(args.symbol, args.interval)?;
    let engine = DataSyncEngine::new(client, store);
    let summary = engine
        .run(&DataSyncConfig {
            market,
            start_time_ms: args
                .start
                .as_deref()
                .map(parse_rfc3339_to_ms)
                .transpose()
                .context("failed to parse --start")?,
            end_time_ms: args
                .end
                .as_deref()
                .map(parse_rfc3339_to_ms)
                .transpose()
                .context("failed to parse --end")?,
            batch_limit: args.limit,
            follow: args.follow,
            poll_interval: Duration::from_secs(args.poll_secs),
            max_loops: args.max_loops,
        })
        .await?;

    println!("iterations: {}", summary.iterations);
    println!("written: {}", summary.written);
    println!(
        "last_open_time: {}",
        summary
            .last_open_time_ms
            .map(ms_to_rfc3339)
            .unwrap_or_else(|| "none".to_string())
    );
    Ok(())
}

fn handle_data_validate(store: &SqliteStore, args: DataValidateArgs) -> Result<()> {
    let market = parse_market(args.symbol, args.interval)?;
    let candles = store.load_candles(
        &market,
        CandleQuery {
            start_time_ms: args
                .start
                .as_deref()
                .map(parse_rfc3339_to_ms)
                .transpose()
                .context("failed to parse --start")?,
            end_time_ms: args
                .end
                .as_deref()
                .map(parse_rfc3339_to_ms)
                .transpose()
                .context("failed to parse --end")?,
            limit: None,
        },
    )?;

    let report = validate_candles(&market, &candles);
    println!(
        "market: {} {} {}",
        report.market.exchange, report.market.symbol, report.market.interval
    );
    println!("candles: {}", report.candle_count);
    println!("issues: {}", report.issues.len());
    for (index, issue) in report.issues.iter().take(20).enumerate() {
        println!("  {:02}: {:?}", index, issue);
    }
    if report.issues.len() > 20 {
        println!("  ... ({} more)", report.issues.len() - 20);
    }
    Ok(())
}

fn handle_backtest(store: &SqliteStore, args: BacktestArgs) -> Result<()> {
    let market = parse_market(args.symbol, args.interval)?;
    let candles = store.load_candles(
        &market,
        CandleQuery {
            start_time_ms: args
                .start
                .as_deref()
                .map(parse_rfc3339_to_ms)
                .transpose()
                .context("failed to parse --start")?,
            end_time_ms: args
                .end
                .as_deref()
                .map(parse_rfc3339_to_ms)
                .transpose()
                .context("failed to parse --end")?,
            limit: None,
        },
    )?;

    let mut strategy = strategy_config(args.fast, args.slow)
        .build()
        .context("failed to build strategy")?;
    let engine = BacktestEngine::new(BacktestConfig {
        initial_cash: args
            .cash
            .parse::<Decimal>()
            .context("failed to parse --cash")?,
        fee_bps: args
            .fee_bps
            .parse::<Decimal>()
            .context("failed to parse --fee-bps")?,
        close_out_at_end: true,
    });

    let result = engine.run(&market, &candles, strategy.as_mut())?;
    println!("strategy: {}", strategy.name());
    println!("final_equity: {}", result.final_equity);
    println!("total_return_pct: {}", result.total_return_pct);
    println!("max_drawdown_pct: {}", result.max_drawdown_pct);
    println!("trade_count: {}", result.trade_count);
    for trade in result.trades.iter().rev().take(5).rev() {
        println!(
            "trade: entry={} @ {} exit={} @ {} qty={} gross_pnl={}",
            trade.entry_time_ms,
            trade.entry_price,
            trade.exit_time_ms,
            trade.exit_price,
            trade.qty,
            trade.gross_quote_pnl
        );
    }
    Ok(())
}

async fn handle_trade_run(
    store: &SqliteStore,
    public_client: &BinanceSpotClient,
    private_client: Option<&BinanceSpotClient>,
    args: TradeRunArgs,
) -> Result<()> {
    let market = parse_market(args.symbol, args.interval)?;
    let engine = LiveTradeEngine::new(
        public_client,
        store,
        store,
        if matches!(args.mode, CliExecutionMode::Live) {
            Some(
                private_client
                    .ok_or_else(|| anyhow!("trade run --mode live requires Binance credentials"))?
                    as &dyn TradingVenue,
            )
        } else {
            None
        },
    );

    let summary = engine
        .run(&LiveTradeConfig {
            market,
            strategy: strategy_config(args.fast, args.slow),
            execution_mode: args.mode.into(),
            quote_order_qty: args
                .quote_order_qty
                .parse::<Decimal>()
                .context("failed to parse --quote-order-qty")?,
            poll_interval: Duration::from_secs(args.poll_secs),
            bootstrap_bars: args.bootstrap_bars,
            bootstrap_enter: args.bootstrap_enter,
            batch_limit: args.limit,
            run_id: args.run_id,
            max_loops: args.max_loops,
        })
        .await?;

    println!("run_id: {}", summary.run_id);
    println!("processed_bars: {}", summary.processed_bars);
    println!("submitted_orders: {}", summary.submitted_orders);
    println!("closed_trades: {}", summary.closed_trades);
    println!(
        "last_processed_open_time: {}",
        summary
            .last_processed_open_time_ms
            .map(ms_to_rfc3339)
            .unwrap_or_else(|| "none".to_string())
    );
    Ok(())
}

async fn handle_trade_close(
    store: &SqliteStore,
    private_client: &BinanceSpotClient,
    args: TradeCloseArgs,
) -> Result<()> {
    let market = parse_market(args.symbol, args.interval)?;
    let rules = private_client.fetch_symbol_rules(&market.symbol).await?;
    let mut run_state = if let Some(run_id) = args.run_id {
        store
            .load_run_state(&run_id)?
            .ok_or_else(|| anyhow!("no run found for run_id={run_id}"))?
    } else {
        store
            .latest_run_for_market(&market, &args.strategy_name)?
            .ok_or_else(|| {
                anyhow!(
                    "no run found for market={} strategy={}",
                    market.symbol,
                    args.strategy_name
                )
            })?
    };

    let balances = private_client.account_balances().await?;
    let free_base_qty = balances
        .iter()
        .find(|balance| balance.asset.eq_ignore_ascii_case(&rules.base_asset))
        .map(|balance| balance.free)
        .unwrap_or(Decimal::ZERO);

    let qty = round_quantity_for_rules(free_base_qty.min(run_state.position.qty), &rules);
    println!("run_id: {}", run_state.run_id);
    println!("sell_qty: {}", qty);
    if !args.yes {
        println!("No order sent. Re-run with --yes to execute the market sell.");
        return Ok(());
    }
    if qty <= Decimal::ZERO {
        bail!("no sellable quantity available for {}", rules.base_asset);
    }

    let order = private_client
        .submit_market_order(&quantforge::MarketOrderRequest {
            symbol: market.symbol.clone(),
            side: Side::Sell,
            quantity: Some(qty),
            quote_order_qty: None,
            new_client_order_id: Some(format!("manual-close-{}", now_utc_ms())),
        })
        .await?;

    store.append_order_event(&run_state.run_id, &order)?;
    if run_state.position.is_open() && order.executed_qty > Decimal::ZERO {
        let entry_price = run_state.position.entry_price.unwrap_or(Decimal::ZERO);
        let exit_price = order.average_price().unwrap_or(entry_price);
        let closed_qty = order.executed_qty.min(run_state.position.qty);
        let trade = ClosedTrade {
            symbol: market.symbol.clone(),
            entry_time_ms: run_state
                .position
                .entry_time_ms
                .unwrap_or(order.transact_time_ms.unwrap_or(now_utc_ms())),
            exit_time_ms: order.transact_time_ms.unwrap_or(now_utc_ms()),
            entry_price,
            exit_price,
            qty: closed_qty,
            gross_quote_pnl: (exit_price - entry_price) * closed_qty,
            entry_order_id: run_state.position.entry_order_id,
            exit_order_id: order.order_id,
        };
        store.append_closed_trade(&run_state.run_id, &trade)?;

        let remaining_qty = (run_state.position.qty - closed_qty).max(Decimal::ZERO);
        run_state.updated_at_ms = now_utc_ms();
        run_state.last_error = None;

        if remaining_qty > Decimal::ZERO {
            run_state.position.qty = remaining_qty;
            run_state.status = quantforge::RunStatus::Running;
            run_state.stopped_at_ms = None;
        } else {
            run_state.position = PositionState::flat();
            run_state.status = quantforge::RunStatus::Stopped;
            run_state.stopped_at_ms = Some(run_state.updated_at_ms);
        }

        store.save_run_state(&run_state)?;
    }

    print_order(&order);
    Ok(())
}

async fn handle_monitor_status(
    store: &SqliteStore,
    private_client: &BinanceSpotClient,
    args: MonitorStatusArgs,
) -> Result<()> {
    let market = parse_market(args.symbol, args.interval)?;
    let rules = private_client.fetch_symbol_rules(&market.symbol).await?;
    let balances = private_client.account_balances().await?;
    let open_orders = private_client.open_orders(Some(&market.symbol)).await?;
    let trades = private_client
        .recent_trades(&market.symbol, args.recent_trades)
        .await?;
    let run = store.latest_run_for_market(&market, &args.strategy_name)?;

    println!("symbol: {}", market.symbol);
    println!("base_asset: {}", rules.base_asset);
    println!("quote_asset: {}", rules.quote_asset);
    println!("time_utc_ms: {}", now_utc_ms());

    if let Some(run) = run {
        println!("latest_run_id: {}", run.run_id);
        println!("run_status: {}", run.status.as_str());
        println!(
            "last_processed_open_time: {}",
            run.last_processed_open_time_ms
                .map(ms_to_rfc3339)
                .unwrap_or_else(|| "none".to_string())
        );
        println!("position_qty: {}", run.position.qty);
        println!(
            "entry_price: {}",
            run.position
                .entry_price
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string())
        );
    } else {
        println!("latest_run_id: none");
    }

    println!("balances:");
    for balance in balances.iter().filter(|balance| {
        balance.asset.eq_ignore_ascii_case(&rules.base_asset)
            || balance.asset.eq_ignore_ascii_case(&rules.quote_asset)
    }) {
        println!(
            "  {} free={} locked={}",
            balance.asset, balance.free, balance.locked
        );
    }

    println!("open_orders: {}", open_orders.len());
    for order in open_orders.iter().take(10) {
        println!(
            "  id={:?} side={} status={} qty={} avg_price={}",
            order.order_id,
            order.side,
            order.status.as_str(),
            order.executed_qty,
            order
                .average_price()
                .map(|value| value.to_string())
                .unwrap_or_else(|| "n/a".to_string())
        );
    }

    println!("recent_trades: {}", trades.len());
    for trade in trades.iter().take(10) {
        println!(
            "  id={} order_id={} side={} qty={} price={} commission={} {} time={}",
            trade.trade_id,
            trade.order_id,
            trade.side,
            trade.qty,
            trade.price,
            trade.commission,
            trade.commission_asset.as_deref().unwrap_or("?"),
            ms_to_rfc3339(trade.time_ms),
        );
    }

    Ok(())
}

async fn handle_monitor_watch(
    store: &SqliteStore,
    private_client: &BinanceSpotClient,
    args: MonitorWatchArgs,
) -> Result<()> {
    let mut loops = 0usize;
    loop {
        println!("---");
        handle_monitor_status(
            store,
            private_client,
            MonitorStatusArgs {
                symbol: args.symbol.clone(),
                interval: args.interval.clone(),
                strategy_name: args.strategy_name.clone(),
                recent_trades: args.recent_trades,
            },
        )
        .await?;

        loops += 1;
        if args.max_loops.map(|max| loops >= max).unwrap_or(false) {
            break;
        }

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(args.poll_secs)) => {}
            _ = tokio::signal::ctrl_c() => break,
        }
    }

    Ok(())
}

async fn handle_monitor_orders(
    private_client: &BinanceSpotClient,
    args: MonitorOrdersArgs,
) -> Result<()> {
    let symbol = Symbol::new(args.symbol)?;
    let orders = private_client.open_orders(Some(&symbol)).await?;
    println!("open_orders: {}", orders.len());
    for order in orders {
        print_order(&order);
    }
    Ok(())
}

async fn handle_monitor_trades(
    private_client: &BinanceSpotClient,
    args: MonitorTradesArgs,
) -> Result<()> {
    let symbol = Symbol::new(args.symbol)?;
    let trades = private_client.recent_trades(&symbol, args.limit).await?;
    println!("recent_trades: {}", trades.len());
    for trade in trades {
        println!(
            "trade_id={} order_id={} side={} qty={} price={} quote_qty={} commission={} {} time={}",
            trade.trade_id,
            trade.order_id,
            trade.side,
            trade.qty,
            trade.price,
            trade.quote_qty,
            trade.commission,
            trade.commission_asset.as_deref().unwrap_or("?"),
            ms_to_rfc3339(trade.time_ms),
        );
    }
    Ok(())
}

async fn handle_monitor_cancel_order(
    private_client: &BinanceSpotClient,
    args: MonitorCancelOrderArgs,
) -> Result<()> {
    let symbol = Symbol::new(args.symbol)?;
    if !args.yes {
        println!("No order canceled. Re-run with --yes to execute the cancel.");
        return Ok(());
    }
    let order = private_client
        .cancel_order(&quantforge::CancelOrderRequest {
            symbol,
            order_id: args.order_id,
            client_order_id: args.client_order_id,
        })
        .await?;
    print_order(&order);
    Ok(())
}

async fn handle_monitor_close_position(
    private_client: &BinanceSpotClient,
    args: MonitorClosePositionArgs,
) -> Result<()> {
    let symbol = Symbol::new(args.symbol)?;
    let rules = private_client.fetch_symbol_rules(&symbol).await?;
    let balances = private_client.account_balances().await?;
    let free_base_qty = balances
        .iter()
        .find(|balance| balance.asset.eq_ignore_ascii_case(&rules.base_asset))
        .map(|balance| balance.free)
        .unwrap_or(Decimal::ZERO);
    let qty = round_quantity_for_rules(free_base_qty, &rules);

    println!("base_asset: {}", rules.base_asset);
    println!("sell_qty: {}", qty);
    if !args.yes {
        println!("No order sent. Re-run with --yes to execute the market sell.");
        return Ok(());
    }
    if qty <= Decimal::ZERO {
        bail!("no sellable quantity available for {}", rules.base_asset);
    }

    let order = private_client
        .submit_market_order(&quantforge::MarketOrderRequest {
            symbol,
            side: Side::Sell,
            quantity: Some(qty),
            quote_order_qty: None,
            new_client_order_id: Some(format!("manual-close-{}", now_utc_ms())),
        })
        .await?;
    print_order(&order);
    Ok(())
}

fn round_quantity_for_rules(qty: Decimal, rules: &quantforge::SymbolRules) -> Decimal {
    match rules.effective_market_step_size() {
        Some(step_size) => round_down_to_step(qty, step_size),
        None => qty,
    }
}

fn print_order(order: &quantforge::ExchangeOrder) {
    println!(
        "order: id={:?} client_id={:?} symbol={} side={} status={} executed_qty={} cumulative_quote_qty={} avg_price={}",
        order.order_id,
        order.client_order_id,
        order.symbol,
        order.side,
        order.status.as_str(),
        order.executed_qty,
        order.cumulative_quote_qty,
        order
            .average_price()
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string())
    );
}
