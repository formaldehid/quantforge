use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use rust_decimal::Decimal;
use std::path::PathBuf;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use url::Url;

use quantforge::sdk::strategies::SmaCrossStrategy;
use quantforge::{
    BacktestConfig, BacktestEngine, BinanceSpotClient, CandleQuery, CandleStore, ExchangeId,
    Interval, KlineRequest, MarketDataSource, MarketId, SqliteCandleStore, Strategy, Symbol,
    parse_rfc3339_to_ms, validate_candles,
};

#[derive(Parser, Debug)]
#[command(
    name = "quantforge",
    version,
    about = "Deterministic CLI-first market data ingestion and backtesting framework."
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

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, global = true, env = "QF_LOG", default_value = "info")]
    log_level: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Download Binance Spot OHLCV candles into SQLite.
    Download {
        #[arg(long)]
        symbol: String,
        #[arg(long, default_value = "1m")]
        interval: String,
        #[arg(long)]
        start: String,
        #[arg(long)]
        end: String,
        #[arg(long, default_value = "https://api.binance.com")]
        base_url: String,
        #[arg(long, default_value_t = 1000)]
        limit: u16,
    },

    /// Validate stored candles for duplicates, gaps, ordering, and OHLC sanity.
    Validate {
        #[arg(long)]
        symbol: String,
        #[arg(long, default_value = "1m")]
        interval: String,
        #[arg(long)]
        start: Option<String>,
        #[arg(long)]
        end: Option<String>,
    },

    /// Backtest the built-in SMA crossover example strategy.
    Backtest {
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
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log_level)?;

    if let Some(parent) = cli.db.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create parent directory {}", parent.display()))?;
    }

    let store = SqliteCandleStore::new(&cli.db);
    store.init().context("failed to initialize sqlite store")?;

    match cli.command {
        Command::Download {
            symbol,
            interval,
            start,
            end,
            base_url,
            limit,
        } => {
            let market = parse_market(symbol, interval)?;
            let start_ms = parse_rfc3339_to_ms(&start).context("failed to parse --start")?;
            let end_ms = parse_rfc3339_to_ms(&end).context("failed to parse --end")?;
            if end_ms < start_ms {
                bail!("--end must be greater than or equal to --start");
            }

            let base_url = Url::parse(&base_url).context("failed to parse --base-url")?;
            let client = BinanceSpotClient::new(base_url);
            download_klines(&client, &store, &market, start_ms, end_ms, limit).await?;
        }
        Command::Validate {
            symbol,
            interval,
            start,
            end,
        } => {
            let market = parse_market(symbol, interval)?;
            let candles = store.load_candles(
                &market,
                CandleQuery {
                    start_time_ms: start
                        .as_deref()
                        .map(parse_rfc3339_to_ms)
                        .transpose()
                        .context("failed to parse --start")?,
                    end_time_ms: end
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

            if report.is_ok() {
                info!("validation passed without issues");
            } else {
                warn!(issues = report.issues.len(), "validation reported issues");
            }
        }
        Command::Backtest {
            symbol,
            interval,
            start,
            end,
            fast,
            slow,
            cash,
            fee_bps,
        } => {
            let market = parse_market(symbol, interval)?;
            let candles = store.load_candles(
                &market,
                CandleQuery {
                    start_time_ms: start
                        .as_deref()
                        .map(parse_rfc3339_to_ms)
                        .transpose()
                        .context("failed to parse --start")?,
                    end_time_ms: end
                        .as_deref()
                        .map(parse_rfc3339_to_ms)
                        .transpose()
                        .context("failed to parse --end")?,
                    limit: None,
                },
            )?;

            let mut strategy = SmaCrossStrategy::new(fast, slow)?;
            let engine = BacktestEngine::new(BacktestConfig {
                initial_cash: cash.parse::<Decimal>().context("failed to parse --cash")?,
                fee_bps: fee_bps
                    .parse::<Decimal>()
                    .context("failed to parse --fee-bps")?,
                close_out_at_end: true,
            });
            let result = engine.run(&market, &candles, &mut strategy)?;

            println!("strategy: {}", strategy.name());
            println!("final_equity: {}", result.final_equity);
            println!("total_return_pct: {}", result.total_return_pct);
            println!("max_drawdown_pct: {}", result.max_drawdown_pct);
            println!("trade_count: {}", result.trade_count);
            for trade in result.trades.iter().rev().take(5).rev() {
                println!(
                    "trade: entry={} @ {} exit={} @ {} qty={} pnl={}",
                    trade.entry_time_ms,
                    trade.entry_price,
                    trade.exit_time_ms,
                    trade.exit_price,
                    trade.qty,
                    trade.pnl
                );
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

async fn download_klines(
    source: &dyn MarketDataSource,
    store: &dyn CandleStore,
    market: &MarketId,
    start_ms: i64,
    end_ms: i64,
    limit: u16,
) -> Result<()> {
    let step_ms = market.interval.step_ms();
    let mut cursor = start_ms;
    let mut total = 0usize;

    while cursor <= end_ms {
        let batch = source
            .fetch_klines(&KlineRequest {
                symbol: market.symbol.clone(),
                interval: market.interval,
                start_time_ms: Some(cursor),
                end_time_ms: Some(end_ms),
                limit: Some(limit),
            })
            .await
            .context("failed to fetch klines")?;

        if batch.is_empty() {
            info!(cursor, "no more candles returned; stopping");
            break;
        }

        total += store
            .upsert_candles(market, &batch)
            .context("failed to upsert candles")?;

        let last_open_time_ms = batch
            .last()
            .map(|candle| candle.open_time_ms)
            .context("empty batch after non-empty check")?;
        let next_cursor = last_open_time_ms + step_ms;
        if next_cursor <= cursor {
            warn!(
                cursor,
                next_cursor, "cursor did not advance; stopping to avoid infinite loop"
            );
            break;
        }

        cursor = next_cursor;
        info!(written = batch.len(), total, cursor, "download progress");
    }

    info!(total, "download completed");
    Ok(())
}
