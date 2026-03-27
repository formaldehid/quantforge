use crate::EngineError;
use crate::{CandleStore, KlineRequest, MarketDataSource, MarketId, now_utc_ms};
use std::time::Duration;
use tracing::{info, warn};

#[derive(Clone, Debug)]
pub struct DataSyncConfig {
    pub market: MarketId,
    pub start_time_ms: Option<i64>,
    pub end_time_ms: Option<i64>,
    pub batch_limit: u16,
    pub follow: bool,
    pub poll_interval: Duration,
    pub max_loops: Option<usize>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DataSyncSummary {
    pub iterations: usize,
    pub written: usize,
    pub last_open_time_ms: Option<i64>,
}

pub struct DataSyncEngine<'a> {
    source: &'a dyn MarketDataSource,
    store: &'a dyn CandleStore,
}

impl<'a> std::fmt::Debug for DataSyncEngine<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataSyncEngine").finish_non_exhaustive()
    }
}

impl<'a> DataSyncEngine<'a> {
    pub fn new(source: &'a dyn MarketDataSource, store: &'a dyn CandleStore) -> Self {
        Self { source, store }
    }

    pub async fn run(&self, cfg: &DataSyncConfig) -> Result<DataSyncSummary, EngineError> {
        let mut summary = DataSyncSummary::default();
        let mut loops = 0usize;
        let step_ms = cfg.market.interval.step_ms();
        let mut next_start_time_ms = initial_start_time_ms(cfg, now_utc_ms());

        loop {
            let now_ms = now_utc_ms();
            let end_time_ms = effective_end_time_ms(cfg, now_ms);

            if next_start_time_ms <= end_time_ms {
                let written = sync_market_range(
                    self.source,
                    self.store,
                    &cfg.market,
                    next_start_time_ms,
                    end_time_ms,
                    cfg.batch_limit,
                )
                .await?;
                summary.written += written;
                summary.last_open_time_ms = self.store.max_open_time_ms(&cfg.market)?;

                if let Some(max_open_time_ms) = summary.last_open_time_ms {
                    next_start_time_ms = next_start_time_ms.max(max_open_time_ms + step_ms);
                }

                info!(
                    written,
                    total_written = summary.written,
                    next_start_time_ms,
                    last_open_time_ms = ?summary.last_open_time_ms,
                    "data sync iteration finished"
                );
            } else {
                info!(
                    next_start_time_ms,
                    end_time_ms, "data sync is waiting for the requested time window"
                );
            }

            loops += 1;
            summary.iterations = loops;

            if !should_continue(cfg, next_start_time_ms, loops) {
                break;
            }
            if sleep_or_shutdown(cfg.poll_interval).await {
                break;
            }
        }

        Ok(summary)
    }
}

fn initial_start_time_ms(cfg: &DataSyncConfig, now_ms: i64) -> i64 {
    cfg.start_time_ms.unwrap_or(now_ms)
}

fn effective_end_time_ms(cfg: &DataSyncConfig, now_ms: i64) -> i64 {
    cfg.end_time_ms
        .map(|end_time_ms| end_time_ms.min(now_ms))
        .unwrap_or(now_ms)
}

fn should_continue(cfg: &DataSyncConfig, next_start_time_ms: i64, loops: usize) -> bool {
    if cfg.max_loops.map(|max| loops >= max).unwrap_or(false) {
        return false;
    }

    match cfg.end_time_ms {
        None => true,
        Some(end_time_ms) => cfg.follow && next_start_time_ms <= end_time_ms,
    }
}

pub(crate) async fn sync_market_range(
    source: &dyn MarketDataSource,
    store: &dyn CandleStore,
    market: &MarketId,
    start_ms: i64,
    end_ms: i64,
    batch_limit: u16,
) -> Result<usize, EngineError> {
    if end_ms < start_ms {
        return Ok(0);
    }

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
                limit: Some(batch_limit.min(1000)),
            })
            .await?;

        if batch.is_empty() {
            break;
        }

        total += store.upsert_candles(market, &batch)?;

        let last_open_time_ms = batch
            .last()
            .map(|candle| candle.open_time_ms)
            .ok_or_else(|| EngineError::InvalidState("expected non-empty batch".to_string()))?;
        let next_cursor = last_open_time_ms + step_ms;
        if next_cursor <= cursor {
            warn!(cursor, next_cursor, "data sync cursor did not advance");
            break;
        }
        cursor = next_cursor;
    }

    Ok(total)
}

pub(crate) async fn sleep_or_shutdown(duration: Duration) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(duration) => false,
        _ = tokio::signal::ctrl_c() => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExchangeId, Interval, Symbol};

    fn market() -> MarketId {
        MarketId::new(
            ExchangeId::BinanceSpot,
            Symbol::new("BTCUSDT").expect("symbol"),
            Interval::M1,
        )
    }

    fn config(
        start_time_ms: Option<i64>,
        end_time_ms: Option<i64>,
        follow: bool,
    ) -> DataSyncConfig {
        DataSyncConfig {
            market: market(),
            start_time_ms,
            end_time_ms,
            batch_limit: 1000,
            follow,
            poll_interval: Duration::from_secs(1),
            max_loops: None,
        }
    }

    #[test]
    fn omitted_start_defaults_to_now() {
        let cfg = config(None, Some(5_000), false);
        assert_eq!(initial_start_time_ms(&cfg, 12_345), 12_345);
    }

    #[test]
    fn omitted_end_uses_current_time_for_each_iteration() {
        let cfg = config(Some(1_000), None, false);
        assert_eq!(effective_end_time_ms(&cfg, 9_999), 9_999);
    }

    #[test]
    fn explicit_end_is_capped_at_current_time() {
        let cfg = config(Some(1_000), Some(20_000), false);
        assert_eq!(effective_end_time_ms(&cfg, 9_999), 9_999);
    }

    #[test]
    fn omitted_end_keeps_sync_running_without_follow_flag() {
        let cfg = config(Some(1_000), None, false);
        assert!(should_continue(&cfg, 2_000, 1));
    }

    #[test]
    fn bounded_range_without_follow_stops_after_first_iteration() {
        let cfg = config(Some(1_000), Some(5_000), false);
        assert!(!should_continue(&cfg, 2_000, 1));
    }

    #[test]
    fn bounded_range_with_follow_continues_until_end_is_reached() {
        let cfg = config(Some(1_000), Some(5_000), true);
        assert!(should_continue(&cfg, 5_000, 1));
        assert!(!should_continue(&cfg, 5_001, 1));
    }
}
