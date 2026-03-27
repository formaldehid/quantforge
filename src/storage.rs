use crate::{
    BotRunState, Candle, CandleQuery, CandleStore, ClosedTrade, ExchangeOrder, MarketId,
    RunJournalStore, StorageError, TimestampMs,
};
use rusqlite::{Connection, OptionalExtension, ToSql, params, params_from_iter};
use rust_decimal::Decimal;
use std::{
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};
use tracing::info;

const SCHEMA_VERSION: &str = "2";

#[derive(Clone, Debug)]
pub struct SqliteStore {
    path: PathBuf,
}

impl SqliteStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn open(&self) -> Result<Connection, StorageError> {
        let connection = Connection::open(&self.path).map_err(StorageError::other)?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(StorageError::other)?;
        connection
            .pragma_update(None, "synchronous", "NORMAL")
            .map_err(StorageError::other)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(StorageError::other)?;
        Ok(connection)
    }

    fn initialize_schema(&self) -> Result<(), StorageError> {
        let connection = self.open()?;
        connection
            .execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS meta (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS candles (
                  exchange TEXT NOT NULL,
                  symbol TEXT NOT NULL,
                  interval TEXT NOT NULL,
                  open_time_ms INTEGER NOT NULL,
                  close_time_ms INTEGER NOT NULL,
                  open TEXT NOT NULL,
                  high TEXT NOT NULL,
                  low TEXT NOT NULL,
                  close TEXT NOT NULL,
                  volume TEXT NOT NULL,
                  trades INTEGER,
                  PRIMARY KEY (exchange, symbol, interval, open_time_ms)
                );

                CREATE INDEX IF NOT EXISTS idx_candles_market_time
                  ON candles(exchange, symbol, interval, open_time_ms);

                CREATE TABLE IF NOT EXISTS bot_runs (
                  run_id TEXT PRIMARY KEY,
                  exchange TEXT NOT NULL,
                  symbol TEXT NOT NULL,
                  interval TEXT NOT NULL,
                  strategy_name TEXT NOT NULL,
                  status TEXT NOT NULL,
                  state_json TEXT NOT NULL,
                  started_at_ms INTEGER NOT NULL,
                  updated_at_ms INTEGER NOT NULL,
                  stopped_at_ms INTEGER,
                  last_error TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_bot_runs_market
                  ON bot_runs(exchange, symbol, interval, strategy_name, updated_at_ms DESC);

                CREATE TABLE IF NOT EXISTS order_events (
                  seq INTEGER PRIMARY KEY AUTOINCREMENT,
                  run_id TEXT NOT NULL,
                  symbol TEXT NOT NULL,
                  side TEXT NOT NULL,
                  order_type TEXT NOT NULL,
                  status TEXT NOT NULL,
                  order_id INTEGER,
                  client_order_id TEXT,
                  transact_time_ms INTEGER,
                  raw_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_order_events_run
                  ON order_events(run_id, seq DESC);

                CREATE TABLE IF NOT EXISTS closed_trades (
                  seq INTEGER PRIMARY KEY AUTOINCREMENT,
                  run_id TEXT NOT NULL,
                  symbol TEXT NOT NULL,
                  entry_time_ms INTEGER NOT NULL,
                  exit_time_ms INTEGER NOT NULL,
                  entry_price TEXT NOT NULL,
                  exit_price TEXT NOT NULL,
                  qty TEXT NOT NULL,
                  gross_quote_pnl TEXT NOT NULL,
                  entry_order_id INTEGER,
                  exit_order_id INTEGER
                );

                CREATE INDEX IF NOT EXISTS idx_closed_trades_run
                  ON closed_trades(run_id, seq DESC);
                "#,
            )
            .map_err(StorageError::other)?;

        connection
            .execute(
                r#"
                INSERT INTO meta(key, value)
                VALUES ('schema_version', ?1)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                "#,
                [SCHEMA_VERSION],
            )
            .map_err(StorageError::other)?;

        info!(db_path = %self.path.display(), "sqlite store initialized");
        Ok(())
    }
}

impl CandleStore for SqliteStore {
    fn init(&self) -> Result<(), StorageError> {
        self.initialize_schema()
    }

    fn upsert_candles(&self, market: &MarketId, candles: &[Candle]) -> Result<usize, StorageError> {
        if candles.is_empty() {
            return Ok(0);
        }

        let mut connection = self.open()?;
        let transaction = connection.transaction().map_err(StorageError::other)?;
        let mut written = 0usize;

        {
            let mut stmt = transaction
                .prepare_cached(
                    r#"
                    INSERT INTO candles(
                      exchange, symbol, interval, open_time_ms, close_time_ms,
                      open, high, low, close, volume, trades
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                    ON CONFLICT(exchange, symbol, interval, open_time_ms) DO UPDATE SET
                      close_time_ms = excluded.close_time_ms,
                      open = excluded.open,
                      high = excluded.high,
                      low = excluded.low,
                      close = excluded.close,
                      volume = excluded.volume,
                      trades = excluded.trades
                    "#,
                )
                .map_err(StorageError::other)?;

            for candle in candles {
                stmt.execute(params![
                    market.exchange.as_str(),
                    market.symbol.as_str(),
                    market.interval.as_str(),
                    candle.open_time_ms,
                    candle.close_time_ms,
                    candle.open.to_string(),
                    candle.high.to_string(),
                    candle.low.to_string(),
                    candle.close.to_string(),
                    candle.volume.to_string(),
                    candle.trades.map(|value| value as i64),
                ])
                .map_err(StorageError::other)?;
                written += 1;
            }
        }

        transaction.commit().map_err(StorageError::other)?;
        Ok(written)
    }

    fn load_candles(
        &self,
        market: &MarketId,
        query: CandleQuery,
    ) -> Result<Vec<Candle>, StorageError> {
        let connection = self.open()?;

        let mut sql = String::from(
            r#"
            SELECT open_time_ms, close_time_ms, open, high, low, close, volume, trades
            FROM candles
            WHERE exchange = ?1 AND symbol = ?2 AND interval = ?3
            "#,
        );

        let exchange = market.exchange.as_str().to_string();
        let symbol = market.symbol.as_str().to_string();
        let interval = market.interval.as_str().to_string();
        let start_time_ms = query.start_time_ms;
        let end_time_ms = query.end_time_ms;

        let mut sql_params: Vec<&dyn ToSql> = vec![&exchange, &symbol, &interval];

        if let Some(ref start_value) = start_time_ms {
            sql.push_str(&format!(" AND open_time_ms >= ?{}", sql_params.len() + 1));
            sql_params.push(start_value);
        }
        if let Some(ref end_value) = end_time_ms {
            sql.push_str(&format!(" AND open_time_ms <= ?{}", sql_params.len() + 1));
            sql_params.push(end_value);
        }

        sql.push_str(" ORDER BY open_time_ms ASC");
        if let Some(limit) = query.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }

        let mut stmt = connection.prepare(&sql).map_err(StorageError::other)?;
        let rows = stmt
            .query_map(params_from_iter(sql_params), |row| {
                parse_candle_row(
                    row.get(0)?,
                    row.get(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                )
            })
            .map_err(StorageError::other)?;

        let mut candles = Vec::new();
        for row in rows {
            candles.push(row.map_err(StorageError::other)?);
        }
        Ok(candles)
    }

    fn load_recent_candles(
        &self,
        market: &MarketId,
        limit: usize,
    ) -> Result<Vec<Candle>, StorageError> {
        let connection = self.open()?;
        let mut stmt = connection
            .prepare(
                r#"
                SELECT open_time_ms, close_time_ms, open, high, low, close, volume, trades
                FROM candles
                WHERE exchange = ?1 AND symbol = ?2 AND interval = ?3
                ORDER BY open_time_ms DESC
                LIMIT ?4
                "#,
            )
            .map_err(StorageError::other)?;

        let rows = stmt
            .query_map(
                params![
                    market.exchange.as_str(),
                    market.symbol.as_str(),
                    market.interval.as_str(),
                    limit as i64
                ],
                |row| {
                    parse_candle_row(
                        row.get(0)?,
                        row.get(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, Option<i64>>(7)?,
                    )
                },
            )
            .map_err(StorageError::other)?;

        let mut candles = Vec::new();
        for row in rows {
            candles.push(row.map_err(StorageError::other)?);
        }
        candles.reverse();
        Ok(candles)
    }

    fn max_open_time_ms(&self, market: &MarketId) -> Result<Option<TimestampMs>, StorageError> {
        let connection = self.open()?;
        let value = connection
            .query_row(
                r#"
                SELECT MAX(open_time_ms)
                FROM candles
                WHERE exchange = ?1 AND symbol = ?2 AND interval = ?3
                "#,
                params![
                    market.exchange.as_str(),
                    market.symbol.as_str(),
                    market.interval.as_str()
                ],
                |row| row.get::<_, Option<i64>>(0),
            )
            .map_err(StorageError::other)?;
        Ok(value)
    }
}

impl RunJournalStore for SqliteStore {
    fn init(&self) -> Result<(), StorageError> {
        self.initialize_schema()
    }

    fn save_run_state(&self, state: &BotRunState) -> Result<(), StorageError> {
        let connection = self.open()?;
        let state_json = serde_json::to_string(state).map_err(StorageError::other)?;
        connection
            .execute(
                r#"
                INSERT INTO bot_runs(
                  run_id, exchange, symbol, interval, strategy_name, status,
                  state_json, started_at_ms, updated_at_ms, stopped_at_ms, last_error
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                ON CONFLICT(run_id) DO UPDATE SET
                  status = excluded.status,
                  state_json = excluded.state_json,
                  updated_at_ms = excluded.updated_at_ms,
                  stopped_at_ms = excluded.stopped_at_ms,
                  last_error = excluded.last_error
                "#,
                params![
                    &state.run_id,
                    state.market.exchange.as_str(),
                    state.market.symbol.as_str(),
                    state.market.interval.as_str(),
                    &state.strategy_name,
                    state.status.as_str(),
                    state_json,
                    state.started_at_ms,
                    state.updated_at_ms,
                    state.stopped_at_ms,
                    state.last_error.as_deref(),
                ],
            )
            .map_err(StorageError::other)?;
        Ok(())
    }

    fn load_run_state(&self, run_id: &str) -> Result<Option<BotRunState>, StorageError> {
        let connection = self.open()?;
        let state_json = connection
            .query_row(
                "SELECT state_json FROM bot_runs WHERE run_id = ?1",
                [run_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(StorageError::other)?;

        match state_json {
            Some(json) => {
                let state =
                    serde_json::from_str::<BotRunState>(&json).map_err(StorageError::other)?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    fn latest_run_for_market(
        &self,
        market: &MarketId,
        strategy_name: &str,
    ) -> Result<Option<BotRunState>, StorageError> {
        let connection = self.open()?;
        let state_json = connection
            .query_row(
                r#"
                SELECT state_json
                FROM bot_runs
                WHERE exchange = ?1
                  AND symbol = ?2
                  AND interval = ?3
                  AND strategy_name = ?4
                ORDER BY updated_at_ms DESC
                LIMIT 1
                "#,
                params![
                    market.exchange.as_str(),
                    market.symbol.as_str(),
                    market.interval.as_str(),
                    strategy_name
                ],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(StorageError::other)?;

        match state_json {
            Some(json) => {
                let state =
                    serde_json::from_str::<BotRunState>(&json).map_err(StorageError::other)?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    fn append_order_event(&self, run_id: &str, order: &ExchangeOrder) -> Result<(), StorageError> {
        let connection = self.open()?;
        let raw_json = serde_json::to_string(order).map_err(StorageError::other)?;
        connection
            .execute(
                r#"
                INSERT INTO order_events(
                  run_id, symbol, side, order_type, status, order_id, client_order_id,
                  transact_time_ms, raw_json
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                "#,
                params![
                    run_id,
                    order.symbol.as_str(),
                    order.side.as_str(),
                    &order.order_type,
                    order.status.as_str(),
                    order.order_id,
                    order.client_order_id.as_deref(),
                    order.transact_time_ms,
                    raw_json
                ],
            )
            .map_err(StorageError::other)?;
        Ok(())
    }

    fn append_closed_trade(&self, run_id: &str, trade: &ClosedTrade) -> Result<(), StorageError> {
        let connection = self.open()?;
        connection
            .execute(
                r#"
                INSERT INTO closed_trades(
                  run_id, symbol, entry_time_ms, exit_time_ms, entry_price, exit_price,
                  qty, gross_quote_pnl, entry_order_id, exit_order_id
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                "#,
                params![
                    run_id,
                    trade.symbol.as_str(),
                    trade.entry_time_ms,
                    trade.exit_time_ms,
                    trade.entry_price.to_string(),
                    trade.exit_price.to_string(),
                    trade.qty.to_string(),
                    trade.gross_quote_pnl.to_string(),
                    trade.entry_order_id,
                    trade.exit_order_id
                ],
            )
            .map_err(StorageError::other)?;
        Ok(())
    }

    fn list_order_events(
        &self,
        run_id: &str,
        limit: usize,
    ) -> Result<Vec<ExchangeOrder>, StorageError> {
        let connection = self.open()?;
        let mut stmt = connection
            .prepare(
                r#"
                SELECT raw_json
                FROM order_events
                WHERE run_id = ?1
                ORDER BY seq DESC
                LIMIT ?2
                "#,
            )
            .map_err(StorageError::other)?;

        let rows = stmt
            .query_map(params![run_id, limit as i64], |row| row.get::<_, String>(0))
            .map_err(StorageError::other)?;

        let mut items = Vec::new();
        for row in rows {
            let raw_json = row.map_err(StorageError::other)?;
            let order =
                serde_json::from_str::<ExchangeOrder>(&raw_json).map_err(StorageError::other)?;
            items.push(order);
        }
        Ok(items)
    }

    fn list_closed_trades(
        &self,
        run_id: &str,
        limit: usize,
    ) -> Result<Vec<ClosedTrade>, StorageError> {
        let connection = self.open()?;
        let mut stmt = connection
            .prepare(
                r#"
                SELECT symbol, entry_time_ms, exit_time_ms, entry_price, exit_price,
                       qty, gross_quote_pnl, entry_order_id, exit_order_id
                FROM closed_trades
                WHERE run_id = ?1
                ORDER BY seq DESC
                LIMIT ?2
                "#,
            )
            .map_err(StorageError::other)?;

        let rows = stmt
            .query_map(params![run_id, limit as i64], |row| {
                Ok(ClosedTrade {
                    symbol: row
                        .get::<_, String>(0)?
                        .parse()
                        .map_err(to_from_sql_error)?,
                    entry_time_ms: row.get(1)?,
                    exit_time_ms: row.get(2)?,
                    entry_price: parse_decimal_str(&row.get::<_, String>(3)?)?,
                    exit_price: parse_decimal_str(&row.get::<_, String>(4)?)?,
                    qty: parse_decimal_str(&row.get::<_, String>(5)?)?,
                    gross_quote_pnl: parse_decimal_str(&row.get::<_, String>(6)?)?,
                    entry_order_id: row.get(7)?,
                    exit_order_id: row.get(8)?,
                })
            })
            .map_err(StorageError::other)?;

        let mut trades = Vec::new();
        for row in rows {
            trades.push(row.map_err(StorageError::other)?);
        }
        Ok(trades)
    }
}

#[allow(clippy::too_many_arguments)]
fn parse_candle_row(
    open_time_ms: i64,
    close_time_ms: i64,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
    trades: Option<i64>,
) -> Result<Candle, rusqlite::Error> {
    Ok(Candle {
        open_time_ms,
        close_time_ms,
        open: parse_decimal_str(&open)?,
        high: parse_decimal_str(&high)?,
        low: parse_decimal_str(&low)?,
        close: parse_decimal_str(&close)?,
        volume: parse_decimal_str(&volume)?,
        trades: trades.map(|value| value as u64),
    })
}

fn parse_decimal_str(raw: &str) -> Result<Decimal, rusqlite::Error> {
    Decimal::from_str(raw).map_err(to_from_sql_error)
}

fn to_from_sql_error<E>(err: E) -> rusqlite::Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExchangeId, Interval, PositionState, RunStatus, Side, Symbol, now_utc_ms};
    use tempfile::tempdir;

    fn market() -> MarketId {
        MarketId::new(
            ExchangeId::BinanceSpot,
            Symbol::new("BTCUSDT").expect("symbol"),
            Interval::M1,
        )
    }

    #[test]
    fn sqlite_roundtrip_for_candles_and_runs() {
        let tempdir = tempdir().expect("tempdir");
        let db_path = tempdir.path().join("market.sqlite");
        let store = SqliteStore::new(&db_path);
        CandleStore::init(&store).expect("init");

        let first = Candle {
            open_time_ms: 1_700_000_000_000,
            close_time_ms: 1_700_000_059_999,
            open: Decimal::from_str("100").expect("decimal"),
            high: Decimal::from_str("110").expect("decimal"),
            low: Decimal::from_str("90").expect("decimal"),
            close: Decimal::from_str("105").expect("decimal"),
            volume: Decimal::from_str("12.34").expect("decimal"),
            trades: Some(42),
        };

        store
            .upsert_candles(&market(), std::slice::from_ref(&first))
            .expect("upsert");
        let loaded = store
            .load_recent_candles(&market(), 1)
            .expect("load recent");
        assert_eq!(loaded, vec![first]);

        let run_state = BotRunState {
            run_id: "run-1".to_string(),
            market: market(),
            strategy_name: "sma_cross".to_string(),
            strategy_config: serde_json::json!({"kind":"sma_cross","fast":20,"slow":50}),
            status: RunStatus::Running,
            last_processed_open_time_ms: Some(1_700_000_000_000),
            started_at_ms: now_utc_ms(),
            updated_at_ms: now_utc_ms(),
            stopped_at_ms: None,
            last_error: None,
            position: PositionState::flat(),
        };

        RunJournalStore::save_run_state(&store, &run_state).expect("save run");
        let loaded_run = store
            .load_run_state("run-1")
            .expect("load run")
            .expect("run state");
        assert_eq!(loaded_run.run_id, "run-1");

        let order = ExchangeOrder {
            symbol: Symbol::new("BTCUSDT").expect("symbol"),
            side: Side::Buy,
            order_type: "MARKET".to_string(),
            status: crate::OrderStatus::Filled,
            order_id: Some(7),
            client_order_id: Some("abc".to_string()),
            requested_qty: None,
            requested_quote_qty: Some(Decimal::from_str("100").expect("decimal")),
            executed_qty: Decimal::from_str("0.01").expect("decimal"),
            cumulative_quote_qty: Decimal::from_str("100").expect("decimal"),
            avg_price: Some(Decimal::from_str("10000").expect("decimal")),
            transact_time_ms: Some(1),
            fills: Vec::new(),
            raw: serde_json::json!({}),
        };
        store
            .append_order_event("run-1", &order)
            .expect("append order");
        assert_eq!(
            store
                .list_order_events("run-1", 10)
                .expect("list order")
                .len(),
            1
        );

        let trade = ClosedTrade {
            symbol: Symbol::new("BTCUSDT").expect("symbol"),
            entry_time_ms: 1,
            exit_time_ms: 2,
            entry_price: Decimal::from_str("10000").expect("decimal"),
            exit_price: Decimal::from_str("10100").expect("decimal"),
            qty: Decimal::from_str("0.01").expect("decimal"),
            gross_quote_pnl: Decimal::from_str("1").expect("decimal"),
            entry_order_id: Some(7),
            exit_order_id: Some(8),
        };
        store
            .append_closed_trade("run-1", &trade)
            .expect("append trade");
        assert_eq!(
            store
                .list_closed_trades("run-1", 10)
                .expect("list trade")
                .len(),
            1
        );
    }
}
