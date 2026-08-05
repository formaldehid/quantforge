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

/// Version stamp for the SQLite schema, compared on every `init`.
///
/// Bump this when the DDL below or the shape of `state_json`/`raw_json`
/// changes incompatibly. Until 1.0.0 there is no backward compatibility and
/// no migrations: a version mismatch means delete the database and re-sync.
const SCHEMA_VERSION: &str = "3";

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

        let existing_version: Option<String> = connection
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(StorageError::other)?;
        if let Some(found) = existing_version {
            if found != SCHEMA_VERSION {
                return Err(StorageError::SchemaVersionMismatch {
                    found,
                    expected: SCHEMA_VERSION.to_string(),
                });
            }
        }

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

    // `state_json` is the single source of truth for run state; the
    // `status`, `stopped_at_ms`, and `last_error` columns are denormalized
    // copies for ad-hoc SQL/display only and are never read back into a
    // `BotRunState`.
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
    use crate::{
        ExchangeId, ExecutionMode, Interval, OrderStatus, PositionState, RunStatus, Side, Symbol,
        now_utc_ms,
    };
    use tempfile::tempdir;

    fn market() -> MarketId {
        MarketId::new(
            ExchangeId::BinanceSpot,
            Symbol::new("BTCUSDT").expect("symbol"),
            Interval::M1,
        )
    }

    fn market_for(symbol: &str, interval: Interval) -> MarketId {
        MarketId::new(
            ExchangeId::BinanceSpot,
            Symbol::new(symbol).expect("symbol"),
            interval,
        )
    }

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).expect("decimal")
    }

    // The TempDir guard is returned so the database file outlives the test
    // body; :memory: is not an option because every store method opens a
    // fresh connection.
    fn temp_store() -> (tempfile::TempDir, SqliteStore) {
        let tempdir = tempdir().expect("tempdir");
        let store = SqliteStore::new(tempdir.path().join("market.sqlite"));
        CandleStore::init(&store).expect("init");
        (tempdir, store)
    }

    // Deterministic candle: `close` is the distinguishing field, everything
    // else is fixed so full-struct equality pins the whole TEXT round trip.
    fn candle(open_time_ms: TimestampMs, close: &str) -> Candle {
        Candle {
            open_time_ms,
            close_time_ms: open_time_ms + 59_999,
            open: dec("100"),
            high: dec("110"),
            low: dec("90"),
            close: dec(close),
            volume: dec("12.34"),
            trades: Some(42),
        }
    }

    // Deterministic twin of the resume fixture in `src/live.rs`: an open
    // position and a processed-bar cursor, exactly the state a restarted bot
    // depends on.
    fn run_state(run_id: &str) -> BotRunState {
        BotRunState {
            run_id: run_id.to_string(),
            market: market(),
            strategy_name: "sma_cross".to_string(),
            strategy_config: serde_json::json!({"kind":"sma_cross","fast":20,"slow":50}),
            execution_mode: ExecutionMode::DryRun,
            status: RunStatus::Running,
            last_processed_open_time_ms: Some(1_700_000_000_000),
            started_at_ms: 1_700_000_000_000,
            updated_at_ms: 1_700_000_060_000,
            stopped_at_ms: None,
            last_error: None,
            position: PositionState {
                qty: dec("0.0254"),
                entry_price: Some(dec("9900")),
                entry_time_ms: Some(1_700_000_000_000),
                entry_order_id: Some(7),
            },
        }
    }

    // `order_id` doubles as `transact_time_ms` so events stay distinguishable.
    fn order(order_id: i64) -> ExchangeOrder {
        ExchangeOrder {
            symbol: Symbol::new("BTCUSDT").expect("symbol"),
            side: Side::Buy,
            order_type: "MARKET".to_string(),
            status: OrderStatus::Filled,
            order_id: Some(order_id),
            client_order_id: Some(format!("qf-{order_id}")),
            requested_qty: None,
            requested_quote_qty: Some(dec("100")),
            executed_qty: Some(dec("0.01")),
            cumulative_quote_qty: Some(dec("100")),
            avg_price: Some(dec("10000")),
            transact_time_ms: Some(order_id),
            fills: Some(Vec::new()),
            raw: serde_json::json!({}),
        }
    }

    fn closed_trade(entry_time_ms: TimestampMs) -> ClosedTrade {
        ClosedTrade {
            symbol: Symbol::new("BTCUSDT").expect("symbol"),
            entry_time_ms,
            exit_time_ms: entry_time_ms + 60_000,
            entry_price: dec("10000"),
            exit_price: dec("10100"),
            qty: dec("0.01"),
            gross_quote_pnl: dec("1"),
            entry_order_id: Some(7),
            exit_order_id: Some(8),
        }
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

        let saved_state = BotRunState {
            run_id: "run-1".to_string(),
            market: market(),
            strategy_name: "sma_cross".to_string(),
            strategy_config: serde_json::json!({"kind":"sma_cross","fast":20,"slow":50}),
            status: RunStatus::Running,
            last_processed_open_time_ms: Some(1_700_000_000_000),
            execution_mode: crate::ExecutionMode::DryRun,
            started_at_ms: now_utc_ms(),
            updated_at_ms: now_utc_ms(),
            stopped_at_ms: None,
            last_error: None,
            position: PositionState::flat(),
        };

        RunJournalStore::save_run_state(&store, &saved_state).expect("save run");
        let loaded_run = store
            .load_run_state("run-1")
            .expect("load run")
            .expect("run state");
        assert_eq!(loaded_run, saved_state);

        let order = ExchangeOrder {
            symbol: Symbol::new("BTCUSDT").expect("symbol"),
            side: Side::Buy,
            order_type: "MARKET".to_string(),
            status: crate::OrderStatus::Filled,
            order_id: Some(7),
            client_order_id: Some("abc".to_string()),
            requested_qty: None,
            requested_quote_qty: Some(Decimal::from_str("100").expect("decimal")),
            executed_qty: Some(Decimal::from_str("0.01").expect("decimal")),
            cumulative_quote_qty: Some(Decimal::from_str("100").expect("decimal")),
            avg_price: Some(Decimal::from_str("10000").expect("decimal")),
            transact_time_ms: Some(1),
            fills: Some(Vec::new()),
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

    #[test]
    fn upsert_candles_counts_executed_statements_not_new_rows() {
        let (_tempdir, store) = temp_store();
        let market = market();
        let candles = vec![candle(0, "101"), candle(60_000, "102")];

        assert_eq!(store.upsert_candles(&market, &[]).expect("empty upsert"), 0);
        assert_eq!(
            store
                .upsert_candles(&market, &candles)
                .expect("first upsert"),
            2
        );
        // A repeat sync re-executes the upsert per candle: the count reports
        // executed statements, not newly inserted rows.
        assert_eq!(
            store
                .upsert_candles(&market, &candles)
                .expect("repeat upsert"),
            2
        );

        let loaded = store
            .load_candles(&market, CandleQuery::default())
            .expect("load");
        assert_eq!(loaded, candles);
    }

    #[test]
    fn duplicate_candle_upsert_overwrites_all_non_key_fields_in_place() {
        let (_tempdir, store) = temp_store();
        let market = market();
        store
            .upsert_candles(&market, &[candle(0, "105")])
            .expect("first upsert");

        // Same (exchange, symbol, interval, open_time_ms) key, every non-key
        // field changed: the conflict clause must overwrite them all.
        let replacement = Candle {
            close_time_ms: 59_000,
            open: dec("101"),
            high: dec("111"),
            low: dec("91"),
            close: dec("106"),
            volume: dec("43.21"),
            trades: Some(43),
            ..candle(0, "105")
        };
        store
            .upsert_candles(&market, std::slice::from_ref(&replacement))
            .expect("replacement upsert");

        let loaded = store
            .load_candles(&market, CandleQuery::default())
            .expect("load");
        assert_eq!(loaded, vec![replacement]);
    }

    #[test]
    fn load_candles_applies_inclusive_bounds_ascending_order_and_limit() {
        let (_tempdir, store) = temp_store();
        let market = market();
        // Closes fall as open times rise, so a sort by any other column
        // cannot masquerade as time order.
        let ordered: Vec<Candle> = ["105", "104", "103", "102", "101"]
            .iter()
            .enumerate()
            .map(|(index, close)| candle(index as i64 * 60_000, close))
            .collect();
        // Upserted out of order: reads must sort by open_time_ms, not by
        // insertion order.
        let shuffled = vec![
            ordered[3].clone(),
            ordered[0].clone(),
            ordered[4].clone(),
            ordered[2].clone(),
            ordered[1].clone(),
        ];
        store.upsert_candles(&market, &shuffled).expect("upsert");

        let bounded = store
            .load_candles(
                &market,
                CandleQuery {
                    start_time_ms: Some(60_000),
                    end_time_ms: Some(180_000),
                    limit: None,
                },
            )
            .expect("bounded load");
        assert_eq!(bounded, ordered[1..=3].to_vec());

        let unbounded = store
            .load_candles(&market, CandleQuery::default())
            .expect("unbounded load");
        assert_eq!(unbounded, ordered);

        // LIMIT keeps the oldest candles; load_recent_candles keeps the
        // newest (still returned ascending).
        let limited = store
            .load_candles(
                &market,
                CandleQuery {
                    start_time_ms: None,
                    end_time_ms: None,
                    limit: Some(2),
                },
            )
            .expect("limited load");
        assert_eq!(limited, ordered[..2].to_vec());

        let recent = store.load_recent_candles(&market, 2).expect("recent load");
        assert_eq!(recent, ordered[3..].to_vec());
    }

    #[test]
    fn candle_markets_are_isolated_within_a_shared_database() {
        let (_tempdir, store) = temp_store();
        let btc_1m = market();
        let eth_1m = market_for("ETHUSDT", Interval::M1);
        let btc_5m = market_for("BTCUSDT", Interval::M5);

        // The same open time in three markets sharing one database; the
        // testnet-vs-production gotcha is exactly this keyspace.
        let btc_candle = candle(0, "101");
        let eth_candle = candle(0, "102");
        let btc_5m_candle = candle(0, "103");
        store
            .upsert_candles(&btc_1m, std::slice::from_ref(&btc_candle))
            .expect("btc 1m upsert");
        store
            .upsert_candles(&eth_1m, std::slice::from_ref(&eth_candle))
            .expect("eth 1m upsert");
        store
            .upsert_candles(&btc_5m, std::slice::from_ref(&btc_5m_candle))
            .expect("btc 5m upsert");

        assert_eq!(
            store
                .load_candles(&btc_1m, CandleQuery::default())
                .expect("btc 1m load"),
            vec![btc_candle]
        );
        assert_eq!(
            store
                .load_candles(&eth_1m, CandleQuery::default())
                .expect("eth 1m load"),
            vec![eth_candle]
        );
        assert_eq!(
            store
                .load_candles(&btc_5m, CandleQuery::default())
                .expect("btc 5m load"),
            vec![btc_5m_candle]
        );
        assert_eq!(
            store.max_open_time_ms(&btc_1m).expect("btc 1m max"),
            Some(0)
        );
        assert_eq!(
            store
                .max_open_time_ms(&market_for("ETHUSDT", Interval::M5))
                .expect("untouched market max"),
            None
        );
    }

    #[test]
    fn decimal_scale_survives_the_candle_text_round_trip() {
        let (_tempdir, store) = temp_store();
        let market = market();
        // `Decimal` equality ignores scale ("1.0" == "1.00"), so the TEXT
        // storage rail is pinned through the rendered strings instead.
        let stored = Candle {
            open: dec("100.50"),
            high: dec("110.000"),
            low: dec("0.010"),
            close: dec("105.5000"),
            volume: dec("12.340"),
            ..candle(0, "105")
        };
        store
            .upsert_candles(&market, std::slice::from_ref(&stored))
            .expect("upsert");

        let loaded = store
            .load_candles(&market, CandleQuery::default())
            .expect("load")
            .pop()
            .expect("one candle");
        assert_eq!(loaded.open.to_string(), "100.50");
        assert_eq!(loaded.high.to_string(), "110.000");
        assert_eq!(loaded.low.to_string(), "0.010");
        assert_eq!(loaded.close.to_string(), "105.5000");
        assert_eq!(loaded.volume.to_string(), "12.340");
    }

    #[test]
    fn bot_run_state_round_trips_through_state_json_in_full() {
        let (_tempdir, store) = temp_store();
        let state = run_state("run-1");
        store.save_run_state(&state).expect("save");

        let loaded = store
            .load_run_state("run-1")
            .expect("load")
            .expect("run state");
        assert_eq!(loaded, state);

        assert_eq!(store.load_run_state("missing").expect("missing load"), None);
    }

    #[test]
    fn save_run_state_updates_the_existing_row_for_a_run_id() {
        let (_tempdir, store) = temp_store();
        let initial = run_state("run-1");
        store.save_run_state(&initial).expect("save initial");

        // The every-bar persistence path of the live engine: same run_id,
        // advanced cursor, changed status and position. Identity columns and
        // started_at_ms stay frozen at first insert (only the market columns
        // still serve as query keys for latest_run_for_market); state_json is
        // the single source of truth and must carry every update.
        let updated = BotRunState {
            status: RunStatus::Stopped,
            last_processed_open_time_ms: Some(1_700_000_120_000),
            updated_at_ms: 1_700_000_180_000,
            stopped_at_ms: Some(1_700_000_180_000),
            position: PositionState::flat(),
            ..initial.clone()
        };
        store.save_run_state(&updated).expect("save updated");

        let loaded = store
            .load_run_state("run-1")
            .expect("load")
            .expect("run state");
        assert_eq!(loaded, updated);

        let connection = Connection::open(store.path()).expect("open");
        let rows: i64 = connection
            .query_row("SELECT COUNT(*) FROM bot_runs", [], |row| row.get(0))
            .expect("count rows");
        assert_eq!(rows, 1);
    }

    #[test]
    fn latest_run_for_market_returns_the_most_recently_updated_matching_run() {
        let (_tempdir, store) = temp_store();
        let older = BotRunState {
            updated_at_ms: 1_000,
            ..run_state("run-a")
        };
        let newer = BotRunState {
            updated_at_ms: 2_000,
            ..run_state("run-b")
        };
        // Same market, different strategy, newest timestamp: must not win.
        let other_strategy = BotRunState {
            strategy_name: "other_strategy".to_string(),
            updated_at_ms: 3_000,
            ..run_state("run-c")
        };
        // Same symbol and strategy, different interval, newest timestamp:
        // must not win either.
        let other_interval = BotRunState {
            market: market_for("BTCUSDT", Interval::M5),
            updated_at_ms: 4_000,
            ..run_state("run-d")
        };
        store.save_run_state(&older).expect("save older");
        store.save_run_state(&newer).expect("save newer");
        store
            .save_run_state(&other_strategy)
            .expect("save other strategy");
        store
            .save_run_state(&other_interval)
            .expect("save other interval");

        let latest = store
            .latest_run_for_market(&market(), "sma_cross")
            .expect("latest")
            .expect("run state");
        assert_eq!(latest, newer);

        assert_eq!(
            store
                .latest_run_for_market(&market_for("ETHUSDT", Interval::M1), "sma_cross")
                .expect("other market latest"),
            None
        );
    }

    #[test]
    fn order_events_round_trip_from_raw_json_newest_first() {
        let (_tempdir, store) = temp_store();
        for id in 1..=3 {
            store
                .append_order_event("run-1", &order(id))
                .expect("append order");
        }

        let events = store.list_order_events("run-1", 10).expect("list all");
        assert_eq!(events, vec![order(3), order(2), order(1)]);

        // The limit keeps the newest events.
        let limited = store.list_order_events("run-1", 2).expect("list limited");
        assert_eq!(limited, vec![order(3), order(2)]);
    }

    #[test]
    fn appending_the_same_order_event_twice_stores_two_rows() {
        let (_tempdir, store) = temp_store();
        let event = order(7);
        store
            .append_order_event("run-1", &event)
            .expect("first append");
        store
            .append_order_event("run-1", &event)
            .expect("second append");

        // Plain INSERT with no dedup: a crash-replay double-append stays
        // visible in the journal instead of being silently merged.
        let events = store.list_order_events("run-1", 10).expect("list");
        assert_eq!(events, vec![event.clone(), event]);
    }

    #[test]
    fn closed_trades_round_trip_column_by_column_newest_first() {
        let (_tempdir, store) = temp_store();
        let first = closed_trade(1_000);
        // None order ids and a scale-bearing qty must survive the
        // column-by-column reader.
        let second = ClosedTrade {
            entry_order_id: None,
            exit_order_id: None,
            qty: dec("0.010"),
            ..closed_trade(2_000)
        };
        store
            .append_closed_trade("run-1", &first)
            .expect("append first");
        store
            .append_closed_trade("run-1", &second)
            .expect("append second");

        let trades = store.list_closed_trades("run-1", 10).expect("list all");
        assert_eq!(trades, vec![second.clone(), first.clone()]);
        assert_eq!(trades[0].qty.to_string(), "0.010");

        let newest_only = store.list_closed_trades("run-1", 1).expect("list newest");
        assert_eq!(newest_only, vec![second]);
    }

    // Restart/resume assumptions pinned by the next two tests:
    // - data sync resumes from the candle high-water mark (`max_open_time_ms`
    //   + interval step, src/data_sync.rs); holes below the max are never
    //   revisited — backfilling needs an explicit bounded --start/--end run;
    // - a live/dry-run bot resumes by run_id from `state_json` alone
    //   (`load_or_create_run_state` in src/live.rs); the identity checks
    //   (market, strategy, config, execution mode) live in
    //   `apply_resume_checks` and are unit-tested in live.rs without SQLite;
    // - sibling columns (`status`, `stopped_at_ms`, `last_error`) are
    //   display-only copies and are never read back;
    // - re-`init` over an existing database is idempotent and re-stamps the
    //   schema version (the mismatch refusal has its own test below).

    #[test]
    fn a_fresh_store_over_an_existing_database_resumes_candle_sync_from_the_high_water_mark() {
        let tempdir = tempdir().expect("tempdir");
        let db_path = tempdir.path().join("market.sqlite");
        let market = market();

        let first_process = SqliteStore::new(&db_path);
        CandleStore::init(&first_process).expect("first init");
        let candles: Vec<Candle> = (0i64..3).map(|i| candle(i * 60_000, "101")).collect();
        first_process
            .upsert_candles(&market, &candles)
            .expect("upsert");
        // Models a process exit; connections are per-call anyway.
        drop(first_process);

        let second_process = SqliteStore::new(&db_path);
        CandleStore::init(&second_process).expect("second init");
        assert_eq!(
            second_process.max_open_time_ms(&market).expect("max"),
            Some(120_000)
        );
        assert_eq!(
            second_process
                .load_candles(&market, CandleQuery::default())
                .expect("load"),
            candles
        );
    }

    #[test]
    fn a_fresh_store_over_an_existing_database_loads_run_state_for_resume() {
        let tempdir = tempdir().expect("tempdir");
        let db_path = tempdir.path().join("market.sqlite");

        let first_process = SqliteStore::new(&db_path);
        RunJournalStore::init(&first_process).expect("first init");
        let state = run_state("run-1");
        first_process.save_run_state(&state).expect("save");
        drop(first_process);

        let second_process = SqliteStore::new(&db_path);
        RunJournalStore::init(&second_process).expect("second init");
        let loaded = second_process
            .load_run_state("run-1")
            .expect("load")
            .expect("run state");
        // The open position and processed-bar cursor a restarted bot resumes
        // from.
        assert_eq!(loaded, state);
    }

    #[test]
    fn sqlite_columns_store_wire_forms_while_json_payloads_store_serde_tokens() {
        let (_tempdir, store) = temp_store();
        store.save_run_state(&run_state("run-1")).expect("save run");
        store
            .append_order_event("run-1", &order(1))
            .expect("append order");

        let connection = Connection::open(store.path()).expect("open");
        let (exchange, interval, status, state_json): (String, String, String, String) = connection
            .query_row(
                r#"
                    SELECT exchange, interval, status, state_json
                    FROM bot_runs
                    WHERE run_id = 'run-1'
                    "#,
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("bot run row");
        assert_eq!(exchange, "binance_spot");
        assert_eq!(interval, "1m");
        assert_eq!(status, "running");
        assert!(state_json.contains("\"BinanceSpot\""), "got {state_json}");
        assert!(state_json.contains("\"M1\""), "got {state_json}");
        assert!(state_json.contains("\"Running\""), "got {state_json}");
        assert!(state_json.contains("\"DryRun\""), "got {state_json}");

        let (side, raw_json): (String, String) = connection
            .query_row(
                "SELECT side, raw_json FROM order_events WHERE run_id = 'run-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("order event row");
        assert_eq!(side, "BUY");
        assert!(raw_json.contains("\"Buy\""), "got {raw_json}");
    }

    #[test]
    fn init_rejects_databases_with_a_different_schema_version() {
        let tempdir = tempdir().expect("tempdir");
        let db_path = tempdir.path().join("market.sqlite");

        let store = SqliteStore::new(&db_path);
        CandleStore::init(&store).expect("first init");

        let connection = Connection::open(&db_path).expect("open");
        connection
            .execute(
                "UPDATE meta SET value = '1' WHERE key = 'schema_version'",
                [],
            )
            .expect("doctor version");
        drop(connection);

        let error = CandleStore::init(&store).expect_err("version mismatch");
        assert!(matches!(error, StorageError::SchemaVersionMismatch { .. }));
        assert!(
            error.to_string().contains("schema version 1"),
            "got {error}"
        );
        assert!(
            error
                .to_string()
                .contains(&format!("supported version {SCHEMA_VERSION}")),
            "got {error}"
        );
    }
}
