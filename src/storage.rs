use rusqlite::{Connection, OptionalExtension, ToSql, params, params_from_iter};
use rust_decimal::Decimal;
use std::{
    error::Error as StdError,
    path::{Path, PathBuf},
};
use thiserror::Error;
use tracing::info;

use crate::model::{Candle, MarketId, TimestampMs};

const SCHEMA_VERSION: &str = "1";

#[derive(Clone, Debug, Default)]
pub struct CandleQuery {
    pub start_time_ms: Option<TimestampMs>,
    pub end_time_ms: Option<TimestampMs>,
    pub limit: Option<usize>,
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("storage error")]
    Other(#[source] Box<dyn StdError + Send + Sync>),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}

impl StorageError {
    pub fn other<E>(err: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Other(Box::new(err))
    }
}

pub trait CandleStore: Send + Sync {
    fn init(&self) -> Result<(), StorageError>;
    fn upsert_candles(&self, market: &MarketId, candles: &[Candle]) -> Result<usize, StorageError>;
    fn load_candles(
        &self,
        market: &MarketId,
        query: CandleQuery,
    ) -> Result<Vec<Candle>, StorageError>;
    fn max_open_time_ms(&self, market: &MarketId) -> Result<Option<TimestampMs>, StorageError>;
}

#[derive(Clone, Debug)]
pub struct SqliteCandleStore {
    path: PathBuf,
}

impl SqliteCandleStore {
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
        Ok(connection)
    }
}

impl CandleStore for SqliteCandleStore {
    fn init(&self) -> Result<(), StorageError> {
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
                "#,
            )
            .map_err(StorageError::other)?;

        let version = connection
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(StorageError::other)?;

        if version.is_none() {
            connection
                .execute(
                    "INSERT INTO meta(key, value) VALUES ('schema_version', ?1)",
                    [SCHEMA_VERSION],
                )
                .map_err(StorageError::other)?;
        }

        info!(db_path = %self.path.display(), "sqlite candle store initialized");
        Ok(())
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

        let mut params: Vec<&dyn ToSql> = vec![&exchange, &symbol, &interval];

        if let Some(ref start_time_ms_value) = start_time_ms {
            sql.push_str(&format!(" AND open_time_ms >= ?{}", params.len() + 1));
            params.push(start_time_ms_value);
        }

        if let Some(ref end_time_ms_value) = end_time_ms {
            sql.push_str(&format!(" AND open_time_ms <= ?{}", params.len() + 1));
            params.push(end_time_ms_value);
        }

        sql.push_str(" ORDER BY open_time_ms ASC");
        if let Some(limit) = query.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }

        let mut stmt = connection.prepare(&sql).map_err(StorageError::other)?;
        let rows = stmt
            .query_map(params_from_iter(params), |row| {
                let open = row.get::<_, String>(2)?;
                let high = row.get::<_, String>(3)?;
                let low = row.get::<_, String>(4)?;
                let close = row.get::<_, String>(5)?;
                let volume = row.get::<_, String>(6)?;

                parse_row(
                    row.get(0)?,
                    row.get(1)?,
                    &open,
                    &high,
                    &low,
                    &close,
                    &volume,
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

fn parse_decimal(input: &str, field: &str) -> Result<Decimal, rusqlite::Error> {
    input.parse::<Decimal>().map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("failed to parse {field}: {e}"),
            )),
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn parse_row(
    open_time_ms: i64,
    close_time_ms: i64,
    open: &str,
    high: &str,
    low: &str,
    close: &str,
    volume: &str,
    trades: Option<i64>,
) -> Result<Candle, rusqlite::Error> {
    Ok(Candle {
        open_time_ms,
        close_time_ms,
        open: parse_decimal(open, "open")?,
        high: parse_decimal(high, "high")?,
        low: parse_decimal(low, "low")?,
        close: parse_decimal(close, "close")?,
        volume: parse_decimal(volume, "volume")?,
        trades: trades.map(|value| value as u64),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ExchangeId, Interval, Symbol};
    use std::str::FromStr;
    use tempfile::tempdir;

    #[test]
    fn sqlite_roundtrip() {
        let tempdir = tempdir().expect("tempdir");
        let db_path = tempdir.path().join("market.sqlite");
        let store = SqliteCandleStore::new(&db_path);
        store.init().expect("init");

        let market = MarketId::new(
            ExchangeId::BinanceSpot,
            Symbol::new("BTCUSDT").expect("symbol"),
            Interval::M1,
        );

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
        let second = Candle {
            open_time_ms: 1_700_000_060_000,
            close_time_ms: 1_700_000_119_999,
            open: Decimal::from_str("105").expect("decimal"),
            high: Decimal::from_str("120").expect("decimal"),
            low: Decimal::from_str("100").expect("decimal"),
            close: Decimal::from_str("115").expect("decimal"),
            volume: Decimal::from_str("56.78").expect("decimal"),
            trades: Some(43),
        };

        let count = store
            .upsert_candles(&market, &[first.clone(), second.clone()])
            .expect("upsert");
        assert_eq!(count, 2);

        let loaded = store
            .load_candles(
                &market,
                CandleQuery {
                    start_time_ms: Some(first.open_time_ms),
                    end_time_ms: Some(second.open_time_ms),
                    limit: None,
                },
            )
            .expect("load");

        assert_eq!(loaded, vec![first, second]);
        assert_eq!(
            store.max_open_time_ms(&market).expect("max"),
            Some(1_700_000_060_000)
        );
    }
}
