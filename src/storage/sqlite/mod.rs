//! SQLite-backed store: connection setup, schema initialization, and the
//! row-decoding helpers shared by both responsibilities. The work is split
//! across private sibling modules — `candles` implements `CandleStore`,
//! `journal` implements `RunJournalStore`. Only [`SqliteStore`] leaves this
//! namespace; the schema and the row decoders are not API.

mod candles;
mod journal;

use crate::StorageError;
use rusqlite::{Connection, OptionalExtension};
use rust_decimal::Decimal;
use std::{
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};
use tracing::info;

/// Version stamp for the SQLite schema, compared on every `init`.
///
/// Bump this when the DDL in this module or its submodules, or the shape of
/// `state_json`/`raw_json`, changes incompatibly. Until 1.0.0 there is no
/// backward compatibility and no migrations: a version mismatch means delete
/// the database and re-sync.
const SCHEMA_VERSION: &str = "3";

// Indentation inside this literal is load-bearing: SQLite records the exact
// statement text in `sqlite_master`, so reformatting it would change the
// schema of freshly created databases.
const META_DDL: &str = r#"
                CREATE TABLE IF NOT EXISTS meta (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL
                );
"#;

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
        for ddl in [META_DDL, candles::SCHEMA_DDL, journal::SCHEMA_DDL] {
            connection.execute_batch(ddl).map_err(StorageError::other)?;
        }

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
    use crate::CandleStore;
    use tempfile::tempdir;

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
