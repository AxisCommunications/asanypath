// Copyright (C) 2026 Axis Communications AB, Lund, Sweden
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pyo3::exceptions::{PyOSError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

fn database_error(error: rusqlite::Error) -> PyErr {
    PyOSError::new_err(format!("Download cache database: {error}"))
}

fn now() -> PyResult<f64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|time| time.as_secs_f64())
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))
}

/// Persistent byte-only cache shared through SQLite transactions.
#[pyclass]
pub struct DownloadCache {
    connection: Mutex<Option<Connection>>,
    #[pyo3(get)]
    size_limit: i64,
}

impl DownloadCache {
    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&mut Connection) -> rusqlite::Result<T>,
    ) -> PyResult<T> {
        let mut guard = self
            .connection
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Download cache lock poisoned"))?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("Download cache is closed"))?;
        operation(connection).map_err(database_error)
    }
}

#[pymethods]
impl DownloadCache {
    #[new]
    fn new(py: Python<'_>, directory: PathBuf, size_limit: i64) -> PyResult<Self> {
        if size_limit <= 0 {
            return Err(PyValueError::new_err("size_limit must be positive"));
        }
        py.detach(|| {
            // Never open or migrate a legacy DiskCache database.
            let directory = directory.join("native-v1");
            std::fs::create_dir_all(&directory).map_err(|error| {
                PyOSError::new_err(format!("Create download cache directory: {error}"))
            })?;
            let connection =
                Connection::open(directory.join("cache.sqlite3")).map_err(database_error)?;
            connection
                .busy_timeout(Duration::from_secs(60))
                .map_err(database_error)?;
            connection
                .execute_batch(
                    "PRAGMA trusted_schema = OFF;
                     PRAGMA auto_vacuum = FULL;
                     PRAGMA journal_mode = TRUNCATE;
                     CREATE TABLE IF NOT EXISTS entries (
                         key TEXT PRIMARY KEY NOT NULL,
                         value BLOB NOT NULL CHECK(typeof(value) = 'blob'),
                         expires REAL,
                         accessed INTEGER NOT NULL
                     );
                     CREATE INDEX IF NOT EXISTS entries_lru ON entries(accessed);",
                )
                .map_err(database_error)?;
            Ok(Self {
                connection: Mutex::new(Some(connection)),
                size_limit,
            })
        })
    }

    fn get(&self, py: Python<'_>, key: String) -> PyResult<Option<Py<PyBytes>>> {
        let timestamp = now()?;
        let value: Option<Vec<u8>> = py.detach(|| {
            self.with_connection(|connection| {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute(
                    "DELETE FROM entries WHERE key = ?1 AND expires <= ?2",
                    params![key, timestamp],
                )?;
                let value = transaction
                    .query_row("SELECT value FROM entries WHERE key = ?1", [&key], |row| {
                        row.get(0)
                    })
                    .optional()?;
                if value.is_some() {
                    transaction.execute(
                        "UPDATE entries SET accessed =
                         (SELECT COALESCE(MAX(accessed), 0) + 1 FROM entries) WHERE key = ?1",
                        [&key],
                    )?;
                }
                transaction.commit()?;
                Ok(value)
            })
        })?;
        Ok(value.map(|value| PyBytes::new(py, &value).unbind()))
    }

    #[pyo3(signature = (key, value, expire=None))]
    fn set(
        &self,
        py: Python<'_>,
        key: String,
        value: Vec<u8>,
        expire: Option<f64>,
    ) -> PyResult<()> {
        let timestamp = now()?;
        if let Some(ttl) = expire
            && (!ttl.is_finite() || !(timestamp + ttl).is_finite())
        {
            return Err(PyValueError::new_err("ttl must be finite"));
        }
        py.detach(|| {
            self.with_connection(|connection| {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute(
                    "DELETE FROM entries WHERE key = ?1 OR expires <= ?2",
                    params![key, timestamp],
                )?;
                let size = key.len().saturating_add(value.len()) as u64;
                // Oversized objects are returned to the caller but never retained.
                if size <= self.size_limit as u64 && expire.is_none_or(|ttl| ttl > 0.0) {
                    transaction.execute(
                        "DELETE FROM entries WHERE key IN (
                             SELECT key FROM (
                                 SELECT key, SUM(length(CAST(key AS BLOB)) + length(value))
                                     OVER (ORDER BY accessed DESC ROWS UNBOUNDED PRECEDING) AS used
                                 FROM entries
                             ) WHERE used > ?1
                         )",
                        [self.size_limit - size as i64],
                    )?;
                    transaction.execute(
                        "INSERT INTO entries(key, value, expires, accessed)
                         VALUES (?1, ?2, ?3, (SELECT COALESCE(MAX(accessed), 0) + 1 FROM entries))",
                        params![key, value, expire.map(|ttl| timestamp + ttl)],
                    )?;
                }
                transaction.commit()
            })
        })
    }

    fn delete(&self, py: Python<'_>, key: String) -> PyResult<()> {
        py.detach(|| {
            self.with_connection(|connection| {
                connection.execute("DELETE FROM entries WHERE key = ?1", [key])?;
                Ok(())
            })
        })
    }

    fn clear(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            self.with_connection(|connection| {
                connection.execute("DELETE FROM entries", [])?;
                Ok(())
            })
        })
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            let mut guard = self
                .connection
                .lock()
                .map_err(|_| PyRuntimeError::new_err("Download cache lock poisoned"))?;
            if let Some(connection) = guard.take() {
                connection.close().map_err(|(connection, error)| {
                    *guard = Some(connection);
                    database_error(error)
                })?;
            }
            Ok(())
        })
    }
}
