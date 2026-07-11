/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! The default [`Store`]: a single sqlite file (WAL) created lazily on the first commit.
//! Small payloads inline; payloads above a threshold spill to a blake3 content-addressed
//! file beside it, so audio and images do not bloat the index.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rusqlite::{Connection, params};

use crate::checkpoint::{Committed, Coverage, Status, Store, StoredItem};

const SPILL_THRESHOLD: usize = 256 * 1024;

const SCHEMA: &str = "
    PRAGMA journal_mode = WAL;
    CREATE TABLE IF NOT EXISTS checkpoint (
        name TEXT NOT NULL,
        key TEXT NOT NULL,
        status INTEGER NOT NULL,
        payload BLOB,
        spill TEXT,
        coverage BLOB NOT NULL,
        PRIMARY KEY (name, key)
    );
    CREATE TABLE IF NOT EXISTS source_extent (
        source TEXT PRIMARY KEY,
        extent BLOB NOT NULL
    );
";

/// A sqlite-backed checkpoint index. The connection and file are created on first write,
/// so a run that commits no checkpoint leaves nothing on disk.
pub struct SqliteStore {
    path: PathBuf,
    conn: Mutex<Option<Connection>>,
}

impl SqliteStore {
    pub fn open(path: impl Into<PathBuf>) -> Self {
        SqliteStore {
            path: path.into(),
            conn: Mutex::new(None),
        }
    }

    fn blobs_dir(&self) -> PathBuf {
        match self.path.parent() {
            Some(dir) if !dir.as_os_str().is_empty() => dir.join(".bask-blobs"),
            _ => PathBuf::from(".bask-blobs"),
        }
    }

    /// Run `f` against a connection, creating the file and schema on first use.
    fn write<T>(&self, f: impl FnOnce(&Connection) -> anyhow::Result<T>) -> anyhow::Result<T> {
        let mut guard = self.conn.lock().unwrap();
        if guard.is_none() {
            let conn = Connection::open(&self.path)?;
            conn.execute_batch(SCHEMA)?;
            *guard = Some(conn);
        }
        f(guard.as_ref().unwrap())
    }

    /// Run `f` against an existing file; if nothing was ever committed, yield `default`.
    fn read<T>(
        &self,
        default: T,
        f: impl FnOnce(&Connection) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        let mut guard = self.conn.lock().unwrap();
        if guard.is_none() {
            if !self.path.exists() {
                return Ok(default);
            }
            let conn = Connection::open(&self.path)?;
            conn.execute_batch(SCHEMA)?;
            *guard = Some(conn);
        }
        f(guard.as_ref().unwrap())
    }

    fn load_payload(
        &self,
        dir: &Path,
        inline: Option<Vec<u8>>,
        spill: Option<String>,
    ) -> Option<Vec<u8>> {
        match (inline, spill) {
            (Some(bytes), _) => Some(bytes),
            (None, Some(name)) => std::fs::read(dir.join(name)).ok(),
            (None, None) => None,
        }
    }
}

impl Store for SqliteStore {
    fn statuses(&self) -> anyhow::Result<Vec<(String, String, Status)>> {
        self.read(Vec::new(), |conn| {
            let mut stmt = conn.prepare("SELECT name, key, status FROM checkpoint")?;
            let rows = stmt
                .query_map([], |r| {
                    let status = if r.get::<_, i64>(2)? == 1 {
                        Status::Consumed
                    } else {
                        Status::Stored
                    };
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, status))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
    }

    fn covered(&self) -> anyhow::Result<Coverage> {
        self.read(Coverage::empty(), |conn| {
            let mut stmt = conn.prepare("SELECT coverage FROM checkpoint")?;
            let mut cov = Coverage::empty();
            let rows = stmt.query_map([], |r| r.get::<_, Vec<u8>>(0))?;
            for bytes in rows {
                cov.union_with(&Coverage::from_bytes(&bytes?));
            }
            Ok(cov)
        })
    }

    fn extents(&self) -> anyhow::Result<HashMap<String, Coverage>> {
        self.read(HashMap::new(), |conn| {
            let mut stmt = conn.prepare("SELECT source, extent FROM source_extent")?;
            let rows = stmt.query_map([], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?))
            })?;
            let mut out = HashMap::new();
            for row in rows {
                let (source, bytes) = row?;
                out.insert(source, Coverage::from_bytes(&bytes));
            }
            Ok(out)
        })
    }

    fn stored_items(&self, name: &str) -> anyhow::Result<Vec<StoredItem>> {
        let dir = self.blobs_dir();
        self.read(Vec::new(), |conn| {
            let mut stmt = conn.prepare(
                "SELECT key, payload, spill, coverage FROM checkpoint WHERE name = ?1 AND status = 0",
            )?;
            let rows = stmt
                .query_map(params![name], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Option<Vec<u8>>>(1)?,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, Vec<u8>>(3)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows
                .into_iter()
                .filter_map(|(key, inline, spill, cov)| {
                    self.load_payload(&dir, inline, spill).map(|payload| StoredItem {
                        key,
                        payload,
                        coverage: Coverage::from_bytes(&cov),
                    })
                })
                .collect())
        })
    }

    fn commit(&self, rec: &Committed) -> anyhow::Result<()> {
        let (inline, spill) = match &rec.payload {
            Some(bytes) if bytes.len() > SPILL_THRESHOLD => {
                let dir = self.blobs_dir();
                std::fs::create_dir_all(&dir)?;
                let name = format!("{}.bin", blake3::hash(bytes).to_hex());
                std::fs::write(dir.join(&name), bytes)?;
                (None, Some(name))
            }
            Some(bytes) => (Some(bytes.clone()), None),
            None => (None, None),
        };
        self.write(|conn| {
            conn.execute(
                "INSERT OR REPLACE INTO checkpoint (name, key, status, payload, spill, coverage)
                 VALUES (?1, ?2, 0, ?3, ?4, ?5)",
                params![rec.name, rec.key, inline, spill, rec.coverage.to_bytes()],
            )?;
            Ok(())
        })
    }

    fn consume(&self, name: &str, key: &str) -> anyhow::Result<()> {
        self.write(|conn| {
            conn.execute(
                "UPDATE checkpoint SET status = 1 WHERE name = ?1 AND key = ?2",
                params![name, key],
            )?;
            Ok(())
        })
    }

    fn record_extent(&self, source: &str, extent: &Coverage) -> anyhow::Result<()> {
        self.write(|conn| {
            conn.execute(
                "INSERT OR REPLACE INTO source_extent (source, extent) VALUES (?1, ?2)",
                params![source, extent.to_bytes()],
            )?;
            Ok(())
        })
    }
}
