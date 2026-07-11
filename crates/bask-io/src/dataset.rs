/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! A directory-backed [`Dataset`]: each data-carrying checkpoint materializes into a
//! content-addressed `<b3[:2]>/<b3[2:]>.parquet` shard, and one `bask.sqlite` in the same
//! directory reserves the checkpoint index (status, coverage, extents) plus the shard
//! registry. Provenance coverage drives supersession: when a later save re-derives the same
//! source rows, the shards it replaced are dropped and their files garbage-collected, so a
//! `save -> edit -> resave` pipeline leaves only the final shards on disk.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bask_core::{Committed, Coverage, Dataset, Status, Store, StoredItem};
use rusqlite::{Connection, params};

const SCHEMA: &str = "
    PRAGMA journal_mode = WAL;
    CREATE TABLE IF NOT EXISTS checkpoint (
        name TEXT NOT NULL,
        key TEXT NOT NULL,
        status INTEGER NOT NULL,
        coverage BLOB NOT NULL,
        shard TEXT,
        seq INTEGER,
        live INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (name, key)
    );
    CREATE TABLE IF NOT EXISTS source_extent (
        source TEXT PRIMARY KEY,
        extent BLOB NOT NULL
    );
";

/// A directory of content-addressed shards over a single `bask.sqlite`. Cheap to clone
/// (shared handle): keep one to bind on the engine and one to [`read`](Dataset::read) back.
#[derive(Clone)]
pub struct FileDataset {
    inner: Arc<Inner>,
}

struct Inner {
    dir: PathBuf,
    ext: String,
    conn: Mutex<Connection>,
}

impl FileDataset {
    /// Open (creating it if absent) a dataset directory whose shards are `.parquet`.
    pub fn open(dir: impl Into<PathBuf>) -> anyhow::Result<Self> {
        Self::open_ext(dir, "parquet")
    }

    /// Open a dataset whose shard files carry an explicit extension (the shard wire format
    /// is the checkpoint's `encode`; this only names the files a consumer globs).
    pub fn open_ext(dir: impl Into<PathBuf>, ext: impl Into<String>) -> anyhow::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        let conn = Connection::open(dir.join("bask.sqlite"))?;
        conn.execute_batch(SCHEMA)?;
        Ok(FileDataset {
            inner: Arc::new(Inner {
                dir,
                ext: ext.into(),
                conn: Mutex::new(conn),
            }),
        })
    }

    /// The dataset directory; consumers glob `*.parquet` under it for the live shards.
    pub fn dir(&self) -> &Path {
        &self.inner.dir
    }

    fn shard_path(&self, hash: &str) -> PathBuf {
        self.inner
            .dir
            .join(&hash[..2])
            .join(format!("{}.{}", &hash[2..], self.inner.ext))
    }

    /// Content-address the bytes, write the shard once, and point the row at it. Shared by
    /// [`Store::commit`] (plain-store use) and [`Dataset::put`] (which also supersedes).
    fn materialize(
        &self,
        conn: &Connection,
        name: &str,
        key: &str,
        bytes: &[u8],
    ) -> anyhow::Result<()> {
        let hash = blake3::hash(bytes).to_hex().to_string();
        let path = self.shard_path(&hash);
        if !path.exists() {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&path, bytes)?;
        }
        let seq: i64 = conn.query_row(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM checkpoint",
            [],
            |r| r.get(0),
        )?;
        conn.execute(
            "UPDATE checkpoint SET shard = ?1, seq = ?2, live = 1 WHERE name = ?3 AND key = ?4",
            params![hash, seq, name, key],
        )?;
        Ok(())
    }

    /// Drop every live shard whose coverage a strictly-newer save has fully re-derived, then
    /// delete the files no live row still references. Coverage is permanent, so the newer/
    /// older order is by materialization `seq` regardless of a shard's current liveness.
    fn supersede(&self, conn: &Connection) -> anyhow::Result<()> {
        let mut stmt = conn.prepare(
            "SELECT name, key, seq, coverage, shard, live FROM checkpoint WHERE shard IS NOT NULL",
        )?;
        let shards = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                    Coverage::from_bytes(&r.get::<_, Vec<u8>>(3)?),
                    r.get::<_, String>(4)?,
                    r.get::<_, i64>(5)? == 1,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        let mut dead = Vec::new();
        for (name, key, seq, coverage, hash, live) in &shards {
            if !*live || coverage.is_empty() {
                continue;
            }
            let mut newer = Coverage::empty();
            for (.., other_seq, other_cov, _, _) in &shards {
                if other_seq > seq {
                    newer.union_with(other_cov);
                }
            }
            if coverage.is_subset_of(&newer) {
                conn.execute(
                    "UPDATE checkpoint SET live = 0 WHERE name = ?1 AND key = ?2",
                    params![name, key],
                )?;
                dead.push(hash.clone());
            }
        }

        let live: HashSet<String> = {
            let mut stmt = conn.prepare(
                "SELECT DISTINCT shard FROM checkpoint WHERE live = 1 AND shard IS NOT NULL",
            )?;
            stmt.query_map([], |r| r.get::<_, String>(0))?
                .collect::<Result<_, _>>()?
        };
        for hash in dead {
            if !live.contains(&hash) {
                let _ = std::fs::remove_file(self.shard_path(&hash));
            }
        }
        Ok(())
    }

    fn shards(
        &self,
        conn: &Connection,
        name: Option<&str>,
        only_stored: bool,
    ) -> anyhow::Result<Vec<StoredItem>> {
        let mut sql = String::from(
            "SELECT key, coverage, shard FROM checkpoint WHERE live = 1 AND shard IS NOT NULL",
        );
        if name.is_some() {
            sql.push_str(" AND name = ?1");
        }
        if only_stored {
            sql.push_str(" AND status = 0");
        }
        let mut stmt = conn.prepare(&sql)?;
        let map = |r: &rusqlite::Row| {
            Ok((
                r.get::<_, String>(0)?,
                Coverage::from_bytes(&r.get::<_, Vec<u8>>(1)?),
                r.get::<_, String>(2)?,
            ))
        };
        let rows = match name {
            Some(name) => stmt
                .query_map(params![name], map)?
                .collect::<Result<Vec<_>, _>>()?,
            None => stmt.query_map([], map)?.collect::<Result<Vec<_>, _>>()?,
        };
        rows.into_iter()
            .map(|(key, coverage, hash)| {
                Ok(StoredItem {
                    key,
                    payload: std::fs::read(self.shard_path(&hash))?,
                    coverage,
                })
            })
            .collect()
    }
}

impl Store for FileDataset {
    fn statuses(&self) -> anyhow::Result<Vec<(String, String, Status)>> {
        let conn = self.inner.conn.lock().unwrap();
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
    }

    fn covered(&self) -> anyhow::Result<Coverage> {
        let conn = self.inner.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT coverage FROM checkpoint")?;
        let mut cov = Coverage::empty();
        for bytes in stmt.query_map([], |r| r.get::<_, Vec<u8>>(0))? {
            cov.union_with(&Coverage::from_bytes(&bytes?));
        }
        Ok(cov)
    }

    fn extents(&self) -> anyhow::Result<std::collections::HashMap<String, Coverage>> {
        let conn = self.inner.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT source, extent FROM source_extent")?;
        let mut out = std::collections::HashMap::new();
        for row in stmt.query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?))
        })? {
            let (source, bytes) = row?;
            out.insert(source, Coverage::from_bytes(&bytes));
        }
        Ok(out)
    }

    fn stored_items(&self, name: &str) -> anyhow::Result<Vec<StoredItem>> {
        let conn = self.inner.conn.lock().unwrap();
        self.shards(&conn, Some(name), true)
    }

    fn commit(&self, rec: &Committed) -> anyhow::Result<()> {
        let conn = self.inner.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO checkpoint (name, key, status, coverage, shard, seq, live)
             VALUES (?1, ?2, 0, ?3, NULL, NULL, 0)",
            params![rec.name, rec.key, rec.coverage.to_bytes()],
        )?;
        if let Some(bytes) = &rec.payload {
            self.materialize(&conn, &rec.name, &rec.key, bytes)?;
        }
        Ok(())
    }

    fn consume(&self, name: &str, key: &str) -> anyhow::Result<()> {
        let conn = self.inner.conn.lock().unwrap();
        conn.execute(
            "UPDATE checkpoint SET status = 1 WHERE name = ?1 AND key = ?2",
            params![name, key],
        )?;
        Ok(())
    }

    fn record_extent(&self, source: &str, extent: &Coverage) -> anyhow::Result<()> {
        let conn = self.inner.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO source_extent (source, extent) VALUES (?1, ?2)",
            params![source, extent.to_bytes()],
        )?;
        Ok(())
    }
}

impl Dataset for FileDataset {
    fn store(&self) -> Arc<dyn Store> {
        Arc::new(self.clone())
    }

    fn put(&self, item: &Committed) -> anyhow::Result<()> {
        let Some(bytes) = &item.payload else {
            return Ok(());
        };
        let conn = self.inner.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO checkpoint (name, key, status, coverage, live)
             VALUES (?1, ?2, 0, ?3, 0)",
            params![item.name, item.key, item.coverage.to_bytes()],
        )?;
        self.materialize(&conn, &item.name, &item.key, bytes)?;
        self.supersede(&conn)?;
        Ok(())
    }

    fn stored(&self, name: &str) -> anyhow::Result<Vec<StoredItem>> {
        let conn = self.inner.conn.lock().unwrap();
        self.shards(&conn, Some(name), false)
    }

    fn read(&self) -> anyhow::Result<Vec<StoredItem>> {
        let conn = self.inner.conn.lock().unwrap();
        self.shards(&conn, None, false)
    }
}
