// SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
//
// SPDX-License-Identifier: MIT OR Apache-2.0
//! A custom [`Dataset`] backed by a SQL database. The engine's `FileDataset` writes Parquet
//! shards to a directory; here we instead implement the trait against a sqlite table -- the
//! same shape a Postgres connector would take -- so materialized checkpoints are rows in a
//! database and provenance coverage compacts superseded shards in place. The pipeline is
//! `feed -> chunk -> save -> edit -> resave`: the resave re-derives the same source rows, so
//! only the final shards remain live.

use std::sync::{Arc, Mutex};

use bask::prelude::*;
use bask::{Checkpoint, Committed, Coverage, Dataset, Status, Store, StoredItem};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

/// A dataset whose shards live as rows in a sqlite `entry` table: `status` and `coverage`
/// are the checkpoint index the trait reserves, `payload`/`seq`/`live` the shard registry.
#[derive(Clone)]
struct SqlDataset {
    conn: Arc<Mutex<Connection>>,
}

impl SqlDataset {
    fn open(path: &std::path::Path) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS entry (
                name TEXT NOT NULL, key TEXT NOT NULL, status INTEGER NOT NULL,
                coverage BLOB NOT NULL, payload BLOB, seq INTEGER, live INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (name, key));
             CREATE TABLE IF NOT EXISTS extent (source TEXT PRIMARY KEY, coverage BLOB NOT NULL);",
        )?;
        Ok(SqlDataset {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn live(&self, name: Option<&str>) -> anyhow::Result<Vec<StoredItem>> {
        let conn = self.conn.lock().unwrap();
        let mut sql = String::from(
            "SELECT key, payload, coverage FROM entry WHERE live = 1 AND payload IS NOT NULL",
        );
        if name.is_some() {
            sql.push_str(" AND name = ?1");
        }
        let mut stmt = conn.prepare(&sql)?;
        let map = |r: &rusqlite::Row| {
            Ok(StoredItem {
                key: r.get::<_, String>(0)?,
                payload: r.get::<_, Vec<u8>>(1)?,
                coverage: Coverage::from_bytes(&r.get::<_, Vec<u8>>(2)?),
            })
        };
        let rows = match name {
            Some(name) => stmt
                .query_map(params![name], map)?
                .collect::<Result<_, _>>()?,
            None => stmt.query_map([], map)?.collect::<Result<_, _>>()?,
        };
        Ok(rows)
    }
}

impl Store for SqlDataset {
    fn statuses(&self) -> anyhow::Result<Vec<(String, String, Status)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT name, key, status FROM entry")?;
        let rows = stmt
            .query_map([], |r| {
                let status = if r.get::<_, i64>(2)? == 1 {
                    Status::Consumed
                } else {
                    Status::Stored
                };
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, status))
            })?
            .collect::<Result<_, _>>()?;
        Ok(rows)
    }

    fn covered(&self) -> anyhow::Result<Coverage> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT coverage FROM entry")?;
        let mut cov = Coverage::empty();
        for bytes in stmt.query_map([], |r| r.get::<_, Vec<u8>>(0))? {
            cov.union_with(&Coverage::from_bytes(&bytes?));
        }
        Ok(cov)
    }

    fn extents(&self) -> anyhow::Result<std::collections::HashMap<String, Coverage>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT source, coverage FROM extent")?;
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
        self.live(Some(name))
    }

    fn commit(&self, rec: &Committed) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO entry (name, key, status, coverage, payload, seq, live)
             VALUES (?1, ?2, 0, ?3, NULL, NULL, 0)",
            params![rec.name, rec.key, rec.coverage.to_bytes()],
        )?;
        Ok(())
    }

    fn consume(&self, name: &str, key: &str) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE entry SET status = 1 WHERE name = ?1 AND key = ?2",
            params![name, key],
        )?;
        Ok(())
    }

    fn record_extent(&self, source: &str, extent: &Coverage) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO extent (source, coverage) VALUES (?1, ?2)",
            params![source, extent.to_bytes()],
        )?;
        Ok(())
    }
}

impl Dataset for SqlDataset {
    fn store(&self) -> Arc<dyn Store> {
        Arc::new(self.clone())
    }

    fn put(&self, item: &Committed) -> anyhow::Result<()> {
        let Some(payload) = &item.payload else {
            return Ok(());
        };
        let conn = self.conn.lock().unwrap();
        let seq: i64 = conn.query_row("SELECT COALESCE(MAX(seq), 0) + 1 FROM entry", [], |r| {
            r.get(0)
        })?;
        conn.execute(
            "UPDATE entry SET payload = ?1, seq = ?2, live = 1 WHERE name = ?3 AND key = ?4",
            params![payload, seq, item.name, item.key],
        )?;
        // Supersede: a live shard whose rows a strictly-newer save re-derived is retired.
        let mut stmt =
            conn.prepare("SELECT name, key, seq, coverage FROM entry WHERE payload IS NOT NULL")?;
        let shards = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                    Coverage::from_bytes(&r.get::<_, Vec<u8>>(3)?),
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (name, key, seq, coverage) in &shards {
            if coverage.is_empty() {
                continue;
            }
            let mut newer = Coverage::empty();
            for (.., other_seq, other_cov) in &shards {
                if other_seq > seq {
                    newer.union_with(other_cov);
                }
            }
            if coverage.is_subset_of(&newer) {
                conn.execute(
                    "UPDATE entry SET live = 0, payload = NULL WHERE name = ?1 AND key = ?2",
                    params![name, key],
                )?;
            }
        }
        Ok(())
    }

    fn stored(&self, name: &str) -> anyhow::Result<Vec<StoredItem>> {
        self.live(Some(name))
    }

    fn read(&self) -> anyhow::Result<Vec<StoredItem>> {
        self.live(None)
    }
}

struct Feed;
struct Line(u64);

#[derive(Serialize, Deserialize, Checkpoint)]
struct Saved {
    #[key]
    id: String,
    value: u64,
}

#[derive(Serialize, Deserialize, Checkpoint)]
struct Resaved {
    #[key]
    id: String,
    value: u64,
}

struct Reader;
#[async_trait]
impl Worker for Reader {
    type Task = Feed;
    async fn process(&self, _feed: &Feed, ctx: &Context) -> anyhow::Result<()> {
        for i in 0..10 {
            ctx.emit_keyed(i, Line(i)).await?;
        }
        Ok(())
    }
}

struct Enrich;
#[async_trait]
impl Worker for Enrich {
    type Task = Line;
    async fn process(&self, line: &Line, ctx: &Context) -> anyhow::Result<()> {
        ctx.route::<Chunk>(line.0).await?;
        Ok(())
    }
}

// Edits each saved group and resaves it, covering the same source rows -> supersedes Saved.
struct Edit;
#[async_trait]
impl Worker for Edit {
    type Task = Saved;
    async fn process(&self, saved: &Saved, ctx: &Context) -> anyhow::Result<()> {
        ctx.emit(Resaved {
            id: saved.id.clone(),
            value: saved.value * 10,
        })
        .await?;
        Ok(())
    }
}

const GROUP: usize = 5;
struct Chunk;
impl Router for Chunk {
    type Input = u64;
    type State = Vec<u64>;
    type Output = ();
    fn route(state: &mut Vec<u64>, input: u64, out: &mut Emit) {
        state.push(input);
        if state.len() >= GROUP {
            out.emit(Saved {
                id: format!("g{}", state[0]),
                value: std::mem::take(state).iter().sum(),
            });
        }
    }
    fn merge(left: &mut Vec<u64>, right: Vec<u64>) {
        left.extend(right);
    }
    fn finalize(_state: Vec<u64>) {}
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = std::env::temp_dir().join("bask-custom-dataset.sqlite");
    let _ = std::fs::remove_file(&path);
    let data = SqlDataset::open(&path)?;

    let report = Engine::builder()
        .worker(Reader)
        .worker(Enrich)
        .worker(Edit)
        .router::<Chunk>()
        .concurrency(1)
        .dataset(data.clone())
        .source("feed", Feed)
        .run()
        .await?;

    println!(
        "processed {} tasks into {}",
        report.stats.processed,
        path.display()
    );
    let mut live: Vec<_> = data
        .read()?
        .into_iter()
        .map(|shard| serde_json::from_slice::<Resaved>(&shard.payload).unwrap())
        .collect();
    live.sort_by(|a, b| a.id.cmp(&b.id));
    println!("live shards after save -> edit -> resave: {}", live.len());
    for shard in &live {
        println!("  {} = {}", shard.id, shard.value);
    }
    Ok(())
}
