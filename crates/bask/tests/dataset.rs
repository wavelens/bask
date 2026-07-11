// SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
//
// SPDX-License-Identifier: MIT OR Apache-2.0
#![cfg(feature = "dataset")]
//! The engine materializes data-carrying checkpoints into a [`FileDataset`] as shards, and a
//! later run reads the latest committed dataset: the fully-covered source is skipped whole.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use bask::data::FileDataset;
use bask::prelude::*;
use bask::{Checkpoint, Dataset};
use serde::{Deserialize, Serialize};

struct Feed;
struct Line(u64);

#[derive(Serialize, Deserialize, Checkpoint)]
struct Saved {
    #[key]
    id: String,
    value: u64,
}

struct Reader {
    reads: Arc<AtomicUsize>,
}
#[async_trait]
impl Worker for Reader {
    type Task = Feed;
    async fn process(&self, _feed: &Feed, ctx: &Context) -> anyhow::Result<()> {
        self.reads.fetch_add(1, SeqCst);
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

const GROUP: usize = 5;
struct Chunk;
impl Router for Chunk {
    type Input = u64;
    type State = Vec<u64>;
    type Output = ();
    fn route(state: &mut Vec<u64>, input: u64, out: &mut Emit) {
        state.push(input);
        if state.len() >= GROUP {
            emit_group(std::mem::take(state), out);
        }
    }
    fn merge(left: &mut Vec<u64>, right: Vec<u64>) {
        left.extend(right);
    }
    fn flush(state: &mut Vec<u64>, out: &mut Emit) {
        if !state.is_empty() {
            emit_group(std::mem::take(state), out);
        }
    }
    fn finalize(_state: Vec<u64>) {}
}
fn emit_group(rows: Vec<u64>, out: &mut Emit) {
    out.emit(Saved {
        id: format!("g{}", rows[0]),
        value: rows.iter().sum(),
    });
}

fn engine(dataset: &FileDataset, reads: &Arc<AtomicUsize>) -> Engine {
    Engine::builder()
        .worker(Reader {
            reads: reads.clone(),
        })
        .worker(Enrich)
        .router::<Chunk>()
        .concurrency(1)
        .dataset(dataset.clone())
        .source("feed", Feed)
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn materializes_shards_then_resumes_from_dataset() {
    let dir = std::env::temp_dir().join(format!("bask-engine-dataset-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let reads = Arc::new(AtomicUsize::new(0));

    let dataset = FileDataset::open_ext(&dir, "json").unwrap();
    let first = engine(&dataset, &reads).run().await.unwrap();
    assert_eq!(reads.load(SeqCst), 1, "source read once");
    // 1 reader + 10 lines + 2 terminal Saved materializations.
    assert_eq!(first.stats.processed, 13);
    assert_eq!(dataset.read().unwrap().len(), 2, "two shards materialized");

    // A fresh handle reruns: the source is fully covered, so it is skipped whole and the
    // dataset still holds the latest snapshot.
    let dataset = FileDataset::open_ext(&dir, "json").unwrap();
    let second = engine(&dataset, &reads).run().await.unwrap();
    assert_eq!(reads.load(SeqCst), 1, "source not re-read on resume");
    assert_eq!(second.stats.processed, 0);
    assert_eq!(dataset.read().unwrap().len(), 2);

    std::fs::remove_dir_all(&dir).ok();
}
