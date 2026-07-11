/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! `select_tasks` (the CLI's `--tasks`) makes a checkpoint a terminal boundary: it
//! materializes but its downstream worker never runs, and a later run selecting the
//! downstream checkpoint resumes from it without re-reading the source.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use bask_core::prelude::*;
use bask_core::{Checkpoint, MemStore, SqliteStore};
use serde::{Deserialize, Serialize};

struct Feed;
struct Line(u64);

#[derive(Serialize, Deserialize, Checkpoint)]
struct Saved {
    #[key]
    id: String,
}

#[derive(Serialize, Deserialize, Checkpoint)]
struct Resaved {
    #[key]
    id: String,
}

struct Reader {
    reads: Arc<AtomicUsize>,
}
#[async_trait]
impl Worker for Reader {
    type Task = Feed;
    async fn process(&self, _feed: &Feed, ctx: &Context) -> anyhow::Result<()> {
        self.reads.fetch_add(1, SeqCst);
        for i in 0..6 {
            ctx.emit_keyed(i, Line(i)).await?;
        }
        Ok(())
    }
}

struct Convert;
#[async_trait]
impl Worker for Convert {
    type Task = Line;
    async fn process(&self, line: &Line, ctx: &Context) -> anyhow::Result<()> {
        ctx.emit(Saved {
            id: format!("row-{}", line.0),
        })
        .await?;
        Ok(())
    }
}

struct Edit {
    edits: Arc<AtomicUsize>,
}
#[async_trait]
impl Worker for Edit {
    type Task = Saved;
    async fn process(&self, saved: &Saved, ctx: &Context) -> anyhow::Result<()> {
        self.edits.fetch_add(1, SeqCst);
        ctx.emit(Resaved {
            id: saved.id.clone(),
        })
        .await?;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_checkpoint_is_a_terminal_boundary() {
    let edits = Arc::new(AtomicUsize::new(0));
    let report = Engine::builder()
        .worker(Reader {
            reads: Arc::new(AtomicUsize::new(0)),
        })
        .worker(Convert)
        .worker(Edit {
            edits: edits.clone(),
        })
        .store(MemStore::default())
        .source("feed", Feed)
        .select_tasks(["Saved".to_string()])
        .run()
        .await
        .unwrap();
    // 1 reader + 6 converts + 6 terminal Saved materializations; Edit and Resaved never run.
    assert_eq!(
        edits.load(SeqCst),
        0,
        "Saved is terminal, so Edit never runs"
    );
    assert_eq!(report.stats.processed, 13);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn select_downstream_resumes_from_the_boundary() {
    let dir = std::env::temp_dir().join(format!("bask-select-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("bask.sqlite");
    let reads = Arc::new(AtomicUsize::new(0));
    let edits = Arc::new(AtomicUsize::new(0));

    let engine = |task: &str, reads: Arc<AtomicUsize>, edits: Arc<AtomicUsize>| {
        Engine::builder()
            .worker(Reader { reads })
            .worker(Convert)
            .worker(Edit { edits })
            .store(SqliteStore::open(&path))
            .source("feed", Feed)
            .select_tasks([task.to_string()])
    };

    // Run 1 stops at Saved (Edit never runs); run 2 selects Resaved and resumes from the
    // stored Saved items, so the source is skipped whole and Edit runs each once.
    engine("Saved", reads.clone(), edits.clone())
        .run()
        .await
        .unwrap();
    assert_eq!(reads.load(SeqCst), 1);
    assert_eq!(edits.load(SeqCst), 0);

    engine("Resaved", reads.clone(), edits.clone())
        .run()
        .await
        .unwrap();
    assert_eq!(reads.load(SeqCst), 1, "source not re-read on resume");
    assert_eq!(
        edits.load(SeqCst),
        6,
        "each stored Saved reseeded and edited once"
    );

    std::fs::remove_dir_all(&dir).ok();
}
