/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! End-to-end checkpoint behavior: materialize-and-skip, provenance-driven seed pruning,
//! process-later reseeding, key-only side effects, and sqlite persistence across runs.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use bask_core::prelude::*;
use bask_core::{Checkpoint, MemStore, SqliteStore};
use serde::{Deserialize, Serialize};

// A source seed and the row/line tasks that flow from it.
struct Csv;
struct Line(u64);

// The durable restore point: the store is the output.
#[derive(Serialize, Deserialize, Checkpoint)]
struct Saved {
    #[key]
    id: String,
    value: u64,
}

// Reads a fixed number of rows, stamping each with its source ordinal; an optional
// shutdown handle lets a test interrupt the run right after the rows are dispatched.
struct Reader {
    rows: u64,
    reads: Arc<AtomicUsize>,
    interrupt: Option<Shutdown>,
}
#[async_trait]
impl Worker for Reader {
    type Task = Csv;
    async fn process(&self, _csv: &Csv, ctx: &Context) -> anyhow::Result<()> {
        self.reads.fetch_add(1, SeqCst);
        for i in 0..self.rows {
            ctx.emit_keyed(i, Line(i)).await?;
        }
        if let Some(shutdown) = &self.interrupt {
            shutdown.trigger();
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

// Groups lines into fixed batches, emitting one Saved per full group and flushing the
// remainder; each Saved covers the union of the rows folded into it.
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

// The "process later" consumer registered only on the second run.
struct Consume {
    consumed: Arc<AtomicUsize>,
}
#[async_trait]
impl Worker for Consume {
    type Task = Saved;
    async fn process(&self, _saved: &Saved, _ctx: &Context) -> anyhow::Result<()> {
        self.consumed.fetch_add(1, SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resume_prunes_source_then_reseeds_for_process_later() {
    let store = Arc::new(MemStore::default());
    let reads = Arc::new(AtomicUsize::new(0));

    // Run 1: read ten rows, chunk into two groups, materialize both as terminal Saved.
    let first = Engine::builder()
        .worker(Reader {
            rows: 10,
            reads: reads.clone(),
            interrupt: None,
        })
        .worker(Enrich)
        .router::<Chunk>()
        .concurrency(1)
        .store(store.clone())
        .source("csv", Csv)
        .run()
        .await
        .unwrap();
    assert_eq!(reads.load(SeqCst), 1, "source read once");
    // 1 reader + 10 lines + 2 terminal Saved materializations.
    assert_eq!(first.stats.processed, 13);
    assert_eq!(first.stats.skipped, 0);

    // Run 2: the source is fully covered, so it is skipped whole (no re-read); the two
    // stored Saved are reseeded and consumed by the newly registered worker.
    let consumed = Arc::new(AtomicUsize::new(0));
    let second = Engine::builder()
        .worker(Reader {
            rows: 10,
            reads: reads.clone(),
            interrupt: None,
        })
        .worker(Enrich)
        .worker(Consume {
            consumed: consumed.clone(),
        })
        .router::<Chunk>()
        .concurrency(1)
        .store(store.clone())
        .source("csv", Csv)
        .run()
        .await
        .unwrap();
    assert_eq!(reads.load(SeqCst), 1, "source not re-read on resume");
    assert_eq!(consumed.load(SeqCst), 2, "both stored items reseeded");
    assert_eq!(second.stats.processed, 2);
    assert_eq!(second.stats.skipped, 0);
}

// A duplicate key reaching a terminal checkpoint twice is materialized once.
struct Twice;
#[derive(Serialize, Deserialize, Checkpoint)]
struct Once {
    #[key]
    id: String,
}
struct Fan;
#[async_trait]
impl Worker for Fan {
    type Task = Twice;
    async fn process(&self, _t: &Twice, ctx: &Context) -> anyhow::Result<()> {
        ctx.emit(Once {
            id: "dup".to_string(),
        })
        .await?;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_key_is_materialized_once() {
    let report = Engine::builder()
        .worker(Fan)
        .concurrency(1)
        .store(MemStore::default())
        .seed(Twice)
        .seed(Twice)
        .run()
        .await
        .unwrap();
    // 2 fan workers + 1 materialization; a second materialization would make it 4.
    assert_eq!(report.stats.processed, 3);
    assert_eq!(report.stats.skipped, 1, "the duplicate is skipped");
}

// A key-only checkpoint runs its side effect once, then skips on a shared store.
#[derive(Serialize, Deserialize, Checkpoint)]
#[checkpoint(key_only)]
struct Sent {
    #[key]
    id: u64,
}
struct Mailer {
    sends: Arc<AtomicUsize>,
}
#[async_trait]
impl Worker for Mailer {
    type Task = Sent;
    async fn process(&self, _s: &Sent, _ctx: &Context) -> anyhow::Result<()> {
        self.sends.fetch_add(1, SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn key_only_side_effect_runs_once_across_runs() {
    let store = Arc::new(MemStore::default());
    let sends = Arc::new(AtomicUsize::new(0));
    for _ in 0..2 {
        Engine::builder()
            .worker(Mailer {
                sends: sends.clone(),
            })
            .concurrency(1)
            .store(store.clone())
            .seed(Sent { id: 7 })
            .run()
            .await
            .unwrap();
    }
    assert_eq!(sends.load(SeqCst), 1, "side effect fired exactly once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_store_persists_coverage_across_processes() {
    let dir = std::env::temp_dir().join(format!("bask-ckpt-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("bask.sqlite");
    let reads = Arc::new(AtomicUsize::new(0));

    for _ in 0..2 {
        Engine::builder()
            .worker(Reader {
                rows: 10,
                reads: reads.clone(),
                interrupt: None,
            })
            .worker(Enrich)
            .router::<Chunk>()
            .concurrency(1)
            .store(SqliteStore::open(&path))
            .source("csv", Csv)
            .run()
            .await
            .unwrap();
    }

    assert!(path.exists(), "bask.sqlite was created");
    assert_eq!(
        reads.load(SeqCst),
        1,
        "second process skipped the covered source"
    );
    std::fs::remove_dir_all(&dir).ok();
}

// Counts consumptions per group key so a resumed run can prove each ran exactly once.
struct Tally {
    seen: Arc<std::sync::Mutex<std::collections::HashMap<String, usize>>>,
}
#[async_trait]
impl Worker for Tally {
    type Task = Saved;
    async fn process(&self, saved: &Saved, _ctx: &Context) -> anyhow::Result<()> {
        *self
            .seen
            .lock()
            .unwrap()
            .entry(saved.id.clone())
            .or_default() += 1;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupt_then_resume_consumes_each_group_once() {
    let store = Arc::new(MemStore::default());
    let seen = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    let reads = Arc::new(AtomicUsize::new(0));

    // Run 1: the source dispatches every row then trips a grace-zero shutdown, so the
    // downstream Saved work is cut mid-flight and the source extent is never recorded.
    let shutdown = Shutdown::new();
    let first = Engine::builder()
        .worker(Reader {
            rows: 10,
            reads: reads.clone(),
            interrupt: Some(shutdown.clone()),
        })
        .worker(Enrich)
        .worker(Tally { seen: seen.clone() })
        .router::<Chunk>()
        .concurrency(1)
        .store(store.clone())
        .source("csv", Csv)
        .shutdown(shutdown)
        .grace_period(std::time::Duration::ZERO)
        .run()
        .await
        .unwrap();
    assert!(first.interrupted, "run 1 was interrupted");

    // Run 2: the source re-reads (its extent was never recorded), and reseeds plus claims
    // ensure every group is consumed exactly once across both runs, never twice.
    let second = Engine::builder()
        .worker(Reader {
            rows: 10,
            reads: reads.clone(),
            interrupt: None,
        })
        .worker(Enrich)
        .worker(Tally { seen: seen.clone() })
        .router::<Chunk>()
        .concurrency(1)
        .store(store.clone())
        .source("csv", Csv)
        .run()
        .await
        .unwrap();
    assert!(!second.interrupted, "run 2 completed");

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2, "both groups consumed");
    assert!(
        seen.values().all(|&n| n == 1),
        "each group consumed exactly once: {seen:?}"
    );
}
