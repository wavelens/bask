/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::time::Duration;

use bask::prelude::*;

struct Ping;

struct Hits;
impl Aggregator for Hits {
    type Input = u64;
    type State = u64;
    type Output = u64;
    fn fold(state: &mut u64, input: u64) {
        *state += input;
    }
    fn merge(left: &mut u64, right: u64) {
        *left += right;
    }
    fn finalize(state: u64) -> u64 {
        state
    }
}

struct Fickle {
    hang: bool,
}
#[async_trait]
impl Worker for Fickle {
    type Task = Ping;
    async fn process(&self, _ping: &Ping, ctx: &Context) -> anyhow::Result<()> {
        if self.hang {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
        ctx.aggregate::<Hits>(1);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_is_retried_on_another_instance() {
    let report = tokio::time::timeout(
        Duration::from_secs(10),
        Engine::builder()
            .worker_cfg(
                Fickle { hang: true },
                WorkerCfg::new()
                    .label("hangs")
                    .timeout(Duration::from_millis(50)),
            )
            .worker_cfg(
                Fickle { hang: false },
                WorkerCfg::new()
                    .label("fast")
                    .timeout(Duration::from_millis(50)),
            )
            .aggregator::<Hits>()
            .retry(RetryPolicy::new().max_attempts(2).avoid_failed())
            .concurrency(2)
            .seed(Ping)
            .run(),
    )
    .await
    .expect("run hung")
    .unwrap();

    assert_eq!(*report.output::<Hits>().unwrap(), 1);
    assert_eq!(report.stats.processed, 1);
    assert_eq!(report.stats.retried, 1);
    assert!(report.failures.is_empty());
    assert!(!report.interrupted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exhausted_timeouts_fail_terminally() {
    let report = tokio::time::timeout(
        Duration::from_secs(10),
        Engine::builder()
            .worker_cfg(
                Fickle { hang: true },
                WorkerCfg::new().timeout(Duration::from_millis(30)),
            )
            .aggregator::<Hits>()
            .retry(RetryPolicy::new().max_attempts(3))
            .seed(Ping)
            .run(),
    )
    .await
    .expect("run hung")
    .unwrap();

    assert_eq!(report.stats.failed, 1);
    assert_eq!(report.stats.retried, 2);
    assert_eq!(report.failures.len(), 1);
    assert!(
        report.failures[0].error.contains("timed out"),
        "unexpected error: {}",
        report.failures[0].error
    );
}

struct HangTask;
struct FastTask;

struct Hang;
#[async_trait]
impl Worker for Hang {
    type Task = HangTask;
    async fn process(&self, _task: &HangTask, _ctx: &Context) -> anyhow::Result<()> {
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Ok(())
    }
}

struct Fast;
#[async_trait]
impl Worker for Fast {
    type Task = FastTask;
    async fn process(&self, _task: &FastTask, ctx: &Context) -> anyhow::Result<()> {
        ctx.aggregate::<Hits>(1);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_releases_permits() {
    // With a single concurrency slot, the fast task can only run if the hung task
    // released its permit on timeout; a leak would hang the run past the guard.
    let report = tokio::time::timeout(
        Duration::from_secs(10),
        Engine::builder()
            .worker_cfg(Hang, WorkerCfg::new().timeout(Duration::from_millis(50)))
            .worker(Fast)
            .aggregator::<Hits>()
            .concurrency(1)
            .seed(HangTask)
            .seed(FastTask)
            .run(),
    )
    .await
    .expect("permit leaked: run hung")
    .unwrap();

    assert_eq!(*report.output::<Hits>().unwrap(), 1);
    assert_eq!(report.stats.processed, 1);
    assert_eq!(report.stats.failed, 1);
}

struct Unit;

struct Slow {
    done: Arc<AtomicUsize>,
}
#[async_trait]
impl Worker for Slow {
    type Task = Unit;
    async fn process(&self, _unit: &Unit, _ctx: &Context) -> anyhow::Result<()> {
        tokio::time::sleep(Duration::from_millis(20)).await;
        self.done.fetch_add(1, SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_shutdown_drains_within_grace() {
    const N: usize = 100;
    let done = Arc::new(AtomicUsize::new(0));
    let shutdown = Shutdown::new();

    let trigger = tokio::spawn({
        let done = done.clone();
        let shutdown = shutdown.clone();
        async move {
            while done.load(SeqCst) < 8 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            shutdown.trigger();
        }
    });

    let mut builder = Engine::builder()
        .worker(Slow { done: done.clone() })
        .concurrency(8)
        .shutdown(shutdown.clone())
        .grace_period(Duration::from_secs(10));
    for _ in 0..N {
        builder = builder.seed(Unit);
    }
    let report = tokio::time::timeout(Duration::from_secs(20), builder.run())
        .await
        .expect("shutdown did not drain")
        .unwrap();
    trigger.await.unwrap();

    assert!(report.interrupted);
    assert_eq!(report.stats.failed, 0);
    assert!(report.stats.processed >= 8);
    assert!(report.unfinished > 0, "expected abandoned queued work");
    assert_eq!(report.stats.processed as usize + report.unfinished, N);
}

struct Sleeper {
    started: Arc<AtomicUsize>,
}
#[async_trait]
impl Worker for Sleeper {
    type Task = Ping;
    async fn process(&self, _ping: &Ping, _ctx: &Context) -> anyhow::Result<()> {
        self.started.fetch_add(1, SeqCst);
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_aborts_in_flight() {
    const N: usize = 4;
    let started = Arc::new(AtomicUsize::new(0));
    let shutdown = Shutdown::new();

    let trigger = tokio::spawn({
        let started = started.clone();
        let shutdown = shutdown.clone();
        async move {
            while started.load(SeqCst) < N {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            shutdown.trigger();
        }
    });

    let mut builder = Engine::builder()
        .worker(Sleeper {
            started: started.clone(),
        })
        .concurrency(N)
        .shutdown(shutdown.clone())
        .grace_period(Duration::ZERO);
    for _ in 0..N {
        builder = builder.seed(Ping);
    }
    let report = tokio::time::timeout(Duration::from_secs(5), builder.run())
        .await
        .expect("cancellation did not abort the sleeping workers")
        .unwrap();
    trigger.await.unwrap();

    assert!(report.interrupted);
    assert_eq!(report.stats.processed, 0);
    assert_eq!(report.unfinished, N);
}
