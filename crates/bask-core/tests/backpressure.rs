/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::time::Duration;

use bask_core::prelude::*;

struct Fan(usize);
struct Leaf;

struct Source;
#[async_trait]
impl Worker for Source {
    type Task = Fan;
    async fn process(&self, fan: &Fan, ctx: &Context) -> anyhow::Result<()> {
        for _ in 0..fan.0 {
            ctx.emit(Leaf).await?;
        }
        Ok(())
    }
}

struct Sink;
#[async_trait]
impl Worker for Sink {
    type Task = Leaf;
    async fn process(&self, _leaf: &Leaf, ctx: &Context) -> anyhow::Result<()> {
        ctx.route::<Count>(1).await?;
        Ok(())
    }
}

struct Count;
impl Router for Count {
    type Input = u64;
    type State = u64;
    type Output = u64;
    fn route(state: &mut u64, input: u64, _out: &mut Emit) {
        *state += input;
    }
    fn merge(left: &mut u64, right: u64) {
        *left += right;
    }
    fn finalize(state: u64) -> u64 {
        state
    }
}

/// Records the largest queue depth the engine ever reported.
struct PeakQueue(Arc<AtomicUsize>);
impl Monitor for PeakQueue {
    fn sample(&mut self, snapshot: &Snapshot) {
        self.0.fetch_max(snapshot.queued, SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fan_out_stays_bounded() {
    const N: usize = 100_000;
    const CAP: usize = 64;
    let peak = Arc::new(AtomicUsize::new(0));

    let report = Engine::builder()
        .worker(Source)
        .worker(Sink)
        .router::<Count>()
        .concurrency(4)
        .queue_capacity(CAP)
        .sample_interval(Duration::from_micros(200))
        .monitor(PeakQueue(peak.clone()))
        .seed(Fan(N))
        .run()
        .await
        .unwrap();

    assert_eq!(*report.output::<Count>().unwrap(), N as u64);
    let peak = peak.load(SeqCst);
    assert!(peak > 0, "monitor never observed a non-empty queue");
    assert!(
        peak <= CAP + 16,
        "queue depth {peak} exceeded the bound; an unbounded queue would reach ~{N}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_deadlock_when_concurrency_below_producers() {
    const N: usize = 5_000;
    let report = tokio::time::timeout(
        Duration::from_secs(30),
        Engine::builder()
            .worker(Source)
            .worker(Sink)
            .router::<Count>()
            .concurrency(1)
            .queue_capacity(8)
            .seed(Fan(N))
            .run(),
    )
    .await
    .expect("run deadlocked under backpressure")
    .unwrap();

    assert_eq!(*report.output::<Count>().unwrap(), N as u64);
}

struct Job {
    fan: usize,
}

struct SelfFan;
#[async_trait]
impl Worker for SelfFan {
    type Task = Job;
    async fn process(&self, job: &Job, ctx: &Context) -> anyhow::Result<()> {
        ctx.route::<Count>(1).await?;
        for _ in 0..job.fan {
            ctx.emit(Job { fan: 0 }).await?;
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn self_emitting_single_instance_does_not_deadlock() {
    const N: usize = 5_000;
    let report = tokio::time::timeout(
        Duration::from_secs(30),
        Engine::builder()
            .worker(SelfFan)
            .router::<Count>()
            .concurrency(1)
            .queue_capacity(8)
            .seed(Job { fan: N })
            .run(),
    )
    .await
    .expect("run deadlocked under backpressure")
    .unwrap();

    assert_eq!(*report.output::<Count>().unwrap(), (N + 1) as u64);
}
