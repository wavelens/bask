/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Router as a batcher: a stream of readings is folded into fixed-size batches, each
//! full batch is emitted downstream, and the trailing partial batch flushes at
//! end-of-run. A second router reduces the per-batch sums into a grand total.
//!
//! Run with: cargo run --example router
use bask_core::prelude::*;

struct Reading(u64);
struct Batch(Vec<u64>);

struct Batcher;
impl Router for Batcher {
    type Input = u64;
    type State = Vec<u64>;
    type Output = ();
    fn route(buf: &mut Vec<u64>, n: u64, out: &mut Emit) {
        buf.push(n);
        if buf.len() >= 4 {
            out.emit(Batch(std::mem::take(buf)));
        }
    }
    fn merge(left: &mut Vec<u64>, right: Vec<u64>) {
        left.extend(right);
    }
    fn flush(buf: &mut Vec<u64>, out: &mut Emit) {
        if !buf.is_empty() {
            out.emit(Batch(std::mem::take(buf)));
        }
    }
    fn finalize(_buf: Vec<u64>) {}
}

struct Total;
impl Router for Total {
    type Input = u64;
    type State = u64;
    type Output = u64;
    fn route(sum: &mut u64, n: u64, _out: &mut Emit) {
        *sum += n;
    }
    fn merge(left: &mut u64, right: u64) {
        *left += right;
    }
    fn finalize(sum: u64) -> u64 {
        sum
    }
}

struct Ingest;
#[async_trait]
impl Worker for Ingest {
    type Task = Reading;
    async fn process(&self, reading: &Reading, ctx: &Context) -> anyhow::Result<()> {
        ctx.route::<Batcher>(reading.0).await?;
        Ok(())
    }
}

struct Process;
#[async_trait]
impl Worker for Process {
    type Task = Batch;
    async fn process(&self, batch: &Batch, ctx: &Context) -> anyhow::Result<()> {
        let sum: u64 = batch.0.iter().sum();
        println!("batch of {} sums to {sum}", batch.0.len());
        ctx.route::<Total>(sum).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut builder = Engine::builder()
        .worker(Ingest)
        .worker(Process)
        .router::<Batcher>()
        .router::<Total>()
        .concurrency(1);
    for i in 1..=10 {
        builder = builder.seed(Reading(i));
    }
    let report = builder.run().await?;

    println!("grand total = {}", report.output::<Total>().unwrap());
    Ok(())
}
