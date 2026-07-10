/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use bask::prelude::*;

struct Number(u64);
struct Even(u64);

/// Routes even inputs downstream as `Even`, filters odd ones, and counts every input.
struct Classify;
impl Router for Classify {
    type Input = u64;
    type State = u64;
    type Output = u64;
    fn route(seen: &mut u64, n: u64, out: &mut Emit) {
        *seen += 1;
        if n.is_multiple_of(2) {
            out.emit(Even(n));
        }
    }
    fn merge(left: &mut u64, right: u64) {
        *left += right;
    }
    fn finalize(seen: u64) -> u64 {
        seen
    }
}

struct SumEven;
impl Router for SumEven {
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

struct Feed;
#[async_trait]
impl Worker for Feed {
    type Task = Number;
    async fn process(&self, n: &Number, ctx: &Context) -> anyhow::Result<()> {
        ctx.route::<Classify>(n.0).await?;
        Ok(())
    }
}

struct Collect;
#[async_trait]
impl Worker for Collect {
    type Task = Even;
    async fn process(&self, e: &Even, ctx: &Context) -> anyhow::Result<()> {
        ctx.route::<SumEven>(e.0).await?;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_routes_and_filters() {
    let mut builder = Engine::builder()
        .worker(Feed)
        .worker(Collect)
        .router::<Classify>()
        .router::<SumEven>();
    for n in 0..10 {
        builder = builder.seed(Number(n));
    }
    let report = builder.run().await.unwrap();

    assert_eq!(*report.output::<Classify>().unwrap(), 10); // saw every input
    assert_eq!(*report.output::<SumEven>().unwrap(), 2 + 4 + 6 + 8); // only evens routed on
    assert_eq!(report.stats.failed, 0);
}

struct Item(u64);
struct Batch(Vec<u64>);

/// Buffers inputs into batches of three, emitting a `Batch` whenever the buffer fills
/// and flushing the trailing partial batch at end-of-run (model 2, for free).
struct Batcher;
impl Router for Batcher {
    type Input = u64;
    type State = Vec<u64>;
    type Output = ();
    fn route(buf: &mut Vec<u64>, n: u64, out: &mut Emit) {
        buf.push(n);
        if buf.len() >= 3 {
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

struct Tally;
impl Router for Tally {
    type Input = usize;
    type State = (usize, usize);
    type Output = (usize, usize);
    fn route(state: &mut (usize, usize), size: usize, _out: &mut Emit) {
        state.0 += 1;
        state.1 += size;
    }
    fn merge(left: &mut (usize, usize), right: (usize, usize)) {
        left.0 += right.0;
        left.1 += right.1;
    }
    fn finalize(state: (usize, usize)) -> (usize, usize) {
        state
    }
}

struct FeedItems;
#[async_trait]
impl Worker for FeedItems {
    type Task = Item;
    async fn process(&self, item: &Item, ctx: &Context) -> anyhow::Result<()> {
        ctx.route::<Batcher>(item.0).await?;
        Ok(())
    }
}

struct Consume;
#[async_trait]
impl Worker for Consume {
    type Task = Batch;
    async fn process(&self, batch: &Batch, ctx: &Context) -> anyhow::Result<()> {
        ctx.route::<Tally>(batch.0.len()).await?;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_batches_and_flushes_the_trailing_batch() {
    let mut builder = Engine::builder()
        .worker(FeedItems)
        .worker(Consume)
        .router::<Batcher>()
        .router::<Tally>()
        .concurrency(1); // single shard, so batching is deterministic
    for i in 0..7 {
        builder = builder.seed(Item(i));
    }
    let report = builder.run().await.unwrap();

    let (batches, items) = *report.output::<Tally>().unwrap();
    assert_eq!(items, 7, "every item must reach a batch");
    assert_eq!(
        batches, 3,
        "two full batches of 3 plus one flushed trailing batch of 1"
    );
    assert_eq!(report.stats.failed, 0);
}
