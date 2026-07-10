/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::collections::HashMap;

use bask::prelude::*;

struct Document {
    text: String,
}
struct Word(String);

struct Split;
#[async_trait]
impl Worker for Split {
    type Task = Document;
    async fn process(&self, doc: &Document, ctx: &Context) -> anyhow::Result<()> {
        for word in doc.text.split_whitespace() {
            ctx.emit(Word(word.to_lowercase())).await?;
        }
        Ok(())
    }
}

struct Count;
#[async_trait]
impl Worker for Count {
    type Task = Word;
    async fn process(&self, word: &Word, ctx: &Context) -> anyhow::Result<()> {
        ctx.aggregate::<WordCount>(word.0.clone());
        Ok(())
    }
}

struct WordCount;
impl Aggregator for WordCount {
    type Input = String;
    type State = HashMap<String, u64>;
    type Output = HashMap<String, u64>;
    fn fold(state: &mut Self::State, word: String) {
        *state.entry(word).or_default() += 1;
    }
    fn merge(left: &mut Self::State, right: Self::State) {
        for (word, n) in right {
            *left.entry(word).or_default() += n;
        }
    }
    fn finalize(state: Self::State) -> Self::Output {
        state
    }
}

#[tokio::test]
async fn counts_words_across_emitted_tasks() {
    let report = Engine::builder()
        .worker(Split)
        .worker(Count)
        .aggregator::<WordCount>()
        .seed(Document {
            text: "a b a c b a".to_string(),
        })
        .run()
        .await
        .unwrap();

    let counts = report.output::<WordCount>().unwrap();
    assert_eq!(counts.get("a"), Some(&3));
    assert_eq!(counts.get("b"), Some(&2));
    assert_eq!(counts.get("c"), Some(&1));
    assert_eq!(report.stats.failed, 0);
}

struct Ping;

struct Flaky {
    fail: bool,
}
#[async_trait]
impl Worker for Flaky {
    type Task = Ping;
    async fn process(&self, _task: &Ping, ctx: &Context) -> anyhow::Result<()> {
        if self.fail {
            anyhow::bail!("instance is down");
        }
        ctx.aggregate::<Hits>(1);
        Ok(())
    }
}

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

#[tokio::test]
async fn retry_lands_on_a_different_instance() {
    let report = Engine::builder()
        .worker_cfg(Flaky { fail: true }, WorkerCfg::new().label("proxy-A"))
        .worker_cfg(Flaky { fail: false }, WorkerCfg::new().label("proxy-B"))
        .aggregator::<Hits>()
        .retry(RetryPolicy::new().max_attempts(2).avoid_failed())
        .concurrency(1)
        .seed(Ping)
        .run()
        .await
        .unwrap();

    assert_eq!(*report.output::<Hits>().unwrap(), 1);
    assert!(
        report.failures.is_empty(),
        "unexpected failures: {:?}",
        report.failures
    );
    assert_eq!(report.stats.retried, 1);
    assert_eq!(report.stats.processed, 1);
}

#[tokio::test]
async fn exhausted_retries_are_reported() {
    let report = Engine::builder()
        .worker(Flaky { fail: true })
        .aggregator::<Hits>()
        .retry(RetryPolicy::new().max_attempts(3))
        .seed(Ping)
        .run()
        .await
        .unwrap();

    assert_eq!(report.output::<Hits>().copied(), Some(0));
    assert_eq!(report.stats.failed, 1);
    assert_eq!(report.stats.retried, 2);
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].attempts, 3);
}
