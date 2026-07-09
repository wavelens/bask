/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Dedup + instance-aware retry. Raw links (with duplicates) are deduped into one
//! Fetch per distinct URL; two proxy instances fetch each with failover, and a
//! host no proxy can serve exhausts its retries and lands in the failure list.
use std::time::Duration;

use bask::prelude::*;

struct Link {
    url: String,
}
struct Fetch {
    url: String,
}

/// The dedup set: admit each URL once. Its marker type carries the key type.
struct SeenUrls;
impl Dedup for SeenUrls {
    type Key = String;
}

/// Gate emission on the dedup set, so each distinct URL becomes a single Fetch.
struct Dedupe;
#[async_trait]
impl Worker for Dedupe {
    type Task = Link;
    async fn process(&self, link: &Link, ctx: &Context) -> anyhow::Result<()> {
        if ctx.first_seen::<SeenUrls>(link.url.clone()) {
            ctx.emit(Fetch { url: link.url.clone() }).await?;
        }
        Ok(())
    }
}

/// One worker instance bound to a proxy that cannot reach some host suffixes.
struct Proxy {
    name: &'static str,
    blocks: &'static [&'static str],
}
#[async_trait]
impl Worker for Proxy {
    type Task = Fetch;
    async fn process(&self, fetch: &Fetch, ctx: &Context) -> anyhow::Result<()> {
        if self.blocks.iter().any(|suffix| fetch.url.ends_with(*suffix)) {
            anyhow::bail!("{} cannot reach {}", self.name, fetch.url);
        }
        ctx.aggregate::<Served>((fetch.url.clone(), self.name.to_string()));
        Ok(())
    }
}

/// Records which proxy ultimately served each url.
struct Served;
impl Aggregator for Served {
    type Input = (String, String);
    type State = Vec<(String, String)>;
    type Output = Vec<(String, String)>;
    fn fold(state: &mut Self::State, hit: (String, String)) {
        state.push(hit);
    }
    fn merge(left: &mut Self::State, right: Self::State) {
        left.extend(right);
    }
    fn finalize(mut state: Self::State) -> Self::Output {
        state.sort();
        state
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let backoff = Backoff::Exponential {
        base: Duration::from_millis(10),
        factor: 2.0,
        max: Duration::from_millis(100),
    };

    let links = ["a.com", "b.ru", "a.com", "c.cn", "d.onion", "b.ru"];
    let mut builder = Engine::builder()
        .worker(Dedupe)
        .worker_cfg(Proxy { name: "eu", blocks: &[".ru", ".onion"] }, WorkerCfg::new().label("eu"))
        .worker_cfg(Proxy { name: "us", blocks: &[".cn", ".onion"] }, WorkerCfg::new().label("us"))
        .aggregator::<Served>()
        .dedup::<SeenUrls>()
        .retry(RetryPolicy::new().max_attempts(3).avoid_failed().backoff(backoff))
        .concurrency(1);
    for url in links {
        builder = builder.seed(Link { url: url.to_string() });
    }
    let report = builder.run().await?;

    println!("seeded {} links, {} unique urls", links.len(), report.unique::<SeenUrls>());
    println!("served:");
    for (url, proxy) in report.output::<Served>().unwrap() {
        println!("  {url:8} via {proxy}");
    }
    println!("\nfailed:");
    for failure in &report.failures {
        println!("  after {} attempts: {}", failure.attempts, failure.error);
    }
    println!("\nstats: {:?}", report.stats);
    Ok(())
}
