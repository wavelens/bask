/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bask_core::prelude::*;

struct Job(u32);

/// Records which instance (by attribute value) ultimately served the work.
struct Served;
impl Router for Served {
    type Input = &'static str;
    type State = Vec<&'static str>;
    type Output = Vec<&'static str>;
    fn route(state: &mut Self::State, gpu: &'static str, _out: &mut Emit) {
        state.push(gpu);
    }
    fn merge(left: &mut Self::State, right: Self::State) {
        left.extend(right);
    }
    fn finalize(mut state: Self::State) -> Self::Output {
        state.sort();
        state
    }
}

/// A gpu-bound worker: instances given a `hint` fail with it, the rest serve the job.
struct Gpu {
    kind: &'static str,
    hint: Option<RetryOn>,
}
#[async_trait]
impl Worker for Gpu {
    type Task = Job;
    async fn process(&self, _job: &Job, ctx: &Context) -> anyhow::Result<()> {
        match &self.hint {
            Some(on) => Err(anyhow::anyhow!("{} failed", self.kind)).retry_on(on.clone()),
            None => {
                ctx.route::<Served>(self.kind).await?;
                Ok(())
            }
        }
    }
}

#[tokio::test]
async fn retry_hint_steers_to_a_different_attribute() {
    let report = Engine::builder()
        .worker_cfg(
            Gpu {
                kind: "a100",
                hint: Some(RetryOn::DifferentAttr("gpu".into())),
            },
            WorkerCfg::new().attr("gpu", "a100"),
        )
        .worker_cfg(
            Gpu {
                kind: "h100",
                hint: None,
            },
            WorkerCfg::new().attr("gpu", "h100"),
        )
        .router::<Served>()
        .retry(RetryPolicy::new().max_attempts(2))
        .concurrency(1)
        .seed(Job(1))
        .run()
        .await
        .unwrap();

    assert_eq!(report.output::<Served>().unwrap(), &vec!["h100"]);
    assert_eq!(report.stats.retried, 1);
    assert_eq!(report.stats.failed, 0);
}

#[tokio::test]
async fn any_with_predicate_selects_by_attribute() {
    let report = Engine::builder()
        .worker_cfg(
            Gpu {
                kind: "a100",
                hint: Some(RetryOn::AnyWith(Arc::new(|a: &Attrs| {
                    a.get("gpu") == Some("h100")
                }))),
            },
            WorkerCfg::new().attr("gpu", "a100"),
        )
        .worker_cfg(
            Gpu {
                kind: "h100",
                hint: None,
            },
            WorkerCfg::new().attr("gpu", "h100"),
        )
        .router::<Served>()
        .retry(RetryPolicy::new().max_attempts(2))
        .concurrency(1)
        .seed(Job(1))
        .run()
        .await
        .unwrap();

    assert_eq!(report.output::<Served>().unwrap(), &vec!["h100"]);
    assert_eq!(report.stats.failed, 0);
}

/// Two instances sharing a one-permit pool never run at the same time.
struct Busy {
    current: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}
#[async_trait]
impl Worker for Busy {
    type Task = Job;
    async fn process(&self, _job: &Job, _ctx: &Context) -> anyhow::Result<()> {
        let now = self.current.fetch_add(1, SeqCst) + 1;
        self.peak.fetch_max(now, SeqCst);
        tokio::time::sleep(Duration::from_millis(20)).await;
        self.current.fetch_sub(1, SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn resource_permits_cap_concurrency_across_instances() {
    let current = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut builder = Engine::builder()
        .resource("gpu", 1)
        .concurrency(4)
        .worker_cfg(
            Busy {
                current: current.clone(),
                peak: peak.clone(),
            },
            WorkerCfg::new().label("g0").requires("gpu"),
        )
        .worker_cfg(
            Busy {
                current: current.clone(),
                peak: peak.clone(),
            },
            WorkerCfg::new().label("g1").requires("gpu"),
        );
    for i in 0..6 {
        builder = builder.seed(Job(i));
    }
    let report = builder.run().await.unwrap();

    assert_eq!(report.stats.processed, 6);
    assert_eq!(
        peak.load(SeqCst),
        1,
        "a one-permit pool must serialize its instances"
    );
}

struct Boom;
#[async_trait]
impl Worker for Boom {
    type Task = Job;
    async fn process(&self, _job: &Job, _ctx: &Context) -> anyhow::Result<()> {
        Err(anyhow::anyhow!("unrecoverable")).fatal()
    }
}

#[tokio::test]
async fn fatal_skips_retry_and_dead_letter_carries_the_payload() {
    let letters = Arc::new(Mutex::new(Vec::<(u32, u32)>::new()));
    let sink = {
        let letters = letters.clone();
        move |letter: DeadLetter| {
            let id = letter.payload.downcast_ref::<Job>().map_or(0, |j| j.0);
            letters.lock().unwrap().push((id, letter.attempts));
        }
    };
    let report = Engine::builder()
        .worker(Boom)
        .retry(RetryPolicy::new().max_attempts(5))
        .dead_letter(sink)
        .concurrency(1)
        .seed(Job(7))
        .run()
        .await
        .unwrap();

    assert_eq!(report.stats.retried, 0);
    assert_eq!(report.stats.failed, 1);
    assert_eq!(*letters.lock().unwrap(), vec![(7, 1)]);
}

struct Flaky;
#[async_trait]
impl Worker for Flaky {
    type Task = Job;
    async fn process(&self, _job: &Job, _ctx: &Context) -> anyhow::Result<()> {
        anyhow::bail!("always fails")
    }
}

#[tokio::test]
async fn per_worker_policy_overrides_the_engine_default() {
    let report = Engine::builder()
        .worker_cfg(
            Flaky,
            WorkerCfg::new().retry(RetryPolicy::new().max_attempts(1)),
        )
        .retry(RetryPolicy::new().max_attempts(5))
        .concurrency(1)
        .seed(Job(1))
        .run()
        .await
        .unwrap();

    assert_eq!(
        report.stats.retried, 0,
        "a per-worker cap of 1 overrides the engine default of 5"
    );
    assert_eq!(report.stats.failed, 1);
}
