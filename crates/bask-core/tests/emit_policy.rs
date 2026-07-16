/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use bask_core::prelude::*;
use bask_core::{DeadLetter, DeadLetterSink, DynWorker, EmitPolicy};

struct Job;
struct Step;
struct Forbidden;

impl EmitPolicy for Job {
    fn declare(allow: &mut Allow) {
        allow.allow::<Step>();
    }
}

struct StepWorker;
#[async_trait]
impl Worker for StepWorker {
    type Task = Step;
    async fn process(&self, _step: &Step, _ctx: &Context) -> anyhow::Result<()> {
        Ok(())
    }
}

struct EmitsStep;
#[async_trait]
impl Worker for EmitsStep {
    type Task = Job;
    async fn process(&self, _job: &Job, ctx: &Context) -> anyhow::Result<()> {
        ctx.emit(Step).await?;
        Ok(())
    }
}

struct EmitsForbidden;
#[async_trait]
impl Worker for EmitsForbidden {
    type Task = Job;
    async fn process(&self, _job: &Job, ctx: &Context) -> anyhow::Result<()> {
        ctx.emit(Forbidden).await?;
        Ok(())
    }
}

struct Capture(Arc<AtomicUsize>);
impl DeadLetterSink for Capture {
    fn dead_letter(&self, _letter: DeadLetter) {
        self.0.fetch_add(1, SeqCst);
    }
}

#[tokio::test]
async fn allowed_emit_succeeds() {
    let report = Engine::builder()
        .worker(EmitsStep)
        .worker(StepWorker)
        .emit_policy::<Job>()
        .seed(Job)
        .run()
        .await
        .unwrap();
    assert_eq!(report.stats.failed, 0);
    assert_eq!(report.stats.processed, 2);
}

#[tokio::test]
async fn disallowed_emit_fails_terminally_without_retry() {
    let captured = Arc::new(AtomicUsize::new(0));
    let report = Engine::builder()
        .retry(RetryPolicy::new().max_attempts(5))
        .dead_letter(Capture(captured.clone()))
        .worker(EmitsForbidden)
        .emit_policy::<Job>()
        .seed(Job)
        .run()
        .await
        .unwrap();
    assert_eq!(report.stats.failed, 1);
    assert_eq!(report.stats.retried, 0);
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].attempts, 1);
    assert!(report.failures[0].error.contains("may not emit"));
    assert_eq!(captured.load(SeqCst), 1);
}

struct ForbiddenWorker;
#[async_trait]
impl Worker for ForbiddenWorker {
    type Task = Forbidden;
    async fn process(&self, _forbidden: &Forbidden, _ctx: &Context) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn unconstrained_task_emits_freely() {
    let report = Engine::builder()
        .worker(EmitsForbidden)
        .worker(ForbiddenWorker)
        .seed(Job)
        .run()
        .await
        .unwrap();
    assert_eq!(report.stats.failed, 0);
}

struct DynEmit {
    target: u64,
}
#[async_trait]
impl DynWorker for DynEmit {
    async fn process(
        &self,
        _payload: &(dyn std::any::Any + Send + Sync),
        ctx: &Context,
    ) -> anyhow::Result<()> {
        ctx.emitter()
            .emit_dyn(self.target, "Target", Box::new(()))?;
        Ok(())
    }
}

struct DynSink;
#[async_trait]
impl DynWorker for DynSink {
    async fn process(
        &self,
        _payload: &(dyn std::any::Any + Send + Sync),
        _ctx: &Context,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn dynamic_emit_policy_is_enforced() {
    let report = Engine::builder()
        .worker_dyn(1, Arc::new(DynEmit { target: 2 }), "Src", WorkerCfg::new())
        .emit_policy_dyn(1, "Src", vec![])
        .seed_dyn(1, "Src", Box::new(()))
        .run()
        .await
        .unwrap();
    assert_eq!(report.stats.failed, 1);
    assert!(report.failures[0].error.contains("may not emit"));

    let report = Engine::builder()
        .worker_dyn(1, Arc::new(DynEmit { target: 2 }), "Src", WorkerCfg::new())
        .worker_dyn(2, Arc::new(DynSink), "Target", WorkerCfg::new())
        .emit_policy_dyn(1, "Src", vec![(2, "Target")])
        .seed_dyn(1, "Src", Box::new(()))
        .run()
        .await
        .unwrap();
    assert_eq!(report.stats.failed, 0);
}
