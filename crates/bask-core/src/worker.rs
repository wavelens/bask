/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::any::Any;
use std::time::Duration;

use async_trait::async_trait;

use crate::context::Context;
use crate::resource::Attrs;
use crate::retry::RetryPolicy;
use crate::task::Task;

/// A unit of the compute plane: consumes one task type, may emit more via `ctx`.
#[async_trait]
pub trait Worker: Send + Sync + 'static {
    type Task: Task;

    /// Called once per instance before the run; use it for params that need setup
    /// (open a proxy connection, authenticate).
    async fn on_start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn process(&self, task: &Self::Task, ctx: &Context) -> anyhow::Result<()>;

    async fn on_stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// The registry-facing, type-erased worker. Implement this directly to build a
/// dynamic front-end (the Python bindings do); typed [`Worker`]s get it for free.
#[async_trait]
pub trait DynWorker: Send + Sync + 'static {
    async fn on_start(&self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn process(&self, payload: &(dyn Any + Send + Sync), ctx: &Context)
    -> anyhow::Result<()>;
    async fn on_stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

pub(crate) struct Holder<W: Worker>(pub W);

#[async_trait]
impl<W: Worker> DynWorker for Holder<W> {
    async fn on_start(&self) -> anyhow::Result<()> {
        self.0.on_start().await
    }

    async fn process(
        &self,
        payload: &(dyn Any + Send + Sync),
        ctx: &Context,
    ) -> anyhow::Result<()> {
        let task = payload.downcast_ref::<W::Task>().ok_or_else(|| {
            anyhow::anyhow!("payload is not {}", std::any::type_name::<W::Task>())
        })?;
        self.0.process(task, ctx).await
    }

    async fn on_stop(&self) -> anyhow::Result<()> {
        self.0.on_stop().await
    }
}

/// Per-instance registration options: a display label, a concurrency cap, a per-task
/// timeout after which `process` is cancelled and routed through retry, the resource
/// attributes selection reads, the named resource pools the instance draws a permit
/// from, and an optional retry policy overriding the engine default.
#[derive(Default)]
pub struct WorkerCfg {
    pub(crate) label: Option<String>,
    pub(crate) concurrency: Option<usize>,
    pub(crate) timeout: Option<Duration>,
    pub(crate) attrs: Attrs,
    pub(crate) requires: Vec<String>,
    pub(crate) retry: Option<RetryPolicy>,
}

impl WorkerCfg {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = Some(n);
        self
    }
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
    /// Tag this instance with a resource attribute (e.g. `attr("gpu", "a100")`) that
    /// attribute-aware retry selection can match on.
    pub fn attr(mut self, key: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        self.attrs.set(key.as_ref(), value.as_ref());
        self
    }
    /// Require a permit from a named resource pool declared with
    /// [`EngineBuilder::resource`](crate::EngineBuilder::resource) before each task runs.
    pub fn requires(mut self, resource: impl Into<String>) -> Self {
        self.requires.push(resource.into());
        self
    }
    /// Override the engine's default retry policy for this instance's failures.
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = Some(retry);
        self
    }
}
