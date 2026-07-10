/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::any::Any;

use async_trait::async_trait;

use crate::context::Context;
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

/// Per-instance registration options: a display label and a concurrency cap.
#[derive(Default)]
pub struct WorkerCfg {
    pub(crate) label: Option<String>,
    pub(crate) concurrency: Option<usize>,
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
}
