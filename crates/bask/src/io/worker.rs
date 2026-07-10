/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use super::{Keyed, ReadOptions, Sink, SinkRegistry, SourceRegistry, WriteOptions};
use crate::context::Context;
use crate::worker::Worker;

/// A seed task naming a target to read; routed to the [`SourceWorker`] for `Item`.
/// Generic in `Item` so record and blob source workers never share a routing key.
pub struct Read<Item> {
    pub target: String,
    pub options: ReadOptions,
    _marker: PhantomData<fn() -> Item>,
}

impl<Item> Read<Item> {
    pub fn new(target: impl Into<String>) -> Self {
        Read {
            target: target.into(),
            options: ReadOptions::default(),
            _marker: PhantomData,
        }
    }

    pub fn chunk_rows(mut self, rows: usize) -> Self {
        self.options.chunk_rows = rows;
        self
    }
}

/// Bridges a [`SourceRegistry`] into the pipeline: opens the source named by each
/// [`Read`] task and emits its items downstream under the #4 backpressure.
pub struct SourceWorker<Item> {
    registry: Arc<SourceRegistry<Item>>,
}

impl<Item: 'static> SourceWorker<Item> {
    pub fn new(registry: SourceRegistry<Item>) -> Self {
        SourceWorker {
            registry: Arc::new(registry),
        }
    }

    pub fn shared(registry: Arc<SourceRegistry<Item>>) -> Self {
        SourceWorker { registry }
    }
}

#[async_trait]
impl<Item> Worker for SourceWorker<Item>
where
    Item: Send + Sync + 'static,
{
    type Task = Read<Item>;

    async fn process(&self, task: &Read<Item>, ctx: &Context) -> anyhow::Result<()> {
        let mut source = self.registry.open(&task.target, &task.options)?;
        while let Some(item) = source.next().await? {
            ctx.emit(item).await?;
        }
        Ok(())
    }
}

/// Bridges a single opened [`Sink`] out of the pipeline: writes each `Keyed<Item>` it
/// receives and finalizes the sink once the run drains, via `on_stop`.
pub struct SinkWorker<Item> {
    sink: Mutex<Option<Box<dyn Sink<Item>>>>,
}

impl<Item: Send + Sync + 'static> SinkWorker<Item> {
    pub fn new(sink: Box<dyn Sink<Item>>) -> Self {
        SinkWorker {
            sink: Mutex::new(Some(sink)),
        }
    }

    pub fn open(registry: &SinkRegistry<Item>, target: &str) -> anyhow::Result<Self> {
        Self::open_with(registry, target, WriteOptions::default())
    }

    pub fn open_with(
        registry: &SinkRegistry<Item>,
        target: &str,
        options: WriteOptions,
    ) -> anyhow::Result<Self> {
        Ok(Self::new(registry.open(target, &options)?))
    }
}

#[async_trait]
impl<Item> Worker for SinkWorker<Item>
where
    Item: Send + Sync + 'static,
{
    type Task = Keyed<Item>;

    async fn process(&self, item: &Keyed<Item>, _ctx: &Context) -> anyhow::Result<()> {
        let mut guard = self.sink.lock().await;
        let sink = guard
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("sink already finished"))?;
        sink.write(item).await
    }

    async fn on_stop(&self) -> anyhow::Result<()> {
        let mut guard = self.sink.lock().await;
        if let Some(mut sink) = guard.take() {
            sink.finish().await?;
        }
        Ok(())
    }
}
