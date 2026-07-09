/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! bask — build any data pipeline as an emergent graph of tasks and workers.
//!
//! Two orthogonal planes: a compute plane of independent [`Worker`]s that consume
//! typed [`Task`]s and emit more, and an aggregation plane of [`Aggregator`]s that
//! collect results outside the worker graph.

mod aggregator;
mod context;
mod dedup;
mod engine;
mod error;
mod metrics;
mod monitor;
mod registry;
mod report;
mod retry;
mod scheduler;
mod task;
mod worker;

pub use aggregator::Aggregator;
pub use context::Context;
pub use dedup::Dedup;
pub use engine::{Engine, EngineBuilder};
pub use error::{Error, Result};
pub use metrics::{Snapshot, WorkerStat};
pub use monitor::{LiveConsole, Monitor};
pub use report::{RunReport, Stats, TaskFailure};
pub use retry::{Backoff, InstanceChoice, RetryPolicy};
pub use scheduler::Emitter;
pub use task::Task;
pub use worker::{DynWorker, Worker, WorkerCfg};

pub mod prelude {
    pub use crate::{
        Aggregator, Backoff, Context, Dedup, Engine, InstanceChoice, LiveConsole, Monitor,
        RetryPolicy, RunReport, Snapshot, Task, Worker, WorkerCfg,
    };
    pub use anyhow;
    pub use async_trait::async_trait;
}
