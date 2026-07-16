/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Bask core - the task-queue pipeline engine.
//!
//! Two orthogonal planes: a compute plane of independent [`Worker`]s that consume
//! typed [`Task`]s and emit more, and a routing plane of [`Router`]s that fold a task
//! stream into state and may emit, route, filter, or batch derived tasks. The IO plane,
//! formats, and predefined tasks build on this crate; most users depend on `bask`.

mod checkpoint;
#[cfg(feature = "cli")]
pub mod cli;
mod context;
mod deadletter;
mod dedup;
mod emit_policy;
mod engine;
mod error;
mod interrupt;
mod metrics;
mod monitor;
mod registry;
mod report;
mod resource;
mod retry;
mod router;
mod scheduler;
#[cfg(feature = "checkpoint")]
mod sqlite;
mod task;
mod worker;

pub use checkpoint::{
    CheckpointOps, Committed, Coverage, Dataset, MemStore, Status, Store, StoredItem,
};
pub use context::Context;
pub use deadletter::{DeadLetter, DeadLetterSink};
pub use dedup::Dedup;
pub use emit_policy::{Allow, EmitPolicy};
pub use engine::{Engine, EngineBuilder, TaskInfo};
pub use error::{Error, Result};
pub use interrupt::{Cancellation, Shutdown};
pub use metrics::{Snapshot, WorkerStat};
pub use monitor::{JsonConsole, LiveConsole, Monitor};
pub use report::{RunReport, Stats, TaskFailure};
pub use resource::{Attrs, Select};
pub use retry::{Backoff, RetryExt, RetryOn, RetryPolicy};
pub use router::{Emit, Router};
pub use scheduler::Emitter;
pub use task::Task;
pub use worker::{DynWorker, Worker, WorkerCfg};

#[cfg(feature = "checkpoint")]
pub use bask_macros::Checkpoint;
#[cfg(feature = "macros")]
pub use bask_macros::EmitPolicy;
#[cfg(feature = "checkpoint")]
pub use checkpoint::{Checkpoint, CheckpointInfo};
#[cfg(feature = "macros")]
pub use emit_policy::EmitPolicyInfo;
#[cfg(feature = "macros")]
pub use inventory;
#[cfg(feature = "checkpoint")]
pub use sqlite::SqliteStore;

pub mod prelude {
    pub use crate::{
        Allow, Attrs, Backoff, Context, DeadLetter, DeadLetterSink, Dedup, Emit, EmitPolicy,
        Engine, LiveConsole, Monitor, RetryExt, RetryOn, RetryPolicy, Router, RunReport, Select,
        Shutdown, Snapshot, Task, Worker, WorkerCfg,
    };
    pub use anyhow;
    pub use async_trait::async_trait;
}
