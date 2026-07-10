/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Bask - Build Tasks
//!
//! Two orthogonal planes: a compute plane of independent [`Worker`]s that consume
//! typed [`Task`]s and emit more, and a routing plane of [`Router`]s that fold a task
//! stream into state and may emit, route, filter, or batch derived tasks.

mod context;
mod deadletter;
mod dedup;
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
mod task;
mod worker;

#[cfg(feature = "io")]
pub mod io;

#[cfg(feature = "formats")]
pub mod formats;

pub use context::Context;
pub use deadletter::{DeadLetter, DeadLetterSink};
pub use dedup::Dedup;
pub use engine::{Engine, EngineBuilder};
pub use error::{Error, Result};
pub use interrupt::{Cancellation, Shutdown};
pub use metrics::{Snapshot, WorkerStat};
pub use monitor::{LiveConsole, Monitor};
pub use report::{RunReport, Stats, TaskFailure};
pub use resource::{Attrs, Select};
pub use retry::{Backoff, RetryExt, RetryOn, RetryPolicy};
pub use router::{Emit, Router};
pub use scheduler::Emitter;
pub use task::Task;
pub use worker::{DynWorker, Worker, WorkerCfg};

pub mod prelude {
    pub use crate::{
        Attrs, Backoff, Context, DeadLetter, DeadLetterSink, Dedup, Emit, Engine, LiveConsole,
        Monitor, RetryExt, RetryOn, RetryPolicy, Router, RunReport, Select, Shutdown, Snapshot,
        Task, Worker, WorkerCfg,
    };
    pub use anyhow;
    pub use async_trait::async_trait;
}
