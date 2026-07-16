/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Bask - Build Tasks
//!
//! The batteries-included facade. The engine lives in [`bask_core`] and is re-exported
//! here at the crate root and via [`prelude`]; the pluggable IO plane, columnar formats,
//! and predefined tasks are available as [`io`], [`formats`], and [`tasks`] once their
//! features are on. Depend on `bask` and reach for the submodules you need.

pub use bask_core::*;

/// The pluggable IO plane: sources and sinks selected by extension or URI scheme.
#[cfg(feature = "io")]
pub use bask_io as io;

/// Columnar file formats (Arrow, Parquet, CSV, JSONL) and the record IO adapters.
#[cfg(feature = "formats")]
pub use bask_formats as formats;

/// Predefined workers and routers: row-count batching and chunking.
#[cfg(feature = "formats")]
pub use bask_tasks as tasks;

/// LLM agent workers: models that emit tasks along a source task's EmitPolicy DAG.
#[cfg(feature = "agents")]
pub use bask_agents as agents;

/// Sharded, self-compacting checkpoint datasets: the [`Dataset`](bask_core::Dataset) trait
/// and the directory-backed [`FileDataset`](bask_io::FileDataset) built on it.
#[cfg(feature = "dataset")]
pub mod data {
    pub use bask_core::Dataset;
    pub use bask_io::FileDataset;
}

/// The one Rust CLI frontend: `bask::cli::run(engine, std::env::args())` gives a script the
/// `run` / `list-tasks` / `--tasks` interface, live progress, and `--json`, with `--dataset`
/// wired to a [`FileDataset`](bask_io::FileDataset).
#[cfg(feature = "cli")]
pub mod cli {
    use std::sync::Arc;

    use bask_core::{Dataset, EngineBuilder};
    use bask_io::FileDataset;

    pub use bask_core::cli::Cli;

    /// Parse `args`, apply the run-level flags, and run or `list-tasks`; returns an exit code.
    pub async fn run(builder: EngineBuilder, args: impl IntoIterator<Item = String>) -> i32 {
        Cli::new()
            .dataset_opener(|dir| Ok(Arc::new(FileDataset::open(dir)?) as Arc<dyn Dataset>))
            .run(builder, args)
            .await
    }
}
