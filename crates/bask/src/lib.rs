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
