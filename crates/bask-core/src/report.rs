/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

use crate::dedup::Dedup;
use crate::router::Router;

#[derive(Default)]
pub(crate) struct AtomicStats {
    pub processed: AtomicU64,
    pub retried: AtomicU64,
    pub failed: AtomicU64,
    pub skipped: AtomicU64,
}

impl AtomicStats {
    pub fn snapshot(&self) -> Stats {
        Stats {
            processed: self.processed.load(SeqCst),
            retried: self.retried.load(SeqCst),
            failed: self.failed.load(SeqCst),
            skipped: self.skipped.load(SeqCst),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub processed: u64,
    pub retried: u64,
    pub failed: u64,
    /// Checkpoint hits already in the store, so the work was not redone.
    pub skipped: u64,
}

#[derive(Debug, Clone)]
pub struct TaskFailure {
    pub task_type: &'static str,
    pub instance: String,
    pub attempts: u32,
    pub error: String,
}

/// The outcome of a run: router outputs, counters, and terminal failures.
pub struct RunReport {
    pub(crate) outputs: HashMap<TypeId, Box<dyn Any + Send>>,
    pub(crate) unique: HashMap<TypeId, usize>,
    pub stats: Stats,
    pub failures: Vec<TaskFailure>,
    /// Whether a shutdown was requested before the run drained on its own.
    pub interrupted: bool,
    /// Tasks that existed but were never processed (abandoned queue plus cancelled work).
    pub unfinished: usize,
}

impl RunReport {
    pub fn output<R: Router>(&self) -> Option<&R::Output> {
        self.outputs
            .get(&TypeId::of::<R>())
            .and_then(|b| b.downcast_ref::<R::Output>())
    }

    /// The number of distinct keys admitted by dedup set `D`.
    pub fn unique<D: Dedup>(&self) -> usize {
        self.unique.get(&TypeId::of::<D>()).copied().unwrap_or(0)
    }
}
