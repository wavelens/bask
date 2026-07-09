/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::task::{RouteKey, TriedMask};
use crate::worker::DynWorker;

/// One configured worker within a group (e.g. a fetcher bound to a specific proxy).
pub(crate) struct Instance {
    pub worker: Arc<dyn DynWorker>,
    pub label: String,
    pub id: u16,
    pub permits: Arc<Semaphore>,
    pub capacity: usize,
    pub active: AtomicUsize,
}

/// All instances registered for a single task type, plus live counters for observation.
pub(crate) struct Group {
    pub instances: Vec<Instance>,
    pub worker_type: &'static str,
    pub queued: AtomicUsize,
    pub processed: AtomicU64,
}

impl Group {
    /// Least-loaded eligible instance; `avoid` skips those already tried on retry.
    pub fn select(&self, tried: TriedMask, avoid: bool) -> Option<&Instance> {
        self.instances
            .iter()
            .filter(|i| !avoid || !tried.contains(i.id))
            .min_by_key(|i| i.active.load(SeqCst))
    }
}

#[derive(Default)]
pub(crate) struct Registry {
    pub groups: HashMap<RouteKey, Group>,
}
