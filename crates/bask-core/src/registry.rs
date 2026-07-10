/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::resource::{Attrs, Select};
use crate::retry::RetryPolicy;
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
    pub timeout: Option<Duration>,
    pub attrs: Attrs,
    pub resources: Vec<Arc<Semaphore>>,
    pub retry: Option<RetryPolicy>,
}

/// All instances registered for a single task type, plus live counters for observation.
pub(crate) struct Group {
    pub instances: Vec<Instance>,
    pub worker_type: &'static str,
    pub queued: AtomicUsize,
    pub processed: AtomicU64,
}

impl Group {
    /// Least-loaded instance the `select` constraint admits, evaluated against the
    /// instance the task `last` ran on so same/different-attribute retries have a
    /// reference point.
    pub fn select(
        &self,
        tried: TriedMask,
        last: Option<u16>,
        select: &Select,
    ) -> Option<&Instance> {
        let last_attrs = last
            .and_then(|id| self.instances.iter().find(|i| i.id == id))
            .map(|i| &i.attrs);
        self.instances
            .iter()
            .filter(|i| select.eligible(i.id, &i.attrs, tried, last, last_attrs))
            .min_by_key(|i| i.active.load(SeqCst))
    }
}

#[derive(Default)]
pub(crate) struct Registry {
    pub groups: HashMap<RouteKey, Group>,
}
