/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! One-shot latches that drive graceful interruption: [`Shutdown`] is the public
//! request to wind a run down; [`Cancel`] is the engine's hard-abort escalation.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};

use tokio::sync::Notify;

/// A clonable boolean that can be set once and awaited by many observers.
#[derive(Clone, Default)]
struct Latch {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    set: AtomicBool,
    notify: Notify,
}

impl Latch {
    fn set(&self) {
        if !self.inner.set.swap(true, SeqCst) {
            self.inner.notify.notify_waiters();
        }
    }

    fn is_set(&self) -> bool {
        self.inner.set.load(SeqCst)
    }

    async fn wait(&self) {
        if self.is_set() {
            return;
        }
        let notified = self.inner.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.is_set() {
            return;
        }
        notified.await;
    }
}

/// A clonable handle used to request a graceful shutdown of a running engine.
/// Pass it via [`EngineBuilder::shutdown`](crate::EngineBuilder::shutdown) and call
/// [`trigger`](Shutdown::trigger) from a signal handler or any other task.
#[derive(Clone, Default)]
pub struct Shutdown {
    latch: Latch,
}

impl Shutdown {
    pub fn new() -> Self {
        Self::default()
    }

    /// Request the run to stop pulling new work and drain within its grace period.
    pub fn trigger(&self) {
        self.latch.set();
    }

    pub fn is_triggered(&self) -> bool {
        self.latch.is_set()
    }

    pub(crate) async fn triggered(&self) {
        self.latch.wait().await;
    }
}

/// The engine's in-flight abort signal, exposed to workers read-only via
/// [`Context`](crate::Context) for cooperative cancellation.
#[derive(Clone, Default)]
pub(crate) struct Cancel {
    latch: Latch,
}

impl Cancel {
    pub(crate) fn cancel(&self) {
        self.latch.set();
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.latch.is_set()
    }

    pub(crate) async fn cancelled(&self) {
        self.latch.wait().await;
    }
}
