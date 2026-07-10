/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::any::Any;

/// A task that exhausted its retries or was marked [`RetryOn::Fatal`](crate::RetryOn).
/// Its payload rides along type-erased so a sink can persist, log, or re-route it.
pub struct DeadLetter {
    pub task_type: &'static str,
    pub payload: Box<dyn Any + Send + Sync>,
    pub attempts: u32,
    pub error: String,
    pub instance: Option<String>,
}

/// Receives tasks that failed terminally; register one with
/// [`EngineBuilder::dead_letter`](crate::EngineBuilder::dead_letter). A bare
/// `Fn(DeadLetter)` is a sink, so a closure suffices for the common case.
pub trait DeadLetterSink: Send + Sync + 'static {
    fn dead_letter(&self, letter: DeadLetter);
}

impl<F: Fn(DeadLetter) + Send + Sync + 'static> DeadLetterSink for F {
    fn dead_letter(&self, letter: DeadLetter) {
        self(letter)
    }
}
