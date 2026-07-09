/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::time::Duration;

/// How failed tasks are retried, and which instance the retry lands on.
#[derive(Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub on_retry: InstanceChoice,
    pub backoff: Backoff,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            on_retry: InstanceChoice::AvoidFailed,
            backoff: Backoff::None,
        }
    }
}

impl RetryPolicy {
    pub fn new() -> Self {
        Self::default()
    }
    /// Total tries including the first (1 = no retry).
    pub fn max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n.max(1);
        self
    }
    pub fn avoid_failed(mut self) -> Self {
        self.on_retry = InstanceChoice::AvoidFailed;
        self
    }
    pub fn any_instance(mut self) -> Self {
        self.on_retry = InstanceChoice::Any;
        self
    }
    pub fn backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }
    pub(crate) fn delay(&self, attempt: u32) -> Option<Duration> {
        self.backoff.delay(attempt)
    }
}

/// Which instance a retry is dispatched to.
#[derive(Clone, Copy)]
pub enum InstanceChoice {
    /// Memorize failed instances and skip them; reset once all are exhausted.
    AvoidFailed,
    /// Retry on any instance, including the one that just failed.
    Any,
}

#[derive(Clone, Copy)]
pub enum Backoff {
    None,
    Fixed(Duration),
    Exponential { base: Duration, factor: f64, max: Duration },
}

impl Backoff {
    pub(crate) fn delay(&self, attempt: u32) -> Option<Duration> {
        match *self {
            Backoff::None => None,
            Backoff::Fixed(d) => Some(d),
            Backoff::Exponential { base, factor, max } => {
                let mult = factor.powi(attempt.saturating_sub(1) as i32);
                Some(base.mul_f64(mult).min(max))
            }
        }
    }
}
