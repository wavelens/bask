/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use crate::resource::{Attrs, Select};

/// A routing hint a worker attaches to its error to steer the retry, resolved by the
/// [`RetryPolicy`] into an instance-selection constraint. `Fatal` skips retry entirely.
#[derive(Clone)]
pub enum RetryOn {
    SameInstance,
    DifferentInstance,
    DifferentAttr(String),
    AnyWith(Arc<dyn Fn(&Attrs) -> bool + Send + Sync>),
    Fatal,
}

/// Attach a [`RetryOn`] hint to a failing result, e.g.
/// `gpu_run().retry_on(RetryOn::DifferentAttr("gpu".into()))?` or `parse().fatal()?`.
pub trait RetryExt<T> {
    fn retry_on(self, on: RetryOn) -> anyhow::Result<T>;
    fn fatal(self) -> anyhow::Result<T>;
}

impl<T, E: Into<anyhow::Error>> RetryExt<T> for Result<T, E> {
    fn retry_on(self, on: RetryOn) -> anyhow::Result<T> {
        self.map_err(|e| {
            let e = e.into();
            anyhow::Error::from(Hint {
                on,
                message: format!("{e:#}"),
            })
        })
    }
    fn fatal(self) -> anyhow::Result<T> {
        self.retry_on(RetryOn::Fatal)
    }
}

/// The error carrier that threads a [`RetryOn`] through `anyhow`, flattening the source
/// message so `{:#}` still shows it after the hint is stripped off on the retry path.
struct Hint {
    on: RetryOn,
    message: String,
}

impl fmt::Debug for Hint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl fmt::Display for Hint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for Hint {}

/// The scheduler's decision for a failed task: retry with a selection constraint and an
/// optional backoff delay, or give up and route it to the dead-letter sink.
pub(crate) enum Decision {
    Retry {
        select: Select,
        delay: Option<Duration>,
    },
    Fail,
}

/// How failed tasks are retried: how many attempts, where the retry lands, and the
/// backoff between tries. Set on the engine as a default and overridden per worker with
/// [`WorkerCfg::retry`](crate::WorkerCfg::retry); an error's [`RetryOn`] hint wins over
/// the policy's default selection.
#[derive(Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub on_retry: Select,
    pub backoff: Backoff,
    pub jitter: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            on_retry: Select::AvoidTried,
            backoff: Backoff::None,
            jitter: 0.0,
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
    /// The default selection when the error carries no [`RetryOn`] hint.
    pub fn on_retry(mut self, select: Select) -> Self {
        self.on_retry = select;
        self
    }
    pub fn avoid_failed(self) -> Self {
        self.on_retry(Select::AvoidTried)
    }
    pub fn any_instance(self) -> Self {
        self.on_retry(Select::Any)
    }
    pub fn backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }
    /// Randomly shorten each backoff by up to `fraction` (0..1) to spread retries and
    /// avoid a thundering herd.
    pub fn jitter(mut self, fraction: f64) -> Self {
        self.jitter = fraction.clamp(0.0, 1.0);
        self
    }

    /// Resolve a failure into the next action, honouring a [`RetryOn`] hint over the
    /// policy default and treating `Fatal`/exhaustion as terminal.
    pub(crate) fn decide(&self, next_attempt: u32, err: &anyhow::Error) -> Decision {
        let hint = err.downcast_ref::<Hint>().map(|h| &h.on);
        let policy_violation = err
            .downcast_ref::<crate::Error>()
            .is_some_and(|e| matches!(e, crate::Error::EmitNotAllowed { .. }));
        if policy_violation
            || matches!(hint, Some(RetryOn::Fatal))
            || next_attempt >= self.max_attempts
        {
            return Decision::Fail;
        }
        let select = match hint {
            Some(RetryOn::SameInstance) => Select::SameInstance,
            Some(RetryOn::DifferentInstance) => Select::AvoidTried,
            Some(RetryOn::DifferentAttr(k)) => Select::DifferentAttr(k.clone()),
            Some(RetryOn::AnyWith(p)) => Select::Where(p.clone()),
            Some(RetryOn::Fatal) | None => self.on_retry.clone(),
        };
        Decision::Retry {
            select,
            delay: self.delay(next_attempt),
        }
    }

    fn delay(&self, attempt: u32) -> Option<Duration> {
        self.backoff
            .delay(attempt)
            .map(|d| d.mul_f64(jitter_factor(self.jitter)))
    }
}

#[derive(Clone, Copy)]
pub enum Backoff {
    None,
    Fixed(Duration),
    Exponential {
        base: Duration,
        factor: f64,
        max: Duration,
    },
}

impl Backoff {
    fn delay(&self, attempt: u32) -> Option<Duration> {
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

/// A dependency-free jitter multiplier in `[1 - fraction, 1]`, drawn from a
/// process-global splitmix64 stream so concurrent retries scatter rather than align.
fn jitter_factor(fraction: f64) -> f64 {
    if fraction <= 0.0 {
        return 1.0;
    }
    static STATE: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);
    let mut z = STATE.fetch_add(0x9E37_79B9_7F4A_7C15, Relaxed);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    let unit = (z >> 11) as f64 / (1u64 << 53) as f64;
    1.0 - fraction * unit
}
