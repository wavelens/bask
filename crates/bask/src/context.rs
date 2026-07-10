/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::sync::Arc;

use crate::aggregator::{Aggregator, Aggregators};
use crate::dedup::{Dedup, Dedups};
use crate::interrupt::Cancel;
use crate::scheduler::{InFlight, Queue, RunSlot, Sent};
use crate::task::{Envelope, Task};

/// Handed to every worker. Its only powers: spawn more work, contribute to aggregation.
pub struct Context {
    pub(crate) queue: Queue,
    pub(crate) in_flight: Arc<InFlight>,
    pub(crate) aggregators: Arc<Aggregators>,
    pub(crate) dedups: Arc<Dedups>,
    pub(crate) shard: usize,
    pub(crate) run: Arc<RunSlot>,
    pub(crate) cancel: Cancel,
}

impl Context {
    /// Enqueue a new task of any type into the shared queue, applying backpressure:
    /// on a full queue the worker yields its run permit so the dispatcher can drain,
    /// then reacquires it before returning, which keeps memory bounded without deadlock.
    pub async fn emit<T: Task>(&self, task: T) -> crate::Result<()> {
        self.in_flight.inc();
        match self.queue.try_send(Envelope::new(task)) {
            Sent::Ok => Ok(()),
            Sent::Full(env) => {
                self.run.release();
                let sent = self.queue.send(env).await;
                self.run.reacquire().await;
                match sent {
                    Ok(()) => Ok(()),
                    Err(_) => {
                        self.in_flight.dec();
                        Err(crate::Error::Stopped)
                    }
                }
            }
            Sent::Closed => {
                self.in_flight.dec();
                Err(crate::Error::Stopped)
            }
        }
    }

    /// Contribute a value to an aggregator registered on the engine.
    pub fn aggregate<A: Aggregator>(&self, input: A::Input) {
        self.aggregators.fold::<A>(self.shard, input);
    }

    /// Test-and-set against a dedup set: `true` the first time `key` is seen, `false`
    /// after. Gate emission with it to admit each distinct task once.
    pub fn first_seen<D: Dedup>(&self, key: D::Key) -> bool {
        self.dedups.first_seen::<D>(key)
    }

    /// Whether a shutdown has escalated to cancellation; long-running workers should
    /// poll this (or await [`cancelled`](Context::cancelled)) and return early.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Resolves once the run is cancelled; select against it to abort a long operation.
    pub async fn cancelled(&self) {
        self.cancel.cancelled().await;
    }
}
