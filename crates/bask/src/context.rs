/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::sync::Arc;

use crate::aggregator::{Aggregator, Aggregators};
use crate::dedup::{Dedup, Dedups};
use crate::scheduler::{InFlight, Queue};
use crate::task::{Envelope, Task};

/// Handed to every worker. Its only powers: spawn more work, contribute to aggregation.
pub struct Context {
    pub(crate) queue: Queue,
    pub(crate) in_flight: Arc<InFlight>,
    pub(crate) aggregators: Arc<Aggregators>,
    pub(crate) dedups: Arc<Dedups>,
    pub(crate) shard: usize,
}

impl Context {
    /// Enqueue a new task of any type into the shared queue.
    pub async fn emit<T: Task>(&self, task: T) -> crate::Result<()> {
        self.in_flight.inc();
        if self.queue.send(Envelope::new(task)).is_err() {
            self.in_flight.dec();
            return Err(crate::Error::Stopped);
        }
        Ok(())
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
}
