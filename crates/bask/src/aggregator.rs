/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// The aggregation plane: a parallel reducer (monoid) fed from workers via
/// [`Context::aggregate`](crate::Context::aggregate), kept out of the worker graph.
pub trait Aggregator: Send + Sync + 'static {
    type Input: Send + 'static;
    type State: Send + Default + 'static;
    type Output: Send + 'static;

    fn fold(state: &mut Self::State, input: Self::Input);
    fn merge(left: &mut Self::State, right: Self::State);
    fn finalize(state: Self::State) -> Self::Output;
}

/// Sharded state for one aggregator: workers fold into a shard, shards merge at the end.
pub(crate) struct Holder<A: Aggregator> {
    shards: Vec<Mutex<A::State>>,
}

impl<A: Aggregator> Holder<A> {
    fn new(shards: usize) -> Self {
        Holder {
            shards: (0..shards.max(1))
                .map(|_| Mutex::new(A::State::default()))
                .collect(),
        }
    }
    fn fold(&self, shard: usize, input: A::Input) {
        let idx = shard % self.shards.len();
        let mut guard = self.shards[idx].lock().unwrap();
        A::fold(&mut guard, input);
    }
}

pub(crate) trait AnyAggregator: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn finalize_erased(&self) -> Box<dyn Any + Send>;
}

impl<A: Aggregator> AnyAggregator for Holder<A> {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn finalize_erased(&self) -> Box<dyn Any + Send> {
        let mut acc = A::State::default();
        for shard in &self.shards {
            let mut guard = shard.lock().unwrap();
            A::merge(&mut acc, std::mem::take(&mut guard));
        }
        Box::new(A::finalize(acc))
    }
}

#[derive(Default)]
pub(crate) struct Aggregators {
    map: HashMap<TypeId, Arc<dyn AnyAggregator>>,
}

impl Aggregators {
    pub fn insert<A: Aggregator>(&mut self, shards: usize) {
        self.map
            .insert(TypeId::of::<A>(), Arc::new(Holder::<A>::new(shards)));
    }

    pub fn fold<A: Aggregator>(&self, shard: usize, input: A::Input) {
        match self.map.get(&TypeId::of::<A>()) {
            Some(h) => h
                .as_any()
                .downcast_ref::<Holder<A>>()
                .expect("aggregator type mismatch")
                .fold(shard, input),
            None => panic!("aggregator {} not registered", std::any::type_name::<A>()),
        }
    }

    pub fn finalize_all(&self) -> HashMap<TypeId, Box<dyn Any + Send>> {
        self.map
            .iter()
            .map(|(k, v)| (*k, v.finalize_erased()))
            .collect()
    }
}
