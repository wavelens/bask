/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::checkpoint::Coverage;
use crate::task::{Envelope, Task};

/// Stamp the rows folded since the last emit onto whatever the router just emitted, then
/// reset the accumulator: a batch/concat covers the union of its inputs, a filtered input
/// folds into the next emit's coverage, and a fan-out shares its input's coverage.
fn stamp(pending: &mut Coverage, out: &mut Emit, start: usize) {
    if out.envelopes.len() > start {
        let cov = std::mem::take(pending);
        for env in &mut out.envelopes[start..] {
            env.keys = cov.clone();
        }
    }
}

/// The tasks a router emits while handling one input. Drained into the queue with
/// backpressure after the router's state lock is released, so folding stays lock-light.
#[derive(Default)]
pub struct Emit {
    pub(crate) envelopes: Vec<Envelope>,
}

impl Emit {
    /// Emit a task downstream: none = filter, a new type = route, many = fan-out or a batch.
    pub fn emit<T: Task>(&mut self, task: T) {
        self.envelopes.push(Envelope::new(task));
    }

    /// Emit a dynamically-typed task for a front-end that routes by its own type system
    /// (e.g. the Python bindings flushing their routers).
    pub fn emit_dyn(
        &mut self,
        key: u64,
        type_name: &'static str,
        payload: Box<dyn std::any::Any + Send + Sync>,
    ) {
        self.envelopes
            .push(Envelope::new_dyn(key, type_name, payload));
    }

    /// Emit a dynamically-typed task carrying explicit `coverage`; a dynamic router flushes
    /// its trailing batch here with the union of the rows folded into it.
    pub fn emit_dyn_covered(
        &mut self,
        key: u64,
        type_name: &'static str,
        payload: Box<dyn std::any::Any + Send + Sync>,
        coverage: Coverage,
    ) {
        let mut env = Envelope::new_dyn(key, type_name, payload);
        env.keys = coverage;
        self.envelopes.push(env);
    }

    pub fn is_empty(&self) -> bool {
        self.envelopes.is_empty()
    }
}

/// The routing plane: a stateful stream operator fed via [`Context::route`](crate::Context::route).
/// Each input folds into sharded state and may emit derived tasks, so one trait covers
/// reducing, routing, filtering, and batching. `merge`/`finalize` yield a terminal output
/// on the report; `flush` drains buffered work at end-of-run.
pub trait Router: Send + Sync + 'static {
    type Input: Send + 'static;
    type State: Send + Default + 'static;
    type Output: Send + 'static;

    /// Handle one input: update `state` and emit 0..N derived tasks via `out`.
    fn route(state: &mut Self::State, input: Self::Input, out: &mut Emit);

    /// Combine two shard states for the terminal output and end-of-run flush.
    fn merge(left: &mut Self::State, right: Self::State);

    /// Emit buffered work once the run is otherwise idle; run to a fixpoint. The default
    /// emits nothing, so a pure reducer terminates in a single flush epoch.
    fn flush(state: &mut Self::State, out: &mut Emit) {
        let _ = (state, out);
    }

    /// Produce the terminal output from the merged state.
    fn finalize(state: Self::State) -> Self::Output;
}

/// One router shard: user state plus the coverage folded since its last emit.
struct Shard<R: Router> {
    state: R::State,
    pending: Coverage,
}

/// Sharded state for one router: inputs fold into a shard, shards merge at the end.
pub(crate) struct Holder<R: Router> {
    shards: Vec<Mutex<Shard<R>>>,
}

impl<R: Router> Holder<R> {
    fn new(shards: usize) -> Self {
        Holder {
            shards: (0..shards.max(1))
                .map(|_| {
                    Mutex::new(Shard {
                        state: R::State::default(),
                        pending: Coverage::empty(),
                    })
                })
                .collect(),
        }
    }

    fn route(&self, shard: usize, input: R::Input, cov: &Coverage, out: &mut Emit) {
        let idx = shard % self.shards.len();
        let mut guard = self.shards[idx].lock().unwrap();
        guard.pending.union_with(cov);
        let start = out.envelopes.len();
        R::route(&mut guard.state, input, out);
        stamp(&mut guard.pending, out, start);
    }
}

pub(crate) trait AnyRouter: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn flush(&self, out: &mut Emit);
    fn finalize_erased(&self) -> Box<dyn Any + Send>;
}

impl<R: Router> AnyRouter for Holder<R> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn flush(&self, out: &mut Emit) {
        for shard in &self.shards {
            let mut guard = shard.lock().unwrap();
            let start = out.envelopes.len();
            R::flush(&mut guard.state, out);
            stamp(&mut guard.pending, out, start);
        }
    }

    fn finalize_erased(&self) -> Box<dyn Any + Send> {
        let mut acc = R::State::default();
        for shard in &self.shards {
            let mut guard = shard.lock().unwrap();
            R::merge(&mut acc, std::mem::take(&mut guard.state));
        }
        Box::new(R::finalize(acc))
    }
}

#[derive(Default)]
pub(crate) struct Routers {
    map: HashMap<TypeId, Arc<dyn AnyRouter>>,
}

impl Routers {
    pub fn insert<R: Router>(&mut self, shards: usize) {
        self.map
            .insert(TypeId::of::<R>(), Arc::new(Holder::<R>::new(shards)));
    }

    pub fn route<R: Router>(&self, shard: usize, input: R::Input, cov: &Coverage, out: &mut Emit) {
        match self.map.get(&TypeId::of::<R>()) {
            Some(h) => h
                .as_any()
                .downcast_ref::<Holder<R>>()
                .expect("router type mismatch")
                .route(shard, input, cov, out),
            None => panic!("router {} not registered", std::any::type_name::<R>()),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Drain every router's buffered work into `out`; called once per flush epoch.
    pub fn flush_all(&self, out: &mut Emit) {
        for router in self.map.values() {
            router.flush(out);
        }
    }

    pub fn finalize_all(&self) -> HashMap<TypeId, Box<dyn Any + Send>> {
        self.map
            .iter()
            .map(|(k, v)| (*k, v.finalize_erased()))
            .collect()
    }
}
