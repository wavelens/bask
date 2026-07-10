/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::aggregator::{Aggregator, Aggregators};
use crate::dedup::{Dedup, Dedups};
use crate::monitor::Monitor;
use crate::registry::{Group, Instance, Registry};
use crate::report::RunReport;
use crate::retry::RetryPolicy;
use crate::scheduler;
use crate::task::{Envelope, RouteKey, Task};
use crate::worker::{DynWorker, Holder, Worker, WorkerCfg};

struct InstanceSpec {
    worker: Arc<dyn DynWorker>,
    label: Option<String>,
    concurrency: Option<usize>,
    type_name: &'static str,
}

type AggFactory = Box<dyn FnOnce(&mut Aggregators, usize)>;
type DedupFactory = Box<dyn FnOnce(&mut Dedups, usize)>;

pub struct EngineBuilder {
    specs: HashMap<RouteKey, Vec<InstanceSpec>>,
    aggregators: Vec<AggFactory>,
    dedups: Vec<DedupFactory>,
    retry: RetryPolicy,
    concurrency: usize,
    seeds: Vec<Envelope>,
    monitor: Option<Box<dyn Monitor>>,
    sample_interval: Duration,
}

pub struct Engine {
    registry: Registry,
    aggregators: Aggregators,
    dedups: Dedups,
    retry: RetryPolicy,
    concurrency: usize,
    seeds: Vec<Envelope>,
    monitor: Option<Box<dyn Monitor>>,
    sample_interval: Duration,
}

impl Engine {
    pub fn builder() -> EngineBuilder {
        EngineBuilder {
            specs: HashMap::new(),
            aggregators: Vec::new(),
            dedups: Vec::new(),
            retry: RetryPolicy::default(),
            concurrency: default_parallelism(),
            seeds: Vec::new(),
            monitor: None,
            sample_interval: Duration::from_millis(200),
        }
    }

    pub async fn run(self) -> crate::Result<RunReport> {
        scheduler::run(
            Arc::new(self.registry),
            Arc::new(self.aggregators),
            Arc::new(self.dedups),
            self.retry,
            self.concurrency,
            self.seeds,
            self.monitor,
            self.sample_interval,
        )
        .await
    }
}

impl EngineBuilder {
    /// Register a worker instance with default label and concurrency.
    pub fn worker<W: Worker>(self, worker: W) -> Self {
        self.worker_cfg(worker, WorkerCfg::default())
    }

    /// Register a worker instance with an explicit label and/or concurrency.
    /// Registering the same worker type more than once forms a group of instances.
    pub fn worker_cfg<W: Worker>(mut self, worker: W, cfg: WorkerCfg) -> Self {
        let spec = InstanceSpec {
            worker: Arc::new(Holder(worker)),
            label: cfg.label,
            concurrency: cfg.concurrency,
            type_name: std::any::type_name::<W>(),
        };
        self.specs
            .entry(RouteKey::Static(TypeId::of::<W::Task>()))
            .or_default()
            .push(spec);
        self
    }

    /// Register a dynamically-typed worker instance under a runtime routing `key`.
    /// Used by front-ends (e.g. the Python bindings) that route by their own type system.
    pub fn worker_dyn(
        mut self,
        key: u64,
        worker: Arc<dyn DynWorker>,
        type_name: &'static str,
        cfg: WorkerCfg,
    ) -> Self {
        let spec = InstanceSpec {
            worker,
            label: cfg.label,
            concurrency: cfg.concurrency,
            type_name,
        };
        self.specs.entry(RouteKey::Dyn(key)).or_default().push(spec);
        self
    }

    /// Seed a dynamically-typed task under a runtime routing `key`.
    pub fn seed_dyn(
        mut self,
        key: u64,
        type_name: &'static str,
        payload: Box<dyn std::any::Any + Send + Sync>,
    ) -> Self {
        self.seeds.push(Envelope::new_dyn(key, type_name, payload));
        self
    }

    pub fn aggregator<A: Aggregator>(mut self) -> Self {
        self.aggregators
            .push(Box::new(|aggs: &mut Aggregators, shards| {
                aggs.insert::<A>(shards)
            }));
        self
    }

    /// Register a dedup set; gate emission with [`Context::first_seen`](crate::Context::first_seen).
    pub fn dedup<D: Dedup>(mut self) -> Self {
        self.dedups.push(Box::new(|dedups: &mut Dedups, shards| {
            dedups.insert::<D>(shards)
        }));
        self
    }

    pub fn seed<T: Task>(mut self, task: T) -> Self {
        self.seeds.push(Envelope::new(task));
        self
    }

    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        self
    }

    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Observe live load (queue depth, per-type concurrency) at `sample_interval`.
    pub fn monitor<M: Monitor + 'static>(mut self, monitor: M) -> Self {
        self.monitor = Some(Box::new(monitor));
        self
    }

    pub fn sample_interval(mut self, interval: Duration) -> Self {
        self.sample_interval = interval;
        self
    }

    pub fn build(self) -> Engine {
        let concurrency = self.concurrency;
        let mut registry = Registry::default();
        for (key, specs) in self.specs {
            assert!(
                specs.len() <= 64,
                "at most 64 worker instances per task type"
            );
            let worker_type = specs.first().map_or("unknown", |s| s.type_name);
            let instances = specs
                .into_iter()
                .enumerate()
                .map(|(i, s)| {
                    let id = i as u16;
                    let label = s.label.unwrap_or_else(|| format!("{}#{id}", s.type_name));
                    let cap = s.concurrency.unwrap_or(concurrency).max(1);
                    Instance {
                        worker: s.worker,
                        label,
                        id,
                        permits: Arc::new(Semaphore::new(cap)),
                        capacity: cap,
                        active: AtomicUsize::new(0),
                    }
                })
                .collect();
            registry.groups.insert(
                key,
                Group {
                    instances,
                    worker_type,
                    queued: AtomicUsize::new(0),
                    processed: AtomicU64::new(0),
                },
            );
        }
        let mut aggregators = Aggregators::default();
        for factory in self.aggregators {
            factory(&mut aggregators, concurrency);
        }
        let mut dedups = Dedups::default();
        for factory in self.dedups {
            factory(&mut dedups, concurrency);
        }
        Engine {
            registry,
            aggregators,
            dedups,
            retry: self.retry,
            concurrency,
            seeds: self.seeds,
            monitor: self.monitor,
            sample_interval: self.sample_interval,
        }
    }

    pub async fn run(self) -> crate::Result<RunReport> {
        self.build().run().await
    }
}

fn default_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}
