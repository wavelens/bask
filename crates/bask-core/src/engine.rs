/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::checkpoint::{CheckpointOps, Checkpoints, Dataset, Durability, Store};
use crate::deadletter::DeadLetterSink;
use crate::dedup::{Dedup, Dedups};
use crate::interrupt::Shutdown;
use crate::monitor::Monitor;
use crate::registry::{Group, Instance, Registry};
use crate::report::RunReport;
use crate::resource::Attrs;
use crate::retry::RetryPolicy;
use crate::router::{Emit, Router, Routers};
use crate::scheduler::{self, Interrupt};
use crate::task::{Envelope, RouteKey, Task};
use crate::worker::{DynWorker, Holder, Worker, WorkerCfg};

struct InstanceSpec {
    worker: Arc<dyn DynWorker>,
    label: Option<String>,
    concurrency: Option<usize>,
    timeout: Option<Duration>,
    type_name: &'static str,
    attrs: Attrs,
    requires: Vec<String>,
    retry: Option<RetryPolicy>,
}

type RouterFactory = Box<dyn FnOnce(&mut Routers, usize) + Send>;
type DedupFactory = Box<dyn FnOnce(&mut Dedups, usize) + Send>;

/// One addressable checkpoint task for `list-tasks`: its store name, the worker type that
/// consumes it (if any), and how many index items are stored (pending) vs consumed (done).
pub struct TaskInfo {
    pub name: String,
    pub worker_type: Option<&'static str>,
    pub stored: usize,
    pub done: usize,
}

pub struct EngineBuilder {
    specs: HashMap<RouteKey, Vec<InstanceSpec>>,
    routers: Vec<RouterFactory>,
    dedups: Vec<DedupFactory>,
    retry: RetryPolicy,
    concurrency: usize,
    queue_capacity: Option<usize>,
    timeout: Option<Duration>,
    shutdown: Option<Shutdown>,
    grace: Duration,
    catch_ctrl_c: bool,
    seeds: Vec<Envelope>,
    monitor: Option<Box<dyn Monitor>>,
    sample_interval: Duration,
    flush_hook: Option<scheduler::FlushHook>,
    resources: HashMap<String, usize>,
    dead_letter: Option<Arc<dyn DeadLetterSink>>,
    checkpoints: Vec<(RouteKey, Arc<dyn CheckpointOps>)>,
    store: Option<Arc<dyn Store>>,
    dataset: Option<Arc<dyn Dataset>>,
    selection: Option<HashSet<String>>,
}

pub struct Engine {
    registry: Registry,
    routers: Routers,
    dedups: Dedups,
    retry: RetryPolicy,
    concurrency: usize,
    queue_capacity: usize,
    interrupt: Interrupt,
    seeds: Vec<Envelope>,
    monitor: Option<Box<dyn Monitor>>,
    sample_interval: Duration,
    flush_hook: Option<scheduler::FlushHook>,
    dead_letter: Option<Arc<dyn DeadLetterSink>>,
    checkpoints: Checkpoints,
    store: Option<Arc<dyn Store>>,
    dataset: Option<Arc<dyn Dataset>>,
    selection: Option<HashSet<String>>,
}

impl Engine {
    pub fn builder() -> EngineBuilder {
        EngineBuilder {
            specs: HashMap::new(),
            routers: Vec::new(),
            dedups: Vec::new(),
            retry: RetryPolicy::default(),
            concurrency: default_parallelism(),
            queue_capacity: None,
            timeout: None,
            shutdown: None,
            grace: Duration::from_secs(30),
            catch_ctrl_c: false,
            seeds: Vec::new(),
            monitor: None,
            sample_interval: Duration::from_millis(200),
            flush_hook: None,
            resources: HashMap::new(),
            dead_letter: None,
            checkpoints: Vec::new(),
            store: None,
            dataset: None,
            selection: None,
        }
    }

    /// The checkpoint names, the addressable `--tasks` units; used by the CLI to validate a
    /// selection.
    pub fn checkpoint_names(&self) -> Vec<&str> {
        self.checkpoints.names()
    }

    /// The checkpoints as addressable tasks with their index status, for `list-tasks`. Reads
    /// the store (or the default `bask.sqlite`) without running.
    pub fn tasks(&self) -> crate::Result<Vec<TaskInfo>> {
        let store = self.store.clone().unwrap_or_else(default_store);
        let statuses = store.statuses().map_err(crate::Error::Store)?;
        let count = |name: &str, want: crate::Status| {
            statuses
                .iter()
                .filter(|(n, _, status)| n == name && *status == want)
                .count()
        };
        let mut tasks: Vec<TaskInfo> = self
            .checkpoints
            .iter()
            .map(|(route_key, ops)| TaskInfo {
                name: ops.name().to_string(),
                worker_type: self.registry.groups.get(route_key).map(|g| g.worker_type),
                stored: count(ops.name(), crate::Status::Stored),
                done: count(ops.name(), crate::Status::Consumed),
            })
            .collect();
        tasks.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(tasks)
    }

    pub async fn run(self) -> crate::Result<RunReport> {
        let durability = if self.checkpoints.is_empty() {
            None
        } else {
            let store = self.store.unwrap_or_else(default_store);
            let durability = Durability::new(self.checkpoints, store, self.dataset, self.selection)
                .map_err(crate::Error::Store)?;
            Some(Arc::new(durability))
        };
        scheduler::run(
            Arc::new(self.registry),
            Arc::new(self.routers),
            Arc::new(self.dedups),
            self.retry,
            self.concurrency,
            self.queue_capacity,
            self.interrupt,
            self.seeds,
            self.monitor,
            self.sample_interval,
            self.flush_hook,
            self.dead_letter,
            durability,
        )
        .await
    }
}

/// The default checkpoint store: `bask.sqlite` in the working directory, created lazily.
/// Without the `checkpoint` feature there is no sqlite, so dynamic checkpoints fall back
/// to an in-memory store.
fn default_store() -> Arc<dyn Store> {
    #[cfg(feature = "checkpoint")]
    {
        Arc::new(crate::sqlite::SqliteStore::open("bask.sqlite"))
    }
    #[cfg(not(feature = "checkpoint"))]
    {
        Arc::new(crate::checkpoint::MemStore::default())
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
            timeout: cfg.timeout,
            type_name: std::any::type_name::<W>(),
            attrs: cfg.attrs,
            requires: cfg.requires,
            retry: cfg.retry,
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
            timeout: cfg.timeout,
            type_name,
            attrs: cfg.attrs,
            requires: cfg.requires,
            retry: cfg.retry,
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

    /// Seed a dynamically-typed source under a runtime routing `key`, tagged with a stable
    /// `id` so its extent can be recorded and its descendants attributed (see [`source`]).
    ///
    /// [`source`]: EngineBuilder::source
    pub fn seed_source_dyn(
        mut self,
        id: impl Into<String>,
        key: u64,
        type_name: &'static str,
        payload: Box<dyn std::any::Any + Send + Sync>,
    ) -> Self {
        let mut env = Envelope::new_dyn(key, type_name, payload);
        env.source = Some(Arc::from(id.into()));
        self.seeds.push(env);
        self
    }

    /// Register a router; feed it from a worker with [`Context::route`](crate::Context::route).
    pub fn router<R: Router>(mut self) -> Self {
        self.routers.push(Box::new(|routers: &mut Routers, shards| {
            routers.insert::<R>(shards)
        }));
        self
    }

    /// Run a callback once per flush epoch so a dynamic front-end can contribute buffered
    /// emissions from routers the core cannot name (used by the Python bindings).
    pub fn flush_hook<F: FnMut(&mut Emit) + Send + 'static>(mut self, hook: F) -> Self {
        self.flush_hook = Some(Box::new(hook));
        self
    }

    /// Declare a named resource pool with `permits` slots, shared across every instance
    /// that [`requires`](crate::WorkerCfg::requires) it (e.g. `resource("gpu", 4)`).
    pub fn resource(mut self, name: impl Into<String>, permits: usize) -> Self {
        self.resources.insert(name.into(), permits.max(1));
        self
    }

    /// Route terminally-failed tasks (retries exhausted or [`RetryOn::Fatal`](crate::RetryOn))
    /// to a sink, which receives the type-erased payload alongside the error.
    pub fn dead_letter<S: DeadLetterSink>(mut self, sink: S) -> Self {
        self.dead_letter = Some(Arc::new(sink));
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

    /// Seed a source: a task whose descendants stamp source rows (via
    /// [`Context::emit_keyed`](crate::Context::emit_keyed)) under the stable `id`. Once a
    /// clean pass records the source's extent, a later run skips it whole if a checkpoint
    /// already covers every row (no CSV re-read).
    pub fn source<T: Task>(mut self, id: impl Into<String>, task: T) -> Self {
        let mut env = Envelope::new(task);
        env.source = Some(Arc::from(id.into()));
        self.seeds.push(env);
        self
    }

    /// Back checkpoints with a specific [`Store`] instead of the default `bask.sqlite`.
    /// Pass a [`MemStore`](crate::MemStore) to keep durability opt-out but still dedup.
    pub fn store<S: Store + 'static>(mut self, store: S) -> Self {
        self.store = Some(Arc::new(store));
        self
    }

    /// Materialize data-carrying checkpoints into a [`Dataset`]: their payloads become
    /// self-compacting shards and the dataset's own [`Store`] backs the index, so it is both
    /// where the pipeline writes and where a later run reads. Supersedes any [`store`].
    ///
    /// [`store`]: EngineBuilder::store
    pub fn dataset<D: Dataset + 'static>(self, dataset: D) -> Self {
        self.dataset_arc(Arc::new(dataset))
    }

    /// Bind an already-boxed [`Dataset`]; the CLI uses this to apply `--dataset` from a
    /// front-end-provided opener.
    pub fn dataset_arc(mut self, dataset: Arc<dyn Dataset>) -> Self {
        self.store = Some(dataset.store());
        self.dataset = Some(dataset);
        self
    }

    /// Restrict the run to the named checkpoints: each becomes a terminal boundary that
    /// materializes but does not run its downstream worker, so the pipeline stops there
    /// while its feeders still run. The CLI's `--tasks` maps onto this; combined with resume
    /// it lets a later run continue from a boundary a prior run stopped at.
    pub fn select_tasks(mut self, names: impl IntoIterator<Item = String>) -> Self {
        self.selection = Some(names.into_iter().collect());
        self
    }

    /// Register a dynamically-typed checkpoint under a runtime routing `key`; used by
    /// front-ends (the Python bindings) whose task types live outside Rust's type system.
    pub fn checkpoint_dyn(mut self, key: u64, ops: Arc<dyn CheckpointOps>) -> Self {
        self.checkpoints.push((RouteKey::Dyn(key), ops));
        self
    }

    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        self
    }

    /// Bound the shared task queue; `emit` blocks once it is full. Defaults to
    /// `16 * concurrency` (floor 256) when unset.
    pub fn queue_capacity(mut self, n: usize) -> Self {
        self.queue_capacity = Some(n.max(1));
        self
    }

    /// Default per-task timeout applied to every worker without its own
    /// [`WorkerCfg::timeout`]; on elapse the task is cancelled and retried.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Register a handle that requests a graceful shutdown when triggered.
    pub fn shutdown(mut self, shutdown: Shutdown) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    /// How long in-flight work may finish after a shutdown before it is cancelled.
    pub fn grace_period(mut self, grace: Duration) -> Self {
        self.grace = grace;
        self
    }

    /// Trigger a graceful shutdown on the first Ctrl-C (SIGINT).
    pub fn catch_ctrl_c(mut self) -> Self {
        self.catch_ctrl_c = true;
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
        let queue_capacity = self
            .queue_capacity
            .unwrap_or_else(|| concurrency.saturating_mul(16).max(256));
        let default_timeout = self.timeout;
        let interrupt = Interrupt {
            shutdown: self.shutdown.unwrap_or_default(),
            grace: self.grace,
            catch_ctrl_c: self.catch_ctrl_c,
        };
        let pools: HashMap<String, Arc<Semaphore>> = self
            .resources
            .into_iter()
            .map(|(name, permits)| (name, Arc::new(Semaphore::new(permits))))
            .collect();
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
                    let resources = s
                        .requires
                        .iter()
                        .map(|name| {
                            pools.get(name).cloned().unwrap_or_else(|| {
                                panic!("worker requires undeclared resource {name:?}")
                            })
                        })
                        .collect();
                    Instance {
                        worker: s.worker,
                        label,
                        id,
                        permits: Arc::new(Semaphore::new(cap)),
                        capacity: cap,
                        active: AtomicUsize::new(0),
                        timeout: s.timeout.or(default_timeout),
                        attrs: s.attrs,
                        resources,
                        retry: s.retry,
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
        let mut routers = Routers::default();
        for factory in self.routers {
            factory(&mut routers, concurrency);
        }
        let mut dedups = Dedups::default();
        for factory in self.dedups {
            factory(&mut dedups, concurrency);
        }
        let mut checkpoints = Checkpoints::default();
        #[cfg(feature = "checkpoint")]
        for (type_id, ops) in crate::checkpoint::registered() {
            checkpoints.insert(RouteKey::Static(type_id), ops);
        }
        for (key, ops) in self.checkpoints {
            checkpoints.insert(key, ops);
        }
        Engine {
            registry,
            routers,
            dedups,
            retry: self.retry,
            concurrency,
            queue_capacity,
            interrupt,
            seeds: self.seeds,
            monitor: self.monitor,
            sample_interval: self.sample_interval,
            flush_hook: self.flush_hook,
            dead_letter: self.dead_letter,
            checkpoints,
            store: self.store,
            dataset: self.dataset,
            selection: self.selection,
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
