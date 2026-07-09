/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, Notify, OwnedSemaphorePermit, Semaphore};

use crate::aggregator::Aggregators;
use crate::context::Context;
use crate::dedup::Dedups;
use crate::metrics::{Snapshot, WorkerStat};
use crate::monitor::Monitor;
use crate::registry::Registry;
use crate::report::{AtomicStats, RunReport, TaskFailure};
use crate::retry::{InstanceChoice, RetryPolicy};
use crate::task::{Envelope, TriedMask};

/// A clonable producer handle that keeps global and per-type queue depth accurate.
#[derive(Clone)]
pub(crate) struct Queue {
    tx: mpsc::UnboundedSender<Envelope>,
    registry: Arc<Registry>,
    depth: Arc<AtomicUsize>,
}

impl Queue {
    pub fn send(&self, env: Envelope) -> Result<(), Envelope> {
        self.depth.fetch_add(1, SeqCst);
        if let Some(group) = self.registry.groups.get(&env.key) {
            group.queued.fetch_add(1, SeqCst);
        }
        match self.tx.send(env) {
            Ok(()) => Ok(()),
            Err(err) => {
                let env = err.0;
                self.depth.fetch_sub(1, SeqCst);
                if let Some(group) = self.registry.groups.get(&env.key) {
                    group.queued.fetch_sub(1, SeqCst);
                }
                Err(env)
            }
        }
    }
}

/// A `'static`, clonable handle for emitting dynamic tasks from a front-end that
/// cannot name Rust types (e.g. the Python bindings). Obtained via [`Context::emitter`].
pub struct Emitter {
    queue: Queue,
    in_flight: Arc<InFlight>,
}

impl Emitter {
    pub fn emit_dyn(
        &self,
        key: u64,
        type_name: &'static str,
        payload: Box<dyn std::any::Any + Send + Sync>,
    ) -> crate::Result<()> {
        self.in_flight.inc();
        if self.queue.send(Envelope::new_dyn(key, type_name, payload)).is_err() {
            self.in_flight.dec();
            return Err(crate::Error::Stopped);
        }
        Ok(())
    }
}

impl crate::context::Context {
    /// A detached emit handle usable for the run's lifetime; used by dynamic front-ends.
    pub fn emitter(&self) -> Emitter {
        Emitter { queue: self.queue.clone(), in_flight: self.in_flight.clone() }
    }
}

/// Termination detection: counts tasks that exist (queued, running, or awaiting a
/// retry delay) and wakes the loop when the count reaches zero.
pub(crate) struct InFlight {
    count: AtomicUsize,
    idle: Notify,
}

impl InFlight {
    fn new() -> Self {
        Self { count: AtomicUsize::new(0), idle: Notify::new() }
    }
    pub fn inc(&self) {
        self.count.fetch_add(1, SeqCst);
    }
    pub fn dec(&self) {
        if self.count.fetch_sub(1, SeqCst) == 1 {
            self.idle.notify_one();
        }
    }
    fn is_zero(&self) -> bool {
        self.count.load(SeqCst) == 0
    }
    fn count(&self) -> usize {
        self.count.load(SeqCst)
    }
    async fn wait_idle(&self) {
        self.idle.notified().await;
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run(
    registry: Arc<Registry>,
    aggregators: Arc<Aggregators>,
    dedups: Arc<Dedups>,
    retry: RetryPolicy,
    concurrency: usize,
    seeds: Vec<Envelope>,
    mut monitor: Option<Box<dyn Monitor>>,
    sample_interval: Duration,
) -> crate::Result<RunReport> {
    for group in registry.groups.values() {
        for inst in &group.instances {
            inst.worker.on_start().await.map_err(crate::Error::Worker)?;
        }
    }

    let in_flight = Arc::new(InFlight::new());
    let sem = Arc::new(Semaphore::new(concurrency));
    let shards = concurrency.max(1);
    let stats = Arc::new(AtomicStats::default());
    let failures = Arc::new(Mutex::new(Vec::<TaskFailure>::new()));
    let depth = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::unbounded_channel::<Envelope>();
    let queue = Queue { tx, registry: registry.clone(), depth: depth.clone() };

    for env in seeds {
        in_flight.inc();
        let _ = queue.send(env);
    }

    let mut ticker = tokio::time::interval(sample_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut seq: usize = 0;
    while !in_flight.is_zero() {
        tokio::select! {
            maybe = rx.recv() => {
                let Some(env) = maybe else { break };
                depth.fetch_sub(1, SeqCst);
                if let Some(group) = registry.groups.get(&env.key) {
                    group.queued.fetch_sub(1, SeqCst);
                }
                let permit = sem.clone().acquire_owned().await.expect("semaphore closed");
                let shard = seq % shards;
                seq = seq.wrapping_add(1);
                dispatch(Dispatch {
                    env,
                    permit,
                    shard,
                    registry: registry.clone(),
                    aggregators: aggregators.clone(),
                    dedups: dedups.clone(),
                    in_flight: in_flight.clone(),
                    queue: queue.clone(),
                    retry: retry.clone(),
                    stats: stats.clone(),
                    failures: failures.clone(),
                });
            }
            _ = in_flight.wait_idle() => {}
            _ = ticker.tick() => {
                if let Some(m) = monitor.as_mut() {
                    m.sample(&snapshot(&registry, in_flight.count(), depth.load(SeqCst), &stats));
                }
            }
        }
    }

    for group in registry.groups.values() {
        for inst in &group.instances {
            let _ = inst.worker.on_stop().await;
        }
    }

    let outputs = aggregators.finalize_all();
    let unique = dedups.sizes();
    let failures = std::mem::take(&mut *failures.lock().unwrap());
    let report = RunReport { outputs, unique, stats: stats.snapshot(), failures };

    if let Some(m) = monitor.as_mut() {
        m.sample(&snapshot(&registry, in_flight.count(), depth.load(SeqCst), &stats));
        m.finish(&report);
    }

    Ok(report)
}

fn snapshot(registry: &Registry, in_flight: usize, queued: usize, stats: &AtomicStats) -> Snapshot {
    let mut workers: Vec<WorkerStat> = registry
        .groups
        .values()
        .map(|group| WorkerStat {
            worker_type: group.worker_type,
            instances: group.instances.len(),
            active: group.instances.iter().map(|i| i.active.load(SeqCst)).sum(),
            capacity: group.instances.iter().map(|i| i.capacity).sum(),
            queued: group.queued.load(SeqCst),
            processed: group.processed.load(SeqCst),
        })
        .collect();
    workers.sort_by(|a, b| a.worker_type.cmp(b.worker_type));
    Snapshot {
        in_flight,
        queued,
        processed: stats.processed.load(SeqCst),
        retried: stats.retried.load(SeqCst),
        failed: stats.failed.load(SeqCst),
        workers,
    }
}

struct Dispatch {
    env: Envelope,
    permit: OwnedSemaphorePermit,
    shard: usize,
    registry: Arc<Registry>,
    aggregators: Arc<Aggregators>,
    dedups: Arc<Dedups>,
    in_flight: Arc<InFlight>,
    queue: Queue,
    retry: RetryPolicy,
    stats: Arc<AtomicStats>,
    failures: Arc<Mutex<Vec<TaskFailure>>>,
}

fn dispatch(d: Dispatch) {
    tokio::spawn(async move {
        let Dispatch {
            mut env,
            permit,
            shard,
            registry,
            aggregators,
            dedups,
            in_flight,
            queue,
            retry,
            stats,
            failures,
        } = d;
        let _permit = permit; // released when this task ends, freeing a concurrency slot

        let Some(group) = registry.groups.get(&env.key) else {
            stats.failed.fetch_add(1, SeqCst);
            failures.lock().unwrap().push(TaskFailure {
                task_type: env.type_name,
                instance: "-".to_string(),
                attempts: env.attempt + 1,
                error: "no worker registered for task type".to_string(),
            });
            in_flight.dec();
            return;
        };

        let avoid = matches!(retry.on_retry, InstanceChoice::AvoidFailed);
        let inst = match group.select(env.tried, avoid) {
            Some(i) => i,
            None => {
                env.tried = TriedMask::empty();
                match group.select(env.tried, avoid) {
                    Some(i) => i,
                    None => {
                        stats.failed.fetch_add(1, SeqCst);
                        in_flight.dec();
                        return;
                    }
                }
            }
        };
        let inst_id = inst.id;
        let inst_label = inst.label.clone();

        let _iperm =
            inst.permits.clone().acquire_owned().await.expect("instance semaphore closed");
        inst.active.fetch_add(1, SeqCst);
        let ctx = Context {
            queue: queue.clone(),
            in_flight: in_flight.clone(),
            aggregators: aggregators.clone(),
            dedups: dedups.clone(),
            shard,
        };
        let res = inst.worker.process(env.payload.as_ref(), &ctx).await;
        inst.active.fetch_sub(1, SeqCst);
        drop(ctx);
        drop(_iperm);

        match res {
            Ok(()) => {
                stats.processed.fetch_add(1, SeqCst);
                group.processed.fetch_add(1, SeqCst);
                in_flight.dec();
            }
            Err(err) => {
                let next = env.attempt + 1;
                if next < retry.max_attempts {
                    stats.retried.fetch_add(1, SeqCst);
                    env.attempt = next;
                    if avoid {
                        env.tried = env.tried.with(inst_id);
                    }
                    match retry.delay(next) {
                        None => {
                            let _ = queue.send(env);
                        }
                        Some(delay) => {
                            let queue = queue.clone();
                            tokio::spawn(async move {
                                tokio::time::sleep(delay).await;
                                let _ = queue.send(env);
                            });
                        }
                    }
                } else {
                    stats.failed.fetch_add(1, SeqCst);
                    failures.lock().unwrap().push(TaskFailure {
                        task_type: env.type_name,
                        instance: inst_label,
                        attempts: next,
                        error: format!("{err:#}"),
                    });
                    in_flight.dec();
                }
            }
        }
    });
}
