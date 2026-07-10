/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc};

use crate::context::Context;
use crate::dedup::Dedups;
use crate::interrupt::{Cancel, Shutdown};
use crate::metrics::{Snapshot, WorkerStat};
use crate::monitor::Monitor;
use crate::registry::Registry;
use crate::report::{AtomicStats, RunReport, TaskFailure};
use crate::retry::{InstanceChoice, RetryPolicy};
use crate::router::{Emit, Routers};
use crate::task::{Envelope, RouteKey, TriedMask};

/// Outcome of a non-blocking enqueue attempt.
pub(crate) enum Sent {
    Ok,
    Full(Envelope),
    Closed,
}

/// A clonable producer handle over the bounded queue that keeps global and
/// per-type depth counters accurate across every enqueue path.
#[derive(Clone)]
pub(crate) struct Queue {
    tx: mpsc::Sender<Envelope>,
    registry: Arc<Registry>,
    depth: Arc<AtomicUsize>,
}

impl Queue {
    fn note_enqueued(&self, key: RouteKey) {
        self.depth.fetch_add(1, SeqCst);
        if let Some(group) = self.registry.groups.get(&key) {
            group.queued.fetch_add(1, SeqCst);
        }
    }

    pub(crate) fn note_dequeued(&self, key: RouteKey) {
        self.depth.fetch_sub(1, SeqCst);
        if let Some(group) = self.registry.groups.get(&key) {
            group.queued.fetch_sub(1, SeqCst);
        }
    }

    /// Non-blocking enqueue for the hot path; hands the envelope back when full.
    pub(crate) fn try_send(&self, env: Envelope) -> Sent {
        let key = env.key;
        match self.tx.try_send(env) {
            Ok(()) => {
                self.note_enqueued(key);
                Sent::Ok
            }
            Err(mpsc::error::TrySendError::Full(env)) => Sent::Full(env),
            Err(mpsc::error::TrySendError::Closed(_)) => Sent::Closed,
        }
    }

    /// Async enqueue that awaits capacity; the caller must not hold a run permit.
    pub(crate) async fn send(&self, env: Envelope) -> Result<(), Envelope> {
        let key = env.key;
        match self.tx.send(env).await {
            Ok(()) => {
                self.note_enqueued(key);
                Ok(())
            }
            Err(err) => Err(err.0),
        }
    }

    /// Blocking enqueue for synchronous front-ends running off the async threads.
    pub(crate) fn blocking_send(&self, env: Envelope) -> Result<(), Envelope> {
        let key = env.key;
        match self.tx.blocking_send(env) {
            Ok(()) => {
                self.note_enqueued(key);
                Ok(())
            }
            Err(err) => Err(err.0),
        }
    }
}

/// The concurrency permits a running task holds. When a worker parks in `emit`
/// awaiting queue capacity it releases them so the dispatcher can drain other
/// work, then reacquires before resuming: a producing task never blocks the
/// only consumer, so the bounded queue cannot deadlock (progress invariant).
pub(crate) struct RunSlot {
    global: Arc<Semaphore>,
    instance: Arc<Semaphore>,
    held: Mutex<Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)>>,
}

impl RunSlot {
    fn new(
        global: Arc<Semaphore>,
        instance: Arc<Semaphore>,
        global_permit: OwnedSemaphorePermit,
        instance_permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            global,
            instance,
            held: Mutex::new(Some((global_permit, instance_permit))),
        }
    }

    pub(crate) fn release(&self) {
        *self.held.lock().unwrap() = None;
    }

    pub(crate) async fn reacquire(&self) {
        let global = self
            .global
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed");
        let instance = self
            .instance
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed");
        *self.held.lock().unwrap() = Some((global, instance));
    }
}

/// A `'static`, clonable handle for emitting dynamic tasks from a front-end that
/// cannot name Rust types (e.g. the Python bindings). Obtained via [`Context::emitter`].
pub struct Emitter {
    queue: Queue,
    in_flight: Arc<InFlight>,
    run: Arc<RunSlot>,
}

impl Emitter {
    /// Enqueue a dynamic task. On a full queue this yields the caller's run permit
    /// and blocks the (off-runtime) calling thread until capacity frees; the GIL
    /// bounds concurrency on resume, so it does not reacquire.
    pub fn emit_dyn(
        &self,
        key: u64,
        type_name: &'static str,
        payload: Box<dyn std::any::Any + Send + Sync>,
    ) -> crate::Result<()> {
        self.in_flight.inc();
        match self
            .queue
            .try_send(Envelope::new_dyn(key, type_name, payload))
        {
            Sent::Ok => Ok(()),
            Sent::Full(env) => {
                self.run.release();
                match self.queue.blocking_send(env) {
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
}

impl crate::context::Context {
    /// A detached emit handle usable for the run's lifetime; used by dynamic front-ends.
    pub fn emitter(&self) -> Emitter {
        Emitter {
            queue: self.queue.clone(),
            in_flight: self.in_flight.clone(),
            run: self.run.clone(),
        }
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
        Self {
            count: AtomicUsize::new(0),
            idle: Notify::new(),
        }
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

/// Graceful-interruption configuration handed to a run.
pub(crate) struct Interrupt {
    pub shutdown: Shutdown,
    pub grace: Duration,
    pub catch_ctrl_c: bool,
}

/// A callback invoked once per flush epoch, letting a dynamic front-end (e.g. the Python
/// bindings) contribute buffered emissions from routers the core cannot name.
pub(crate) type FlushHook = Box<dyn FnMut(&mut Emit) + Send>;

/// How a single `process` invocation ended.
enum Outcome {
    Done(anyhow::Result<()>),
    Cancelled,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run(
    registry: Arc<Registry>,
    routers: Arc<Routers>,
    dedups: Arc<Dedups>,
    retry: RetryPolicy,
    concurrency: usize,
    queue_capacity: usize,
    interrupt: Interrupt,
    seeds: Vec<Envelope>,
    mut monitor: Option<Box<dyn Monitor>>,
    sample_interval: Duration,
    mut flush_hook: Option<FlushHook>,
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
    let unfinished = Arc::new(AtomicUsize::new(0));
    let cancel = Cancel::default();
    let (tx, mut rx) = mpsc::channel::<Envelope>(queue_capacity.max(1));
    let queue = Queue {
        tx,
        registry: registry.clone(),
        depth: depth.clone(),
    };

    let Interrupt {
        shutdown,
        grace,
        catch_ctrl_c,
    } = interrupt;
    let ctrl_c = catch_ctrl_c.then(|| {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                shutdown.trigger();
            }
        })
    });

    // Count seeds up front so the loop cannot terminate before they land, then feed
    // them through the bounded queue concurrently with draining (seeds may exceed it).
    for _ in &seeds {
        in_flight.inc();
    }
    tokio::spawn({
        let queue = queue.clone();
        let in_flight = in_flight.clone();
        async move {
            let mut pending = seeds.len();
            for env in seeds {
                if queue.send(env).await.is_err() {
                    break;
                }
                pending -= 1;
            }
            for _ in 0..pending {
                in_flight.dec();
            }
        }
    });

    let mut ticker = tokio::time::interval(sample_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Phase 1: dispatch to quiescence, then flush routers; repeat until a flush epoch
    // emits nothing, so a trailing batch still flows before the run ends.
    let shutdown_fut = shutdown.triggered();
    tokio::pin!(shutdown_fut);
    let mut seq: usize = 0;
    'epochs: loop {
        while !in_flight.is_zero() {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = &mut shutdown_fut => break,
                maybe = rx.recv() => {
                    let Some(env) = maybe else { break 'epochs };
                    queue.note_dequeued(env.key);
                    let permit = sem.clone().acquire_owned().await.expect("semaphore closed");
                    let shard = seq % shards;
                    seq = seq.wrapping_add(1);
                    dispatch(Dispatch {
                        env,
                        permit,
                        shard,
                        sem: sem.clone(),
                        registry: registry.clone(),
                        routers: routers.clone(),
                        dedups: dedups.clone(),
                        in_flight: in_flight.clone(),
                        queue: queue.clone(),
                        retry: retry.clone(),
                        stats: stats.clone(),
                        failures: failures.clone(),
                        cancel: cancel.clone(),
                        unfinished: unfinished.clone(),
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
        if shutdown.is_triggered() || (routers.is_empty() && flush_hook.is_none()) {
            break;
        }
        let mut out = Emit::default();
        routers.flush_all(&mut out);
        if let Some(hook) = flush_hook.as_mut() {
            hook(&mut out);
        }
        let envelopes = out.envelopes;
        if envelopes.is_empty() {
            break;
        }
        for _ in &envelopes {
            in_flight.inc();
        }
        tokio::spawn({
            let queue = queue.clone();
            let in_flight = in_flight.clone();
            async move {
                let mut pending = envelopes.len();
                for env in envelopes {
                    if queue.send(env).await.is_err() {
                        break;
                    }
                    pending -= 1;
                }
                for _ in 0..pending {
                    in_flight.dec();
                }
            }
        });
    }

    // Phase 2: no new work is dispatched; let in-flight tasks finish within the grace
    // period, cancel whatever remains, and account for every abandoned task.
    let interrupted = shutdown.is_triggered();
    if interrupted {
        let grace_timer = tokio::time::sleep(grace);
        tokio::pin!(grace_timer);
        let mut cancelled = false;
        while !in_flight.is_zero() {
            tokio::select! {
                _ = &mut grace_timer, if !cancelled => {
                    cancel.cancel();
                    cancelled = true;
                }
                maybe = rx.recv() => {
                    if let Some(env) = maybe {
                        queue.note_dequeued(env.key);
                        unfinished.fetch_add(1, SeqCst);
                        in_flight.dec();
                    }
                }
                _ = in_flight.wait_idle() => {}
                _ = ticker.tick() => {
                    if let Some(m) = monitor.as_mut() {
                        m.sample(&snapshot(&registry, in_flight.count(), depth.load(SeqCst), &stats));
                    }
                }
            }
        }
    }
    if let Some(handle) = ctrl_c {
        handle.abort();
    }

    // Drain hook: sinks flush and finalize here, so a finalize failure surfaces as a
    // terminal task failure rather than silent data loss.
    for group in registry.groups.values() {
        for inst in &group.instances {
            if let Err(err) = inst.worker.on_stop().await {
                stats.failed.fetch_add(1, SeqCst);
                failures.lock().unwrap().push(TaskFailure {
                    task_type: group.worker_type,
                    instance: inst.label.clone(),
                    attempts: 1,
                    error: format!("{err:#}"),
                });
            }
        }
    }

    let outputs = routers.finalize_all();
    let unique = dedups.sizes();
    let failures = std::mem::take(&mut *failures.lock().unwrap());
    let report = RunReport {
        outputs,
        unique,
        stats: stats.snapshot(),
        failures,
        interrupted,
        unfinished: unfinished.load(SeqCst),
    };

    if let Some(m) = monitor.as_mut() {
        m.sample(&snapshot(
            &registry,
            in_flight.count(),
            depth.load(SeqCst),
            &stats,
        ));
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
    sem: Arc<Semaphore>,
    registry: Arc<Registry>,
    routers: Arc<Routers>,
    dedups: Arc<Dedups>,
    in_flight: Arc<InFlight>,
    queue: Queue,
    retry: RetryPolicy,
    stats: Arc<AtomicStats>,
    failures: Arc<Mutex<Vec<TaskFailure>>>,
    cancel: Cancel,
    unfinished: Arc<AtomicUsize>,
}

fn dispatch(d: Dispatch) {
    tokio::spawn(async move {
        let Dispatch {
            mut env,
            permit,
            shard,
            sem,
            registry,
            routers,
            dedups,
            in_flight,
            queue,
            retry,
            stats,
            failures,
            cancel,
            unfinished,
        } = d;

        let Some(group) = registry.groups.get(&env.key) else {
            drop(permit);
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
                        drop(permit);
                        stats.failed.fetch_add(1, SeqCst);
                        in_flight.dec();
                        return;
                    }
                }
            }
        };
        let inst_id = inst.id;
        let inst_label = inst.label.clone();

        let iperm = inst
            .permits
            .clone()
            .acquire_owned()
            .await
            .expect("instance semaphore closed");
        inst.active.fetch_add(1, SeqCst);
        let run = Arc::new(RunSlot::new(sem, inst.permits.clone(), permit, iperm));
        let ctx = Context {
            queue: queue.clone(),
            in_flight: in_flight.clone(),
            routers: routers.clone(),
            dedups: dedups.clone(),
            shard,
            run: run.clone(),
            cancel: cancel.clone(),
        };
        let outcome = match inst.timeout {
            Some(dur) => tokio::select! {
                biased;
                _ = cancel.cancelled() => Outcome::Cancelled,
                r = tokio::time::timeout(dur, inst.worker.process(env.payload.as_ref(), &ctx)) => {
                    Outcome::Done(r.unwrap_or_else(|_| Err(anyhow::anyhow!("timed out after {dur:?}"))))
                }
            },
            None => tokio::select! {
                biased;
                _ = cancel.cancelled() => Outcome::Cancelled,
                res = inst.worker.process(env.payload.as_ref(), &ctx) => Outcome::Done(res),
            },
        };
        inst.active.fetch_sub(1, SeqCst);
        drop(ctx);
        drop(run); // release concurrency permits before any backpressured re-enqueue

        let res = match outcome {
            Outcome::Cancelled => {
                unfinished.fetch_add(1, SeqCst);
                in_flight.dec();
                return;
            }
            Outcome::Done(res) => res,
        };
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
                            let _ = queue.send(env).await;
                        }
                        Some(delay) => {
                            let queue = queue.clone();
                            let cancel = cancel.clone();
                            let in_flight = in_flight.clone();
                            let unfinished = unfinished.clone();
                            tokio::spawn(async move {
                                tokio::select! {
                                    _ = tokio::time::sleep(delay) => {
                                        let _ = queue.send(env).await;
                                    }
                                    _ = cancel.cancelled() => {
                                        unfinished.fetch_add(1, SeqCst);
                                        in_flight.dec();
                                    }
                                }
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
