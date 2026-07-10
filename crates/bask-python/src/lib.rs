/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Low-level pyo3 bindings that drive the Rust `bask` engine with Python workers.
//! The Pythonic decorator API lives in `python/bask/__init__.py` on top of this.
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use bask::{
    Backoff, Cancellation, Context, DynWorker, Emitter, LiveConsole, RetryPolicy, Shutdown,
    WorkerCfg,
};

/// Interns a Python class name to a `'static` string, keyed by the class pointer.
/// The set of task classes is small and lives for the process, so leaking once is fine.
fn intern(key: u64, name: &str) -> &'static str {
    static NAMES: OnceLock<Mutex<HashMap<u64, &'static str>>> = OnceLock::new();
    let map = NAMES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock().unwrap();
    guard
        .entry(key)
        .or_insert_with(|| Box::leak(name.to_owned().into_boxed_str()))
}

/// The routing key of a class is its object pointer (equal to Python `id(cls)`).
fn class_key_name(cls: &Bound<'_, PyAny>) -> PyResult<(u64, String)> {
    let key = cls.as_ptr() as u64;
    let name: String = cls.getattr("__name__")?.extract()?;
    Ok((key, name))
}

/// A registered Python worker: a `process(task, ctx)` callable plus the shared
/// router and dedup registries it feeds.
struct PyWorker {
    process: Py<PyAny>,
    routers: Py<PyAny>,
    dedups: Py<PyAny>,
}

#[async_trait]
impl DynWorker for PyWorker {
    async fn process(
        &self,
        payload: &(dyn Any + Send + Sync),
        ctx: &Context,
    ) -> anyhow::Result<()> {
        let (task, process, routers, dedups) = Python::attach(|py| {
            let task = payload
                .downcast_ref::<Py<PyAny>>()
                .expect("python payload")
                .clone_ref(py);
            (
                task,
                self.process.clone_ref(py),
                self.routers.clone_ref(py),
                self.dedups.clone_ref(py),
            )
        });
        let emitter = ctx.emitter();
        let cancellation = ctx.cancellation();

        // Run the (blocking) Python call off the async worker threads so the router
        // is never starved; the GIL still serializes Python execution.
        let outcome = tokio::task::spawn_blocking(move || -> Result<(), String> {
            Python::attach(|py| {
                let ctx = Bound::new(
                    py,
                    Ctx {
                        emitter,
                        cancellation,
                        routers,
                        dedups,
                    },
                )
                .map_err(|e| e.to_string())?;
                process
                    .bind(py)
                    .call1((task.bind(py), ctx))
                    .map_err(|e| e.to_string())?;
                Ok(())
            })
        })
        .await;

        match outcome {
            Ok(Ok(())) => Ok(()),
            Ok(Err(message)) => Err(anyhow::anyhow!(message)),
            Err(join) => Err(anyhow::anyhow!("worker thread panicked: {join}")),
        }
    }
}

/// Handed to each Python worker as `ctx`.
#[pyclass]
struct Ctx {
    emitter: Emitter,
    cancellation: Cancellation,
    routers: Py<PyAny>,
    dedups: Py<PyAny>,
}

#[pymethods]
impl Ctx {
    /// Enqueue a new task; routed by its Python class.
    fn emit(&self, py: Python<'_>, task: Py<PyAny>) -> PyResult<()> {
        let ty = task.bind(py).get_type();
        let (key, name) = class_key_name(ty.as_any())?;
        let type_name = intern(key, &name);
        // Release the GIL while emitting: a full queue parks this worker in `emit_dyn`,
        // and the dispatcher needs the GIL to run the Python workers that drain it, so
        // holding it here would deadlock under backpressure.
        py.detach(|| self.emitter.emit_dyn(key, type_name, Box::new(task)))
            .map_err(|_| PyRuntimeError::new_err("engine stopped"))?;
        Ok(())
    }

    /// Feed a value to the router registered for `router_cls`. Its `route(value, out)`
    /// folds the value into state and may `out.emit(task)` derived tasks, which are
    /// enqueued here under the same backpressure as `emit`.
    fn route(&self, py: Python<'_>, router_cls: Py<PyAny>, value: Py<PyAny>) -> PyResult<()> {
        let router = self.routers.bind(py).get_item(router_cls)?;
        let out = Bound::new(py, RouterOut { buffer: Vec::new() })?;
        router.call_method1("route", (value, &out))?;
        let buffered = std::mem::take(&mut out.borrow_mut().buffer);
        py.detach(|| {
            for (key, type_name, payload) in buffered {
                self.emitter
                    .emit_dyn(key, type_name, Box::new(payload))
                    .map_err(|_| PyRuntimeError::new_err("engine stopped"))?;
            }
            Ok(())
        })
    }

    /// Test-and-set against the dedup set `marker`: `True` the first time `key` is
    /// seen, `False` after. Serialized by the GIL, so it is atomic across workers.
    fn first_seen(&self, py: Python<'_>, marker: Py<PyAny>, key: Py<PyAny>) -> PyResult<bool> {
        let seen = self.dedups.bind(py).get_item(marker)?;
        if seen
            .call_method1("__contains__", (key.bind(py),))?
            .extract::<bool>()?
        {
            return Ok(false);
        }
        seen.call_method1("add", (key,))?;
        Ok(true)
    }

    /// Whether a graceful shutdown has escalated to cancellation; long-running workers
    /// should poll this and return early.
    fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

/// The emit handle a Python router receives as `out`; buffers tasks routed by class.
#[pyclass]
struct RouterOut {
    buffer: Vec<(u64, &'static str, Py<PyAny>)>,
}

#[pymethods]
impl RouterOut {
    /// Emit a task downstream: none = filter, a new class = route, many = fan-out or a batch.
    fn emit(&mut self, py: Python<'_>, task: Py<PyAny>) -> PyResult<()> {
        let ty = task.bind(py).get_type();
        let (key, name) = class_key_name(ty.as_any())?;
        self.buffer.push((key, intern(key, &name), task));
        Ok(())
    }
}

struct Registration {
    key: u64,
    type_name: &'static str,
    process: Py<PyAny>,
    label: Option<String>,
    concurrency: Option<usize>,
    timeout_ms: Option<u64>,
}

struct Seed {
    key: u64,
    type_name: &'static str,
    payload: Py<PyAny>,
}

/// A handle to request a graceful shutdown; pass to `Engine.run(shutdown=...)` and call
/// `trigger()` from another thread or a signal handler.
#[pyclass(name = "Shutdown")]
struct PyShutdown {
    inner: Shutdown,
}

#[pymethods]
impl PyShutdown {
    #[new]
    fn new() -> Self {
        PyShutdown {
            inner: Shutdown::new(),
        }
    }

    fn trigger(&self) {
        self.inner.trigger();
    }

    fn is_triggered(&self) -> bool {
        self.inner.is_triggered()
    }
}

/// Accumulates registrations, then runs the Rust engine with Python workers.
#[pyclass]
struct Engine {
    concurrency: usize,
    max_attempts: u32,
    avoid_failed: bool,
    backoff_ms: u64,
    sample_interval_ms: u64,
    queue_capacity: Option<usize>,
    timeout_ms: Option<u64>,
    grace_ms: Option<u64>,
    catch_ctrl_c: bool,
    registrations: Vec<Registration>,
    seeds: Vec<Seed>,
}

#[pymethods]
impl Engine {
    #[new]
    #[pyo3(signature = (concurrency, max_attempts=1, avoid_failed=true, backoff_ms=0, sample_interval_ms=200, queue_capacity=None, timeout_ms=None, grace_ms=None, catch_ctrl_c=false))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        concurrency: usize,
        max_attempts: u32,
        avoid_failed: bool,
        backoff_ms: u64,
        sample_interval_ms: u64,
        queue_capacity: Option<usize>,
        timeout_ms: Option<u64>,
        grace_ms: Option<u64>,
        catch_ctrl_c: bool,
    ) -> Self {
        Engine {
            concurrency: concurrency.max(1),
            max_attempts,
            avoid_failed,
            backoff_ms,
            sample_interval_ms,
            queue_capacity,
            timeout_ms,
            grace_ms,
            catch_ctrl_c,
            registrations: Vec::new(),
            seeds: Vec::new(),
        }
    }

    #[pyo3(signature = (task_cls, process, label=None, concurrency=None, timeout_ms=None))]
    fn register(
        &mut self,
        py: Python<'_>,
        task_cls: Py<PyAny>,
        process: Py<PyAny>,
        label: Option<String>,
        concurrency: Option<usize>,
        timeout_ms: Option<u64>,
    ) -> PyResult<()> {
        let (key, name) = class_key_name(task_cls.bind(py))?;
        self.registrations.push(Registration {
            key,
            type_name: intern(key, &name),
            process,
            label,
            concurrency,
            timeout_ms,
        });
        Ok(())
    }

    fn seed(&mut self, py: Python<'_>, task: Py<PyAny>) -> PyResult<()> {
        let ty = task.bind(py).get_type();
        let (key, name) = class_key_name(ty.as_any())?;
        self.seeds.push(Seed {
            key,
            type_name: intern(key, &name),
            payload: task,
        });
        Ok(())
    }

    #[pyo3(signature = (routers, dedups, live=false, shutdown=None))]
    fn run(
        &self,
        py: Python<'_>,
        routers: Py<PyAny>,
        dedups: Py<PyAny>,
        live: bool,
        shutdown: Option<Py<PyShutdown>>,
    ) -> PyResult<Py<PyAny>> {
        let mut retry = RetryPolicy::new().max_attempts(self.max_attempts);
        retry = if self.avoid_failed {
            retry.avoid_failed()
        } else {
            retry.any_instance()
        };
        if self.backoff_ms > 0 {
            retry = retry.backoff(Backoff::Fixed(Duration::from_millis(self.backoff_ms)));
        }

        let mut builder = bask::Engine::builder()
            .concurrency(self.concurrency)
            .retry(retry)
            .sample_interval(Duration::from_millis(self.sample_interval_ms));
        if let Some(capacity) = self.queue_capacity {
            builder = builder.queue_capacity(capacity);
        }
        if let Some(ms) = self.timeout_ms {
            builder = builder.timeout(Duration::from_millis(ms));
        }
        if let Some(ms) = self.grace_ms {
            builder = builder.grace_period(Duration::from_millis(ms));
        }
        if self.catch_ctrl_c {
            builder = builder.catch_ctrl_c();
        }
        if let Some(handle) = &shutdown {
            builder = builder.shutdown(handle.borrow(py).inner.clone());
        }
        if live {
            builder = builder.monitor(LiveConsole::new());
        }
        for reg in &self.registrations {
            let worker: Arc<dyn DynWorker> = Arc::new(PyWorker {
                process: reg.process.clone_ref(py),
                routers: routers.clone_ref(py),
                dedups: dedups.clone_ref(py),
            });
            let mut cfg = WorkerCfg::new();
            if let Some(label) = &reg.label {
                cfg = cfg.label(label.clone());
            }
            if let Some(c) = reg.concurrency {
                cfg = cfg.concurrency(c);
            }
            if let Some(ms) = reg.timeout_ms {
                cfg = cfg.timeout(Duration::from_millis(ms));
            }
            builder = builder.worker_dyn(reg.key, worker, reg.type_name, cfg);
        }
        for seed in &self.seeds {
            let payload = Box::new(seed.payload.clone_ref(py));
            builder = builder.seed_dyn(seed.key, seed.type_name, payload);
        }

        // Flush Python routers each epoch, so a batching router's trailing batch still
        // flows before the run ends (model 2), mirroring the core flush-epoch.
        let flush_routers = routers.clone_ref(py);
        builder = builder.flush_hook(move |out| {
            Python::attach(|py| {
                let Ok(values) = flush_routers.bind(py).call_method0("values") else {
                    return;
                };
                let Ok(iter) = values.try_iter() else {
                    return;
                };
                for router in iter.flatten() {
                    if !router.hasattr("flush").unwrap_or(false) {
                        continue;
                    }
                    let Ok(collected) = Bound::new(py, RouterOut { buffer: Vec::new() }) else {
                        continue;
                    };
                    if router.call_method1("flush", (&collected,)).is_err() {
                        continue;
                    }
                    let buffered = std::mem::take(&mut collected.borrow_mut().buffer);
                    for (key, type_name, payload) in buffered {
                        out.emit_dyn(key, type_name, Box::new(payload));
                    }
                }
            });
        });

        let engine = builder.build();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| PyRuntimeError::new_err(format!("tokio runtime: {e}")))?;
        // Drop the runtime with the GIL released. A graceful shutdown can leave cancelled
        // `spawn_blocking` workers running; the runtime's drop waits for them and they need
        // the GIL to finish, so holding it here would deadlock.
        let report = py
            .detach(|| {
                let report = runtime.block_on(engine.run());
                drop(runtime);
                report
            })
            .map_err(|e| PyRuntimeError::new_err(format!("{e}")))?;

        let dict = PyDict::new(py);
        dict.set_item("processed", report.stats.processed)?;
        dict.set_item("retried", report.stats.retried)?;
        dict.set_item("failed", report.stats.failed)?;
        dict.set_item("interrupted", report.interrupted)?;
        dict.set_item("unfinished", report.unfinished)?;
        let failures = PyList::empty(py);
        for failure in &report.failures {
            let item = PyDict::new(py);
            item.set_item("task_type", failure.task_type)?;
            item.set_item("instance", failure.instance.as_str())?;
            item.set_item("attempts", failure.attempts)?;
            item.set_item("error", failure.error.as_str())?;
            failures.append(item)?;
        }
        dict.set_item("failures", failures)?;
        Ok(dict.into_any().unbind())
    }
}

#[pymodule]
fn _bask(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Engine>()?;
    m.add_class::<PyShutdown>()?;
    Ok(())
}
