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

use bask::{Backoff, Context, DynWorker, Emitter, LiveConsole, RetryPolicy, WorkerCfg};

/// Interns a Python class name to a `'static` string, keyed by the class pointer.
/// The set of task classes is small and lives for the process, so leaking once is fine.
fn intern(key: u64, name: &str) -> &'static str {
    static NAMES: OnceLock<Mutex<HashMap<u64, &'static str>>> = OnceLock::new();
    let map = NAMES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock().unwrap();
    guard.entry(key).or_insert_with(|| Box::leak(name.to_owned().into_boxed_str()))
}

/// The routing key of a class is its object pointer (equal to Python `id(cls)`).
fn class_key_name(cls: &Bound<'_, PyAny>) -> PyResult<(u64, String)> {
    let key = cls.as_ptr() as u64;
    let name: String = cls.getattr("__name__")?.extract()?;
    Ok((key, name))
}

/// A registered Python worker: a `process(task, ctx)` callable plus the shared
/// aggregator and dedup registries it folds into.
struct PyWorker {
    process: Py<PyAny>,
    aggregators: Py<PyAny>,
    dedups: Py<PyAny>,
}

#[async_trait]
impl DynWorker for PyWorker {
    async fn process(
        &self,
        payload: &(dyn Any + Send + Sync),
        ctx: &Context,
    ) -> anyhow::Result<()> {
        let (task, process, aggregators, dedups) = Python::with_gil(|py| {
            let task = payload.downcast_ref::<Py<PyAny>>().expect("python payload").clone_ref(py);
            (
                task,
                self.process.clone_ref(py),
                self.aggregators.clone_ref(py),
                self.dedups.clone_ref(py),
            )
        });
        let emitter = ctx.emitter();

        // Run the (blocking) Python call off the async worker threads so the router
        // is never starved; the GIL still serializes Python execution.
        let outcome = tokio::task::spawn_blocking(move || -> Result<(), String> {
            Python::with_gil(|py| {
                let ctx = Bound::new(py, Ctx { emitter, aggregators, dedups })
                    .map_err(|e| e.to_string())?;
                process.bind(py).call1((task.bind(py), ctx)).map_err(|e| e.to_string())?;
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
    aggregators: Py<PyAny>,
    dedups: Py<PyAny>,
}

#[pymethods]
impl Ctx {
    /// Enqueue a new task; routed by its Python class.
    fn emit(&self, py: Python<'_>, task: Py<PyAny>) -> PyResult<()> {
        let ty = task.bind(py).get_type();
        let (key, name) = class_key_name(ty.as_any())?;
        self.emitter
            .emit_dyn(key, intern(key, &name), Box::new(task))
            .map_err(|_| PyRuntimeError::new_err("engine stopped"))?;
        Ok(())
    }

    /// Contribute a value to the aggregator registered for `agg_cls`.
    fn aggregate(&self, py: Python<'_>, agg_cls: Py<PyAny>, value: Py<PyAny>) -> PyResult<()> {
        let aggregator = self.aggregators.bind(py).get_item(agg_cls)?;
        aggregator.call_method1("fold", (value,))?;
        Ok(())
    }

    /// Test-and-set against the dedup set `marker`: `True` the first time `key` is
    /// seen, `False` after. Serialized by the GIL, so it is atomic across workers.
    fn first_seen(&self, py: Python<'_>, marker: Py<PyAny>, key: Py<PyAny>) -> PyResult<bool> {
        let seen = self.dedups.bind(py).get_item(marker)?;
        if seen.call_method1("__contains__", (key.bind(py),))?.extract::<bool>()? {
            return Ok(false);
        }
        seen.call_method1("add", (key,))?;
        Ok(true)
    }
}

struct Registration {
    key: u64,
    type_name: &'static str,
    process: Py<PyAny>,
    label: Option<String>,
    concurrency: Option<usize>,
}

struct Seed {
    key: u64,
    type_name: &'static str,
    payload: Py<PyAny>,
}

/// Accumulates registrations, then runs the Rust engine with Python workers.
#[pyclass]
struct Engine {
    concurrency: usize,
    max_attempts: u32,
    avoid_failed: bool,
    backoff_ms: u64,
    sample_interval_ms: u64,
    registrations: Vec<Registration>,
    seeds: Vec<Seed>,
}

#[pymethods]
impl Engine {
    #[new]
    #[pyo3(signature = (concurrency, max_attempts=1, avoid_failed=true, backoff_ms=0, sample_interval_ms=200))]
    fn new(
        concurrency: usize,
        max_attempts: u32,
        avoid_failed: bool,
        backoff_ms: u64,
        sample_interval_ms: u64,
    ) -> Self {
        Engine {
            concurrency: concurrency.max(1),
            max_attempts,
            avoid_failed,
            backoff_ms,
            sample_interval_ms,
            registrations: Vec::new(),
            seeds: Vec::new(),
        }
    }

    #[pyo3(signature = (task_cls, process, label=None, concurrency=None))]
    fn register(
        &mut self,
        py: Python<'_>,
        task_cls: Py<PyAny>,
        process: Py<PyAny>,
        label: Option<String>,
        concurrency: Option<usize>,
    ) -> PyResult<()> {
        let (key, name) = class_key_name(task_cls.bind(py))?;
        self.registrations.push(Registration {
            key,
            type_name: intern(key, &name),
            process,
            label,
            concurrency,
        });
        Ok(())
    }

    fn seed(&mut self, py: Python<'_>, task: Py<PyAny>) -> PyResult<()> {
        let ty = task.bind(py).get_type();
        let (key, name) = class_key_name(ty.as_any())?;
        self.seeds.push(Seed { key, type_name: intern(key, &name), payload: task });
        Ok(())
    }

    #[pyo3(signature = (aggregators, dedups, live=false))]
    fn run(
        &self,
        py: Python<'_>,
        aggregators: Py<PyAny>,
        dedups: Py<PyAny>,
        live: bool,
    ) -> PyResult<Py<PyAny>> {
        let mut retry = RetryPolicy::new().max_attempts(self.max_attempts);
        retry = if self.avoid_failed { retry.avoid_failed() } else { retry.any_instance() };
        if self.backoff_ms > 0 {
            retry = retry.backoff(Backoff::Fixed(Duration::from_millis(self.backoff_ms)));
        }

        let mut builder = bask::Engine::builder()
            .concurrency(self.concurrency)
            .retry(retry)
            .sample_interval(Duration::from_millis(self.sample_interval_ms));
        if live {
            builder = builder.monitor(LiveConsole::new());
        }
        for reg in &self.registrations {
            let worker: Arc<dyn DynWorker> = Arc::new(PyWorker {
                process: reg.process.clone_ref(py),
                aggregators: aggregators.clone_ref(py),
                dedups: dedups.clone_ref(py),
            });
            let mut cfg = WorkerCfg::new();
            if let Some(label) = &reg.label {
                cfg = cfg.label(label.clone());
            }
            if let Some(c) = reg.concurrency {
                cfg = cfg.concurrency(c);
            }
            builder = builder.worker_dyn(reg.key, worker, reg.type_name, cfg);
        }
        for seed in &self.seeds {
            let payload = Box::new(seed.payload.clone_ref(py));
            builder = builder.seed_dyn(seed.key, seed.type_name, payload);
        }

        let engine = builder.build();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| PyRuntimeError::new_err(format!("tokio runtime: {e}")))?;
        let report = py
            .allow_threads(|| runtime.block_on(engine.run()))
            .map_err(|e| PyRuntimeError::new_err(format!("{e}")))?;

        let dict = PyDict::new(py);
        dict.set_item("processed", report.stats.processed)?;
        dict.set_item("retried", report.stats.retried)?;
        dict.set_item("failed", report.stats.failed)?;
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
    Ok(())
}
