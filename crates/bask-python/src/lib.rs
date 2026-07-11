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
use pyo3::types::{PyBytes, PyDict, PyList};

use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use bask_core::{
    Backoff, Cancellation, CheckpointOps, Committed, Context, Coverage, Dataset, DeadLetter,
    DeadLetterSink, DynWorker, Emitter, LiveConsole, RetryExt, RetryOn, RetryPolicy, Shutdown,
    SqliteStore, Status, Store, StoredItem, WorkerCfg,
};
use bask_io::FileDataset;

/// A retry hint a Python worker attaches to its exception via `_bask_retry`, mapped onto
/// the Rust [`RetryOn`]. The predicate variant (`AnyWith`) is Rust-only.
enum HintTag {
    Fatal,
    SameInstance,
    DifferentInstance,
    DifferentAttr(String),
}

impl From<HintTag> for RetryOn {
    fn from(tag: HintTag) -> Self {
        match tag {
            HintTag::Fatal => RetryOn::Fatal,
            HintTag::SameInstance => RetryOn::SameInstance,
            HintTag::DifferentInstance => RetryOn::DifferentInstance,
            HintTag::DifferentAttr(key) => RetryOn::DifferentAttr(key),
        }
    }
}

/// How a Python `process` call failed: its message and any retry hint it carried.
struct WorkerFail {
    message: String,
    hint: Option<HintTag>,
}

/// Read the `_bask_retry` tag a raised exception may carry (e.g. `("different_attr", "gpu")`).
fn hint_from_pyerr(py: Python<'_>, err: &PyErr) -> Option<HintTag> {
    let tag = err.value(py).as_any().getattr("_bask_retry").ok()?;
    if tag.is_none() {
        return None;
    }
    let parts: Vec<String> = tag.extract().ok()?;
    Some(match parts.first()?.as_str() {
        "fatal" => HintTag::Fatal,
        "same_instance" => HintTag::SameInstance,
        "different_instance" => HintTag::DifferentInstance,
        "different_attr" => HintTag::DifferentAttr(parts.get(1)?.clone()),
        _ => return None,
    })
}

/// Build a Rust retry policy from the Python `Retry` scalars.
fn make_retry(max_attempts: u32, avoid_failed: bool, backoff_ms: u64, jitter: f64) -> RetryPolicy {
    let mut retry = RetryPolicy::new().max_attempts(max_attempts);
    retry = if avoid_failed {
        retry.avoid_failed()
    } else {
        retry.any_instance()
    };
    if backoff_ms > 0 {
        retry = retry.backoff(Backoff::Fixed(Duration::from_millis(backoff_ms)));
    }
    retry.jitter(jitter)
}

/// A dead-letter sink that calls back into Python with the failed task and its error.
struct PyDeadLetter {
    callback: Py<PyAny>,
}

impl DeadLetterSink for PyDeadLetter {
    fn dead_letter(&self, letter: DeadLetter) {
        Python::attach(|py| {
            let dict = PyDict::new(py);
            if let Some(task) = letter.payload.downcast_ref::<Py<PyAny>>() {
                let _ = dict.set_item("task", task.clone_ref(py));
            }
            let _ = dict.set_item("task_type", letter.task_type);
            let _ = dict.set_item("error", letter.error);
            let _ = dict.set_item("attempts", letter.attempts);
            let _ = dict.set_item("instance", letter.instance);
            let _ = self.callback.bind(py).call1((dict,));
        });
    }
}

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

/// A Python checkpoint class exposed to the engine: `key()`/`encode()` on instances and a
/// `decode(bytes)` classmethod let the durable store materialize and replay Python tasks.
struct PyCheckpoint {
    name: String,
    key_only: bool,
    cls: Py<PyAny>,
}

impl CheckpointOps for PyCheckpoint {
    fn name(&self) -> &str {
        &self.name
    }

    fn key_only(&self) -> bool {
        self.key_only
    }

    fn key(&self, payload: &(dyn std::any::Any + Send + Sync)) -> anyhow::Result<String> {
        Python::attach(|py| -> PyResult<String> {
            let obj = payload.downcast_ref::<Py<PyAny>>().expect("python payload");
            obj.bind(py).call_method0("key")?.str()?.extract()
        })
        .map_err(|e: PyErr| anyhow::anyhow!("checkpoint key(): {e}"))
    }

    fn encode(&self, payload: &(dyn std::any::Any + Send + Sync)) -> anyhow::Result<Vec<u8>> {
        Python::attach(|py| -> PyResult<Vec<u8>> {
            let obj = payload.downcast_ref::<Py<PyAny>>().expect("python payload");
            obj.bind(py).call_method0("encode")?.extract()
        })
        .map_err(|e: PyErr| anyhow::anyhow!("checkpoint encode(): {e}"))
    }

    fn decode(&self, bytes: &[u8]) -> anyhow::Result<Box<dyn std::any::Any + Send + Sync>> {
        Python::attach(|py| -> PyResult<Box<dyn std::any::Any + Send + Sync>> {
            let obj = self
                .cls
                .bind(py)
                .call_method1("decode", (PyBytes::new(py, bytes),))?;
            Ok(Box::new(obj.unbind()))
        })
        .map_err(|e: PyErr| anyhow::anyhow!("checkpoint decode(): {e}"))
    }
}

/// The built-in directory-backed dataset exposed to Python: content-addressed Parquet
/// shards over one `bask.sqlite`, self-compacting by coverage. `read()` yields the live
/// shard payloads for the `bask.data.Dataset` wrapper to decode with pyarrow.
#[pyclass(name = "FileDataset")]
struct PyFileDataset {
    inner: FileDataset,
}

#[pymethods]
impl PyFileDataset {
    #[new]
    fn new(path: String) -> PyResult<Self> {
        FileDataset::open(path)
            .map(|inner| PyFileDataset { inner })
            .map_err(|e| PyRuntimeError::new_err(format!("open dataset: {e}")))
    }

    fn read<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let shards = self
            .inner
            .read()
            .map_err(|e| PyRuntimeError::new_err(format!("read dataset: {e}")))?;
        let out = PyList::empty(py);
        for shard in shards {
            out.append(PyBytes::new(py, &shard.payload))?;
        }
        Ok(out)
    }
}

/// A custom Python object driven as a [`Dataset`]: its `commit`/`put`/`read`/... methods are
/// the durable backing, so a developer implements one protocol to target any database. Rows
/// cross the boundary as `(key, payload, coverage_bytes)` tuples; coverage is opaque bytes
/// (see `coverage_rows`). A shared lock serializes every call, so the Python object sees
/// strictly one-at-a-time access (sqlite releases the GIL mid-call, and the engine's tail
/// `consume` can overlap the next task's `commit`) and need not be thread-safe itself.
#[derive(Clone)]
struct PyDataset {
    obj: Arc<Py<PyAny>>,
    lock: Arc<Mutex<()>>,
}

impl PyDataset {
    fn items(&self, method: &str, name: Option<&str>) -> anyhow::Result<Vec<StoredItem>> {
        let _guard = self.lock.lock().unwrap();
        Python::attach(|py| -> PyResult<Vec<StoredItem>> {
            let bound = self.obj.bind(py);
            let result = match name {
                Some(name) => bound.call_method1(method, (name,))?,
                None => bound.call_method0(method)?,
            };
            let mut out = Vec::new();
            for row in result.try_iter()? {
                let row = row?;
                out.push(StoredItem {
                    key: row.get_item(0)?.extract()?,
                    payload: row.get_item(1)?.extract()?,
                    coverage: Coverage::from_bytes(&row.get_item(2)?.extract::<Vec<u8>>()?),
                });
            }
            Ok(out)
        })
        .map_err(|e: PyErr| anyhow::anyhow!("dataset.{method}(): {e}"))
    }
}

impl Store for PyDataset {
    fn statuses(&self) -> anyhow::Result<Vec<(String, String, Status)>> {
        let _guard = self.lock.lock().unwrap();
        Python::attach(|py| -> PyResult<Vec<(String, String, Status)>> {
            let result = self.obj.bind(py).call_method0("statuses")?;
            let mut out = Vec::new();
            for row in result.try_iter()? {
                let row = row?;
                let status = if row.get_item(2)?.extract::<i64>()? == 1 {
                    Status::Consumed
                } else {
                    Status::Stored
                };
                out.push((
                    row.get_item(0)?.extract()?,
                    row.get_item(1)?.extract()?,
                    status,
                ));
            }
            Ok(out)
        })
        .map_err(|e: PyErr| anyhow::anyhow!("dataset.statuses(): {e}"))
    }

    fn covered(&self) -> anyhow::Result<Coverage> {
        let _guard = self.lock.lock().unwrap();
        Python::attach(|py| -> PyResult<Coverage> {
            let bytes: Vec<u8> = self.obj.bind(py).call_method0("covered")?.extract()?;
            Ok(Coverage::from_bytes(&bytes))
        })
        .map_err(|e: PyErr| anyhow::anyhow!("dataset.covered(): {e}"))
    }

    fn extents(&self) -> anyhow::Result<HashMap<String, Coverage>> {
        let _guard = self.lock.lock().unwrap();
        Python::attach(|py| -> PyResult<HashMap<String, Coverage>> {
            let result = self.obj.bind(py).call_method0("extents")?;
            let mut out = HashMap::new();
            for row in result.try_iter()? {
                let row = row?;
                let source: String = row.get_item(0)?.extract()?;
                out.insert(
                    source,
                    Coverage::from_bytes(&row.get_item(1)?.extract::<Vec<u8>>()?),
                );
            }
            Ok(out)
        })
        .map_err(|e: PyErr| anyhow::anyhow!("dataset.extents(): {e}"))
    }

    fn stored_items(&self, name: &str) -> anyhow::Result<Vec<StoredItem>> {
        self.items("stored_items", Some(name))
    }

    fn commit(&self, rec: &Committed) -> anyhow::Result<()> {
        let _guard = self.lock.lock().unwrap();
        Python::attach(|py| -> PyResult<()> {
            let payload = rec.payload.as_ref().map(|b| PyBytes::new(py, b));
            self.obj.bind(py).call_method1(
                "commit",
                (
                    rec.name.as_str(),
                    rec.key.as_str(),
                    payload,
                    PyBytes::new(py, &rec.coverage.to_bytes()),
                ),
            )?;
            Ok(())
        })
        .map_err(|e: PyErr| anyhow::anyhow!("dataset.commit(): {e}"))
    }

    fn consume(&self, name: &str, key: &str) -> anyhow::Result<()> {
        let _guard = self.lock.lock().unwrap();
        Python::attach(|py| -> PyResult<()> {
            self.obj.bind(py).call_method1("consume", (name, key))?;
            Ok(())
        })
        .map_err(|e: PyErr| anyhow::anyhow!("dataset.consume(): {e}"))
    }

    fn record_extent(&self, source: &str, extent: &Coverage) -> anyhow::Result<()> {
        let _guard = self.lock.lock().unwrap();
        Python::attach(|py| -> PyResult<()> {
            self.obj.bind(py).call_method1(
                "record_extent",
                (source, PyBytes::new(py, &extent.to_bytes())),
            )?;
            Ok(())
        })
        .map_err(|e: PyErr| anyhow::anyhow!("dataset.record_extent(): {e}"))
    }
}

impl Dataset for PyDataset {
    fn store(&self) -> Arc<dyn Store> {
        Arc::new(self.clone())
    }

    fn put(&self, item: &Committed) -> anyhow::Result<()> {
        let Some(bytes) = &item.payload else {
            return Ok(());
        };
        let _guard = self.lock.lock().unwrap();
        Python::attach(|py| -> PyResult<()> {
            self.obj.bind(py).call_method1(
                "put",
                (
                    item.name.as_str(),
                    item.key.as_str(),
                    PyBytes::new(py, bytes),
                    PyBytes::new(py, &item.coverage.to_bytes()),
                ),
            )?;
            Ok(())
        })
        .map_err(|e: PyErr| anyhow::anyhow!("dataset.put(): {e}"))
    }

    fn stored(&self, name: &str) -> anyhow::Result<Vec<StoredItem>> {
        self.items("stored", Some(name))
    }

    fn read(&self) -> anyhow::Result<Vec<StoredItem>> {
        self.items("read", None)
    }
}

/// The source-row ordinals a `Coverage` blob carries, so a custom Python dataset can compute
/// supersession (subset/union) over shard coverage without knowing the wire format.
#[pyfunction]
fn coverage_rows(bytes: &[u8]) -> Vec<u64> {
    Coverage::from_bytes(bytes).iter().collect()
}

/// Encode source-row ordinals back into a `Coverage` blob (the inverse of `coverage_rows`).
#[pyfunction]
fn coverage_to_bytes<'py>(py: Python<'py>, rows: Vec<u64>) -> Bound<'py, PyBytes> {
    let mut cov = Coverage::empty();
    for row in rows {
        cov.insert(row);
    }
    PyBytes::new(py, &cov.to_bytes())
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
        let outcome = tokio::task::spawn_blocking(move || -> Result<(), WorkerFail> {
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
                .map_err(|e| WorkerFail {
                    message: e.to_string(),
                    hint: None,
                })?;
                process.bind(py).call1((task.bind(py), ctx)).map_err(|e| {
                    let hint = hint_from_pyerr(py, &e);
                    WorkerFail {
                        message: e.to_string(),
                        hint,
                    }
                })?;
                Ok(())
            })
        })
        .await;

        match outcome {
            Ok(Ok(())) => Ok(()),
            Ok(Err(fail)) => {
                let res: anyhow::Result<()> = Err(anyhow::anyhow!(fail.message));
                match fail.hint {
                    Some(tag) => res.retry_on(tag.into()),
                    None => res,
                }
            }
            Err(join) => Err(anyhow::anyhow!("worker thread panicked: {join}")),
        }
    }
}

/// Runs the Rust `bask_tasks::chunk` stage over pyarrow data: it reads the `batch`
/// attribute (a pyarrow RecordBatch) off each source task, splits it into fixed-row
/// pieces in Rust, and emits each piece as `piece_cls(pyarrow_batch)`.
struct ChunkerBridge {
    rows: usize,
    piece_key: u64,
    piece_type: &'static str,
    piece_cls: Py<PyAny>,
}

#[async_trait]
impl DynWorker for ChunkerBridge {
    async fn process(
        &self,
        payload: &(dyn Any + Send + Sync),
        ctx: &Context,
    ) -> anyhow::Result<()> {
        let (source, piece_cls) = Python::attach(|py| {
            (
                payload
                    .downcast_ref::<Py<PyAny>>()
                    .expect("python payload")
                    .clone_ref(py),
                self.piece_cls.clone_ref(py),
            )
        });
        let emitter = ctx.emitter();
        let (rows, piece_key, piece_type) = (self.rows, self.piece_key, self.piece_type);

        // Convert and slice off the async threads; the GIL still guards each conversion,
        // and emit releases it so a full queue cannot deadlock the dispatcher.
        let outcome = tokio::task::spawn_blocking(move || -> Result<(), String> {
            let batch = Python::attach(|py| {
                RecordBatch::from_pyarrow_bound(&source.bind(py).getattr("batch")?)
            })
            .map_err(|e: PyErr| e.to_string())?;

            for piece in bask_tasks::chunk(&batch, rows) {
                Python::attach(|py| -> PyResult<()> {
                    let obj = piece_cls.bind(py).call1((piece.to_pyarrow(py)?,))?.unbind();
                    py.detach(|| emitter.emit_dyn(piece_key, piece_type, Box::new(obj)))
                        .map_err(|_| PyRuntimeError::new_err("engine stopped"))
                })
                .map_err(|e| e.to_string())?;
            }
            Ok(())
        })
        .await;

        match outcome {
            Ok(Ok(())) => Ok(()),
            Ok(Err(message)) => Err(anyhow::anyhow!(message)),
            Err(join) => Err(anyhow::anyhow!("chunker thread panicked: {join}")),
        }
    }
}

/// A pyarrow view of the Rust row-count aggregator, driving the `bask.tasks.RowBatch`
/// router: it wraps each full group in `group_cls` and emits it onto the router's `out`,
/// carrying the union of the source rows folded since its last emit so a group-derived
/// checkpoint traces back to exactly the rows it covers (mirroring the Rust routers).
#[pyclass]
struct RowAggregator {
    inner: bask_tasks::RowAggregator,
    group_cls: Py<PyAny>,
    group_key: u64,
    group_type: &'static str,
    pending: Coverage,
    groups: usize,
}

impl RowAggregator {
    /// Wrap each ready group in `group_cls` and buffer it on `out` with the accumulated
    /// coverage, then reset it, so every input folded since the last emit is attributed.
    fn drain(
        &mut self,
        py: Python<'_>,
        out: &Bound<'_, RouterOut>,
        groups: Vec<RecordBatch>,
    ) -> PyResult<()> {
        if groups.is_empty() {
            return Ok(());
        }
        let coverage = std::mem::take(&mut self.pending);
        let mut sink = out.borrow_mut();
        for batch in groups {
            let task = self
                .group_cls
                .bind(py)
                .call1((batch.to_pyarrow(py)?,))?
                .unbind();
            sink.buffer
                .push((self.group_key, self.group_type, task, coverage.clone()));
            self.groups += 1;
        }
        Ok(())
    }
}

#[pymethods]
impl RowAggregator {
    #[new]
    fn new(py: Python<'_>, rows: usize, group_cls: Py<PyAny>) -> PyResult<Self> {
        let (group_key, group_name) = class_key_name(group_cls.bind(py))?;
        Ok(RowAggregator {
            inner: bask_tasks::RowAggregator::new(rows),
            group_key,
            group_type: intern(group_key, &group_name),
            group_cls,
            pending: Coverage::empty(),
            groups: 0,
        })
    }

    /// Fold one input (a pyarrow RecordBatch and the router's `out`) into the aggregate,
    /// emitting any full groups onto `out`. The input's coverage rides on `out`.
    fn push(
        &mut self,
        py: Python<'_>,
        out: &Bound<'_, RouterOut>,
        batch: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.pending.union_with(&out.borrow().coverage);
        let batch = RecordBatch::from_pyarrow_bound(batch)?;
        let groups = self.inner.push(batch);
        self.drain(py, out, groups)
    }

    /// Emit the buffered remainder as a final group onto `out` at end-of-run.
    fn flush(&mut self, py: Python<'_>, out: &Bound<'_, RouterOut>) -> PyResult<()> {
        let groups = self.inner.flush();
        self.drain(py, out, groups)
    }

    /// The number of groups emitted so far (the router's terminal output).
    fn groups(&self) -> usize {
        self.groups
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

    /// Enqueue a task stamped with an explicit source `key` (a row ordinal). A source
    /// worker calls this per row so a downstream checkpoint traces back to exactly the
    /// rows it covers and a completed pass can be skipped on the next run.
    fn emit_keyed(&self, py: Python<'_>, key: u64, task: Py<PyAny>) -> PyResult<()> {
        let ty = task.bind(py).get_type();
        let (route, name) = class_key_name(ty.as_any())?;
        let type_name = intern(route, &name);
        py.detach(|| {
            self.emitter
                .emit_keyed_dyn(key, route, type_name, Box::new(task))
        })
        .map_err(|_| PyRuntimeError::new_err("engine stopped"))?;
        Ok(())
    }

    /// Feed a value to the router registered for `router_cls`. Its `route(value, out)`
    /// folds the value into state and may `out.emit(task)` derived tasks, which are
    /// enqueued here under the same backpressure as `emit`.
    fn route(&self, py: Python<'_>, router_cls: Py<PyAny>, value: Py<PyAny>) -> PyResult<()> {
        let router = self.routers.bind(py).get_item(router_cls)?;
        let out = Bound::new(
            py,
            RouterOut {
                buffer: Vec::new(),
                coverage: self.emitter.coverage(),
            },
        )?;
        router.call_method1("route", (value, &out))?;
        let buffered = std::mem::take(&mut out.borrow_mut().buffer);
        py.detach(|| {
            for (key, type_name, payload, coverage) in buffered {
                self.emitter
                    .emit_covered_dyn(key, type_name, Box::new(payload), coverage)
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

/// The emit handle a Python router receives as `out`; buffers tasks routed by class, each
/// tagged with the coverage to stamp on it. `coverage` is the folding input's rows, which
/// a plain `emit` inherits and the aggregator unions into a group.
#[pyclass]
struct RouterOut {
    buffer: Vec<(u64, &'static str, Py<PyAny>, Coverage)>,
    coverage: Coverage,
}

#[pymethods]
impl RouterOut {
    /// Emit a task downstream: none = filter, a new class = route, many = fan-out or a batch.
    fn emit(&mut self, py: Python<'_>, task: Py<PyAny>) -> PyResult<()> {
        let ty = task.bind(py).get_type();
        let (key, name) = class_key_name(ty.as_any())?;
        let coverage = self.coverage.clone();
        self.buffer.push((key, intern(key, &name), task, coverage));
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
    attrs: Vec<(String, String)>,
    requires: Vec<String>,
    retry: Option<(u32, bool, u64, f64)>,
}

struct Seed {
    key: u64,
    type_name: &'static str,
    payload: Py<PyAny>,
    source: Option<String>,
}

/// A registered Python checkpoint class and the store identity/policy it carries.
struct CheckpointReg {
    key: u64,
    name: String,
    key_only: bool,
    cls: Py<PyAny>,
}

/// A registered `Chunker` stage bridging pyarrow data through the Rust splitter.
struct ChunkerReg {
    source_key: u64,
    source_type: &'static str,
    piece_key: u64,
    piece_type: &'static str,
    piece_cls: Py<PyAny>,
    rows: usize,
    label: Option<String>,
    concurrency: Option<usize>,
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
    jitter: f64,
    sample_interval_ms: u64,
    queue_capacity: Option<usize>,
    timeout_ms: Option<u64>,
    grace_ms: Option<u64>,
    catch_ctrl_c: bool,
    resources: HashMap<String, usize>,
    dead_letter: Option<Py<PyAny>>,
    store_path: Option<String>,
    dataset: Option<Py<PyAny>>,
    registrations: Vec<Registration>,
    chunkers: Vec<ChunkerReg>,
    checkpoints: Vec<CheckpointReg>,
    seeds: Vec<Seed>,
}

#[pymethods]
impl Engine {
    #[new]
    #[pyo3(signature = (concurrency, max_attempts=1, avoid_failed=true, backoff_ms=0, jitter=0.0, sample_interval_ms=200, queue_capacity=None, timeout_ms=None, grace_ms=None, catch_ctrl_c=false, resources=None, dead_letter=None, store_path=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        concurrency: usize,
        max_attempts: u32,
        avoid_failed: bool,
        backoff_ms: u64,
        jitter: f64,
        sample_interval_ms: u64,
        queue_capacity: Option<usize>,
        timeout_ms: Option<u64>,
        grace_ms: Option<u64>,
        catch_ctrl_c: bool,
        resources: Option<HashMap<String, usize>>,
        dead_letter: Option<Py<PyAny>>,
        store_path: Option<String>,
    ) -> Self {
        Engine {
            concurrency: concurrency.max(1),
            max_attempts,
            avoid_failed,
            backoff_ms,
            jitter,
            sample_interval_ms,
            queue_capacity,
            timeout_ms,
            grace_ms,
            catch_ctrl_c,
            resources: resources.unwrap_or_default(),
            dead_letter,
            store_path,
            dataset: None,
            registrations: Vec::new(),
            chunkers: Vec::new(),
            checkpoints: Vec::new(),
            seeds: Vec::new(),
        }
    }

    /// Materialize data-carrying checkpoints into `obj`: either a built-in `FileDataset` or a
    /// custom object implementing the dataset protocol. Supersedes `store_path`.
    fn dataset(&mut self, obj: Py<PyAny>) {
        self.dataset = Some(obj);
    }

    /// Register a Python checkpoint class (a `Checkpoint` subclass) so its instances are
    /// materialized to the store on arrival and skipped/reseeded on a later run.
    fn checkpoint(&mut self, py: Python<'_>, cls: Py<PyAny>) -> PyResult<()> {
        let bound = cls.bind(py);
        let (key, cls_name) = class_key_name(bound)?;
        let name = bound
            .getattr("NAME")
            .ok()
            .and_then(|n| n.extract().ok())
            .unwrap_or(cls_name);
        let key_only = bound
            .getattr("KEY_ONLY")
            .ok()
            .and_then(|k| k.extract().ok())
            .unwrap_or(false);
        self.checkpoints.push(CheckpointReg {
            key,
            name,
            key_only,
            cls,
        });
        Ok(())
    }

    /// Seed a source `task` tagged with a stable `id`; its extent is recorded on a clean
    /// pass so a later run skips it whole when a checkpoint already covers every row.
    fn source(&mut self, py: Python<'_>, task: Py<PyAny>, id: String) -> PyResult<()> {
        let ty = task.bind(py).get_type();
        let (key, name) = class_key_name(ty.as_any())?;
        self.seeds.push(Seed {
            key,
            type_name: intern(key, &name),
            payload: task,
            source: Some(id),
        });
        Ok(())
    }

    /// Register the Rust `bask_tasks::Chunker` stage: each `source_cls` instance's `batch`
    /// (a pyarrow RecordBatch) is split into pieces of at most `rows` rows, and each piece
    /// is emitted as `piece_cls(pyarrow_batch)`.
    #[pyo3(signature = (source_cls, piece_cls, rows, label=None, concurrency=None))]
    fn chunker(
        &mut self,
        py: Python<'_>,
        source_cls: Py<PyAny>,
        piece_cls: Py<PyAny>,
        rows: usize,
        label: Option<String>,
        concurrency: Option<usize>,
    ) -> PyResult<()> {
        let (source_key, source_name) = class_key_name(source_cls.bind(py))?;
        let (piece_key, piece_name) = class_key_name(piece_cls.bind(py))?;
        self.chunkers.push(ChunkerReg {
            source_key,
            source_type: intern(source_key, &source_name),
            piece_key,
            piece_type: intern(piece_key, &piece_name),
            piece_cls,
            rows: rows.max(1),
            label,
            concurrency,
        });
        Ok(())
    }

    #[pyo3(signature = (task_cls, process, label=None, concurrency=None, timeout_ms=None, attrs=None, requires=None, retry=None))]
    #[allow(clippy::too_many_arguments)]
    fn register(
        &mut self,
        py: Python<'_>,
        task_cls: Py<PyAny>,
        process: Py<PyAny>,
        label: Option<String>,
        concurrency: Option<usize>,
        timeout_ms: Option<u64>,
        attrs: Option<HashMap<String, String>>,
        requires: Option<Vec<String>>,
        retry: Option<(u32, bool, u64, f64)>,
    ) -> PyResult<()> {
        let (key, name) = class_key_name(task_cls.bind(py))?;
        self.registrations.push(Registration {
            key,
            type_name: intern(key, &name),
            process,
            label,
            concurrency,
            timeout_ms,
            attrs: attrs.into_iter().flatten().collect(),
            requires: requires.unwrap_or_default(),
            retry,
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
            source: None,
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
        let mut builder = self.assemble(py, &routers, &dedups, shutdown.as_ref());
        if live {
            builder = builder.monitor(LiveConsole::new());
        }
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
        dict.set_item("skipped", report.stats.skipped)?;
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

    /// Drive the Rust CLI frontend over this engine, forwarding `argv`: `run`/`list-tasks`/
    /// `--tasks`/`--json` and exit codes are all Rust, so Python mirrors the binary for free.
    fn cli(
        &self,
        py: Python<'_>,
        routers: Py<PyAny>,
        dedups: Py<PyAny>,
        argv: Vec<String>,
    ) -> PyResult<i32> {
        let builder = self.assemble(py, &routers, &dedups, None);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| PyRuntimeError::new_err(format!("tokio runtime: {e}")))?;
        let code = py.detach(|| {
            let code = runtime.block_on(
                bask_core::cli::Cli::new()
                    .dataset_opener(|dir| Ok(Arc::new(FileDataset::open(dir)?) as Arc<dyn Dataset>))
                    .run(builder, argv),
            );
            drop(runtime);
            code
        });
        Ok(code)
    }
}

impl Engine {
    /// Build the Rust engine from the accumulated registrations, up to but not including the
    /// live monitor (chosen per entrypoint). Shared by [`run`](Engine::run) and
    /// [`cli`](Engine::cli).
    fn assemble(
        &self,
        py: Python<'_>,
        routers: &Py<PyAny>,
        dedups: &Py<PyAny>,
        shutdown: Option<&Py<PyShutdown>>,
    ) -> bask_core::EngineBuilder {
        let retry = make_retry(
            self.max_attempts,
            self.avoid_failed,
            self.backoff_ms,
            self.jitter,
        );

        let mut builder = bask_core::Engine::builder()
            .concurrency(self.concurrency)
            .retry(retry)
            .sample_interval(Duration::from_millis(self.sample_interval_ms));
        for (name, permits) in &self.resources {
            builder = builder.resource(name.clone(), *permits);
        }
        if let Some(callback) = &self.dead_letter {
            builder = builder.dead_letter(PyDeadLetter {
                callback: callback.clone_ref(py),
            });
        }
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
        if let Some(handle) = shutdown {
            builder = builder.shutdown(handle.borrow(py).inner.clone());
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
            for (key, value) in &reg.attrs {
                cfg = cfg.attr(key, value);
            }
            for resource in &reg.requires {
                cfg = cfg.requires(resource.clone());
            }
            if let Some((max_attempts, avoid_failed, backoff_ms, jitter)) = reg.retry {
                cfg = cfg.retry(make_retry(max_attempts, avoid_failed, backoff_ms, jitter));
            }
            builder = builder.worker_dyn(reg.key, worker, reg.type_name, cfg);
        }
        for reg in &self.chunkers {
            let bridge: Arc<dyn DynWorker> = Arc::new(ChunkerBridge {
                rows: reg.rows,
                piece_key: reg.piece_key,
                piece_type: reg.piece_type,
                piece_cls: reg.piece_cls.clone_ref(py),
            });
            let mut cfg = WorkerCfg::new();
            if let Some(label) = &reg.label {
                cfg = cfg.label(label.clone());
            }
            if let Some(c) = reg.concurrency {
                cfg = cfg.concurrency(c);
            }
            builder = builder.worker_dyn(reg.source_key, bridge, reg.source_type, cfg);
        }
        for reg in &self.checkpoints {
            let ops = Arc::new(PyCheckpoint {
                name: reg.name.clone(),
                key_only: reg.key_only,
                cls: reg.cls.clone_ref(py),
            });
            builder = builder.checkpoint_dyn(reg.key, ops);
        }
        if let Some(obj) = &self.dataset {
            match obj.bind(py).extract::<PyRef<PyFileDataset>>() {
                Ok(file) => builder = builder.dataset(file.inner.clone()),
                Err(_) => {
                    builder = builder.dataset(PyDataset {
                        obj: Arc::new(obj.clone_ref(py)),
                        lock: Arc::new(Mutex::new(())),
                    })
                }
            }
        } else if let Some(path) = &self.store_path {
            builder = builder.store(SqliteStore::open(path));
        }
        for seed in &self.seeds {
            let payload = Box::new(seed.payload.clone_ref(py));
            builder = match &seed.source {
                Some(id) => builder.seed_source_dyn(id.clone(), seed.key, seed.type_name, payload),
                None => builder.seed_dyn(seed.key, seed.type_name, payload),
            };
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
                    let collected = RouterOut {
                        buffer: Vec::new(),
                        coverage: Coverage::empty(),
                    };
                    let Ok(collected) = Bound::new(py, collected) else {
                        continue;
                    };
                    if router.call_method1("flush", (&collected,)).is_err() {
                        continue;
                    }
                    let buffered = std::mem::take(&mut collected.borrow_mut().buffer);
                    for (key, type_name, payload, coverage) in buffered {
                        out.emit_dyn_covered(key, type_name, Box::new(payload), coverage);
                    }
                }
            });
        });

        builder
    }
}

#[pymodule]
fn _bask(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Engine>()?;
    m.add_class::<PyShutdown>()?;
    m.add_class::<RowAggregator>()?;
    m.add_class::<PyFileDataset>()?;
    m.add_function(wrap_pyfunction!(coverage_rows, m)?)?;
    m.add_function(wrap_pyfunction!(coverage_to_bytes, m)?)?;
    Ok(())
}
