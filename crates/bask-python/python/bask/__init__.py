# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Bask: Build Tasks

Workers consume typed tasks and may emit more; a separate routing plane folds a
task stream into results. Powered by the Rust `bask` engine via the `_bask` extension.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Callable

from . import _bask
from ._bask import Shutdown

__all__ = [
    "Engine",
    "Retry",
    "Report",
    "Shutdown",
    "Fatal",
    "Retryable",
    "same_instance",
    "different_instance",
    "different_attr",
]


@dataclass
class Retry:
    """Retry policy: attempts, where the retry lands, backoff, and jitter."""

    max_attempts: int = 1
    avoid_failed: bool = True
    backoff_ms: int = 0
    jitter: float = 0.0

    def _tuple(self) -> tuple:
        return (self.max_attempts, self.avoid_failed, self.backoff_ms, self.jitter)


class Fatal(Exception):
    """Raise from a worker to fail the task at once with no retry; the task, if a
    dead-letter sink is set, is handed to it."""

    _bask_retry = ("fatal",)


class Retryable(Exception):
    """Raise from a worker to retry with an explicit instance-selection hint; prefer the
    `same_instance`/`different_instance`/`different_attr` helpers."""

    def __init__(self, hint: tuple, message: str = ""):
        super().__init__(message)
        self._bask_retry = hint


def same_instance(message: str = "") -> Retryable:
    """Retry on the instance that just failed (keep a warm resource)."""
    return Retryable(("same_instance",), message)


def different_instance(message: str = "") -> Retryable:
    """Retry on any instance except those already tried."""
    return Retryable(("different_instance",), message)


def different_attr(key: str, message: str = "") -> Retryable:
    """Retry on an instance whose `key` attribute differs from the failed one's."""
    return Retryable(("different_attr", key), message)


class Report:
    """Outcome of a run: router outputs, counters, and terminal failures."""

    def __init__(self, raw: dict, outputs: dict, unique: dict):
        self.processed: int = raw["processed"]
        self.retried: int = raw["retried"]
        self.failed: int = raw["failed"]
        self.skipped: int = raw["skipped"]
        self.failures: list[dict] = raw["failures"]
        self.interrupted: bool = raw["interrupted"]
        self.unfinished: int = raw["unfinished"]
        self._outputs = outputs
        self._unique = unique

    def output(self, router_cls: type) -> Any:
        return self._outputs.get(router_cls)

    def unique(self, dedup_cls: type) -> int:
        """The number of distinct keys admitted by a dedup set."""
        return self._unique.get(dedup_cls, 0)

    def __repr__(self) -> str:
        return (
            f"Report(processed={self.processed}, retried={self.retried}, "
            f"failed={self.failed}, skipped={self.skipped}, "
            f"interrupted={self.interrupted}, unfinished={self.unfinished})"
        )


@dataclass
class _Registration:
    task_cls: type
    process: Callable
    label: str | None
    concurrency: int | None
    timeout_ms: int | None
    attrs: dict[str, str] | None = None
    requires: list[str] | None = None
    retry: Retry | None = None


class Engine:
    """Builds a pipeline from decorated workers and routers, then runs it."""

    def __init__(
        self,
        concurrency: int | None = None,
        retry: Retry | None = None,
        sample_interval_ms: int = 200,
        queue_capacity: int | None = None,
        timeout_ms: int | None = None,
        grace_ms: int | None = None,
        catch_ctrl_c: bool = False,
        resources: dict[str, int] | None = None,
        dead_letter: Callable[[dict], None] | None = None,
        store: str | None = None,
        dataset: Any | None = None,
    ):
        self._concurrency = concurrency or (os.cpu_count() or 4)
        self._retry = retry or Retry()
        self._sample_interval_ms = sample_interval_ms
        self._queue_capacity = queue_capacity
        self._timeout_ms = timeout_ms
        self._grace_ms = grace_ms
        self._catch_ctrl_c = catch_ctrl_c
        self._resources = resources
        self._dead_letter = dead_letter
        self._store = store
        self._dataset = dataset
        self._registrations: list[_Registration] = []
        self._chunkers: list[tuple] = []
        self._routers: dict[type, Any] = {}
        self._dedups: dict[type, set] = {}
        self._seeds: list[Any] = []
        self._sources: list[tuple[Any, str]] = []

    def worker(
        self,
        task_cls: type,
        *,
        label: str | None = None,
        concurrency: int | None = None,
        timeout_ms: int | None = None,
        attrs: dict[str, str] | None = None,
        requires: list[str] | None = None,
        retry: Retry | None = None,
    ):
        """Decorator: register a function or class as a worker for `task_cls`. `attrs`
        tag the instance for attribute-aware retry; `requires` names resource pools it
        draws a permit from; `retry` overrides the engine default for this instance."""

        def decorate(target):
            self._registrations.append(
                _Registration(
                    task_cls, _as_process(target), label, concurrency, timeout_ms, attrs, requires, retry
                )
            )
            return target

        return decorate

    def register(
        self,
        task_cls: type,
        instance: Any,
        *,
        label: str | None = None,
        concurrency: int | None = None,
        timeout_ms: int | None = None,
        attrs: dict[str, str] | None = None,
        requires: list[str] | None = None,
        retry: Retry | None = None,
    ):
        """Register a pre-built worker instance (for groups with distinct params)."""
        self._registrations.append(
            _Registration(
                task_cls, _as_process(instance), label, concurrency, timeout_ms, attrs, requires, retry
            )
        )
        return instance

    def router(self, cls: type) -> type:
        """Decorator: register a router class. It implements `route(self, value, out)`
        (fold state, optionally `out.emit(task)` to route/filter/batch) and `finalize`."""
        self._routers[cls] = cls()
        return cls

    def dedup(self, marker: type) -> type:
        """Decorator: register a dedup set keyed by the marker class; gate emission
        with ctx.first_seen(marker, key)."""
        self._dedups[marker] = set()
        return marker

    def resource(self, name: str, permits: int) -> "Engine":
        """Declare a named resource pool shared across every worker that requires it."""
        if self._resources is None:
            self._resources = {}
        self._resources[name] = permits
        return self

    def chunker(
        self,
        source_cls: type,
        piece_cls: type,
        rows: int,
        *,
        label: str | None = None,
        concurrency: int | None = None,
    ) -> "Engine":
        """Register the Rust chunker stage. Each `source_cls` instance's `batch` (a pyarrow
        RecordBatch) is split into pieces of at most `rows` rows, each emitted as
        `piece_cls(batch)`. See `bask.tasks.Batch` for a ready-made wrapper."""
        self._chunkers.append((source_cls, piece_cls, rows, label, concurrency))
        return self

    def row_batch(self, key: type, group_cls: type, rows: int) -> "Engine":
        """Register a Rust-backed row-count aggregating router under `key`: feed it with
        `ctx.route(key, batch)`, and it emits `group_cls(batch)` once at least `rows` rows
        accumulate, flushing the remainder at end-of-run. `report.output(key)` is the group
        count."""
        from .tasks import RowBatch

        self._routers[key] = RowBatch(group_cls, rows)
        return self

    def seed(self, task: Any) -> "Engine":
        self._seeds.append(task)
        return self

    def source(self, task: Any, id: str) -> "Engine":
        """Seed a source `task` tagged with a stable `id`. Its worker stamps rows with
        `ctx.emit_keyed(ordinal, task)`; once a clean pass records the extent, a later run
        skips the source whole if a checkpoint already covers every row."""
        self._sources.append((task, id))
        return self

    def run(self, live: bool = False, shutdown: Shutdown | None = None) -> Report:
        from .tasks import Checkpoint

        engine = _bask.Engine(
            self._concurrency,
            self._retry.max_attempts,
            self._retry.avoid_failed,
            self._retry.backoff_ms,
            self._retry.jitter,
            self._sample_interval_ms,
            self._queue_capacity,
            self._timeout_ms,
            self._grace_ms,
            self._catch_ctrl_c,
            self._resources,
            self._dead_letter,
            self._store,
        )
        for reg in self._registrations:
            engine.register(
                reg.task_cls,
                reg.process,
                reg.label,
                reg.concurrency,
                reg.timeout_ms,
                reg.attrs,
                reg.requires,
                reg.retry._tuple() if reg.retry else None,
            )
        for source_cls, piece_cls, rows, label, concurrency in self._chunkers:
            engine.chunker(source_cls, piece_cls, rows, label, concurrency)
        for cls in Checkpoint._registry:
            engine.checkpoint(cls)
        for task in self._seeds:
            engine.seed(task)
        for task, id in self._sources:
            engine.source(task, id)
        if self._dataset is not None:
            engine.dataset(getattr(self._dataset, "_dataset", self._dataset))
        raw = engine.run(self._routers, self._dedups, live, shutdown)
        outputs = {cls: inst.finalize() for cls, inst in self._routers.items()}
        unique = {marker: len(seen) for marker, seen in self._dedups.items()}
        return Report(raw, outputs, unique)


def _as_process(target: Any) -> Callable:
    """Normalize a worker to a `process(task, ctx)` callable."""
    if isinstance(target, type):
        target = target()
    if hasattr(target, "process"):
        return target.process
    return target
