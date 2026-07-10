# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Bask: Build Tasks

Workers consume typed tasks and may emit more; a separate aggregation plane
collects results. Powered by the Rust `bask` engine via the `_bask` extension.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Callable

from . import _bask

__all__ = ["Engine", "Retry", "Report"]


@dataclass
class Retry:
    """Retry policy: how many attempts, and whether to avoid a failed instance."""

    max_attempts: int = 1
    avoid_failed: bool = True
    backoff_ms: int = 0


class Report:
    """Outcome of a run: aggregator outputs, counters, and terminal failures."""

    def __init__(self, raw: dict, outputs: dict, unique: dict):
        self.processed: int = raw["processed"]
        self.retried: int = raw["retried"]
        self.failed: int = raw["failed"]
        self.failures: list[dict] = raw["failures"]
        self._outputs = outputs
        self._unique = unique

    def output(self, agg_cls: type) -> Any:
        return self._outputs.get(agg_cls)

    def unique(self, dedup_cls: type) -> int:
        """The number of distinct keys admitted by a dedup set."""
        return self._unique.get(dedup_cls, 0)

    def __repr__(self) -> str:
        return (
            f"Report(processed={self.processed}, retried={self.retried}, "
            f"failed={self.failed})"
        )


@dataclass
class _Registration:
    task_cls: type
    process: Callable
    label: str | None
    concurrency: int | None


class Engine:
    """Builds a pipeline from decorated workers and aggregators, then runs it."""

    def __init__(
        self,
        concurrency: int | None = None,
        retry: Retry | None = None,
        sample_interval_ms: int = 200,
    ):
        self._concurrency = concurrency or (os.cpu_count() or 4)
        self._retry = retry or Retry()
        self._sample_interval_ms = sample_interval_ms
        self._registrations: list[_Registration] = []
        self._aggregators: dict[type, Any] = {}
        self._dedups: dict[type, set] = {}
        self._seeds: list[Any] = []

    def worker(self, task_cls: type, *, label: str | None = None, concurrency: int | None = None):
        """Decorator: register a function or class as a worker for `task_cls`."""

        def decorate(target):
            self._registrations.append(
                _Registration(task_cls, _as_process(target), label, concurrency)
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
    ):
        """Register a pre-built worker instance (for groups with distinct params)."""
        self._registrations.append(
            _Registration(task_cls, _as_process(instance), label, concurrency)
        )
        return instance

    def aggregator(self, cls: type) -> type:
        """Decorator: register an aggregator class (fold/finalize)."""
        self._aggregators[cls] = cls()
        return cls

    def dedup(self, marker: type) -> type:
        """Decorator: register a dedup set keyed by the marker class; gate emission
        with ctx.first_seen(marker, key)."""
        self._dedups[marker] = set()
        return marker

    def seed(self, task: Any) -> "Engine":
        self._seeds.append(task)
        return self

    def run(self, live: bool = False) -> Report:
        engine = _bask.Engine(
            self._concurrency,
            self._retry.max_attempts,
            self._retry.avoid_failed,
            self._retry.backoff_ms,
            self._sample_interval_ms,
        )
        for reg in self._registrations:
            engine.register(reg.task_cls, reg.process, reg.label, reg.concurrency)
        for task in self._seeds:
            engine.seed(task)
        raw = engine.run(self._aggregators, self._dedups, live)
        outputs = {cls: inst.finalize() for cls, inst in self._aggregators.items()}
        unique = {marker: len(seen) for marker, seen in self._dedups.items()}
        return Report(raw, outputs, unique)


def _as_process(target: Any) -> Callable:
    """Normalize a worker to a `process(task, ctx)` callable."""
    if isinstance(target, type):
        target = target()
    if hasattr(target, "process"):
        return target.process
    return target
