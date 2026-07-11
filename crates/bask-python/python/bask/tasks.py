# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Predefined stages that reuse the Rust `bask-tasks` crate over pyarrow data.

Subclass `Batch` to get a distinct routing type carrying a pyarrow RecordBatch, then wire
the stage with `Engine.chunker(source_cls, piece_cls, rows)`:

    import pyarrow as pa
    from bask import Engine
    from bask.tasks import Batch

    class Whole(Batch): pass
    class Piece(Batch): pass

    engine = Engine()
    engine.chunker(Whole, Piece, rows=8192)

    @engine.worker(Piece)
    def handle(piece, ctx):
        piece.batch  # a pyarrow RecordBatch of <= 8192 rows

    engine.seed(Whole(pa.record_batch({"n": range(100_000)})))
    engine.run()
"""
from __future__ import annotations

import pickle
from typing import Any

from . import _bask

__all__ = ["Batch", "Checkpoint", "RowBatch"]


class Batch:
    """A task wrapping a pyarrow RecordBatch (exposed as `.batch`). Subclass it so each
    stage of a pipeline routes on a distinct type."""

    def __init__(self, batch: Any):
        self.batch = batch


class Checkpoint:
    """Mark a task type as a durable restore point: subclass it and define `key(self)`.
    On arrival the engine materializes the task and records the source rows it covers, so
    a re-run skips finished work and reseeds anything not yet consumed. Set `KEY_ONLY` for
    a side effect with no payload; override `encode`/`decode` for a custom on-disk format
    (the default is `pickle`). `NAME` defaults to the class name and is the store identity."""

    _registry: list[type] = []
    KEY_ONLY = False

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if "NAME" not in cls.__dict__:
            cls.NAME = cls.__name__
        Checkpoint._registry.append(cls)

    def key(self) -> str:
        raise NotImplementedError("a Checkpoint must define key()")

    def encode(self) -> bytes:
        return pickle.dumps(self)

    @classmethod
    def decode(cls, data: bytes) -> Any:
        return pickle.loads(data)


class RowBatch:
    """A router that re-aggregates pyarrow batches into groups of at least `rows` rows,
    reusing the Rust `bask_tasks` aggregator. Register it with
    `engine.row_batch(key, group_cls, rows)` and feed it with `ctx.route(key, batch)`; it
    emits `group_cls(batch)` per full group and flushes the remainder at end-of-run."""

    def __init__(self, group_cls: type, rows: int):
        self._agg = _bask.RowAggregator(rows)
        self._group_cls = group_cls
        self._groups = 0

    def route(self, value: Any, out: Any) -> None:
        for group in self._agg.push(getattr(value, "batch", value)):
            out.emit(self._group_cls(group))
            self._groups += 1

    def flush(self, out: Any) -> None:
        for group in self._agg.flush():
            out.emit(self._group_cls(group))
            self._groups += 1

    def finalize(self) -> int:
        return self._groups
