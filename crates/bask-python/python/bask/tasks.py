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

from typing import Any

from . import _bask

__all__ = ["Batch", "RowBatch"]


class Batch:
    """A task wrapping a pyarrow RecordBatch (exposed as `.batch`). Subclass it so each
    stage of a pipeline routes on a distinct type."""

    def __init__(self, batch: Any):
        self.batch = batch


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
