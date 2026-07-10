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

__all__ = ["Batch"]


class Batch:
    """A task wrapping a pyarrow RecordBatch (exposed as `.batch`). Subclass it so each
    stage of a pipeline routes on a distinct type."""

    def __init__(self, batch: Any):
        self.batch = batch
