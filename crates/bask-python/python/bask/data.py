# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Checkpoint datasets: the built-in directory-backed Parquet `Dataset`, plus the helpers
for writing your own.

A dataset is where data-carrying checkpoints materialize. Bind one with
`Engine(dataset=...)` and each checkpoint's payload becomes a self-compacting shard;
provenance coverage retires the shards a later save re-derives. Point the engine at the
built-in `Dataset` for Parquet-on-disk, or implement the dataset protocol against your own
database -- a plain object with these methods:

    store side (the checkpoint index):
        statuses() -> list[(name, key, status)]   # status: 0 stored, 1 consumed
        covered() -> bytes                         # union coverage, see coverage_rows
        extents() -> list[(source, coverage)]
        stored_items(name) -> list[(key, payload, coverage)]
        commit(name, key, payload | None, coverage)
        consume(name, key)
        record_extent(source, coverage)
    dataset side (the shards):
        put(name, key, payload, coverage)          # write shard + supersede by coverage
        stored(name) -> list[(key, payload, coverage)]
        read() -> list[(key, payload, coverage)]   # the live snapshot

See `examples/dataset.py` for a sqlite-backed implementation.
"""
from __future__ import annotations

import io
from typing import Any, Iterable

from . import _bask
from .tasks import Batch, Checkpoint

__all__ = ["Dataset", "BatchCheckpoint", "coverage_rows", "coverage_to_bytes"]


def coverage_rows(blob: bytes) -> list[int]:
    """The source-row ordinals a coverage blob carries, for computing supersession."""
    return _bask.coverage_rows(blob)


def coverage_to_bytes(rows: Iterable[int]) -> bytes:
    """Encode source-row ordinals into a coverage blob (the inverse of `coverage_rows`)."""
    return _bask.coverage_to_bytes(list(rows))


class Dataset:
    """A directory of content-addressed Parquet shards over one `bask.sqlite`. Bind it with
    `Engine(dataset=Dataset("out"))`; materializing checkpoints spill into it and a later run
    reads the latest committed snapshot. Read it back with `read()`/`to_pyarrow()`/iteration,
    or open the directory's `*.parquet` with pyarrow, pandas, duckdb, or HF `datasets`."""

    def __init__(self, path: str):
        self.path = str(path)
        self._dataset = _bask.FileDataset(self.path)

    def read(self) -> list:
        """The live shards as pyarrow RecordBatches (newest snapshot)."""
        import pyarrow.parquet as pq

        batches = []
        for buf in self._dataset.read():
            batches.extend(pq.read_table(io.BytesIO(buf)).to_batches())
        return batches

    def to_pyarrow(self):
        """The whole live dataset as one pyarrow Table."""
        import pyarrow as pa

        return pa.Table.from_batches(self.read())

    def __iter__(self):
        return iter(self.read())


class BatchCheckpoint(Batch, Checkpoint):
    """A Checkpoint carrying a pyarrow RecordBatch, materialized as a Parquet shard: subclass
    it and define `key(self)`. `encode`/`decode` write and read a self-contained Parquet
    buffer, so a built-in `Dataset`'s shards are real `.parquet` files."""

    def encode(self) -> bytes:
        import pyarrow as pa
        import pyarrow.parquet as pq

        sink = io.BytesIO()
        pq.write_table(pa.Table.from_batches([self.batch]), sink)
        return sink.getvalue()

    @classmethod
    def decode(cls, data: bytes) -> Any:
        import pyarrow.parquet as pq

        return cls(pq.read_table(io.BytesIO(data)).combine_chunks().to_batches()[0])
