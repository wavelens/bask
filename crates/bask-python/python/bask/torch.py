# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Consume a running bask Engine as a PyTorch DataLoader.

bask does the preprocessing (workers, resource pools, backpressure) while torch drains
finished tasks live. The DataLoader runs num_workers=0: bask is the data-loading
parallelism, so set engine concurrency, not torch workers. Import requires torch."""
from __future__ import annotations

import random
from typing import Any, Callable

import torch
from torch.utils.data import DataLoader, Dataset, IterableDataset

__all__ = ["TaskStream", "SnapshotTaskSet", "loader", "default_decode"]


def default_decode(item: Any) -> Any:
    """Decode a collected task into a training sample. A Batch-like task (a `.batch` that is
    a pyarrow RecordBatch, or a bare RecordBatch) becomes a dict of column tensors; anything
    else passes through unchanged for a caller-supplied `collate_fn`."""
    batch = getattr(item, "batch", item)
    if _is_record_batch(batch):
        return {name: _column_tensor(batch, i) for i, name in enumerate(batch.schema.names)}
    return item


def _column_tensor(batch: Any, i: int) -> torch.Tensor:
    # pyarrow-backed numpy arrays are read-only; from_numpy needs a writable copy.
    arr = batch.column(i).to_numpy(zero_copy_only=False)
    if not arr.flags.writeable:
        arr = arr.copy()
    return torch.from_numpy(arr)


def _is_record_batch(obj: Any) -> bool:
    try:
        import pyarrow as pa
    except ImportError:
        return False
    return isinstance(obj, pa.RecordBatch)


class _ShuffleBuffer:
    """A bounded reservoir window that mixes a live stream without dropping items; size <= 0
    is a pass-through. `push` returns an item to yield (or None while filling), `drain`
    flushes the remainder."""

    def __init__(self, size: int, seed: int):
        self._size = max(0, size)
        self._buf: list[Any] = []
        self._rng = random.Random(seed)

    def push(self, item: Any) -> Any:
        if self._size <= 0:
            return item
        if len(self._buf) < self._size:
            self._buf.append(item)
            return None
        j = self._rng.randrange(self._size)
        out = self._buf[j]
        self._buf[j] = item
        return out

    def drain(self):
        self._rng.shuffle(self._buf)
        yield from self._buf
        self._buf = []


class TaskStream(IterableDataset):
    """An IterableDataset over a running Engine. Epoch 0 drains the engine live in stream
    order; if a `snapshot` (a `bask.data.Dataset`) is bound, later epochs replay it with a
    per-epoch shuffle. Use `bask.torch.loader(stream)` to wrap it in a DataLoader."""

    def __init__(
        self,
        engine,
        *,
        decode: Callable[[Any], Any] | None = None,
        shuffle_buffer: int = 0,
        snapshot=None,
        capacity: int = 1024,
        live: bool = False,
        seed: int = 0,
    ):
        self._engine = engine
        self._decode = decode or default_decode
        self._shuffle_buffer = int(shuffle_buffer)
        self._snapshot = snapshot
        self._capacity = capacity
        self._live = live
        self._seed = seed
        self._epoch = 0
        self._drained = False

    def __iter__(self):
        if self._drained and self._snapshot is not None:
            it = self._replay(self._epoch)
        else:
            it = self._drain()
        self._epoch += 1
        return it

    def _drain(self):
        handle = self._engine.stream(capacity=self._capacity, live=self._live)
        buffer = _ShuffleBuffer(self._shuffle_buffer, self._seed)
        try:
            for item in handle:
                out = buffer.push(item)
                if out is not None:
                    yield self._decode(out)
            for out in buffer.drain():
                yield self._decode(out)
        finally:
            handle.close()
            self._drained = True

    def _replay(self, epoch: int):
        batches = self._snapshot.read()
        order = list(range(len(batches)))
        random.Random(self._seed + epoch).shuffle(order)
        for i in order:
            yield self._decode(batches[i])


class SnapshotTaskSet(Dataset):
    """A map-style dataset over a frozen `bask.data.Dataset` snapshot: random access and
    __len__, for the standard DataLoader(shuffle=True) path after a run. Each item is one
    materialized batch."""

    def __init__(self, dataset, *, decode: Callable[[Any], Any] | None = None):
        self._batches = dataset.read()
        self._decode = decode or default_decode

    def __len__(self) -> int:
        return len(self._batches)

    def __getitem__(self, i: int) -> Any:
        return self._decode(self._batches[i])


def loader(source, *, batch_size=None, **kw) -> DataLoader:
    """Wrap a `TaskStream` or `SnapshotTaskSet` in a DataLoader with num_workers=0. batch_size
    defaults to None because bask already batches; set it only for a per-sample stream. With
    batch_size=None, torch's own default collate would coerce a decoded tuple into a list, so
    the default collate_fn here is identity unless the caller overrides it."""
    if kw.pop("num_workers", 0):
        raise ValueError(
            "bask owns data-loading parallelism; keep num_workers=0 and set engine concurrency"
        )
    if batch_size is None:
        kw.setdefault("collate_fn", lambda sample: sample)
    return DataLoader(source, batch_size=batch_size, num_workers=0, **kw)
