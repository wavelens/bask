# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""bask.torch: consume a running engine as a PyTorch DataLoader."""
import pytest

torch = pytest.importorskip("torch")
pa = pytest.importorskip("pyarrow")

import bask
from bask import Worker
from bask.tasks import Batch
from bask.torch import TaskStream, default_decode, loader


class Feed:
    pass


class SampleBatch(Batch):
    pass


class Item:
    def __init__(self, x, y):
        self.x = x
        self.y = y


def _arrow_engine(rows, per_batch):
    engine = bask.Engine(concurrency=2)

    @engine.worker(Feed)
    class Emit(Worker):
        def process(self, _feed, ctx):
            for start in range(0, rows, per_batch):
                stop = min(start + per_batch, rows)
                batch = pa.record_batch(
                    {"x": list(range(start, stop)), "y": [i * 2 for i in range(start, stop)]}
                )
                ctx.emit(SampleBatch(batch))

    engine.collect(SampleBatch)
    engine.seed(Feed())
    return engine


def test_default_decode_batch_to_tensors():
    batch = pa.record_batch({"x": [1, 2, 3], "y": [4, 5, 6]})
    out = default_decode(SampleBatch(batch))
    assert set(out) == {"x", "y"}
    assert out["x"].tolist() == [1, 2, 3]
    assert isinstance(out["x"], torch.Tensor)


def test_live_stream_yields_all_rows():
    stream = TaskStream(_arrow_engine(100, 16))
    seen = []
    for tensors in loader(stream):
        seen.extend(tensors["x"].tolist())
    assert sorted(seen) == list(range(100))


def test_per_sample_passthrough_decode():
    engine = bask.Engine(concurrency=2)

    @engine.worker(Feed)
    class Emit(Worker):
        def process(self, _feed, ctx):
            for i in range(20):
                ctx.emit(Item(i, i * 2))

    engine.collect(Item)
    engine.seed(Feed())

    stream = TaskStream(engine, decode=lambda item: (item.x, item.y))
    pairs = list(loader(stream))
    assert sorted(pairs) == [(i, i * 2) for i in range(20)]


def test_shuffle_buffer_preserves_multiset():
    stream = TaskStream(_arrow_engine(64, 8), shuffle_buffer=4, seed=7)
    seen = []
    for tensors in loader(stream):
        seen.extend(tensors["x"].tolist())
    assert sorted(seen) == list(range(64))


def test_loader_rejects_torch_workers():
    stream = TaskStream(_arrow_engine(8, 8))
    with pytest.raises(ValueError):
        loader(stream, num_workers=2)


def test_loader_accepts_explicit_zero_workers():
    from torch.utils.data import DataLoader

    dl = loader(TaskStream(_arrow_engine(8, 8)), num_workers=0)
    assert isinstance(dl, DataLoader)
