# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Consume a running engine as a PyTorch DataLoader: a source emits Arrow batches, a chunker
splits them, and torch drains the collected batches live while bask does the work. Needs
torch and pyarrow; skips cleanly when they are absent so the example harness stays green."""
import sys

try:
    import pyarrow as pa
    import torch  # noqa: F401
except ImportError:
    print("torch_stream example needs torch and pyarrow; skipping")
    sys.exit(0)

from bask import Engine, Worker
from bask.tasks import Batch
from bask.torch import TaskStream, loader


class Whole(Batch):
    pass


class Piece(Batch):
    pass


engine = Engine(concurrency=4)
engine.chunker(Whole, Piece, rows=256)
engine.collect(Piece)
engine.seed(Whole(pa.record_batch({"x": list(range(4096)), "y": [i * 2 for i in range(4096)]})))

stream = TaskStream(engine)
total = 0
for tensors in loader(stream):
    total += tensors["x"].shape[0]
print("streamed rows:", total)
